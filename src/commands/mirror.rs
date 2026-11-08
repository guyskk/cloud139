use crate::commands::CommandExit;
use crate::services::mirror_service::{self, MirrorOptions};
use crate::services::sync_service::parse_sync_endpoint;
use crate::{debug, info};
use clap::Parser;
use std::io::IsTerminal;

#[derive(Parser, Debug, Clone)]
#[command(
    about = "把本地目录推到云端仓（只增不删，不删除远端已有对象）",
    long_about = "把本地目录推到云端仓：进程内完成遍历、差集、上传、校验、自愈。\n\n\
**只增不删**：远端有、本地没有的对象不会被删除（append-only），只在 JSON 的 remoteOnly 里报告数量。\n\n\
远端仓根不存在时会逐级创建（不会用 mkdir -f 改名）。\n\n\
退出码：0 全部成功；1 有对象失败；2 参数或配置错误。"
)]
pub struct MirrorArgs {
    #[arg(help = "本地仓根目录，例如 /var/lib/beecodeai-backup/restic")]
    pub local: String,

    #[arg(help = "远端仓根，形如 cloud:/beecodeai-backup/prod/restic")]
    pub remote: String,

    #[arg(
        long = "no-verify",
        action = clap::ArgAction::SetFalse,
        help = "跳过 contentHash 校验（默认开启，只比 size 时用）"
    )]
    pub verify: bool,

    #[arg(long, default_value_t = 3, value_name = "N", help = "自愈上限次数")]
    pub heal: u32,

    #[arg(long, help = "只算差异，不发任何写请求")]
    pub dry_run: bool,

    #[arg(long, help = "输出 JSON 汇总（失败时也会输出）")]
    pub json: bool,

    #[arg(long, default_value_t = 100, value_name = "N", help = "列目录每页条数")]
    pub page_size: i32,

    #[arg(
        long,
        default_value_t = 1,
        value_name = "N",
        help = "并发上传数；当前仅支持 1（>1 会因翻页漂移漏对象）"
    )]
    pub jobs: usize,

    #[arg(long, default_value_t = 5, value_name = "N", help = "列目录/上传各自的重试次数")]
    pub retries: u32,

    #[arg(
        long,
        default_value_t = 3,
        value_name = "S",
        help = "重试退避基数（秒），按 3/6/12/24 指数增长"
    )]
    pub retry_backoff: u64,
}

pub async fn execute(args: MirrorArgs) -> anyhow::Result<()> {
    if args.jobs != 1 {
        return Err(CommandExit::new(
            2,
            "暂不支持 --jobs > 1：并发往同一父目录写会因翻页漂移漏对象，请保持串行（默认 1）",
        )
        .into());
    }
    if args.page_size <= 0 || args.page_size > 1000 {
        return Err(
            CommandExit::new(2, "--page-size 必须在 1..=1000 之间".to_string()).into(),
        );
    }
    if args.retry_backoff > 600 {
        return Err(CommandExit::new(2, "--retry-backoff 不能超过 600 秒".to_string()).into());
    }

    match parse_sync_endpoint(&args.remote) {
        crate::domain::SyncEndpoint::Cloud(_) => {}
        crate::domain::SyncEndpoint::Local(_) => {
            return Err(CommandExit::new(
                2,
                "远端必须是 cloud:/path 形式，例如 cloud:/beecodeai-backup/prod/restic".to_string(),
            )
            .into());
        }
    }

    // 本地仓根不存在属于参数错误（exit 2），不能混进「对象失败」的 1。
    if !std::path::Path::new(&args.local).is_dir() {
        return Err(CommandExit::new(
            2,
            format!("本地仓根不存在或不是目录: {}", args.local),
        )
        .into());
    }

    let config = crate::config::Config::load().map_err(|err| {
        CommandExit::new(2, format!("读取配置失败: {err}"))
    })?;

    let options = MirrorOptions {
        verify: args.verify,
        heal: args.heal,
        dry_run: args.dry_run,
        page_size: args.page_size,
        retries: args.retries,
        retry_backoff_secs: args.retry_backoff,
    };

    debug!(
        "mirror: local={}, remote={}, verify={}, dry_run={}",
        args.local, args.remote, args.verify, args.dry_run
    );

    let report = match mirror_service::run(&config, &args.local, &args.remote, &options).await {
        Ok(report) => report,
        Err(err) => {
            // 契约：失败也要给调用方一份可解析的汇总（44659 用它写 latest.json）。
            let message = format!("{err:#}");
            if args.json {
                let fatal = serde_json::json!({
                    "objects": 0,
                    "uploaded": 0,
                    "skipped": 0,
                    "verified": 0,
                    "hashMissing": 0,
                    "healed": 0,
                    "remoteOnly": 0,
                    "bytesUploaded": 0,
                    "seconds": 0.0,
                    "failures": [{
                        "path": args.local,
                        "error": message,
                        "retries": 0
                    }]
                });
                println!("{}", fatal);
            }
            return Err(CommandExit::new(1, message).into());
        }
    };

    if args.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        info!(
            "对象 {} 个：上传 {}、跳过 {}、校验 {}、缺哈希 {}、自愈 {}、远端独有 {}、上传 {} 字节、耗时 {:.1}s",
            report.objects,
            report.uploaded,
            report.skipped,
            report.verified,
            report.hash_missing,
            report.healed,
            report.remote_only,
            report.bytes_uploaded,
            report.seconds
        );
        for failure in &report.failures {
            crate::error!("失败 {}: {}（重试 {}）", failure.path, failure.error, failure.retries);
        }
        if !std::io::stdout().is_terminal() {
            // 非交互场景保持安静，退出码已经表达结果
        }
    }

    if !report.ok() {
        return Err(CommandExit::new(
            1,
            format!("{} 个对象处理失败", report.failures.len()),
        )
        .into());
    }
    Ok(())
}
