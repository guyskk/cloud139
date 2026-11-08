//! `mirror`: 把本地 restic 仓推到云端，进程内完成遍历、差集、上传、校验、自愈。
//!
//! 设计约束（与备份任务约定的契约）：
//! - **只增不删**：远端多出来的对象不会被删除（append-only），只在 JSON 里报告 `remoteOnly`。
//! - 远端根目录由本命令逐级创建（cloud139 的 mkdir 没有 `-p`，同名加 `-f` 会改名），禁止 `-f`。
//! - 校验按 size + contentHash（统一小写比对）；缺哈希只计入 hashMissing，不判失败。
//! - 失败也要输出完整 JSON，由调用方写 latest.json。

use crate::client::api::{self, HttpClientWrapper};
use crate::client::ClientError;
use crate::config::Config;
use crate::domain::SyncEntryKind;
use crate::models::PersonalFileItem;
use crate::services::sync_service::{SyncScanOptions, parse_sync_endpoint, scan_local};
use crate::{debug, warn};
use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct MirrorOptions {
    pub verify: bool,
    pub heal: u32,
    pub dry_run: bool,
    pub page_size: i32,
    pub retries: u32,
    pub retry_backoff_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MirrorFailure {
    pub path: String,
    pub error: String,
    pub retries: u32,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MirrorReport {
    pub objects: usize,
    pub uploaded: usize,
    pub skipped: usize,
    pub verified: usize,
    pub hash_missing: usize,
    pub healed: usize,
    pub remote_only: usize,
    pub bytes_uploaded: u64,
    pub seconds: f64,
    pub failures: Vec<MirrorFailure>,
}

impl MirrorReport {
    pub fn ok(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone)]
struct LocalObject {
    rel_path: String,
    abs_path: PathBuf,
    size: u64,
    /// 期望的内容 sha256：内容寻址对象用文件名，`config` 现算。
    expected_sha256: Option<String>,
}

#[derive(Debug, Clone)]
struct RemoteObject {
    file_id: String,
    size: i64,
    content_hash: Option<String>,
    content_hash_algorithm: Option<String>,
    is_folder: bool,
}

pub async fn run(
    config: &Config,
    local_root: &str,
    remote_root: &str,
    options: &MirrorOptions,
) -> Result<MirrorReport> {
    let started = Instant::now();
    let mut report = MirrorReport::default();

    let remote_root = match parse_sync_endpoint(remote_root) {
        crate::domain::SyncEndpoint::Cloud(path) => path,
        crate::domain::SyncEndpoint::Local(_) => {
            return Err(anyhow!("远端必须是 cloud:/path 形式"));
        }
    };
    let local_root = PathBuf::from(local_root);
    let locals = scan_local_objects(&local_root)?;
    report.objects = locals.len();

    if options.dry_run {
        // 演练：只算差异，不发任何写请求（远端根也不建）。
        let remote = match scan_remote(config, &remote_root, options).await {
            Ok(remote) => remote,
            Err(err) if is_not_found(&err) => BTreeMap::new(),
            Err(err) => return Err(err),
        };
        for local in &locals {
            match remote.get(&local.rel_path) {
                Some(entry) if !needs_upload(local, entry, options.verify) => {
                    report.skipped += 1;
                }
                _ => report.uploaded += 1,
            }
        }
        report.remote_only = count_remote_only(&remote, &locals);
        report.seconds = started.elapsed().as_secs_f64();
        return Ok(report);
    }

    let mut ensured = std::collections::HashSet::new();
    ensure_path(config, &remote_root, options, &mut ensured).await?;
    let remote = scan_remote(config, &remote_root, options).await?;
    // 远端本来就有东西时，补回/修好的对象算「自愈」；首次铺设不算。
    let repo_preexisting = !remote.is_empty();

    for local in &locals {
        let remote_path = join_remote(&remote_root, &local.rel_path);
        let existing = remote.get(&local.rel_path);

        if let Some(entry) = existing {
            if !needs_upload(local, entry, options.verify) {
                report.skipped += 1;
                if options.verify && entry.content_hash.is_some() {
                    report.verified += 1;
                }
                if options.verify && entry.content_hash.is_none() {
                    report.hash_missing += 1;
                }
                continue;
            }
            // 同名但内容不同：内容寻址对象同名必然同内容，走到这里说明远端那份坏了。
            // 先永久删掉旧的，再重传，避免同名冲突。
            warn!("远端对象与本地不一致，删除后重传: {}", remote_path);
            if let Err(err) = with_retries(options, || async {
                crate::services::delete_service::delete(config, &remote_path, true)
                    .await
                    .map_err(anyhow::Error::from)
            })
            .await
            {
                report.failures.push(MirrorFailure {
                    path: local.rel_path.clone(),
                    error: format!("删除旧对象失败: {err:#}"),
                    retries: options.retries,
                });
                continue;
            }
        }

        match upload_one(config, local, &remote_root, options, &mut ensured).await {
            Ok(bytes) => {
                report.uploaded += 1;
                report.bytes_uploaded += bytes;
                if repo_preexisting {
                    // 远端原本有仓，这个对象是缺了或坏了被补回来的
                    report.healed += 1;
                }
                if options.verify {
                    match verify_one(config, &remote_root, local, options).await {
                        Ok(VerifyOutcome::Ok) => report.verified += 1,
                        Ok(VerifyOutcome::HashMissing) => {
                            report.verified += 1;
                            report.hash_missing += 1;
                        }
                        Ok(VerifyOutcome::Mismatch) | Err(_) => {
                            // 自愈：删远端→重传→复核
                            let mut healed = false;
                            for attempt in 0..options.heal {
                                debug!(
                                    "自愈第 {}/{} 次: {}",
                                    attempt + 1,
                                    options.heal,
                                    local.rel_path
                                );
                                if heal_one(config, &remote_root, local, options, &mut ensured).await.is_ok() {
                                    healed = true;
                                    break;
                                }
                            }
                            if healed {
                                // 已经在 uploaded 分支计过数，这里不重复累加
                            } else {
                                report.failures.push(MirrorFailure {
                                    path: local.rel_path.clone(),
                                    error: "校验失败且自愈未成功".to_string(),
                                    retries: options.heal,
                                });
                            }
                        }
                    }
                }
            }
            Err(err) => report.failures.push(MirrorFailure {
                path: local.rel_path.clone(),
                error: format!("{err:#}"),
                retries: options.retries,
            }),
        }
    }

    report.remote_only = count_remote_only(&remote, &locals);
    report.seconds = started.elapsed().as_secs_f64();
    Ok(report)
}

enum VerifyOutcome {
    Ok,
    HashMissing,
    Mismatch,
}

fn scan_local_objects(root: &Path) -> Result<Vec<LocalObject>> {
    if !root.is_dir() {
        return Err(anyhow!("本地仓根不是目录: {}", root.display()));
    }
    let entries = scan_local(
        root,
        SyncScanOptions {
            recursive: true,
            exclude: Vec::new(),
        },
    )
    .context("扫描本地仓失败")?;

    let mut objects = Vec::new();
    for entry in entries {
        if entry.kind != SyncEntryKind::File {
            continue;
        }
        let abs_path = entry
            .rel_path
            .split('/')
            .filter(|part| !part.is_empty())
            .fold(root.to_path_buf(), |path, part| path.join(part));
        let expected_sha256 = expected_sha256(&entry.rel_path, &abs_path)?;
        objects.push(LocalObject {
            rel_path: entry.rel_path,
            abs_path,
            size: entry.size,
            expected_sha256,
        });
    }
    Ok(objects)
}

/// restic 的对象名就是内容 sha256；`config` 是例外，需要现算。
fn expected_sha256(rel_path: &str, abs_path: &Path) -> Result<Option<String>> {
    let name = rel_path.rsplit('/').next().unwrap_or(rel_path);
    if rel_path == "config" {
        let hash = crate::utils::crypto::calc_file_sha256(&abs_path.to_string_lossy())
            .map_err(|err| anyhow!("计算 config 的 sha256 失败: {err}"))?;
        return Ok(Some(hash.to_lowercase()));
    }
    if name.len() == 64 && name.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(Some(name.to_lowercase()));
    }
    // 不是内容寻址对象（例如未来新增的元数据文件），不做哈希比对。
    Ok(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashCheck {
    Match,
    Mismatch,
    /// 远端没给哈希，或算法不是 SHA256——无法比对，按「缺哈希」处理，不判失败。
    Unavailable,
}

fn check_hash(remote_hash: Option<&str>, algorithm: Option<&str>, expected: &str) -> HashCheck {
    let algorithm_is_sha256 = algorithm
        .map(|value| value.trim().eq_ignore_ascii_case("SHA256"))
        .unwrap_or(false);
    match remote_hash {
        Some(actual) if algorithm_is_sha256 => {
            if hash_matches(actual, expected) {
                HashCheck::Match
            } else {
                HashCheck::Mismatch
            }
        }
        _ => HashCheck::Unavailable,
    }
}

/// 远端独有 = 远端有、本地没有的**文件**（目录不算，目录只是容器的骨架）。
fn count_remote_only(remote: &BTreeMap<String, RemoteObject>, locals: &[LocalObject]) -> usize {
    let local_paths: std::collections::HashSet<&str> =
        locals.iter().map(|item| item.rel_path.as_str()).collect();
    remote
        .iter()
        .filter(|(path, item)| !item.is_folder && !local_paths.contains(path.as_str()))
        .count()
}

fn needs_upload(local: &LocalObject, remote: &RemoteObject, verify: bool) -> bool {
    if remote.is_folder {
        return true;
    }
    if remote.size != local.size as i64 {
        return true;
    }
    if !verify {
        return false;
    }
    match &local.expected_sha256 {
        Some(expected) => matches!(
            check_hash(
                remote.content_hash.as_deref(),
                remote.content_hash_algorithm.as_deref(),
                expected
            ),
            HashCheck::Mismatch
        ),
        None => false,
    }
}

fn hash_matches(remote_hash: &str, expected: &str) -> bool {
    remote_hash.trim().to_lowercase() == expected.trim().to_lowercase()
}

fn join_remote(root: &str, rel_path: &str) -> String {
    if root == "/" {
        format!("/{}", rel_path.trim_start_matches('/'))
    } else {
        format!(
            "{}/{}",
            root.trim_end_matches('/'),
            rel_path.trim_start_matches('/')
        )
    }
}

fn is_already_exists(err: &ClientError) -> bool {
    match err {
        ClientError::Api(message) | ClientError::Other(message) => {
            message.contains("已存在") || message.contains("--force")
        }
        _ => false,
    }
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<ClientError>(),
            Some(ClientError::Api(message)) if message.contains("not found")
        )
    })
}

async fn ensure_path(
    config: &Config,
    path: &str,
    options: &MirrorOptions,
    ensured: &mut std::collections::HashSet<String>,
) -> Result<()> {
    if path == "/" {
        return Ok(());
    }
    let mut prefix = String::new();
    for segment in path.trim_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }
        let parent = if prefix.is_empty() {
            "/".to_string()
        } else {
            prefix.clone()
        };
        let next_prefix = if prefix.is_empty() {
            format!("/{segment}")
        } else {
            format!("{prefix}/{segment}")
        };
        if ensured.contains(&next_prefix) {
            prefix = next_prefix;
            continue;
        }
        let exists = with_retries(options, || async {
            // 列表接口要的是 parentFileId，不能直接传路径（只有 "/" 例外）。
            let parent_id = if parent == "/" {
                "/".to_string()
            } else {
                api::get_file_id_by_path(config, &parent).await?
            };
            let items = api::list_personal_files_with_page_size(
                config,
                &parent_id,
                options.page_size,
                &HttpClientWrapper::new(),
            )
            .await
            .map_err(anyhow::Error::from)?;
            let names: Vec<String> = items
                .iter()
                .filter_map(|item| item.name.clone())
                .collect();
            debug!("mirror: 列 {parent} 得到 {} 项: {:?}", names.len(), names);
            Ok(items
                .iter()
                .any(|item| item.name.as_deref() == Some(segment) && is_folder(item)))
        })
        .await?;

        prefix = next_prefix;
        debug!("mirror: 远端目录 {prefix} 存在={exists}");
        if !exists {
            // 逐级建，禁止 -f：加 -f 会在同名时改名建目录。
            // 列表可能有延迟，mkdir 报「已存在」时视为成功（幂等），其它错误才重试。
            let created = with_retries(options, || async {
                match crate::services::mkdir_service::mkdir(config, &prefix, false).await {
                    Ok(()) => Ok(()),
                    Err(err) if is_already_exists(&err) => Ok(()),
                    Err(err) => Err(anyhow::Error::from(err)),
                }
            })
            .await;
            created.with_context(|| format!("创建远端目录失败: {prefix}"))?;
        }
        ensured.insert(prefix.clone());
    }
    Ok(())
}

async fn scan_remote(
    config: &Config,
    remote_root: &str,
    options: &MirrorOptions,
) -> Result<BTreeMap<String, RemoteObject>> {
    let mut out = BTreeMap::new();
    let root_id = if remote_root == "/" {
        "/".to_string()
    } else {
        api::get_file_id_by_path(config, remote_root).await?
    };
    scan_remote_dir(config, remote_root, "", &root_id, options, &mut out).await?;
    Ok(out)
}

async fn scan_remote_dir(
    config: &Config,
    remote_root: &str,
    rel_parent: &str,
    parent_id: &str,
    options: &MirrorOptions,
    out: &mut BTreeMap<String, RemoteObject>,
) -> Result<()> {
    let items = with_retries(options, || async {
        api::list_personal_files_with_page_size(
            config,
            parent_id,
            options.page_size,
            &HttpClientWrapper::new(),
        )
        .await
        .map_err(anyhow::Error::from)
    })
    .await?;

    for item in &items {
        let name = match item.name.as_deref() {
            Some(name) if !name.is_empty() => name,
            _ => continue,
        };
        let rel_path = if rel_parent.is_empty() {
            name.to_string()
        } else {
            format!("{rel_parent}/{name}")
        };
        let entry = RemoteObject {
            file_id: item.file_id.clone().unwrap_or_default(),
            size: item.size.unwrap_or(0),
            content_hash: item.content_hash.clone(),
            content_hash_algorithm: item.content_hash_algorithm.clone(),
            is_folder: is_folder(item),
        };
        let is_dir = entry.is_folder;
        let child_id = entry.file_id.clone();
        out.insert(rel_path.clone(), entry);
        if is_dir && !child_id.is_empty() {
            Box::pin(scan_remote_dir(
                config,
                remote_root,
                &rel_path,
                &child_id,
                options,
                out,
            ))
            .await?;
        }
    }
    let _ = remote_root;
    Ok(())
}

fn is_folder(item: &PersonalFileItem) -> bool {
    crate::services::sync_service::should_treat_personal_item_as_folder(item)
}

async fn upload_one(
    config: &Config,
    local: &LocalObject,
    remote_root: &str,
    options: &MirrorOptions,
    ensured: &mut std::collections::HashSet<String>,
) -> Result<u64> {
    let parent = parent_remote_path(&join_remote(remote_root, &local.rel_path));
    ensure_path(config, &parent, options, ensured).await?;
    let file_name = local
        .rel_path
        .rsplit('/')
        .next()
        .ok_or_else(|| anyhow!("无法得到文件名: {}", local.rel_path))?
        .to_string();
    let size = local.size as i64;

    with_retries(options, || async {
        crate::services::upload_service::upload(
            config,
            &local.abs_path,
            &parent,
            &file_name,
            size,
            false,
            None,
        )
        .await
        .map_err(anyhow::Error::from)
    })
    .await?;
    Ok(local.size)
}

fn parent_remote_path(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_string(),
        Some(index) => path[..index].to_string(),
        None => "/".to_string(),
    }
}

async fn verify_one(
    config: &Config,
    remote_root: &str,
    local: &LocalObject,
    options: &MirrorOptions,
) -> Result<VerifyOutcome> {
    let remote_path = join_remote(remote_root, &local.rel_path);
    let parent = parent_remote_path(&remote_path);
    let file_name = local
        .rel_path
        .rsplit('/')
        .next()
        .unwrap_or(&local.rel_path)
        .to_string();
    let parent_id = api::get_file_id_by_path(config, &parent).await?;
    let items = with_retries(options, || async {
        api::list_personal_files_with_page_size(
            config,
            &parent_id,
            options.page_size,
            &HttpClientWrapper::new(),
        )
        .await
        .map_err(anyhow::Error::from)
    })
    .await?;

    let item = items
        .iter()
        .find(|item| item.name.as_deref() == Some(file_name.as_str()))
        .ok_or_else(|| anyhow!("上传后远端找不到对象: {remote_path}"))?;

    if item.size.unwrap_or(0) != local.size as i64 {
        return Ok(VerifyOutcome::Mismatch);
    }
    match &local.expected_sha256 {
        Some(expected) => match check_hash(
            item.content_hash.as_deref(),
            item.content_hash_algorithm.as_deref(),
            expected,
        ) {
            HashCheck::Match => Ok(VerifyOutcome::Ok),
            HashCheck::Mismatch => Ok(VerifyOutcome::Mismatch),
            HashCheck::Unavailable => Ok(VerifyOutcome::HashMissing),
        },
        None => Ok(VerifyOutcome::Ok),
    }
}

async fn heal_one(
    config: &Config,
    remote_root: &str,
    local: &LocalObject,
    options: &MirrorOptions,
    ensured: &mut std::collections::HashSet<String>,
) -> Result<()> {
    let remote_path = join_remote(remote_root, &local.rel_path);
    with_retries(options, || async {
        crate::services::delete_service::delete(config, &remote_path, true)
            .await
            .map_err(anyhow::Error::from)
    })
    .await
    .ok();
    upload_one(config, local, remote_root, options, ensured).await?;
    match verify_one(config, remote_root, local, options).await? {
        VerifyOutcome::Mismatch => Err(anyhow!("自愈后校验仍不一致: {}", local.rel_path)),
        _ => Ok(()),
    }
}

async fn with_retries<T, F, Fut>(options: &MirrorOptions, mut action: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        match action().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt >= options.retries {
                    return Err(err.context(format!("重试 {} 次后仍失败", options.retries)));
                }
                let delay = backoff_delay(attempt, options.retry_backoff_secs);
                warn!("操作失败，{} 秒后重试: {err:#}", delay.as_secs());
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

fn backoff_delay(attempt: u32, base_secs: u64) -> Duration {
    if base_secs == 0 {
        return Duration::from_millis(0);
    }
    let factor = 2u64.saturating_pow(attempt.min(10));
    Duration::from_secs(base_secs.saturating_mul(factor))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(rel: &str, size: u64, sha: Option<&str>) -> LocalObject {
        LocalObject {
            rel_path: rel.to_string(),
            abs_path: PathBuf::from(rel),
            size,
            expected_sha256: sha.map(|value| value.to_string()),
        }
    }

    fn remote(size: i64, hash: Option<&str>, alg: Option<&str>, folder: bool) -> RemoteObject {
        RemoteObject {
            file_id: "id".to_string(),
            size,
            content_hash: hash.map(|value| value.to_string()),
            content_hash_algorithm: alg.map(|value| value.to_string()),
            is_folder: folder,
        }
    }

    #[test]
    fn hash_compare_is_case_insensitive() {
        assert!(hash_matches("ABCDEF", "abcdef"));
        assert!(hash_matches(" abc ", "ABC"));
        assert!(!hash_matches("abc", "abd"));
    }

    #[test]
    fn skip_when_size_and_hash_match() {
        let item = local("data/aa/abc", 10, Some("abcdef"));
        let existing = remote(10, Some("ABCDEF"), Some("SHA256"), false);
        assert!(!needs_upload(&item, &existing, true));
    }

    #[test]
    fn upload_when_size_differs() {
        let item = local("data/aa/abc", 11, Some("abcdef"));
        let existing = remote(10, Some("abcdef"), Some("SHA256"), false);
        assert!(needs_upload(&item, &existing, true));
    }

    #[test]
    fn upload_when_hash_differs() {
        let item = local("data/aa/abc", 10, Some("abcdef"));
        let existing = remote(10, Some("000000"), Some("SHA256"), false);
        assert!(needs_upload(&item, &existing, true));
    }

    /// 契约：远端没哈希时按「缺哈希」处理，尺寸一致就跳过、计入 hashMissing，
    /// 不判失败也不反复重传（否则每轮都会重传）。
    #[test]
    fn skip_when_remote_hash_missing_but_size_matches() {
        let item = local("data/aa/abc", 10, Some("abcdef"));
        let existing = remote(10, None, None, false);
        assert!(!needs_upload(&item, &existing, true));
        assert_eq!(
            check_hash(None, None, "abcdef"),
            HashCheck::Unavailable
        );
    }

    #[test]
    fn algorithm_must_be_sha256_for_comparison() {
        let item = local("data/aa/abc", 10, Some("abcdef"));
        // 算法不是 SHA256：无法比对，按不可用处理，不重传
        let other_alg = remote(10, Some("000000"), Some("MD5"), false);
        assert!(!needs_upload(&item, &other_alg, true));
        assert_eq!(
            check_hash(Some("000000"), Some("MD5"), "abcdef"),
            HashCheck::Unavailable
        );
        // 算法大小写不敏感
        assert_eq!(
            check_hash(Some("ABCDEF"), Some("sha256"), "abcdef"),
            HashCheck::Match
        );
    }

    #[test]
    fn upload_when_remote_is_folder() {
        let item = local("data/aa/abc", 10, Some("abcdef"));
        let existing = remote(0, None, None, true);
        assert!(needs_upload(&item, &existing, true));
    }

    #[test]
    fn verify_disabled_only_compares_size() {
        let item = local("data/aa/abc", 10, Some("abcdef"));
        let existing = remote(10, Some("000000"), Some("SHA256"), false);
        assert!(!needs_upload(&item, &existing, false));
    }

    #[test]
    fn backoff_grows_exponentially() {
        assert_eq!(backoff_delay(0, 3).as_secs(), 3);
        assert_eq!(backoff_delay(1, 3).as_secs(), 6);
        assert_eq!(backoff_delay(2, 3).as_secs(), 12);
        assert_eq!(backoff_delay(3, 3).as_secs(), 24);
        assert_eq!(backoff_delay(2, 0).as_secs(), 0);
    }

    #[test]
    fn remote_only_counts_files_not_folders() {
        let mut remote_map = BTreeMap::new();
        remote_map.insert("data".to_string(), remote(0, None, None, true));
        remote_map.insert("data/aa".to_string(), remote(0, None, None, true));
        remote_map.insert("data/aa/x".to_string(), remote(1, None, None, false));
        remote_map.insert("extra".to_string(), remote(2, None, None, false));
        let locals = vec![local("data/aa/x", 1, Some("aa"))];
        // 只有 extra 是远端独有文件；data / data/aa 是目录，不算
        assert_eq!(count_remote_only(&remote_map, &locals), 1);
    }

    #[test]
    fn join_remote_handles_root() {
        assert_eq!(join_remote("/", "config"), "/config");
        assert_eq!(join_remote("/repo", "config"), "/repo/config");
        assert_eq!(join_remote("/repo/", "/config"), "/repo/config");
    }

    #[test]
    fn parent_paths_are_computed() {
        assert_eq!(parent_remote_path("/repo/config"), "/repo");
        assert_eq!(parent_remote_path("/config"), "/");
    }
}
