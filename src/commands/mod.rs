pub mod cp;
pub mod delete;
pub mod download;
pub mod list;
pub mod login;
pub mod mirror;
pub mod mkdir;
pub mod mv;
pub mod rename;
pub mod sync;
pub mod upload;

use std::fmt;

/// 带自定义退出码的命令错误。
#[derive(Debug)]
pub struct CommandExit {
    code: i32,
    message: String,
}

impl CommandExit {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> i32 {
        self.code
    }
}

impl fmt::Display for CommandExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CommandExit {}

/// 按失败性质决定退出码，用于「执行过程中才可能失败」的读操作（扫描、下载等）。
///
/// - **1**：瞬时失败（网络断连、IO 抖动、请求重试耗尽）——这次没跑成，调用方可以重试；
/// - **2**：其它（参数、配置、路径不存在）——重试也不会变好，要人改参数。
///
/// 背景：sync 以前把这类失败一律映射成 2，调用方（备份脚本）会当成配置错误停掉定时任务，
/// 结果「网络抖一下」变成「任务停了等人」。
#[must_use]
pub fn exit_code_for_error(err: &anyhow::Error) -> i32 {
    if crate::utils::retry::is_transient(err) {
        1
    } else {
        2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientError;

    /// 网络/IO 抖动 → 1（调用方可以重试），业务错误 → 2（改参数才有用）。
    #[test]
    fn exit_code_follows_error_nature() {
        let timed_out: anyhow::Error =
            ClientError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")).into();
        assert_eq!(exit_code_for_error(&timed_out), 1, "超时可重试");

        let reset: anyhow::Error =
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset").into();
        assert_eq!(exit_code_for_error(&reset), 1, "断连可重试");

        let not_found: anyhow::Error = ClientError::FileNotFound.into();
        assert_eq!(exit_code_for_error(&not_found), 2, "路径不存在重试没用");

        let business: anyhow::Error = ClientError::Api("文件或目录不存在".to_string()).into();
        assert_eq!(exit_code_for_error(&business), 2);

        let not_logged_in: anyhow::Error = ClientError::NotLoggedIn.into();
        assert_eq!(exit_code_for_error(&not_logged_in), 2, "要人重新登录");
    }

    /// 典型场景：重试耗尽后被包了一层 context，仍要判定为 1。
    #[test]
    fn retry_exhausted_error_still_maps_to_one() {
        let io = ClientError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        let err = anyhow::Error::from(io).context("重试 4 次后仍失败");
        assert_eq!(exit_code_for_error(&err), 1);
    }
}
