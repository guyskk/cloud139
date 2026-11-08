#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Folder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileItem {
    pub name: String,
    pub kind: EntryKind,
    pub size: i64,
    pub modified: String,
    /// 服务端返回的内容哈希（个人云；家庭云/群组暂不提供）。
    pub content_hash: Option<String>,
    /// 哈希算法，实测为 "SHA256"（个人云）。
    pub content_hash_algorithm: Option<String>,
}
