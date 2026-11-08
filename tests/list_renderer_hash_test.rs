//! JSON 输出里的内容哈希契约：字段名、算法大小写、缺失时省略。

use cloud139::domain::{EntryKind, FileItem};
use cloud139::presentation::list_renderer::to_json_with_pagination;
use cloud139::services::list_service::ListResult;

fn file_item(name: &str, hash: Option<(&str, &str)>) -> FileItem {
    FileItem {
        name: name.to_string(),
        kind: EntryKind::File,
        size: 1024,
        modified: "2026-11-08 10:00:00".to_string(),
        content_hash: hash.map(|(h, _)| h.to_string()),
        content_hash_algorithm: hash.map(|(_, a)| a.to_string()),
    }
}

fn folder_item(name: &str) -> FileItem {
    FileItem {
        name: name.to_string(),
        kind: EntryKind::Folder,
        size: 0,
        modified: "2026-11-08 10:00:00".to_string(),
        content_hash: None,
        content_hash_algorithm: None,
    }
}

fn render(items: Vec<FileItem>) -> serde_json::Value {
    let result = ListResult {
        path: "/".to_string(),
        total: items.len() as i32,
        items,
    };
    let json = to_json_with_pagination(&result, Some(1), Some(100)).expect("render should succeed");
    serde_json::from_str(&json).expect("output should be valid json")
}

/// 服务端返回小写 sha256，输出统一大写；字段名按契约用 camelCase。
#[test]
fn exposes_hash_with_uppercase_algorithm() {
    let out = render(vec![file_item("a.bin", Some(("abc123", "sha256")))]);
    let item = &out["items"][0];

    assert_eq!(item["contentHash"], "abc123");
    assert_eq!(item["contentHashAlgorithm"], "SHA256");
    // 原有字段不受影响
    assert_eq!(item["name"], "a.bin");
    assert_eq!(item["type"], "file");
    assert_eq!(item["size"], 1024);
}

/// 没有哈希的对象不能出现这两个键，否则消费方会把空值当成可用哈希。
#[test]
fn omits_hash_fields_when_absent() {
    let out = render(vec![folder_item("dir"), file_item("b.bin", None)]);

    for item in out["items"].as_array().expect("items should be array") {
        assert!(
            item.get("contentHash").is_none(),
            "unexpected contentHash: {item}"
        );
        assert!(
            item.get("contentHashAlgorithm").is_none(),
            "unexpected contentHashAlgorithm: {item}"
        );
    }
}

/// 分页参数要原样回显，消费方据此判断还有没有下一页。
#[test]
fn echoes_pagination_metadata() {
    let out = render(vec![file_item("c.bin", Some(("def456", "sha256")))]);
    assert_eq!(out["page"], 1);
    assert_eq!(out["page_size"], 100);
    assert_eq!(out["total"], 1);
}
