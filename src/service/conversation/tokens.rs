//! 本地 token 估算（迁移自 src/token.rs，删除 OnceLock 全局静态与远程 API 调用路径）
//!
//! 项目唯一的本地 token 估算函数（流式与非流式共用）。
//! 流式路径在 [`super::stream::StreamContext`] 中累积输出文本，最终在
//! `generate_final_events` 阶段一次性调用 [`count_tokens`] 估算 output_tokens。
//!
//! # 计算规则
//! - 非西文字符：每个计 4.0 个字符单位
//! - 西文字符：每个计 1 个字符单位
//! - 4 个字符单位 = 1 token，再乘以基于总长度的分档系数

use crate::interface::http::anthropic::dto::{Message, SystemMessage, Tool};

/// 判断字符是否为非西文字符
fn is_non_western_char(c: char) -> bool {
    !matches!(c,
        '\u{0000}'..='\u{007F}' |
        '\u{0080}'..='\u{00FF}' |
        '\u{0100}'..='\u{024F}' |
        '\u{1E00}'..='\u{1EFF}' |
        '\u{2C60}'..='\u{2C7F}' |
        '\u{A720}'..='\u{A7FF}' |
        '\u{AB30}'..='\u{AB6F}'
    )
}

/// 计算文本的 token 数量（同步纯函数）
///
/// 项目内唯一的本地估算入口。分档系数基于**整段总长度**生效，因此调用方应在
/// 「最终一次性合并的文本」上调用本函数，而不要对单个流式 chunk 反复调用，否则
/// 每个小 chunk 都会落入 `<100` 档（×1.5）造成系统性高估。
pub fn count_tokens(text: &str) -> u64 {
    let char_units: f64 = text
        .chars()
        .map(|c| if is_non_western_char(c) { 4.0 } else { 1.0 })
        .sum();
    let tokens = char_units / 4.0;

    (if tokens < 100.0 {
        tokens * 1.5
    } else if tokens < 200.0 {
        tokens * 1.3
    } else if tokens < 300.0 {
        tokens * 1.25
    } else if tokens < 800.0 {
        tokens * 1.2
    } else {
        tokens * 1.0
    } as u64)
}

/// 估算请求的输入 tokens（async 包装；内部纯计算，未来可切 spawn_blocking）
///
/// 不再支持远程 API（**Breaking Change**：`countTokensApiUrl/Key/AuthType` 三字段已删除）。
pub async fn count_all_tokens(
    system: Option<Vec<SystemMessage>>,
    messages: Vec<Message>,
    tools: Option<Vec<Tool>>,
) -> u64 {
    let mut total = 0;

    if let Some(ref system) = system {
        for msg in system {
            total += count_tokens(&msg.text);
        }
    }

    for msg in &messages {
        if let serde_json::Value::String(s) = &msg.content {
            total += count_tokens(s);
        } else if let serde_json::Value::Array(arr) = &msg.content {
            for item in arr {
                if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                    total += count_tokens(text);
                }
            }
        }
    }

    if let Some(ref tools) = tools {
        for tool in tools {
            total += count_tokens(&tool.name);
            total += count_tokens(&tool.description);
            let input_schema_json = serde_json::to_string(&tool.input_schema).unwrap_or_default();
            total += count_tokens(&input_schema_json);
        }
    }

    total.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_tokens_chinese_text_returns_positive() {
        let n = count_tokens("你好，这是一段中文测试文本");
        assert!(n > 0);
    }

    #[test]
    fn count_tokens_english_text_returns_positive() {
        let n = count_tokens("Hello, this is an English test text.");
        assert!(n > 0);
    }

    #[test]
    fn count_tokens_empty_text_returns_zero() {
        // 实现允许 0；count_all_tokens 才 max(1)
        assert_eq!(count_tokens(""), 0);
    }

    #[tokio::test]
    async fn count_all_tokens_at_least_1_when_empty() {
        assert_eq!(count_all_tokens(None, vec![], None).await, 1);
    }

    #[tokio::test]
    async fn count_all_tokens_includes_tools_name_description_schema() {
        use std::collections::HashMap;
        let tools = vec![Tool {
            tool_type: None,
            name: "fetch_url".to_string(),
            description: "Fetch a URL and return body".to_string(),
            input_schema: HashMap::new(),
            max_uses: None,
        }];
        let with_tools = count_all_tokens(None, vec![], Some(tools)).await;
        let without_tools = count_all_tokens(None, vec![], None).await;
        assert!(with_tools > without_tools);
    }
}
