//! Prompt cache 记账（代理层模拟 Anthropic 缓存语义）
//!
//! Kiro 上游协议没有 Anthropic 式 prompt cache：`meteringEvent` 只按请求单位计量，
//! `contextUsageEvent` 只回报上下文占比，不存在 token 级缓存计价。因此下游客户端
//! （Claude Code / omp 等）发送的 `cache_control` 断点在此前被直接丢弃，usage 中
//! 从不出现 `cache_read_input_tokens`，导致下游显示缓存命中率为 0%、估算成本虚高。
//!
//! 本模块在代理层对齐 Anthropic 的显示语义：
//! - 解析请求中 system / tools / 消息内容块上的 `cache_control` 断点；
//! - 以 (model, 前缀哈希链) 为键跨请求做最长前缀匹配，TTL 默认 5 分钟（`ttl:"1h"` 为 1 小时）；
//! - 输出 `cache_read`（命中前缀）/ `cache_creation`（新写入段）/ `uncached`（断点之后）
//!   三段 token 估算，再由 `split_input` 等比缩放到实际上游 input_tokens。
//!
//! 注意：这是记账层的语义对齐，不改变上游实际处理量；Kiro 按请求计量额度，
//! 缓存命中率不影响真实订阅成本。

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::types::{CacheControl, Message, SystemMessage, Tool};
use crate::token::count_tokens;

/// 默认缓存 TTL（Anthropic ephemeral 默认 5 分钟）
const DEFAULT_TTL_SECS: u64 = 300;
/// 1 小时缓存 TTL（`ttl: "1h"`）
const LONG_TTL_SECS: u64 = 3600;
/// 前缀条目数上限，超过后触发懒清理
const MAX_ENTRIES: usize = 8192;

/// 一次请求的缓存三段拆分（token 估算值）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheUsage {
    /// 命中前缀的估算 tokens
    pub read: i64,
    /// 命中前缀之后到最后一个断点的估算 tokens（本次新写入缓存）
    pub creation: i64,
    /// 最后一个断点之后的估算 tokens（不参与缓存）
    pub uncached: i64,
}

/// 解析断点 TTL
fn cc_ttl_secs(cc: &CacheControl) -> u64 {
    match cc.ttl.as_deref() {
        Some("1h") => LONG_TTL_SECS,
        _ => DEFAULT_TTL_SECS,
    }
}

/// 从消息内容块（raw JSON）解析断点 TTL，取该消息内最后一个断点
fn message_breakpoint_ttl(msg: &Message) -> Option<u64> {
    let arr = msg.content.as_array()?;
    let mut ttl = None;
    for b in arr {
        if let Some(cc) = b.get("cache_control") {
            ttl = Some(match cc.get("ttl") {
                Some(Value::String(s)) if s == "1h" => LONG_TTL_SECS,
                _ => DEFAULT_TTL_SECS,
            });
        }
    }
    ttl
}

/// 参与前缀哈希的单元
struct PrefixUnit {
    /// 规范化序列化字节（已剥离 cache_control，保证内容相同则哈希相同）
    canonical: String,
    /// 该单元 token 估算（与 `token::count_all_tokens_local` 口径一致）
    tokens: i64,
    /// 携带断点时的 TTL
    breakpoint_ttl: Option<u64>,
}

/// 递归剥离 JSON 中的 cache_control 键，返回内容等价的可哈希形式
fn strip_cache_control(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                if k == "cache_control" {
                    continue;
                }
                out.insert(k.clone(), strip_cache_control(val));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(strip_cache_control).collect()),
        other => other.clone(),
    }
}

/// 工具单元的 token 估算（与本地全量估算口径一致）
fn tools_tokens(tools: &[Tool]) -> i64 {
    tools
        .iter()
        .map(|t| {
            count_tokens(&t.name)
                + count_tokens(&t.description)
                + count_tokens(&serde_json::to_string(&t.input_schema).unwrap_or_default())
        })
        .sum::<u64>() as i64
}

/// 消息单元的 token 估算（与本地全量估算口径一致）
fn message_tokens(msg: &Message) -> i64 {
    match &msg.content {
        Value::String(s) => count_tokens(s) as i64,
        Value::Array(arr) => arr
            .iter()
            .filter_map(|b| b.get("text").and_then(|v| v.as_str()))
            .map(|t| count_tokens(t) as i64)
            .sum(),
        _ => 0,
    }
}

/// 构建前缀单元序列：tools（单单元）、system 各块、各消息
///
/// 返回 `None` 表示序列化失败（不应发生），此时放弃缓存记账。
fn build_units(
    system: &Option<Vec<SystemMessage>>,
    messages: &[Message],
    tools: &Option<Vec<Tool>>,
) -> Option<Vec<PrefixUnit>> {
    let mut units = Vec::new();

    if let Some(tools) = tools.as_deref().filter(|t| !t.is_empty()) {
        // 经 Value 中转保证键序确定（input_schema 是 HashMap，直接 to_string 顺序不稳定）
        let value = serde_json::to_value(tools).ok()?;
        let canonical = serde_json::to_string(&strip_cache_control(&value)).ok()?;
        let breakpoint_ttl = tools
            .iter()
            .filter_map(|t| t.cache_control.as_ref())
            .map(cc_ttl_secs)
            .next_back();
        units.push(PrefixUnit {
            canonical,
            tokens: tools_tokens(tools),
            breakpoint_ttl,
        });
    }

    if let Some(system) = system {
        for block in system {
            let canonical =
                serde_json::to_string(&serde_json::json!({ "text": block.text })).ok()?;
            units.push(PrefixUnit {
                canonical,
                tokens: count_tokens(&block.text) as i64,
                breakpoint_ttl: block.cache_control.as_ref().map(cc_ttl_secs),
            });
        }
    }

    for msg in messages {
        let canonical = serde_json::to_string(&serde_json::json!({
            "role": msg.role,
            "content": strip_cache_control(&msg.content),
        }))
        .ok()?;
        units.push(PrefixUnit {
            canonical,
            tokens: message_tokens(msg),
            breakpoint_ttl: message_breakpoint_ttl(msg),
        });
    }

    Some(units)
}

/// 前缀缓存追踪器
#[derive(Default)]
struct PromptCacheTracker {
    /// (model, 前缀哈希) -> 过期时刻
    entries: HashMap<(String, [u8; 32]), Instant>,
}

impl PromptCacheTracker {
    /// 对一次请求计算缓存拆分；`now` 注入以便测试 TTL
    fn compute_at(
        &mut self,
        model: &str,
        system: &Option<Vec<SystemMessage>>,
        messages: &[Message],
        tools: &Option<Vec<Tool>>,
        now: Instant,
    ) -> Option<CacheUsage> {
        let units = build_units(system, messages, tools)?;

        // 客户端未使用 cache_control 时，与 Anthropic 行为一致：不产生缓存统计
        let last_bp = units.iter().rposition(|u| u.breakpoint_ttl.is_some())?;

        // 懒清理：条目过多时先剔除已过期项
        if self.entries.len() > MAX_ENTRIES {
            self.entries.retain(|_, exp| *exp > now);
        }

        // 前缀哈希链与 token 累计
        let mut chain = [0u8; 32];
        let mut hashes = Vec::with_capacity(units.len());
        let mut cumulative = Vec::with_capacity(units.len());
        let mut total = 0i64;
        for u in &units {
            let mut hasher = Sha256::new();
            hasher.update(chain);
            hasher.update(u.canonical.as_bytes());
            chain = hasher.finalize().into();
            hashes.push(chain);
            total += u.tokens;
            cumulative.push(total);
        }

        // 最长命中前缀（条目只在历史断点位置存在，命中即整段前缀一致）
        let mut read = 0i64;
        for i in 0..units.len() {
            let key = (model.to_string(), hashes[i]);
            if self.entries.get(&key).is_some_and(|exp| *exp > now) {
                read = cumulative[i];
            }
        }

        // 写入/刷新当前请求的全部断点（命中会续期，与 Anthropic 一致）
        for (i, u) in units.iter().enumerate() {
            if let Some(ttl) = u.breakpoint_ttl {
                self.entries
                    .insert((model.to_string(), hashes[i]), now + Duration::from_secs(ttl));
            }
        }

        let bp_end = cumulative[last_bp];
        let creation = (bp_end - read).max(0);
        let uncached = (total - read - creation).max(0);
        Some(CacheUsage {
            read,
            creation,
            uncached,
        })
    }
}

static TRACKER: LazyLock<Mutex<PromptCacheTracker>> =
    LazyLock::new(|| Mutex::new(PromptCacheTracker::default()));

/// 对一次请求计算缓存拆分（全局追踪器）
pub fn compute_cache_usage(
    model: &str,
    system: &Option<Vec<SystemMessage>>,
    messages: &[Message],
    tools: &Option<Vec<Tool>>,
) -> Option<CacheUsage> {
    let now = Instant::now();
    TRACKER
        .lock()
        .compute_at(model, system, messages, tools, now)
}

/// 将估算的三段拆分等比缩放到实际 input_tokens
///
/// 返回 (input_tokens, cache_read_input_tokens, cache_creation_input_tokens)，
/// 三者之和恒等于 `actual`。`cache` 为 `None`（未使用断点）时全部计入 input。
pub fn split_input(actual: i64, cache: Option<CacheUsage>) -> (i64, i64, i64) {
    let Some(c) = cache else {
        return (actual, 0, 0);
    };
    let est_total = c.read + c.creation + c.uncached;
    if actual <= 0 || est_total <= 0 {
        return (actual, 0, 0);
    }
    let scale = actual as f64 / est_total as f64;
    let mut read = (c.read as f64 * scale).round() as i64;
    let mut creation = (c.creation as f64 * scale).round() as i64;

    // 修正舍入导致的溢出：优先削减 creation，再削减 read
    if read + creation > actual {
        let overflow = read + creation - actual;
        let from_creation = creation.min(overflow);
        creation -= from_creation;
        read -= overflow - from_creation;
    }

    (actual - read - creation, read, creation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sys(text: &str, bp: bool) -> SystemMessage {
        SystemMessage {
            text: text.to_string(),
            cache_control: bp.then(|| CacheControl {
                cc_type: "ephemeral".to_string(),
                ttl: None,
            }),
        }
    }

    fn msg(role: &str, text: &str, bp: bool) -> Message {
        let mut block = json!({ "type": "text", "text": text });
        if bp {
            block["cache_control"] = json!({ "type": "ephemeral" });
        }
        Message {
            role: role.to_string(),
            content: json!([block]),
        }
    }

    fn tracker() -> PromptCacheTracker {
        PromptCacheTracker::default()
    }

    #[test]
    fn no_breakpoints_returns_none() {
        let mut t = tracker();
        let now = Instant::now();
        let usage = t.compute_at(
            "claude-sonnet-5",
            &Some(vec![sys("system prompt", false)]),
            &[msg("user", "hello", false)],
            &None,
            now,
        );
        assert!(usage.is_none());
    }

    #[test]
    fn first_request_creates_cache() {
        let mut t = tracker();
        let now = Instant::now();
        let usage = t
            .compute_at(
                "claude-sonnet-5",
                &Some(vec![sys("system prompt", true)]),
                &[msg("user", "hello world this is a test", true)],
                &None,
                now,
            )
            .unwrap();
        assert_eq!(usage.read, 0, "首次请求不应有命中");
        assert!(usage.creation > 0, "首请求应产生 cache_creation");
        assert_eq!(
            usage.creation + usage.uncached,
            (count_tokens("system prompt") + count_tokens("hello world this is a test")) as i64
        );
        // 断点在最后一条消息，无未缓存尾部
        assert_eq!(usage.uncached, 0);
    }

    #[test]
    fn second_request_reads_prefix() {
        let mut t = tracker();
        let now = Instant::now();
        let system = Some(vec![sys("system prompt", true)]);

        let _ = t
            .compute_at(
                "claude-sonnet-5",
                &system,
                &[msg("user", "first question", true)],
                &None,
                now,
            )
            .unwrap();

        let usage = t
            .compute_at(
                "claude-sonnet-5",
                &system,
                &[
                    msg("user", "first question", false),
                    msg("assistant", "first answer", false),
                    msg("user", "second question", true),
                ],
                &None,
                now,
            )
            .unwrap();

        let expected_read = (count_tokens("system prompt") + count_tokens("first question")) as i64;
        assert_eq!(usage.read, expected_read, "应命中 system + 首条消息前缀");
        let expected_creation =
            (count_tokens("first answer") + count_tokens("second question")) as i64;
        assert_eq!(usage.creation, expected_creation);
        assert_eq!(usage.uncached, 0);
    }

    #[test]
    fn ttl_expiry_drops_hit() {
        let mut t = tracker();
        let now = Instant::now();
        let system = Some(vec![sys("system prompt", true)]);

        let _ = t
            .compute_at("claude-sonnet-5", &system, &[msg("user", "q1", true)], &None, now)
            .unwrap();

        // 默认 TTL 5 分钟，6 分钟后应 miss
        let usage = t
            .compute_at(
                "claude-sonnet-5",
                &system,
                &[msg("user", "q1", false), msg("user", "q2", true)],
                &None,
                now + Duration::from_secs(360),
            )
            .unwrap();
        assert_eq!(usage.read, 0, "过期后不应命中");
        assert!(usage.creation > 0);
    }

    #[test]
    fn long_ttl_survives() {
        let mut t = tracker();
        let now = Instant::now();
        let system = Some(vec![SystemMessage {
            text: "system prompt".to_string(),
            cache_control: Some(CacheControl {
                cc_type: "ephemeral".to_string(),
                ttl: Some("1h".to_string()),
            }),
        }]);

        let _ = t
            .compute_at("claude-sonnet-5", &system, &[msg("user", "q1", true)], &None, now)
            .unwrap();

        // 1h TTL 下 6 分钟后仍应命中
        let usage = t
            .compute_at(
                "claude-sonnet-5",
                &system,
                &[msg("user", "q1", false), msg("user", "q2", true)],
                &None,
                now + Duration::from_secs(360),
            )
            .unwrap();
        assert!(usage.read > 0, "1h TTL 内应命中");
    }

    #[test]
    fn model_scopes_cache() {
        let mut t = tracker();
        let now = Instant::now();
        let system = Some(vec![sys("system prompt", true)]);

        let _ = t
            .compute_at("claude-sonnet-5", &system, &[msg("user", "q1", true)], &None, now)
            .unwrap();

        let usage = t
            .compute_at(
                "claude-opus-4-8",
                &system,
                &[msg("user", "q1", false), msg("user", "q2", true)],
                &None,
                now,
            )
            .unwrap();
        assert_eq!(usage.read, 0, "不同 model 的缓存应隔离");
    }

    #[test]
    fn content_after_last_breakpoint_is_uncached() {
        let mut t = tracker();
        let now = Instant::now();
        // 断点在 system，最后一条消息不打断点
        let usage = t
            .compute_at(
                "claude-sonnet-5",
                &Some(vec![sys("system prompt", true)]),
                &[msg("user", "question without breakpoint", false)],
                &None,
                now,
            )
            .unwrap();
        assert_eq!(usage.read, 0);
        assert_eq!(
            usage.creation,
            count_tokens("system prompt") as i64,
            "仅 system 段写入缓存"
        );
        assert_eq!(
            usage.uncached,
            count_tokens("question without breakpoint") as i64
        );
    }

    #[test]
    fn tools_unit_participates() {
        let mut t = tracker();
        let now = Instant::now();
        let tools = Some(vec![Tool {
            tool_type: None,
            name: "get_weather".to_string(),
            description: "Get current weather".to_string(),
            input_schema: serde_json::from_str(
                r#"{"type":"object","properties":{"city":{"type":"string"}}}"#,
            )
            .unwrap(),
            max_uses: None,
            cache_control: Some(CacheControl {
                cc_type: "ephemeral".to_string(),
                ttl: None,
            }),
        }]);

        let first = t
            .compute_at(
                "claude-sonnet-5",
                &None,
                &[msg("user", "hello world this is a request", true)],
                &tools,
                now,
            )
            .unwrap();
        assert_eq!(first.read, 0);
        assert!(first.creation > tools_tokens(tools.as_deref().unwrap()));

        let second = t
            .compute_at(
                "claude-sonnet-5",
                &None,
                &[
                    msg("user", "hello world this is a request", false),
                    msg("user", "follow up question here", true),
                ],
                &tools,
                now,
            )
            .unwrap();
        assert!(
            second.read >= tools_tokens(tools.as_deref().unwrap()),
            "工具定义前缀应命中"
        );
    }

    #[test]
    fn split_input_sums_to_actual() {
        let cases = [
            (1000i64, CacheUsage { read: 900, creation: 50, uncached: 50 }),
            (6585, CacheUsage { read: 30, creation: 70, uncached: 900 }),
            (7, CacheUsage { read: 9000, creation: 50, uncached: 50 }),
            (0, CacheUsage { read: 10, creation: 10, uncached: 10 }),
        ];
        for (actual, c) in cases {
            let (input, read, creation) = split_input(actual, Some(c));
            assert_eq!(input + read + creation, actual);
            assert!(input >= 0 && read >= 0 && creation >= 0);
        }
        // None：全量计入 input
        assert_eq!(split_input(42, None), (42, 0, 0));
    }

    #[test]
    fn strip_cache_control_is_recursive() {
        let v = json!({
            "role": "user",
            "content": [
                { "type": "text", "text": "hi", "cache_control": { "type": "ephemeral" } },
                { "type": "tool_result", "content": [{ "type": "text", "text": "ok", "cache_control": {"type": "ephemeral"} }] }
            ]
        });
        let stripped = strip_cache_control(&v);
        let s = serde_json::to_string(&stripped).unwrap();
        assert!(!s.contains("cache_control"));
        assert!(s.contains("\"text\":\"hi\""));
        // 原值不被修改
        assert!(v.to_string().contains("cache_control"));
    }
}
