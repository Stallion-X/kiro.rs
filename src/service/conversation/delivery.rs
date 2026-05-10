//! SseDelivery：Live / Buffered 两种推送策略
//!
//! 本模块持有 handler 参数化使用的 [`DeliveryMode`]。
//! Kiro 事件到 Anthropic SSE 事件的有状态转换位于 [`super::stream`]。

/// SseDelivery 策略类型枚举（与 handler 参数化匹配）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryMode {
    /// 实时推送：每收到事件立即推 SSE
    Live,
    /// 缓冲推送：等流结束后批量推送（修正 input_tokens 后）
    Buffered,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_mode_variants_are_distinct() {
        assert_ne!(DeliveryMode::Live, DeliveryMode::Buffered);
    }
}
