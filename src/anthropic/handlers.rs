//! Anthropic API Handler 函数

use std::convert::Infallible;

use crate::kiro::model::events::Event;
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::token;
use anyhow::Error;
use axum::{
    Json as JsonExtractor,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use serde_json::json;
use std::time::Duration;
use tokio::time::interval;
use uuid::Uuid;

use super::converter::{ConversionError, convert_request};
use super::cache;
use super::middleware::AppState;
use super::stream::{SseEvent, StreamContext};
use super::truncation::{TruncationState, apply_recovery, record_truncated_response};
use super::types::{
    CountTokensRequest, CountTokensResponse, ErrorResponse, MessagesRequest, Model, ModelsResponse,
    OutputConfig, Thinking,
};
use super::websearch;

struct SerializedKiroRequest {
    primary: String,
    reasoning_fallback: Option<String>,
}

impl SerializedKiroRequest {
    fn new(request: KiroRequest) -> Result<Self, serde_json::Error> {
        let reasoning_fallback = request
            .without_reasoning_content()
            .map(|fallback| serde_json::to_string(&fallback))
            .transpose()?;
        let primary = serde_json::to_string(&request)?;
        Ok(Self {
            primary,
            reasoning_fallback,
        })
    }
}

fn record_stream_error(target: &mut Option<String>, message: impl Into<String>) {
    if target.is_none() {
        *target = Some(message.into());
    }
}

fn log_stream_read_error(error: &Error) {
    let Some(diagnostic) = error.downcast_ref::<crate::kiro::stream_response::StreamReadError>()
    else {
        tracing::error!(error = %error, "读取响应流失败");
        return;
    };

    tracing::error!(
        stream_id = %diagnostic.stream_id,
        attempt = diagnostic.attempt,
        http_version = ?diagnostic.http_version,
        content_length = ?diagnostic.content_length,
        transfer_encoding = ?diagnostic.transfer_encoding,
        connection = ?diagnostic.connection,
        aws_request_id = ?diagnostic.aws_request_id,
        elapsed_ms = ?diagnostic.elapsed.as_millis(),
        idle_ms = ?diagnostic.idle.as_millis(),
        chunks_read = diagnostic.chunks_read,
        bytes_read = diagnostic.bytes_read,
        is_timeout = diagnostic.is_timeout(),
        is_connect = diagnostic.is_connect(),
        is_body = diagnostic.is_body(),
        is_decode = diagnostic.is_decode(),
        error_chain = %diagnostic.source_chain(),
        "Kiro 上游响应体读取失败"
    );
}

/// 将 KiroProvider 错误映射为 HTTP 响应
fn map_provider_error(err: Error) -> Response {
    let err_str = err.to_string();

    // 上下文窗口满了（对话历史累积超出模型上下文窗口限制）
    if err_str.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
        tracing::warn!(error = %err, "上游拒绝请求：上下文窗口已满（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Context window is full. Reduce conversation history, system prompt, or tools.",
            )),
        )
            .into_response();
    }

    // 单次输入太长（请求体本身超出上游限制）
    if err_str.contains("Input is too long") {
        tracing::warn!(error = %err, "上游拒绝请求：输入过长（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Input is too long. Reduce the size of your messages.",
            )),
        )
            .into_response();
    }
    tracing::error!("Kiro API 调用失败: {:#}", err);
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::new(
            "api_error",
            format!("上游 API 调用失败: {:#}", err),
        )),
    )
        .into_response()
}

/// GET /v1/models
///
/// 返回可用的模型列表
pub async fn get_models() -> impl IntoResponse {
    tracing::info!("Received GET /v1/models request");

    let models = vec![
        Model {
            id: "gpt-5.6-sol".to_string(),
            object: "model".to_string(),
            created: 1783900800,
            owned_by: "openai".to_string(),
            display_name: "GPT 5.6 Sol".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "gpt-5.6-sol-thinking".to_string(),
            object: "model".to_string(),
            created: 1783900800,
            owned_by: "openai".to_string(),
            display_name: "GPT 5.6 Sol (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "gpt-5.6-terra".to_string(),
            object: "model".to_string(),
            created: 1783900800,
            owned_by: "openai".to_string(),
            display_name: "GPT 5.6 Terra".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "gpt-5.6-terra-thinking".to_string(),
            object: "model".to_string(),
            created: 1783900800,
            owned_by: "openai".to_string(),
            display_name: "GPT 5.6 Terra (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "gpt-5.6-luna".to_string(),
            object: "model".to_string(),
            created: 1783900800,
            owned_by: "openai".to_string(),
            display_name: "GPT 5.6 Luna".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "gpt-5.6-luna-thinking".to_string(),
            object: "model".to_string(),
            created: 1783900800,
            owned_by: "openai".to_string(),
            display_name: "GPT 5.6 Luna (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-5-5".to_string(),
            object: "model".to_string(),
            created: 1790035200, // Sep 22, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-5-5-thinking".to_string(),
            object: "model".to_string(),
            created: 1790035200, // Sep 22, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-5".to_string(),
            object: "model".to_string(),
            created: 1784937600,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-5-thinking".to_string(),
            object: "model".to_string(),
            created: 1784937600,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-4-8".to_string(),
            object: "model".to_string(),
            created: 1779897600, // May 28, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.8".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-4-8-thinking".to_string(),
            object: "model".to_string(),
            created: 1779897600, // May 28, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.8 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-4-7".to_string(),
            object: "model".to_string(),
            created: 1776276000, // Apr 16, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.7".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-4-7-thinking".to_string(),
            object: "model".to_string(),
            created: 1776276000, // Apr 16, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.7 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-4-6".to_string(),
            object: "model".to_string(),
            created: 1770163200, // Feb 4, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.6".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-4-6-thinking".to_string(),
            object: "model".to_string(),
            created: 1770163200, // Feb 4, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.6 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-4-6".to_string(),
            object: "model".to_string(),
            created: 1771286400, // Feb 17, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.6".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-4-6-thinking".to_string(),
            object: "model".to_string(),
            created: 1771286400, // Feb 17, 2026
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.6 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-5".to_string(),
            object: "model".to_string(),
            created: 1783296000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-sonnet-5-thinking".to_string(),
            object: "model".to_string(),
            created: 1783296000,
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 128_000,
        },
        Model {
            id: "claude-opus-4-5-20251101".to_string(),
            object: "model".to_string(),
            created: 1763942400, // Nov 24, 2025
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-opus-4-5-20251101-thinking".to_string(),
            object: "model".to_string(),
            created: 1763942400, // Nov 24, 2025
            owned_by: "anthropic".to_string(),
            display_name: "Claude Opus 4.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-4-5-20250929".to_string(),
            object: "model".to_string(),
            created: 1759104000, // Sep 29, 2025
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-sonnet-4-5-20250929-thinking".to_string(),
            object: "model".to_string(),
            created: 1759104000, // Sep 29, 2025
            owned_by: "anthropic".to_string(),
            display_name: "Claude Sonnet 4.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-haiku-4-5-20251001".to_string(),
            object: "model".to_string(),
            created: 1760486400, // Oct 15, 2025
            owned_by: "anthropic".to_string(),
            display_name: "Claude Haiku 4.5".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
        Model {
            id: "claude-haiku-4-5-20251001-thinking".to_string(),
            object: "model".to_string(),
            created: 1760486400, // Oct 15, 2025
            owned_by: "anthropic".to_string(),
            display_name: "Claude Haiku 4.5 (Thinking)".to_string(),
            model_type: "chat".to_string(),
            max_tokens: 64000,
        },
    ];

    Json(ModelsResponse {
        object: "list".to_string(),
        data: models,
    })
}

/// POST /v1/messages
///
/// 创建消息（对话）
pub async fn post_messages(
    State(state): State<AppState>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages request"
    );
    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    apply_recovery(&mut payload, &state.truncation_state);

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求
    let conversion_result = match convert_request(&payload) {
        Ok(result) => result,
        Err(e) => {
            let (error_type, message) = match &e {
                ConversionError::UnsupportedModel(model) => {
                    ("invalid_request_error", format!("模型不支持: {}", model))
                }
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", "消息列表为空".to_string())
                }
                ConversionError::InvalidThinking(message) => {
                    ("invalid_request_error", message.clone())
                }
                ConversionError::InvalidDocument(message) => {
                    ("invalid_request_error", format!("文档无效: {}", message))
                }
                ConversionError::TooManyDocuments(count) => (
                    "invalid_request_error",
                    format!("文档数量超过 Kiro 限制: {} > 5", count),
                ),
                ConversionError::DuplicateDocumentName(name) => {
                    ("invalid_request_error", format!("文档名称重复: {}", name))
                }
            };
            tracing::warn!("请求转换失败: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(error_type, message)),
            )
                .into_response();
        }
    };

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
        additional_model_request_fields: conversion_result.additional_model_request_fields,
    };

    let request_body = match SerializedKiroRequest::new(kiro_request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("序列化请求失败: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    tracing::debug!("Kiro request body: {}", request_body.primary);

    // 计算 prompt cache 拆分（需在 payload 字段被移动前完成）
    let cache_usage = cache::compute_cache_usage(
        &payload.model,
        &payload.system,
        &payload.messages,
        &payload.tools,
    );

    // 估算输入 tokens
    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);

    let tool_name_map = conversion_result.tool_name_map;

    if payload.stream {
        // 流式响应
        handle_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            thinking_enabled,
            tool_name_map,
            state.truncation_state.clone(),
            cache_usage,
        )
        .await
    } else {
        // 非流式响应：仅在配置开启时提取 thinking 块
        let extract_thinking = state.extract_thinking && thinking_enabled;
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            extract_thinking,
            tool_name_map,
            state.truncation_state.clone(),
            cache_usage,
        )
        .await
    }
}

/// 处理流式请求
async fn handle_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &SerializedKiroRequest,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
    truncation_state: std::sync::Arc<TruncationState>,
    cache_usage: Option<crate::anthropic::cache::CacheUsage>,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider
        .call_api_stream(
            &request_body.primary,
            request_body.reasoning_fallback.as_deref(),
        )
        .await
    {
        Ok(resp) => resp,
        Err(e) => return map_provider_error(e),
    };

    // 创建流处理上下文
    let mut ctx =
        StreamContext::new_with_thinking(model, input_tokens, thinking_enabled, tool_name_map)
            .with_cache_usage(cache_usage);

    // 生成初始事件
    let initial_events = ctx.generate_initial_events();

    // 创建 SSE 流
    let stream = create_sse_stream(response, ctx, initial_events, truncation_state);

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Ping 事件间隔（25秒）
const PING_INTERVAL_SECS: u64 = 25;

/// 创建 ping 事件的 SSE 字符串
fn create_ping_sse() -> Bytes {
    Bytes::from("event: ping\ndata: {\"type\": \"ping\"}\n\n")
}

/// 创建 SSE 事件流
fn create_sse_stream(
    response: crate::kiro::stream_response::KiroStreamResponse,
    ctx: StreamContext,
    initial_events: Vec<SseEvent>,
    truncation_state: std::sync::Arc<TruncationState>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    // 先发送初始事件
    let initial_stream = stream::iter(
        initial_events
            .into_iter()
            .map(|e| Ok(Bytes::from(e.to_sse_string()))),
    );

    // 然后处理 Kiro 响应流，同时每25秒发送 ping 保活
    let body_stream = response.bytes_stream();

    let processing_stream = stream::unfold(
        (body_stream, ctx, EventStreamDecoder::new(), false, interval(Duration::from_secs(PING_INTERVAL_SECS)), truncation_state),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval, truncation_state)| async move {
            if finished {
                return None;
            }

            // 使用 select! 同时等待数据和 ping 定时器
            tokio::select! {
                // 处理数据流
                chunk_result = body_stream.next() => {
                    match chunk_result {
                        Some(Ok(chunk)) => {
                            // 解码事件
                            if let Err(e) = decoder.feed(&chunk) {
                                tracing::error!("解码响应流失败: {}", e);
                                ctx.set_stream_error(format!("Failed to decode Kiro stream: {e}"));
                            }

                            let mut events = Vec::new();
                            for result in decoder.decode_iter() {
                                match result {
                                    Ok(frame) => {
                                        match Event::from_frame(frame) {
                                            Ok(event) => {
                                                let sse_events = ctx.process_kiro_event(&event);
                                                events.extend(sse_events);
                                            }
                                            Err(e) => {
                                                tracing::error!("解析 Kiro 事件失败: {}", e);
                                                ctx.set_stream_error(format!("Failed to parse Kiro event: {e}"));
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("解码事件失败: {}", e);
                                        ctx.set_stream_error(format!("Failed to decode Kiro event: {e}"));
                                    }
                                }
                            }

                            let failed = ctx.is_failed();
                            if failed {
                                events.extend(ctx.generate_final_events());
                            }

                            // 转换为 SSE 字节流
                            let bytes: Vec<Result<Bytes, Infallible>> = events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();

                                Some((stream::iter(bytes), (body_stream, ctx, decoder, failed, ping_interval, truncation_state)))
                        }
                        Some(Err(e)) => {
                            log_stream_read_error(&e);
                            if e
                                .downcast_ref::<crate::kiro::stream_response::StreamTruncatedError>()
                                .is_some()
                            {
                                ctx.mark_truncated(&truncation_state);
                                tracing::warn!(
                                    "上游流被截断，已按 max_tokens 正常收尾并记录恢复状态: {e}"
                                );
                            } else if e.downcast_ref::<
                                    crate::kiro::stream_response::StreamReadError,
                                >()
                                .is_some_and(|error| {
                                    (error.is_body() || error.is_decode()) && !error.is_timeout()
                                })
                            {
                                ctx.mark_truncated(&truncation_state);
                                tracing::warn!(
                                    "上游响应体读取提前结束，已按 max_tokens 正常收尾并记录恢复状态"
                                );
                            } else {
                                ctx.set_stream_error(format!("Failed to read Kiro response stream: {e}"));
                            }
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, truncation_state)))
                        }
                        None => {
                            if let Err(e) = decoder.finish() {
                                tracing::error!("Kiro 响应流提前结束: {}", e);
                                ctx.mark_truncated(&truncation_state);
                                tracing::warn!(
                                    "上游流提前结束且解码未完成，已按 max_tokens 正常收尾并记录恢复状态: {e}"
                                );
                            }
                            if !ctx.completion_seen() {
                                ctx.mark_truncated(&truncation_state);
                            }
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, truncation_state)))
                        }
                    }
                }
                // 发送 ping 保活
                _ = ping_interval.tick() => {
                    tracing::trace!("发送 ping 保活事件");
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                    Some((
                        stream::iter(bytes),
                        (body_stream, ctx, decoder, false, ping_interval, truncation_state),
                    ))
                }
            }
        },
    )
    .flatten();

    initial_stream.chain(processing_stream)
}

use super::converter::get_context_window_size;

/// 处理非流式请求
async fn handle_non_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &SerializedKiroRequest,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
    truncation_state: std::sync::Arc<TruncationState>,
    cache_usage: Option<crate::anthropic::cache::CacheUsage>,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider
        .call_api(
            &request_body.primary,
            request_body.reasoning_fallback.as_deref(),
        )
        .await
    {
        Ok(resp) => resp,
        Err(e) => return map_provider_error(e),
    };

    // 读取响应体
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("读取响应体失败: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("读取响应失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    // 解析事件流
    let mut decoder = EventStreamDecoder::new();
    if let Err(e) = decoder.feed(&body_bytes) {
        return (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::new(
                "api_error",
                format!("Failed to decode Kiro stream: {e}"),
            )),
        )
            .into_response();
    }

    let mut decoded_events = Vec::new();
    let mut stop_reason = "end_turn".to_string();
    let mut stream_error: Option<String> = None;
    let mut completion_signal_seen = false;
    let mut truncated_content = String::new();
    let mut truncated_tool_calls = std::collections::HashMap::new();
    // 从 contextUsageEvent 计算的实际输入 tokens
    let mut context_input_tokens: Option<i32> = None;

    for result in decoder.decode_iter() {
        match result {
            Ok(frame) => {
                match Event::from_frame(frame) {
                    Ok(event) => {
                        match &event {
                            Event::AssistantResponse(assistant) => {
                                truncated_content.push_str(&assistant.content);
                            }
                            Event::ToolUse(tool_use) => {
                                truncated_tool_calls.insert(
                                    tool_use.tool_use_id.clone(),
                                    (tool_use.name.clone(), tool_use.stop),
                                );
                            }
                            Event::Metering(()) => {
                                completion_signal_seen = true;
                            }
                            Event::ContextUsage(context_usage) => {
                                completion_signal_seen = true;
                                // 从上下文使用百分比计算实际的 input_tokens
                                let window_size = get_context_window_size(model);
                                let actual_input_tokens =
                                    (context_usage.context_usage_percentage * (window_size as f64)
                                        / 100.0) as i32;
                                context_input_tokens = Some(actual_input_tokens);
                                // 上下文使用量达到 100% 时，设置 stop_reason 为 model_context_window_exceeded
                                if context_usage.context_usage_percentage >= 100.0 {
                                    stop_reason = "model_context_window_exceeded".to_string();
                                }
                                tracing::debug!(
                                    "收到 contextUsageEvent: {}%, 计算 input_tokens: {}",
                                    context_usage.context_usage_percentage,
                                    actual_input_tokens
                                );
                            }
                            Event::Exception { exception_type, .. }
                                if exception_type == "ContentLengthExceededException" =>
                            {
                                stop_reason = "max_tokens".to_string();
                            }
                            Event::Error {
                                error_code,
                                error_message,
                            } => {
                                record_stream_error(
                                    &mut stream_error,
                                    format!("Kiro error {error_code}: {error_message}"),
                                );
                            }
                            Event::Exception {
                                exception_type,
                                message,
                            } => {
                                record_stream_error(
                                    &mut stream_error,
                                    format!("Kiro exception {exception_type}: {message}"),
                                );
                            }
                            _ => {}
                        }
                        decoded_events.push(event);
                    }
                    Err(e) => {
                        record_stream_error(
                            &mut stream_error,
                            format!("Failed to parse Kiro event: {e}"),
                        );
                    }
                }
            }
            Err(e) => {
                record_stream_error(
                    &mut stream_error,
                    format!("Failed to decode Kiro event: {e}"),
                );
            }
        }
    }

    if let Err(e) = decoder.finish() {
        record_truncated_response(&truncation_state, &truncated_content, &truncated_tool_calls);
        record_stream_error(
            &mut stream_error,
            format!("Kiro response stream was truncated: {e}"),
        );
    }

    if !completion_signal_seen {
        record_truncated_response(&truncation_state, &truncated_content, &truncated_tool_calls);
        if stop_reason == "end_turn" {
            stop_reason = "max_tokens".to_string();
        }
    }

    if let Some(message) = stream_error {
        return (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::new("api_error", message)),
        )
            .into_response();
    }

    let aggregated =
        super::response::aggregate_content(&decoded_events, thinking_enabled, &tool_name_map);

    if aggregated.has_invalid_reasoning {
        return (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::new(
                "api_error",
                "Kiro reasoning stream ended without a signature",
            )),
        )
            .into_response();
    }

    // 确定 stop_reason
    if aggregated.has_tool_use && stop_reason == "end_turn" {
        stop_reason = "tool_use".to_string();
    }
    let content = aggregated.content;

    // 估算输出 tokens
    let output_tokens = token::estimate_output_tokens(&content);

    // 使用从 contextUsageEvent 计算的 input_tokens，如果没有则使用估算值
    let final_input_tokens = context_input_tokens.unwrap_or(input_tokens);
    let (input_tokens, cache_read, cache_creation) =
        cache::split_input(final_input_tokens as i64, cache_usage);

    // 构建 Anthropic 响应
    let response_body = json!({
        "id": format!("msg_{}", Uuid::new_v4().to_string().replace('-', "")),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": cache_creation,
            "cache_read_input_tokens": cache_read
        }
    });

    (StatusCode::OK, Json(response_body)).into_response()
}

/// 检测模型名是否包含 "thinking" 后缀，并在请求未显式配置时填充默认值
///
/// - 原生 reasoning 模型：默认 adaptive 类型
/// - 其他模型：默认 enabled 类型，预算不超过 max_tokens
fn override_thinking_from_model_name(payload: &mut MessagesRequest) {
    let model_lower = payload.model.to_lowercase();
    if !model_lower.contains("thinking") || payload.thinking.is_some() {
        return;
    }

    let native_reasoning = super::converter::model_supports_native_reasoning(&payload.model);

    let thinking_type = if native_reasoning {
        "adaptive"
    } else {
        "enabled"
    };

    tracing::info!(
        model = %payload.model,
        thinking_type = thinking_type,
        "模型名包含 thinking 后缀，覆写 thinking 配置"
    );

    payload.thinking = Some(Thinking {
        thinking_type: thinking_type.to_string(),
        budget_tokens: (thinking_type == "enabled")
            .then(|| 20000.min(payload.max_tokens.saturating_sub(1))),
        display: None,
    });

    if native_reasoning && payload.output_config.is_none() {
        payload.output_config = Some(OutputConfig {
            effort: "high".to_string(),
        });
    }
}

/// POST /v1/messages/count_tokens
///
/// 计算消息的 token 数量
pub async fn count_tokens(
    JsonExtractor(payload): JsonExtractor<CountTokensRequest>,
) -> impl IntoResponse {
    tracing::info!(
        model = %payload.model,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages/count_tokens request"
    );

    let total_tokens = token::count_all_tokens(
        payload.model,
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    Json(CountTokensResponse {
        input_tokens: total_tokens.max(1) as i32,
    })
}

/// POST /cc/v1/messages
///
pub async fn post_messages_cc(
    State(state): State<AppState>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /cc/v1/messages request"
    );

    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    apply_recovery(&mut payload, &state.truncation_state);

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求
    let conversion_result = match convert_request(&payload) {
        Ok(result) => result,
        Err(e) => {
            let (error_type, message) = match &e {
                ConversionError::UnsupportedModel(model) => {
                    ("invalid_request_error", format!("模型不支持: {}", model))
                }
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", "消息列表为空".to_string())
                }
                ConversionError::InvalidThinking(message) => {
                    ("invalid_request_error", message.clone())
                }
                ConversionError::InvalidDocument(message) => {
                    ("invalid_request_error", format!("文档无效: {}", message))
                }
                ConversionError::TooManyDocuments(count) => (
                    "invalid_request_error",
                    format!("文档数量超过 Kiro 限制: {} > 5", count),
                ),
                ConversionError::DuplicateDocumentName(name) => {
                    ("invalid_request_error", format!("文档名称重复: {}", name))
                }
            };
            tracing::warn!("请求转换失败: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(error_type, message)),
            )
                .into_response();
        }
    };

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
        additional_model_request_fields: conversion_result.additional_model_request_fields,
    };

    let request_body = match SerializedKiroRequest::new(kiro_request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("序列化请求失败: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    tracing::debug!("Kiro request body: {}", request_body.primary);

    // 计算 prompt cache 拆分（需在 payload 字段被移动前完成）
    let cache_usage = cache::compute_cache_usage(
        &payload.model,
        &payload.system,
        &payload.messages,
        &payload.tools,
    );

    // 估算输入 tokens
    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);

    let tool_name_map = conversion_result.tool_name_map;

    if payload.stream {
        handle_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            thinking_enabled,
            tool_name_map,
            state.truncation_state.clone(),
            cache_usage,
        )
        .await
    } else {
        // 非流式响应：仅在配置开启时提取 thinking 块
        let extract_thinking = state.extract_thinking && thinking_enabled;
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            extract_thinking,
            tool_name_map,
            state.truncation_state.clone(),
            cache_usage,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_non_stream_error_preserves_first_failure() {
        // Given: an upstream protocol error has already identified the root cause.
        let mut error = None;
        record_stream_error(&mut error, "Kiro error UpstreamError: failed");

        // When: EOF validation also observes a truncated trailing frame.
        record_stream_error(&mut error, "Kiro response stream was truncated");

        // Then: the first actionable failure remains the response error.
        assert_eq!(error.as_deref(), Some("Kiro error UpstreamError: failed"));
    }

    #[tokio::test]
    async fn test_truncated_upstream_stream_emits_error_without_message_stop() {
        // Given: one complete assistant event is followed by a missing HTTP chunk terminator.
        let mut encoded_headers = Vec::new();
        for (name, value) in [
            (":message-type", "event"),
            (":event-type", "assistantResponseEvent"),
        ] {
            encoded_headers.push(name.len() as u8);
            encoded_headers.extend_from_slice(name.as_bytes());
            encoded_headers.push(7);
            encoded_headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
            encoded_headers.extend_from_slice(value.as_bytes());
        }
        let payload = br#"{"content":"recovered"}"#;
        let total_length = 16 + encoded_headers.len() + payload.len();
        let mut frame = Vec::with_capacity(total_length);
        frame.extend_from_slice(&(total_length as u32).to_be_bytes());
        frame.extend_from_slice(&(encoded_headers.len() as u32).to_be_bytes());
        let prelude_crc = crate::kiro::parser::crc::crc32(&frame);
        frame.extend_from_slice(&prelude_crc.to_be_bytes());
        frame.extend_from_slice(&encoded_headers);
        frame.extend_from_slice(payload);
        let message_crc = crate::kiro::parser::crc::crc32(&frame);
        frame.extend_from_slice(&message_crc.to_be_bytes());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address");
        let (close_tx, close_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("client should connect");
            let mut request = [0_u8; 4096];
            let _ = socket
                .read(&mut request)
                .await
                .expect("request should be readable");
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .expect("response headers should be written");
            socket
                .write_all(format!("{:X}\r\n", frame.len()).as_bytes())
                .await
                .expect("chunk size should be written");
            socket
                .write_all(&frame)
                .await
                .expect("event frame should be written");
            socket
                .write_all(b"\r\n")
                .await
                .expect("chunk delimiter should be written");
            close_rx.await.expect("test should request truncation");
        });
        let url = format!("http://{address}/");
        let response = reqwest::get(url.clone())
            .await
            .expect("test response should arrive");
        let retry_calls = Arc::new(AtomicUsize::new(0));
        let retry_calls_for_request = retry_calls.clone();
        let response = crate::kiro::stream_response::KiroStreamResponse::with_retry_request(
            response,
            move || {
                retry_calls_for_request.fetch_add(1, Ordering::SeqCst);
                reqwest::get(url.clone())
            },
        );
        let mut ctx = StreamContext::new_with_thinking(
            "claude-opus-5",
            1,
            false,
            std::collections::HashMap::new(),
        );
        let initial_events = ctx.generate_initial_events();
        let truncation_state = TruncationState::new();

        // When: downstream receives the event before the upstream connection is truncated.
        let mut stream = Box::pin(create_sse_stream(
            response,
            ctx,
            initial_events,
            truncation_state.clone(),
        ));
        let output = tokio::time::timeout(Duration::from_secs(5), async move {
            let mut output = String::new();
            let mut close_tx = Some(close_tx);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.expect("SSE chunks are infallible");
                output
                    .push_str(std::str::from_utf8(&chunk).expect("SSE output should remain UTF-8"));
                if output.contains("recovered")
                    && let Some(sender) = close_tx.take()
                {
                    sender.send(()).expect("server should still be waiting");
                }
            }
            output
        })
        .await
        .expect("SSE stream should terminate after truncation");
        server.await.expect("test server should exit");

        // Then: content is not replayed and truncation completes as a normal message_stop
        // with stop_reason=max_tokens, so clients treat the partial output as usable.
        assert_eq!(output.matches("recovered").count(), 1);
        assert_eq!(output.matches("event: error").count(), 0);
        assert!(output.contains(r#""stop_reason":"max_tokens""#));
        assert!(output.contains("event: message_stop"));
        assert_eq!(retry_calls.load(Ordering::SeqCst), 0);

        let mut next_request = MessagesRequest {
            model: "claude-opus-5".to_string(),
            max_tokens: 100,
            messages: vec![super::super::types::Message {
                role: "assistant".to_string(),
                content: serde_json::json!("recovered"),
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };
        apply_recovery(&mut next_request, &truncation_state);
        assert_eq!(next_request.messages.len(), 2);
        assert_eq!(next_request.messages[1].role, "user");
    }

    #[test]
    fn test_model_suffix_does_not_override_explicit_thinking_configuration() {
        // Given: the caller explicitly disabled thinking and selected low effort.
        let mut payload = MessagesRequest {
            model: "claude-opus-5-thinking".to_string(),
            max_tokens: 4096,
            messages: Vec::new(),
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: Some(Thinking {
                thinking_type: "disabled".to_string(),
                budget_tokens: None,
                display: None,
            }),
            output_config: Some(OutputConfig {
                effort: "low".to_string(),
            }),
            metadata: None,
        };

        // When: the model-name convenience suffix is processed.
        override_thinking_from_model_name(&mut payload);

        // Then: explicit request fields retain precedence.
        let thinking = payload.thinking.expect("thinking config should remain");
        assert_eq!(thinking.thinking_type, "disabled");
        assert_eq!(
            payload
                .output_config
                .expect("output config should remain")
                .effort,
            "low"
        );
    }

    #[test]
    fn test_model_suffix_uses_budget_below_max_tokens() {
        // Given: a synthetic-thinking model has less than the default 20000-token budget available.
        let mut payload = MessagesRequest {
            model: "claude-sonnet-4-5-thinking".to_string(),
            max_tokens: 10000,
            messages: Vec::new(),
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        // When: the model-name convenience suffix supplies thinking defaults.
        override_thinking_from_model_name(&mut payload);

        // Then: the generated budget remains valid for the request's max_tokens.
        assert_eq!(
            payload
                .thinking
                .expect("thinking should be enabled")
                .budget_tokens,
            Some(9999)
        );
    }
}
