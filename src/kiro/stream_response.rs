//! Kiro 流式响应启动阶段的预取与瞬态错误识别。

mod diagnostics;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};

use crate::kiro::model::events::Event;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::kiro::provider::{KiroProvider, STREAM_START_ATTEMPTS, SharedStreamRetryBudget};

use diagnostics::StreamDiagnostics;
pub(crate) use diagnostics::{BufferedStreamError, StreamReadError, StreamTruncatedError};

const TRANSIENT_UPSTREAM_ERROR: &str =
    "Encountered an unexpected error when processing the request, please try again.";
const MAX_PREFETCH_BYTES: usize = 1024 * 1024;

type UpstreamStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static>>;
type RetryFuture = Pin<Box<dyn Future<Output = anyhow::Result<reqwest::Response>> + Send>>;
type RetryRequest = Arc<dyn Fn() -> RetryFuture + Send + Sync>;
type CanRetryRequest = Arc<dyn Fn() -> bool + Send + Sync>;

pub struct KiroStreamResponse {
    initial_response: reqwest::Response,
    retry_request: RetryRequest,
    can_retry_request: CanRetryRequest,
}

struct ProbeState {
    stream: UpstreamStream,
    decoder: EventStreamDecoder,
    prefetched: Vec<Bytes>,
    prefetched_bytes: usize,
    unsigned_reasoning_seen: bool,
}

enum StreamMode {
    Probe(ProbeState),
    Replay(std::vec::IntoIter<Bytes>, UpstreamStream),
    Pass(UpstreamStream),
    Finished,
}

struct RetryStreamState {
    mode: StreamMode,
    retry_request: RetryRequest,
    can_retry_request: CanRetryRequest,
    attempt: usize,
    diagnostics: StreamDiagnostics,
}

enum ProbeDecision {
    Continue,
    Ready,
    Retry(ProbeRetryReason),
}

#[derive(Clone, Copy)]
enum ProbeRetryReason {
    TransientUpstream,
    UnsignedReasoning,
}

impl ProbeRetryReason {
    fn description(self) -> &'static str {
        match self {
            Self::TransientUpstream => "上游返回瞬态异常",
            Self::UnsignedReasoning => "reasoning 流在签名前结束",
        }
    }
}

impl ProbeState {
    fn new(response: reqwest::Response) -> Self {
        Self {
            stream: Box::pin(response.bytes_stream()),
            decoder: EventStreamDecoder::new(),
            prefetched: Vec::new(),
            prefetched_bytes: 0,
            unsigned_reasoning_seen: false,
        }
    }

    fn push(&mut self, chunk: Bytes) -> anyhow::Result<ProbeDecision> {
        self.prefetched_bytes = self
            .prefetched_bytes
            .checked_add(chunk.len())
            .filter(|size| *size <= MAX_PREFETCH_BYTES)
            .context("Kiro 流首事件前的数据超过 1 MiB")?;
        self.decoder
            .feed(&chunk)
            .context("解码 Kiro 流首事件失败")?;
        self.prefetched.push(chunk);

        for frame in self.decoder.decode_iter() {
            let event = Event::from_frame(frame.context("解码 Kiro 流首帧失败")?)
                .context("解析 Kiro 流首事件失败")?;
            if let Event::ReasoningContent(reasoning) = &event {
                if reasoning
                    .redacted_content
                    .as_deref()
                    .is_some_and(|content| !content.is_empty())
                {
                    return if self.unsigned_reasoning_seen {
                        Ok(ProbeDecision::Retry(ProbeRetryReason::UnsignedReasoning))
                    } else {
                        Ok(ProbeDecision::Ready)
                    };
                }
                if reasoning
                    .signature
                    .as_deref()
                    .is_some_and(|signature| !signature.trim().is_empty())
                {
                    self.unsigned_reasoning_seen = false;
                    return Ok(ProbeDecision::Ready);
                }
                self.unsigned_reasoning_seen = true;
                continue;
            }
            if self.unsigned_reasoning_seen && is_terminal_start_event(&event) {
                return Ok(ProbeDecision::Retry(ProbeRetryReason::UnsignedReasoning));
            }
            if is_retryable_start_event(&event) {
                return Ok(ProbeDecision::Retry(ProbeRetryReason::TransientUpstream));
            }
            if is_terminal_start_event(&event) {
                return Ok(ProbeDecision::Ready);
            }
        }
        Ok(ProbeDecision::Continue)
    }

    fn into_replay(self) -> StreamMode {
        StreamMode::Replay(self.prefetched.into_iter(), self.stream)
    }
}

impl RetryStreamState {
    fn new(
        response: reqwest::Response,
        retry_request: RetryRequest,
        can_retry_request: CanRetryRequest,
    ) -> Self {
        let diagnostics = StreamDiagnostics::new(&response);
        Self {
            mode: StreamMode::Probe(ProbeState::new(response)),
            retry_request,
            can_retry_request,
            attempt: 1,
            diagnostics,
        }
    }

    async fn restart(&mut self) -> anyhow::Result<ProbeState> {
        tokio::time::sleep(KiroProvider::retry_delay(self.attempt - 1)).await;
        let response = (self.retry_request)().await?;
        self.attempt += 1;
        self.diagnostics.begin_attempt(&response);
        Ok(ProbeState::new(response))
    }

    fn can_retry(&self) -> bool {
        self.attempt < STREAM_START_ATTEMPTS && (self.can_retry_request)()
    }
}

impl KiroStreamResponse {
    pub(super) fn new(
        provider: Arc<KiroProvider>,
        initial_response: reqwest::Response,
        request_body: &str,
        fallback_request_body: Option<&str>,
        retry_budget: SharedStreamRetryBudget,
    ) -> Self {
        let request_body: Arc<str> = Arc::from(request_body);
        let fallback_request_body: Option<Arc<str>> = fallback_request_body.map(Arc::from);
        let retry_budget_for_request = retry_budget.clone();
        let retry_request = Arc::new(move || {
            let provider = provider.clone();
            let request_body = request_body.clone();
            let fallback_request_body = fallback_request_body.clone();
            let retry_budget = retry_budget_for_request.clone();
            Box::pin(async move {
                provider
                    .call_api_stream_attempt(
                        &request_body,
                        fallback_request_body.as_deref(),
                        retry_budget,
                    )
                    .await
            }) as RetryFuture
        });
        let can_retry_request = Arc::new(move || retry_budget.lock().can_retry());
        Self {
            initial_response,
            retry_request,
            can_retry_request,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_retry_request<F, Fut, E>(
        initial_response: reqwest::Response,
        request: F,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<reqwest::Response, E>> + Send + 'static,
        E: Into<anyhow::Error> + 'static,
    {
        let retry_request = Arc::new(move || {
            let future = request();
            Box::pin(async move { future.await.map_err(Into::into) }) as RetryFuture
        });
        Self {
            initial_response,
            retry_request,
            can_retry_request: Arc::new(|| true),
        }
    }

    pub fn bytes_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>> {
        let state = RetryStreamState::new(
            self.initial_response,
            self.retry_request,
            self.can_retry_request,
        );
        Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                let mode = std::mem::replace(&mut state.mode, StreamMode::Finished);
                match mode {
                    StreamMode::Probe(mut probe) => match probe.stream.next().await {
                        Some(Ok(chunk)) => {
                            state.diagnostics.record_chunk(chunk.len());
                            match probe.push(chunk) {
                                Ok(ProbeDecision::Continue) => {
                                    state.mode = StreamMode::Probe(probe)
                                }
                                Ok(ProbeDecision::Ready) => state.mode = probe.into_replay(),
                                Ok(ProbeDecision::Retry(reason)) if state.can_retry() => {
                                    tracing::warn!(
                                        attempt = state.attempt,
                                        max_attempts = STREAM_START_ATTEMPTS,
                                        reason = reason.description(),
                                        "Kiro 流在安全输出前失败，正在重试"
                                    );
                                    match state.restart().await {
                                        Ok(next) => state.mode = StreamMode::Probe(next),
                                        Err(error) => return Some((Err(error), state)),
                                    }
                                }
                                Ok(ProbeDecision::Retry(ProbeRetryReason::TransientUpstream)) => {
                                    state.mode = probe.into_replay()
                                }
                                Ok(ProbeDecision::Retry(ProbeRetryReason::UnsignedReasoning)) => {
                                    return Some((
                                        Err(BufferedStreamError::new(
                                            "Kiro reasoning stream ended without a signature before downstream exposure",
                                            None,
                                        )
                                        .into()),
                                        state,
                                    ));
                                }
                                Err(error) => return Some((Err(error), state)),
                            }
                        }
                        Some(Err(error)) if state.can_retry() => {
                            tracing::warn!(
                                attempt = state.attempt,
                                max_attempts = STREAM_START_ATTEMPTS,
                                error = %error,
                                "Kiro 流首事件读取失败，正在重试"
                            );
                            match state.restart().await {
                                Ok(next) => state.mode = StreamMode::Probe(next),
                                Err(error) => return Some((Err(error), state)),
                            }
                        }
                        Some(Err(error)) => {
                            let error = state.diagnostics.read_error(state.attempt, error);
                            let message = if probe.unsigned_reasoning_seen {
                                "Kiro reasoning stream ended without a signature before downstream exposure"
                            } else {
                                "Kiro stream failed before safe downstream exposure"
                            };
                            return Some((
                                Err(BufferedStreamError::new(message, Some(error.into())).into()),
                                state,
                            ));
                        }
                        None if state.can_retry() => match state.restart().await {
                            Ok(next) => state.mode = StreamMode::Probe(next),
                            Err(error) => return Some((Err(error), state)),
                        },
                        None => {
                            let source = match probe.decoder.finish() {
                                Ok(()) => None,
                                Err(error) => Some(StreamTruncatedError::new(error).into()),
                            };
                            let message = if probe.unsigned_reasoning_seen {
                                "Kiro reasoning stream ended without a signature before downstream exposure"
                            } else {
                                "Kiro stream ended before its first event"
                            };
                            return Some((
                                Err(BufferedStreamError::new(message, source).into()),
                                state,
                            ));
                        }
                    },
                    StreamMode::Replay(mut chunks, stream) => match chunks.next() {
                        Some(chunk) => {
                            state.mode = StreamMode::Replay(chunks, stream);
                            return Some((Ok(chunk), state));
                        }
                        None => state.mode = StreamMode::Pass(stream),
                    },
                    StreamMode::Pass(mut stream) => match stream.next().await {
                        Some(Ok(chunk)) => {
                            state.diagnostics.record_chunk(chunk.len());
                            state.mode = StreamMode::Pass(stream);
                            return Some((Ok(chunk), state));
                        }
                        Some(Err(error)) => {
                            let error = state.diagnostics.read_error(state.attempt, error);
                            return Some((Err(error.into()), state));
                        }
                        None => return None,
                    },
                    StreamMode::Finished => return None,
                }
            }
        }))
    }
}

fn is_retryable_start_event(event: &Event) -> bool {
    match event {
        Event::Error { error_message, .. } => is_exact_transient_message(error_message),
        Event::Exception { message, .. } => is_exact_transient_message(message),
        Event::AssistantResponse(_)
        | Event::ToolUse(_)
        | Event::Metering(())
        | Event::ContextUsage(_)
        | Event::ReasoningContent(_)
        | Event::Unknown {} => false,
    }
}

fn is_exact_transient_message(payload: &str) -> bool {
    if payload == TRANSIENT_UPSTREAM_ERROR {
        return true;
    }
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| value.get("message")?.as_str().map(str::to_owned))
        .is_some_and(|message| message == TRANSIENT_UPSTREAM_ERROR)
}

fn is_terminal_start_event(event: &Event) -> bool {
    matches!(
        event,
        Event::AssistantResponse(_)
            | Event::ToolUse(_)
            | Event::Error { .. }
            | Event::Exception { .. }
    )
}

#[cfg(test)]
#[path = "stream_response_tests.rs"]
mod tests;
