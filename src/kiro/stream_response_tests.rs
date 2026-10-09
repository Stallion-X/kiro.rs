use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn event_stream_frame(headers: &[(&str, &str)], payload: &str) -> Vec<u8> {
    let mut encoded_headers = Vec::new();
    for (name, value) in headers {
        encoded_headers.push(name.len() as u8);
        encoded_headers.extend_from_slice(name.as_bytes());
        encoded_headers.push(7);
        encoded_headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
        encoded_headers.extend_from_slice(value.as_bytes());
    }

    let total_length = 16 + encoded_headers.len() + payload.len();
    let mut frame = Vec::with_capacity(total_length);
    frame.extend_from_slice(&(total_length as u32).to_be_bytes());
    frame.extend_from_slice(&(encoded_headers.len() as u32).to_be_bytes());
    let prelude_crc = crate::kiro::parser::crc::crc32(&frame);
    frame.extend_from_slice(&prelude_crc.to_be_bytes());
    frame.extend_from_slice(&encoded_headers);
    frame.extend_from_slice(payload.as_bytes());
    let message_crc = crate::kiro::parser::crc::crc32(&frame);
    frame.extend_from_slice(&message_crc.to_be_bytes());
    frame
}

fn exception_frame() -> Vec<u8> {
    event_stream_frame(
        &[(":message-type", "exception"), (":exception-type", "error")],
        &format!(r#"{{"message":"{TRANSIENT_UPSTREAM_ERROR}"}}"#),
    )
}

fn assistant_frame() -> Vec<u8> {
    event_stream_frame(
        &[
            (":message-type", "event"),
            (":event-type", "assistantResponseEvent"),
        ],
        r#"{"content":"recovered"}"#,
    )
}

fn reasoning_frame(text: &str, signature: Option<&str>) -> Vec<u8> {
    let payload = serde_json::json!({
        "text": text,
        "signature": signature,
    });
    event_stream_frame(
        &[
            (":message-type", "event"),
            (":event-type", "reasoningContentEvent"),
        ],
        &payload.to_string(),
    )
}

fn redacted_reasoning_frame(data: &str) -> Vec<u8> {
    let payload = serde_json::json!({"redactedContent": data});
    event_stream_frame(
        &[
            (":message-type", "event"),
            (":event-type", "reasoningContentEvent"),
        ],
        &payload.to_string(),
    )
}

async fn response_server<B>(body: B) -> (String, tokio::task::JoinHandle<()>)
where
    B: Fn(usize) -> Vec<u8> + Clone + Send + Sync + 'static,
{
    let attempts = Arc::new(AtomicUsize::new(0));
    let app = axum::Router::new().route(
        "/",
        axum::routing::get(move || {
            let attempts = attempts.clone();
            let body = body.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                axum::body::Body::from(body(attempt))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });
    (format!("http://{address}/"), server)
}

async fn truncated_response_server(
    body: Vec<u8>,
) -> (
    String,
    usize,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let declared_length = body.len() + 64;
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first_socket, _) = listener.accept().await.expect("client should connect");
        let mut request = [0_u8; 4096];
        let _ = first_socket
            .read(&mut request)
            .await
            .expect("first request should be readable");
        let first_body = exception_frame();
        let first_headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            first_body.len()
        );
        first_socket
            .write_all(first_headers.as_bytes())
            .await
            .expect("first headers should be written");
        first_socket
            .write_all(&first_body)
            .await
            .expect("first body should be written");
        drop(first_socket);

        let (mut socket, _) = listener.accept().await.expect("client should connect");
        let _ = socket
            .read(&mut request)
            .await
            .expect("retry request should be readable");
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {declared_length}\r\nConnection: close\r\n\r\n"
        );
        socket
            .write_all(headers.as_bytes())
            .await
            .expect("headers should be written");
        socket
            .write_all(&body)
            .await
            .expect("partial body should be written");
        close_rx
            .await
            .expect("test should request connection close");
    });
    (
        format!("http://{address}/"),
        declared_length,
        close_tx,
        server,
    )
}

async fn truncated_reasoning_then_recovery_server(
    recovery: Vec<u8>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (mut first_socket, _) = listener.accept().await.expect("client should connect");
        let _ = first_socket
            .read(&mut request)
            .await
            .expect("first request should be readable");
        let incomplete = reasoning_frame("incomplete reasoning", None);
        let first_headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            incomplete.len() + 64
        );
        first_socket
            .write_all(first_headers.as_bytes())
            .await
            .expect("first headers should be written");
        first_socket
            .write_all(&incomplete)
            .await
            .expect("incomplete reasoning should be written");
        drop(first_socket);

        let (mut second_socket, _) = listener.accept().await.expect("client should retry");
        let _ = second_socket
            .read(&mut request)
            .await
            .expect("retry request should be readable");
        let second_headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            recovery.len()
        );
        second_socket
            .write_all(second_headers.as_bytes())
            .await
            .expect("retry headers should be written");
        second_socket
            .write_all(&recovery)
            .await
            .expect("recovery body should be written");
    });
    (format!("http://{address}/"), server)
}

async fn collect(response: KiroStreamResponse) -> anyhow::Result<Vec<u8>> {
    let mut stream = Box::pin(response.bytes_stream());
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk?);
    }
    Ok(body)
}

#[test]
fn transient_exception_matching_is_exact() {
    // Given: exact, wrapped, and merely containing variants of the upstream message.
    let exact = Event::Exception {
        exception_type: "error".to_string(),
        message: format!(r#"{{"message":"{TRANSIENT_UPSTREAM_ERROR}"}}"#),
    };
    let longer = Event::Exception {
        exception_type: "error".to_string(),
        message: format!(r#"{{"message":"prefix {TRANSIENT_UPSTREAM_ERROR}"}}"#),
    };

    // When: stream-start retry eligibility is evaluated.
    let exact_retryable = is_retryable_start_event(&exact);
    let longer_retryable = is_retryable_start_event(&longer);

    // Then: only the protocol's exact transient message is retried.
    assert!(exact_retryable);
    assert!(!longer_retryable);
}

#[tokio::test]
async fn transient_first_event_is_retried_before_response_is_exposed() {
    // Given: the first HTTP stream contains metadata plus the exception, then a valid response.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let metadata = event_stream_frame(
        &[(":message-type", "event"), (":event-type", "unknown")],
        "{}",
    );
    let (url, server) = response_server(move |attempt| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            [metadata.clone(), exception_frame()].concat()
        } else {
            assistant_frame()
        }
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    // When: the downstream begins consuming the response body.
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let actual = collect(response)
        .await
        .expect("the second stream should be exposed");
    server.abort();

    // Then: probing is lazy, retries once, and replays only the recovered bytes.
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(actual, assistant_frame());
}

#[tokio::test]
async fn unsigned_reasoning_is_retried_before_response_is_exposed() {
    // Given: one attempt moves from reasoning to answer without the required signature.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let recovered = [
        reasoning_frame("complete reasoning", Some("sig-valid")),
        assistant_frame(),
    ]
    .concat();
    let expected = recovered.clone();
    let (url, server) = response_server(move |attempt| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            [
                reasoning_frame("incomplete reasoning", None),
                assistant_frame(),
            ]
            .concat()
        } else {
            recovered.clone()
        }
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    // When: the downstream consumes the stream.
    let actual = collect(response)
        .await
        .expect("the signed retry should be exposed");
    server.abort();

    // Then: no bytes from the invalid attempt escape to the Anthropic client.
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn signed_reasoning_is_exposed_without_retry() {
    // Given: the first attempt supplies the required signature.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let expected = [
        reasoning_frame("reasoning", Some("sig-valid")),
        assistant_frame(),
    ]
    .concat();
    let body = expected.clone();
    let (url, server) = response_server(move |_| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        body.clone()
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    // When: the downstream consumes the stream.
    let actual = collect(response)
        .await
        .expect("the signed stream should be exposed");
    server.abort();

    // Then: valid reasoning is replayed unchanged from the first attempt.
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn separately_delivered_reasoning_signature_is_exposed_without_retry() {
    // Given: Kiro sends reasoning text and its signature in separate events.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let expected = [
        reasoning_frame("reasoning", None),
        reasoning_frame("", Some("sig-valid")),
        assistant_frame(),
    ]
    .concat();
    let body = expected.clone();
    let (url, server) = response_server(move |_| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        body.clone()
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    let actual = collect(response)
        .await
        .expect("the complete reasoning block should be exposed");
    server.abort();

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn redacted_reasoning_is_exposed_without_signature() {
    // Given: redacted reasoning is a complete block and does not carry a signature.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let expected = [redacted_reasoning_frame("opaque"), assistant_frame()].concat();
    let body = expected.clone();
    let (url, server) = response_server(move |_| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        body.clone()
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    let actual = collect(response)
        .await
        .expect("redacted reasoning should remain valid");
    server.abort();

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn repeated_unsigned_reasoning_fails_without_exposing_partial_bytes() {
    // Given: every attempt transitions to answer content without a reasoning signature.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let (url, server) = response_server(move |_| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        [reasoning_frame("incomplete", None), assistant_frame()].concat()
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    let mut stream = response.bytes_stream();
    let error = stream
        .next()
        .await
        .expect("the exhausted stream should emit an error")
        .expect_err("unsigned reasoning must remain a visible failure after retries");
    server.abort();

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(error.downcast_ref::<BufferedStreamError>().is_some());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn truncated_unsigned_reasoning_retries_without_leaking_partial_bytes() {
    // Given: the transport drops after unsigned reasoning, matching an unexpected body EOF.
    let expected = [
        reasoning_frame("reasoning", Some("sig-valid")),
        assistant_frame(),
    ]
    .concat();
    let (url, server) = truncated_reasoning_then_recovery_server(expected.clone()).await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    // When: the first upstream body ends before its declared length.
    let actual = collect(response)
        .await
        .expect("the signed retry should recover the stream");
    server.await.expect("test server should exit");

    // Then: only the complete signed attempt reaches the Anthropic stream converter.
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn repeated_transient_exception_is_exposed_after_three_attempts() {
    // Given: every upstream attempt returns the same transient exception.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let (url, server) = response_server(move |_| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        exception_frame()
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    // When: the retry budget is exhausted.
    let actual = collect(response)
        .await
        .expect("the final exception should remain readable");
    server.abort();

    // Then: the third exception is surfaced without a fourth request.
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(actual, exception_frame());
}

#[tokio::test]
async fn empty_stream_is_retried_then_reported_as_an_error() {
    // Given: all upstream attempts end cleanly without any EventStream event.
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let (url, server) = response_server(move |_| {
        server_attempts.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    })
    .await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    // When: no attempt produces a terminal event.
    let error = collect(response)
        .await
        .expect_err("an empty stream must not look like success");
    server.abort();

    // Then: the retry budget is used and EOF remains visible.
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(error.to_string().contains("before its first event"));
}

#[tokio::test]
async fn truncated_http_body_preserves_transport_diagnostics() {
    // Given: HTTP declares a body longer than the valid first event it sends.
    let body = assistant_frame();
    let body_length = body.len();
    let (url, declared_length, close_tx, server) = truncated_response_server(body).await;
    let initial = reqwest::get(url.clone())
        .await
        .expect("initial response should arrive");
    let retry_url = url.clone();
    let response =
        KiroStreamResponse::with_retry_request(initial, move || reqwest::get(retry_url.clone()));

    // When: reqwest receives the valid first event, then observes an early EOF.
    let mut stream = response.bytes_stream();
    let first_chunk = stream
        .next()
        .await
        .expect("the first chunk should arrive")
        .expect("the first chunk should be valid");
    close_tx.send(()).expect("server should still be waiting");
    let error = stream
        .next()
        .await
        .expect("the transport error should arrive")
        .expect_err("the truncated body must fail");
    server.await.expect("test server should exit");

    // Then: the transport failure retains correlation, framing, and read progress.
    let diagnostic = error
        .downcast_ref::<StreamReadError>()
        .expect("transport diagnostics should be preserved");
    assert_eq!(first_chunk.len(), body_length);
    assert_eq!(diagnostic.content_length, Some(declared_length as u64));
    assert_eq!(diagnostic.bytes_read, body_length);
    assert_eq!(diagnostic.http_version, reqwest::Version::HTTP_11);
    assert!(!diagnostic.stream_id.is_nil());
    assert!(!diagnostic.source_chain().is_empty());
    assert!(!diagnostic.is_timeout());
}
