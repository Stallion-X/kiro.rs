use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use http::{HeaderMap, Version, header};
use uuid::Uuid;

use crate::kiro::parser::error::ParseError;

pub(super) struct StreamDiagnostics {
    stream_id: Uuid,
    started_at: Instant,
    last_read_at: Instant,
    response: ResponseMetadata,
    chunks_read: usize,
    bytes_read: usize,
}

struct ResponseMetadata {
    http_version: Version,
    content_length: Option<u64>,
    transfer_encoding: Option<String>,
    connection: Option<String>,
    aws_request_id: Option<String>,
}

#[derive(Debug)]
pub(crate) struct StreamReadError {
    pub(crate) stream_id: Uuid,
    pub(crate) attempt: usize,
    pub(crate) http_version: Version,
    pub(crate) content_length: Option<u64>,
    pub(crate) transfer_encoding: Option<String>,
    pub(crate) connection: Option<String>,
    pub(crate) aws_request_id: Option<String>,
    pub(crate) elapsed: Duration,
    pub(crate) idle: Duration,
    pub(crate) chunks_read: usize,
    pub(crate) bytes_read: usize,
    source: reqwest::Error,
}

#[derive(Debug)]
pub(crate) struct StreamTruncatedError {
    source: ParseError,
}

#[derive(Debug)]
pub(crate) struct BufferedStreamError {
    message: &'static str,
    source: Option<anyhow::Error>,
}

impl StreamDiagnostics {
    pub(super) fn new(response: &reqwest::Response) -> Self {
        let now = Instant::now();
        Self {
            stream_id: Uuid::new_v4(),
            started_at: now,
            last_read_at: now,
            response: ResponseMetadata::from_response(response),
            chunks_read: 0,
            bytes_read: 0,
        }
    }

    pub(super) fn begin_attempt(&mut self, response: &reqwest::Response) {
        self.last_read_at = Instant::now();
        self.response = ResponseMetadata::from_response(response);
        self.chunks_read = 0;
        self.bytes_read = 0;
    }

    pub(super) fn record_chunk(&mut self, bytes: usize) {
        self.last_read_at = Instant::now();
        self.chunks_read = self.chunks_read.saturating_add(1);
        self.bytes_read = self.bytes_read.saturating_add(bytes);
    }

    pub(super) fn read_error(&self, attempt: usize, source: reqwest::Error) -> StreamReadError {
        let now = Instant::now();
        StreamReadError {
            stream_id: self.stream_id,
            attempt,
            http_version: self.response.http_version,
            content_length: self.response.content_length,
            transfer_encoding: self.response.transfer_encoding.clone(),
            connection: self.response.connection.clone(),
            aws_request_id: self.response.aws_request_id.clone(),
            elapsed: now.duration_since(self.started_at),
            idle: now.duration_since(self.last_read_at),
            chunks_read: self.chunks_read,
            bytes_read: self.bytes_read,
            source,
        }
    }
}

impl ResponseMetadata {
    fn from_response(response: &reqwest::Response) -> Self {
        Self {
            http_version: response.version(),
            content_length: response.content_length(),
            transfer_encoding: header_value(response.headers(), header::TRANSFER_ENCODING),
            connection: header_value(response.headers(), header::CONNECTION),
            aws_request_id: header_value(response.headers(), "x-amzn-requestid")
                .or_else(|| header_value(response.headers(), "x-amz-request-id")),
        }
    }
}

impl StreamReadError {
    pub(crate) fn source_chain(&self) -> String {
        let mut chain = Vec::new();
        let mut current: Option<&(dyn Error + 'static)> = Some(&self.source);
        while let Some(error) = current {
            chain.push(error.to_string());
            current = error.source();
        }
        chain.join(": ")
    }

    pub(crate) fn is_timeout(&self) -> bool {
        self.source.is_timeout()
    }

    pub(crate) fn is_connect(&self) -> bool {
        self.source.is_connect()
    }

    pub(crate) fn is_body(&self) -> bool {
        self.source.is_body()
    }

    pub(crate) fn is_decode(&self) -> bool {
        self.source.is_decode()
    }
}

impl StreamTruncatedError {
    pub(super) fn new(source: ParseError) -> Self {
        Self { source }
    }
}

impl BufferedStreamError {
    pub(super) fn new(message: &'static str, source: Option<anyhow::Error>) -> Self {
        Self { message, source }
    }
}

impl fmt::Display for StreamReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl Error for StreamReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl fmt::Display for StreamTruncatedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Kiro response stream was truncated: {}",
            self.source
        )
    }
}

impl Error for StreamTruncatedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl fmt::Display for BufferedStreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)?;
        if let Some(source) = &self.source {
            write!(formatter, ": {source:#}")?;
        }
        Ok(())
    }
}

impl Error for BufferedStreamError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn Error + 'static))
    }
}

fn header_value(headers: &HeaderMap, name: impl http::header::AsHeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}
