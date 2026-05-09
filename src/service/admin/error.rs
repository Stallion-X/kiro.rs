//! Admin API 错误类型定义

use std::fmt;

use axum::http::StatusCode;

use crate::domain::error::{ConfigError, RefreshError};
use crate::interface::http::admin::dto::AdminErrorResponse;
use crate::service::credential_pool::AdminPoolError;

/// Admin 服务错误类型
///
/// 每个变体对应明确的语义，`From<AdminPoolError>` 实现 1-1 结构化映射，
/// 对外 HTTP 响应通过 `error_type` 字段传递精确的错误类型。
#[derive(Debug)]
pub enum AdminServiceError {
    /// 凭据不存在
    NotFound { id: u64 },

    /// 上游服务调用失败（网络等字符串错误）
    UpstreamError(String),

    /// Token 刷新失败（保留结构化 RefreshError）
    RefreshError(RefreshError),

    /// 上游 HTTP 非 2xx 响应
    UpstreamHttp { status: u16, body: String },

    /// 配置持久化失败（保留结构化 ConfigError）
    ConfigError(ConfigError),

    /// 凭据因配置无效被禁用
    DisabledByInvalidConfig(u64),

    /// 凭据已存在（refreshToken 重复）
    DuplicateRefreshToken,

    /// 凭据已存在（kiroApiKey 重复）
    DuplicateApiKey,

    /// refreshToken 已被截断，可能被 Kiro IDE 故意修改
    TruncatedRefreshToken(usize),

    /// refreshToken 为空
    EmptyRefreshToken,

    /// 缺少 refreshToken
    MissingRefreshToken,

    /// kiroApiKey 为空
    EmptyApiKey,

    /// 缺少 kiroApiKey
    MissingApiKey,

    /// 只能删除已禁用的凭据
    NotDisabled(u64),

    /// API Key 凭据不支持刷新 Token
    ApiKeyNotRefreshable,

    /// 通用请求参数校验失败（非凭据本身的问题）
    InvalidRequest(String),
}

impl fmt::Display for AdminServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdminServiceError::NotFound { id } => write!(f, "凭据不存在: {id}"),
            AdminServiceError::UpstreamError(msg) => write!(f, "上游服务错误: {msg}"),
            AdminServiceError::RefreshError(e) => write!(f, "Token 刷新失败: {e}"),
            AdminServiceError::UpstreamHttp { status, body } => {
                write!(f, "上游 HTTP {status}: {body}")
            }
            AdminServiceError::ConfigError(e) => write!(f, "配置持久化失败: {e}"),
            AdminServiceError::DisabledByInvalidConfig(id) => {
                write!(
                    f,
                    "凭据 #{id} 因配置无效被禁用，请修正配置后重启服务"
                )
            }
            AdminServiceError::DuplicateRefreshToken => {
                write!(f, "凭据已存在（refreshToken 重复）")
            }
            AdminServiceError::DuplicateApiKey => {
                write!(f, "凭据已存在（kiroApiKey 重复）")
            }
            AdminServiceError::TruncatedRefreshToken(len) => {
                write!(
                    f,
                    "refreshToken 已被截断（长度: {len} 字符）。\
                     这通常是 Kiro IDE 为了防止凭证被第三方工具使用而故意截断的。"
                )
            }
            AdminServiceError::EmptyRefreshToken => write!(f, "refreshToken 为空"),
            AdminServiceError::MissingRefreshToken => write!(f, "缺少 refreshToken"),
            AdminServiceError::EmptyApiKey => write!(f, "kiroApiKey 为空"),
            AdminServiceError::MissingApiKey => write!(f, "缺少 kiroApiKey"),
            AdminServiceError::NotDisabled(id) => {
                write!(f, "只能删除已禁用的凭据（请先禁用凭据 #{id}）")
            }
            AdminServiceError::ApiKeyNotRefreshable => {
                write!(f, "API Key 凭据不支持刷新 Token")
            }
            AdminServiceError::InvalidRequest(msg) => write!(f, "请求无效: {msg}"),
        }
    }
}

impl std::error::Error for AdminServiceError {}

impl AdminServiceError {
    /// 获取对应的 HTTP 状态码
    pub fn status_code(&self) -> StatusCode {
        match self {
            AdminServiceError::NotFound { .. } => StatusCode::NOT_FOUND,
            AdminServiceError::UpstreamError(_)
            | AdminServiceError::RefreshError(_)
            | AdminServiceError::UpstreamHttp { .. } => {
                StatusCode::BAD_GATEWAY
            }
            AdminServiceError::ConfigError(_)
            | AdminServiceError::DisabledByInvalidConfig(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AdminServiceError::DuplicateRefreshToken
            | AdminServiceError::DuplicateApiKey
            | AdminServiceError::TruncatedRefreshToken(_)
            | AdminServiceError::EmptyRefreshToken
            | AdminServiceError::MissingRefreshToken
            | AdminServiceError::EmptyApiKey
            | AdminServiceError::MissingApiKey
            | AdminServiceError::NotDisabled(_)
            | AdminServiceError::ApiKeyNotRefreshable
            | AdminServiceError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        }
    }

    /// 转换为 API 错误响应
    ///
    /// `error_type` 字段对外为稳定 API 契约，调用方可程序化区分错误类型。
    pub fn into_response(self) -> AdminErrorResponse {
        let msg = self.to_string();
        let error_type = match &self {
            AdminServiceError::NotFound { .. } => "not_found",
            AdminServiceError::UpstreamError(_) => "upstream_error",
            AdminServiceError::RefreshError(_) => "refresh_error",
            AdminServiceError::UpstreamHttp { .. } => "upstream_http_error",
            AdminServiceError::ConfigError(_) => "config_error",
            AdminServiceError::DisabledByInvalidConfig(_) => "disabled_by_invalid_config",
            AdminServiceError::DuplicateRefreshToken => "duplicate_refresh_token",
            AdminServiceError::DuplicateApiKey => "duplicate_api_key",
            AdminServiceError::TruncatedRefreshToken(_) => "truncated_refresh_token",
            AdminServiceError::EmptyRefreshToken => "empty_refresh_token",
            AdminServiceError::MissingRefreshToken => "missing_refresh_token",
            AdminServiceError::EmptyApiKey => "empty_api_key",
            AdminServiceError::MissingApiKey => "missing_api_key",
            AdminServiceError::NotDisabled(_) => "not_disabled",
            AdminServiceError::ApiKeyNotRefreshable => "api_key_not_refreshable",
            AdminServiceError::InvalidRequest(_) => "invalid_request",
        };
        AdminErrorResponse::new(error_type, msg)
    }
}

/// AdminPoolError → AdminServiceError 结构化 1-1 映射
impl From<AdminPoolError> for AdminServiceError {
    fn from(e: AdminPoolError) -> Self {
        match e {
            AdminPoolError::NotFound(id) => AdminServiceError::NotFound { id },
            AdminPoolError::DuplicateRefreshToken => AdminServiceError::DuplicateRefreshToken,
            AdminPoolError::DuplicateApiKey => AdminServiceError::DuplicateApiKey,
            AdminPoolError::TruncatedRefreshToken(len) => {
                AdminServiceError::TruncatedRefreshToken(len)
            }
            AdminPoolError::EmptyRefreshToken => AdminServiceError::EmptyRefreshToken,
            AdminPoolError::EmptyApiKey => AdminServiceError::EmptyApiKey,
            AdminPoolError::MissingRefreshToken => AdminServiceError::MissingRefreshToken,
            AdminPoolError::MissingApiKey => AdminServiceError::MissingApiKey,
            AdminPoolError::NotDisabled(id) => AdminServiceError::NotDisabled(id),
            AdminPoolError::ApiKeyNotRefreshable => AdminServiceError::ApiKeyNotRefreshable,
            AdminPoolError::Refresh(e) => AdminServiceError::RefreshError(e),
            AdminPoolError::UpstreamHttp { status, body } => {
                AdminServiceError::UpstreamHttp { status, body }
            }
            AdminPoolError::Network(e) => AdminServiceError::UpstreamError(e),
            AdminPoolError::Config(e) => AdminServiceError::ConfigError(e),
            AdminPoolError::DisabledByInvalidConfig(id) => {
                AdminServiceError::DisabledByInvalidConfig(id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::error::{ConfigError, RefreshError};

    // ── From<AdminPoolError> 映射 ──────────────────────────────────────

    #[test]
    fn from_admin_pool_error_not_found() {
        let e: AdminServiceError = AdminPoolError::NotFound(42).into();
        assert!(matches!(e, AdminServiceError::NotFound { id: 42 }));
    }

    #[test]
    fn from_admin_pool_error_duplicate_refresh_token() {
        let e: AdminServiceError = AdminPoolError::DuplicateRefreshToken.into();
        assert!(matches!(e, AdminServiceError::DuplicateRefreshToken));
    }

    #[test]
    fn from_admin_pool_error_duplicate_api_key() {
        let e: AdminServiceError = AdminPoolError::DuplicateApiKey.into();
        assert!(matches!(e, AdminServiceError::DuplicateApiKey));
    }

    #[test]
    fn from_admin_pool_error_truncated_refresh_token() {
        let e: AdminServiceError = AdminPoolError::TruncatedRefreshToken(32).into();
        assert!(matches!(e, AdminServiceError::TruncatedRefreshToken(32)));
    }

    #[test]
    fn from_admin_pool_error_empty_refresh_token() {
        let e: AdminServiceError = AdminPoolError::EmptyRefreshToken.into();
        assert!(matches!(e, AdminServiceError::EmptyRefreshToken));
    }

    #[test]
    fn from_admin_pool_error_empty_api_key() {
        let e: AdminServiceError = AdminPoolError::EmptyApiKey.into();
        assert!(matches!(e, AdminServiceError::EmptyApiKey));
    }

    #[test]
    fn from_admin_pool_error_missing_refresh_token() {
        let e: AdminServiceError = AdminPoolError::MissingRefreshToken.into();
        assert!(matches!(e, AdminServiceError::MissingRefreshToken));
    }

    #[test]
    fn from_admin_pool_error_missing_api_key() {
        let e: AdminServiceError = AdminPoolError::MissingApiKey.into();
        assert!(matches!(e, AdminServiceError::MissingApiKey));
    }

    #[test]
    fn from_admin_pool_error_not_disabled() {
        let e: AdminServiceError = AdminPoolError::NotDisabled(7).into();
        assert!(matches!(e, AdminServiceError::NotDisabled(7)));
    }

    #[test]
    fn from_admin_pool_error_api_key_not_refreshable() {
        let e: AdminServiceError = AdminPoolError::ApiKeyNotRefreshable.into();
        assert!(matches!(e, AdminServiceError::ApiKeyNotRefreshable));
    }

    #[test]
    fn from_admin_pool_error_disabled_by_invalid_config() {
        let e: AdminServiceError = AdminPoolError::DisabledByInvalidConfig(99).into();
        assert!(matches!(e, AdminServiceError::DisabledByInvalidConfig(99)));
    }

    #[test]
    fn from_admin_pool_error_refresh_preserves_structure() {
        let e: AdminServiceError = AdminPoolError::Refresh(RefreshError::TokenInvalid).into();
        assert!(matches!(e, AdminServiceError::RefreshError(RefreshError::TokenInvalid)));
    }

    #[test]
    fn from_admin_pool_error_config_preserves_structure() {
        let e: AdminServiceError =
            AdminPoolError::Config(ConfigError::Validation("bad field".into())).into();
        assert!(matches!(e, AdminServiceError::ConfigError(_)));
    }

    #[test]
    fn from_admin_pool_error_upstream_http() {
        let e: AdminServiceError = AdminPoolError::UpstreamHttp {
            status: 502,
            body: "gateway timeout".into(),
        }
        .into();
        assert!(matches!(e, AdminServiceError::UpstreamHttp {
            status: 502,
            body,
        } if body == "gateway timeout"));
    }

    #[test]
    fn from_admin_pool_error_network() {
        let e: AdminServiceError =
            AdminPoolError::Network("connection refused".into()).into();
        assert!(matches!(e, AdminServiceError::UpstreamError(msg) if msg == "connection refused"));
    }

    // ── status_code() ──────────────────────────────────────────────────

    #[test]
    fn status_code_not_found() {
        assert_eq!(
            AdminServiceError::NotFound { id: 1 }.status_code(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn status_code_upstream_error() {
        assert_eq!(
            AdminServiceError::UpstreamError("oops".into()).status_code(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn status_code_refresh_error() {
        assert_eq!(
            AdminServiceError::RefreshError(RefreshError::TokenInvalid).status_code(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn status_code_upstream_http() {
        assert_eq!(
            AdminServiceError::UpstreamHttp { status: 503, body: String::new() }.status_code(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn status_code_config_error() {
        assert_eq!(
            AdminServiceError::ConfigError(ConfigError::Validation("x".into())).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn status_code_disabled_by_invalid_config() {
        assert_eq!(
            AdminServiceError::DisabledByInvalidConfig(1).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn status_code_credential_validation() {
        // 所有凭据校验错误都返回 400
        let variants: &[AdminServiceError] = &[
            AdminServiceError::DuplicateRefreshToken,
            AdminServiceError::DuplicateApiKey,
            AdminServiceError::TruncatedRefreshToken(12),
            AdminServiceError::EmptyRefreshToken,
            AdminServiceError::MissingRefreshToken,
            AdminServiceError::EmptyApiKey,
            AdminServiceError::MissingApiKey,
            AdminServiceError::NotDisabled(3),
            AdminServiceError::ApiKeyNotRefreshable,
            AdminServiceError::InvalidRequest("bad param".into()),
        ];
        for v in variants {
            assert_eq!(
                v.status_code(),
                StatusCode::BAD_REQUEST,
                "variant {v:?} should be 400"
            );
        }
    }

    // ── into_response() ────────────────────────────────────────────────

    #[test]
    fn into_response_not_found() {
        let r = AdminServiceError::NotFound { id: 42 }.into_response();
        assert_eq!(r.error.error_type, "not_found");
        assert!(r.error.message.contains("42"));
    }

    #[test]
    fn into_response_upstream_error() {
        let r =
            AdminServiceError::UpstreamError("connection reset".into()).into_response();
        assert_eq!(r.error.error_type, "upstream_error");
        assert!(r.error.message.contains("connection reset"));
    }

    #[test]
    fn into_response_refresh_error() {
        let r =
            AdminServiceError::RefreshError(RefreshError::TokenInvalid).into_response();
        assert_eq!(r.error.error_type, "refresh_error");
        assert!(r.error.message.contains("invalid_grant"));
    }

    #[test]
    fn into_response_upstream_http() {
        let r = AdminServiceError::UpstreamHttp { status: 502, body: "bad gateway".into() }
            .into_response();
        assert_eq!(r.error.error_type, "upstream_http_error");
        assert!(r.error.message.contains("502"));
    }

    #[test]
    fn into_response_config_error() {
        let r = AdminServiceError::ConfigError(ConfigError::Validation("bad".into()))
            .into_response();
        assert_eq!(r.error.error_type, "config_error");
        assert!(r.error.message.contains("bad"));
    }

    #[test]
    fn into_response_disabled_by_invalid_config() {
        let r = AdminServiceError::DisabledByInvalidConfig(7).into_response();
        assert_eq!(r.error.error_type, "disabled_by_invalid_config");
        assert!(r.error.message.contains('7'));
    }

    #[test]
    fn into_response_duplicate_refresh_token() {
        let r = AdminServiceError::DuplicateRefreshToken.into_response();
        assert_eq!(r.error.error_type, "duplicate_refresh_token");
        assert!(r.error.message.contains("refreshToken"));
    }

    #[test]
    fn into_response_invalid_request() {
        let r = AdminServiceError::InvalidRequest("bad param".into()).into_response();
        assert_eq!(r.error.error_type, "invalid_request");
        assert!(r.error.message.contains("bad param"));
    }

    #[test]
    fn into_response_all_error_types_unique() {
        let responses: Vec<_> = vec![
            AdminServiceError::NotFound { id: 1 }.into_response(),
            AdminServiceError::UpstreamError("x".into()).into_response(),
            AdminServiceError::RefreshError(RefreshError::TokenInvalid).into_response(),
            AdminServiceError::UpstreamHttp { status: 500, body: "x".into() }.into_response(),
            AdminServiceError::ConfigError(ConfigError::Validation("x".into())).into_response(),
            AdminServiceError::DisabledByInvalidConfig(1).into_response(),
            AdminServiceError::DuplicateRefreshToken.into_response(),
            AdminServiceError::DuplicateApiKey.into_response(),
            AdminServiceError::TruncatedRefreshToken(1).into_response(),
            AdminServiceError::EmptyRefreshToken.into_response(),
            AdminServiceError::MissingRefreshToken.into_response(),
            AdminServiceError::EmptyApiKey.into_response(),
            AdminServiceError::MissingApiKey.into_response(),
            AdminServiceError::NotDisabled(1).into_response(),
            AdminServiceError::ApiKeyNotRefreshable.into_response(),
            AdminServiceError::InvalidRequest("x".into()).into_response(),
        ];
        let types: Vec<_> = responses.iter().map(|r| &r.error.error_type).collect();
        // 16 个变体，16 个唯一的 error_type
        let unique: std::collections::HashSet<_> = types.iter().collect();
        assert_eq!(unique.len(), types.len(), "duplicate error_type found");
    }
}
