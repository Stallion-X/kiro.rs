//! Kiro API Provider
//!
//! 核心组件，负责与 Kiro API 通信
//! 支持流式和非流式请求
//! 支持多凭据故障转移和重试
//! 支持按凭据级 endpoint 切换不同 Kiro API 端点

use anyhow::Context;
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::http_client::{ProxyConfig, build_client, build_streaming_client};
use crate::kiro::endpoint::{KiroEndpoint, RequestContext};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::MultiTokenManager;
use crate::model::config::TlsBackend;
use parking_lot::Mutex;

/// 每个凭据的最大重试次数
const MAX_RETRIES_PER_CREDENTIAL: usize = 3;

/// 总重试次数硬上限（避免无限重试）
const MAX_TOTAL_RETRIES: usize = 9;
pub(super) const STREAM_START_ATTEMPTS: usize = 3;
const STREAM_ATTEMPT_RETRY_LIMIT: usize = MAX_TOTAL_RETRIES / STREAM_START_ATTEMPTS;

/// 链路级重试次数（请求根本没送达上游：DNS / 连接 / TLS / 超时）
///
/// 与凭据预算和流式预算解耦：链路故障既不能归咎于凭据（不切换、不禁用），
/// 也不是上游在流里报错（不消耗流式重启预算）。基础预算只有 3 次尝试、
/// 退避 200/400ms，窗口不足 1 秒，扛不住任何一次网络抖动，因此单独补一段窗口。
const LINK_RETRY_LIMIT: usize = 4;

/// 链路级重试退避基数（1s / 2s / 4s / 8s，合计约 15 秒）
const LINK_RETRY_BASE_MS: u64 = 1_000;
const LINK_RETRY_MAX_DELAY_MS: u64 = 8_000;

pub(super) type SharedStreamRetryBudget = Arc<Mutex<StreamRetryBudget>>;

pub(super) struct StreamRetryBudget {
    total_limit: usize,
    total_attempts: usize,
    attempts_by_credential: HashMap<u64, usize>,
}

impl StreamRetryBudget {
    fn new(total_credentials: usize) -> Self {
        Self {
            total_limit: (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES),
            total_attempts: 0,
            attempts_by_credential: HashMap::new(),
        }
    }

    fn try_record(&mut self, credential_id: u64) -> bool {
        let credential_attempts = self
            .attempts_by_credential
            .entry(credential_id)
            .or_default();
        if self.total_attempts >= self.total_limit
            || *credential_attempts >= MAX_RETRIES_PER_CREDENTIAL
        {
            return false;
        }
        self.total_attempts += 1;
        *credential_attempts += 1;
        true
    }

    fn excluded_credentials(&self) -> HashSet<u64> {
        self.attempts_by_credential
            .iter()
            .filter_map(|(&id, &attempts)| (attempts >= MAX_RETRIES_PER_CREDENTIAL).then_some(id))
            .collect()
    }

    /// 退还一次已记录的尝试
    ///
    /// 请求根本没送达上游时（链路故障）不该消耗流式重启预算：否则 3 次链路抖动
    /// 就会把凭据拉进排除集，后续链路级重试拿到「预算已耗尽」而不是继续发请求。
    fn release(&mut self, credential_id: u64) {
        self.total_attempts = self.total_attempts.saturating_sub(1);
        if let Some(attempts) = self.attempts_by_credential.get_mut(&credential_id) {
            *attempts = attempts.saturating_sub(1);
        }
    }

    pub(super) fn can_retry(&self) -> bool {
        self.total_attempts < self.total_limit
    }

    #[cfg(test)]
    fn total_attempts(&self) -> usize {
        self.total_attempts
    }
}

async fn read_error_body(response: reqwest::Response) -> anyhow::Result<String> {
    response.text().await.context("读取上游错误响应体失败")
}

/// 展开错误的完整来源链
///
/// `reqwest::Error` 的 `Display` 只会给出 `error sending request for url (...)`，
/// 真正的链路原因（DNS 解析失败 / 连接被重置 / TLS 握手失败 / 超时）在 `source()` 链里，
/// 不展开就无法区分「上游 5xx」和「本机到上游的链路断了」。
fn describe_error(error: &(dyn std::error::Error + 'static)) -> String {
    let mut description = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        description.push_str(" -> ");
        description.push_str(&cause.to_string());
        source = cause.source();
    }
    description
}

fn is_transient_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429) || status.is_server_error()
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ClientKind {
    Standard,
    Streaming,
}

/// Kiro API Provider
///
/// 核心组件，负责与 Kiro API 通信
/// 支持多凭据故障转移和重试机制
/// 按凭据 `endpoint` 字段选择 [`KiroEndpoint`] 实现
pub struct KiroProvider {
    token_manager: Arc<MultiTokenManager>,
    /// 全局代理配置（用于凭据无自定义代理时的回退）
    global_proxy: Option<ProxyConfig>,
    /// Client 缓存：key = effective proxy config, value = reqwest::Client
    /// 不同代理配置的凭据使用不同的 Client，共享相同代理的凭据复用 Client
    client_cache: Mutex<HashMap<(Option<ProxyConfig>, ClientKind), Client>>,
    /// TLS 后端配置
    tls_backend: TlsBackend,
    /// 端点实现注册表（key: endpoint 名称）
    endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
    /// 默认端点名称（凭据未指定 endpoint 时使用）
    default_endpoint: String,
}

impl KiroProvider {
    /// 创建带代理配置和端点注册表的 KiroProvider 实例
    ///
    /// # Arguments
    /// * `token_manager` - 多凭据 Token 管理器
    /// * `proxy` - 全局代理配置
    /// * `endpoints` - 端点名 → 实现的注册表（至少包含 `default_endpoint` 对应条目）
    /// * `default_endpoint` - 凭据未显式指定 endpoint 时使用的名称
    pub fn with_proxy(
        token_manager: Arc<MultiTokenManager>,
        proxy: Option<ProxyConfig>,
        endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
        default_endpoint: String,
    ) -> Self {
        assert!(
            endpoints.contains_key(&default_endpoint),
            "默认端点 {} 未在 endpoints 注册表中",
            default_endpoint
        );
        let tls_backend = token_manager.config().tls_backend;
        let ca_cert_path = token_manager.config().ca_cert_path.as_deref();
        // 预热：构建全局代理对应的 Client
        let initial_client = build_client(proxy.as_ref(), 720, tls_backend, ca_cert_path)
            .expect("创建 HTTP 客户端失败");
        let mut cache = HashMap::new();
        cache.insert((proxy.clone(), ClientKind::Standard), initial_client);

        Self {
            token_manager,
            global_proxy: proxy,
            client_cache: Mutex::new(cache),
            tls_backend,
            endpoints,
            default_endpoint,
        }
    }

    /// 根据凭据的代理配置获取（或创建并缓存）对应的 reqwest::Client
    fn client_for(
        &self,
        credentials: &KiroCredentials,
        kind: ClientKind,
    ) -> anyhow::Result<Client> {
        let effective = credentials.effective_proxy(self.global_proxy.as_ref());
        let key = (effective.clone(), kind);
        let mut cache = self.client_cache.lock();
        if let Some(client) = cache.get(&key) {
            return Ok(client.clone());
        }
        let ca_cert_path = self.token_manager.config().ca_cert_path.as_deref();
        let client = match kind {
            ClientKind::Standard => {
                build_client(effective.as_ref(), 720, self.tls_backend, ca_cert_path)?
            }
            ClientKind::Streaming => {
                build_streaming_client(effective.as_ref(), 720, self.tls_backend, ca_cert_path)?
            }
        };
        cache.insert(key, client.clone());
        Ok(client)
    }

    /// 根据凭据选择 endpoint 实现
    fn endpoint_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Arc<dyn KiroEndpoint>> {
        let name = credentials
            .endpoint
            .as_deref()
            .unwrap_or(&self.default_endpoint);
        self.endpoints
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("未知端点: {}", name))
    }

    /// 发送非流式 API 请求
    ///
    /// 支持多凭据故障转移（见 [`Self::call_api_with_retry`]）
    pub async fn call_api(
        &self,
        request_body: &str,
        fallback_request_body: Option<&str>,
    ) -> anyhow::Result<reqwest::Response> {
        self.call_api_with_retry(
            request_body,
            fallback_request_body,
            false,
            MAX_TOTAL_RETRIES,
            None,
        )
        .await
    }

    /// 发送流式 API 请求
    pub async fn call_api_stream(
        self: &Arc<Self>,
        request_body: &str,
        fallback_request_body: Option<&str>,
    ) -> anyhow::Result<crate::kiro::stream_response::KiroStreamResponse> {
        let retry_budget = Arc::new(Mutex::new(StreamRetryBudget::new(
            self.token_manager.total_count(),
        )));
        let response = self
            .call_api_stream_attempt(request_body, fallback_request_body, retry_budget.clone())
            .await?;
        Ok(crate::kiro::stream_response::KiroStreamResponse::new(
            self.clone(),
            response,
            request_body,
            fallback_request_body,
            retry_budget,
        ))
    }

    pub(super) async fn call_api_stream_attempt(
        &self,
        request_body: &str,
        fallback_request_body: Option<&str>,
        retry_budget: SharedStreamRetryBudget,
    ) -> anyhow::Result<reqwest::Response> {
        self.call_api_with_retry(
            request_body,
            fallback_request_body,
            true,
            STREAM_ATTEMPT_RETRY_LIMIT,
            Some(&retry_budget),
        )
        .await
    }

    /// 发送 MCP API 请求（WebSearch 等工具调用）
    pub async fn call_mcp(&self, request_body: &str) -> anyhow::Result<reqwest::Response> {
        self.call_mcp_with_retry(request_body).await
    }

    /// 内部方法：带重试逻辑的 MCP API 调用
    async fn call_mcp_with_retry(&self, request_body: &str) -> anyhow::Result<reqwest::Response> {
        let total_credentials = self.token_manager.total_count();
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();

        for attempt in 0..max_retries {
            // MCP 调用（WebSearch 等工具）不涉及模型选择，无需按模型过滤凭据
            let ctx = match self.token_manager.acquire_context(None).await {
                Ok(c) => c,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            let config = self.token_manager.config();
            let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

            let endpoint = match self.endpoint_for(&ctx.credentials) {
                Ok(e) => e,
                Err(e) => {
                    last_error = Some(e);
                    // endpoint 解析失败：记为失败，换下一张凭据
                    self.token_manager.report_failure(ctx.id);
                    continue;
                }
            };

            let rctx = RequestContext {
                credentials: &ctx.credentials,
                token: &ctx.token,
                machine_id: &machine_id,
                config,
            };

            let url = endpoint.mcp_url(&rctx);
            let body = endpoint.transform_mcp_body(request_body, &rctx);

            let base = self
                .client_for(&ctx.credentials, ClientKind::Standard)?
                .post(&url)
                .body(body)
                .header("content-type", "application/json")
                .header("Connection", "close");
            let request = endpoint.decorate_mcp(base, &rctx);

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        "MCP 请求发送失败（尝试 {}/{}）: {}",
                        attempt + 1,
                        max_retries,
                        describe_error(&e)
                    );
                    last_error = Some(e.into());
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            let status = response.status();

            // 成功响应
            if status.is_success() {
                self.token_manager.report_success(ctx.id);
                return Ok(response);
            }

            // 失败响应
            let body = match read_error_body(response).await {
                Ok(body) => body,
                Err(error) if is_transient_status(status) => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = max_retries,
                        %status,
                        %error,
                        "MCP 上游错误响应体读取失败，正在重试"
                    );
                    last_error = Some(error);
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };

            // 402 额度用尽
            if status.as_u16() == 402 && endpoint.is_monthly_request_limit(&body) {
                let has_available = self.token_manager.report_quota_exhausted(ctx.id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {} {}", status, body);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            // 400 Bad Request
            if status.as_u16() == 400 {
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            // 401/403 凭据问题
            if matches!(status.as_u16(), 401 | 403) {
                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if endpoint.is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx.id) {
                    force_refreshed.insert(ctx.id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx.id);
                    match self.token_manager.force_refresh_token_for(ctx.id).await {
                        Ok(()) => {
                            tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx.id);
                            continue;
                        }
                        Err(error)
                            if crate::kiro::token_manager::is_refresh_transport_error(&error) =>
                        {
                            return Err(
                                error.context(format!("凭据 #{} token 强制刷新传输失败", ctx.id))
                            );
                        }
                        Err(error) => {
                            tracing::warn!(
                                "凭据 #{} token 强制刷新失败，计入失败: {:#}",
                                ctx.id,
                                error
                            );
                        }
                    }
                }

                let has_available = self.token_manager.report_failure(ctx.id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {} {}", status, body);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            // 瞬态错误
            if is_transient_status(status) {
                tracing::warn!(
                    "MCP 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                if attempt + 1 < max_retries {
                    sleep(Self::retry_delay(attempt)).await;
                }
                continue;
            }

            // 其他 4xx
            if status.is_client_error() {
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            // 兜底
            last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
            if attempt + 1 < max_retries {
                sleep(Self::retry_delay(attempt)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("MCP 请求失败：已达到最大重试次数（{}次）", max_retries)
        }))
    }

    /// 内部方法：带重试逻辑的 API 调用
    ///
    /// 重试策略：
    /// - 每个凭据最多重试 MAX_RETRIES_PER_CREDENTIAL 次
    /// - 总重试次数 = min(凭据数量 × 每凭据重试次数, MAX_TOTAL_RETRIES)
    /// - 硬上限 9 次，避免无限重试
    pub(super) async fn call_api_with_retry(
        &self,
        request_body: &str,
        fallback_request_body: Option<&str>,
        is_stream: bool,
        retry_limit: usize,
        retry_budget: Option<&SharedStreamRetryBudget>,
    ) -> anyhow::Result<reqwest::Response> {
        let total_credentials = self.token_manager.total_count();
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL)
            .min(MAX_TOTAL_RETRIES)
            .min(retry_limit);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();
        let mut active_request_body = request_body;
        let mut fallback_used = false;
        let api_type = if is_stream { "流式" } else { "非流式" };

        // 尝试从请求体中提取模型信息
        let model = Self::extract_model_from_request(request_body);

        // 非链路级失败（凭据/额度/上游状态）只能用完基础预算，多出来的迭代留给链路级重试
        let max_attempts = max_retries + LINK_RETRY_LIMIT;

        for attempt in 0..max_attempts {
            let non_link_budget_exhausted = attempt + 1 >= max_retries;
            let excluded_credentials = retry_budget
                .map(|budget| budget.lock().excluded_credentials())
                .unwrap_or_default();
            // 获取调用上下文（绑定 index、credentials、token）
            let ctx = match self
                .token_manager
                .acquire_context_excluding(model.as_deref(), &excluded_credentials)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    last_error = Some(e);
                    if non_link_budget_exhausted {
                        break;
                    }
                    continue;
                }
            };

            if retry_budget.is_some_and(|budget| !budget.lock().try_record(ctx.id)) {
                last_error = Some(anyhow::anyhow!(
                    "流式 API 请求失败：凭据 #{} 或总重试预算已耗尽",
                    ctx.id
                ));
                if non_link_budget_exhausted {
                    break;
                }
                continue;
            }

            let config = self.token_manager.config();
            let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

            let endpoint = match self.endpoint_for(&ctx.credentials) {
                Ok(e) => e,
                Err(e) => {
                    last_error = Some(e);
                    self.token_manager.report_failure(ctx.id);
                    if non_link_budget_exhausted {
                        break;
                    }
                    continue;
                }
            };

            let rctx = RequestContext {
                credentials: &ctx.credentials,
                token: &ctx.token,
                machine_id: &machine_id,
                config,
            };

            let url = endpoint.api_url(&rctx);
            let body = endpoint.transform_api_body(active_request_body, &rctx);

            let kind = if is_stream {
                ClientKind::Streaming
            } else {
                ClientKind::Standard
            };
            let client = self.client_for(&ctx.credentials, kind)?;
            let base = client
                .post(&url)
                .body(body)
                .header("content-type", "application/json")
                .header("Connection", "close");
            let request = endpoint.decorate_api(base, &rctx);

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    // 网络错误通常是上游/链路瞬态问题，不应导致"禁用凭据"或"切换凭据"
                    // （否则一段时间网络抖动会把所有凭据都误禁用，需要重启才能恢复）
                    // 基础预算内的尝试算 0，额外的迭代才是链路级重试（决定用哪档退避）
                    let link_retries_used = attempt.saturating_sub(max_retries.saturating_sub(1));
                    tracing::warn!(
                        "API 请求发送失败（尝试 {}/{}，链路级重试 {}/{}）: {}",
                        attempt + 1,
                        max_attempts,
                        link_retries_used,
                        LINK_RETRY_LIMIT,
                        describe_error(&e)
                    );
                    // 请求没有送达上游：退还已记的流式重启预算，让链路级重试继续用同一张凭据
                    if let Some(budget) = retry_budget {
                        budget.lock().release(ctx.id);
                    }
                    last_error = Some(e.into());
                    if attempt + 1 < max_attempts {
                        let backoff = if link_retries_used == 0 {
                            Self::retry_delay(attempt)
                        } else {
                            Self::link_retry_delay(link_retries_used - 1)
                        };
                        sleep(backoff).await;
                    }
                    continue;
                }
            };

            let status = response.status();

            // 成功响应
            if status.is_success() {
                self.token_manager.report_success(ctx.id);
                return Ok(response);
            }

            // 失败响应：读取 body 用于日志/错误信息
            let body = match read_error_body(response).await {
                Ok(body) => body,
                Err(error) if is_transient_status(status) => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = max_retries,
                        %status,
                        %error,
                        "API 上游错误响应体读取失败，正在重试"
                    );
                    last_error = Some(error);
                    if non_link_budget_exhausted {
                        break;
                    }
                    sleep(Self::retry_delay(attempt)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };

            if !fallback_used
                && Self::is_thinking_signature_invalid(status, &body)
                && let Some(fallback) = fallback_request_body
            {
                tracing::warn!("上游拒绝 thinking signature，剥离历史 reasoningContent 后重试一次");
                active_request_body = fallback;
                fallback_used = true;
                if non_link_budget_exhausted {
                    break;
                }
                continue;
            }

            // 402 Payment Required 且额度用尽：禁用凭据并故障转移
            if status.as_u16() == 402 && endpoint.is_monthly_request_limit(&body) {
                tracing::warn!(
                    "API 请求失败（额度已用尽，禁用凭据并切换，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );

                let has_available = self.token_manager.report_quota_exhausted(ctx.id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {} {}",
                        api_type,
                        status,
                        body
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                if non_link_budget_exhausted {
                    break;
                }
                continue;
            }

            // 400 Bad Request - 请求问题，重试/切换凭据无意义
            if status.as_u16() == 400 {
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 401/403 - 更可能是凭据/权限问题：计入失败并允许故障转移
            if matches!(status.as_u16(), 401 | 403) {
                tracing::warn!(
                    "API 请求失败（可能为凭据错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );

                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if endpoint.is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx.id) {
                    force_refreshed.insert(ctx.id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx.id);
                    match self.token_manager.force_refresh_token_for(ctx.id).await {
                        Ok(()) => {
                            tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx.id);
                            if non_link_budget_exhausted {
                                break;
                            }
                            continue;
                        }
                        Err(error)
                            if crate::kiro::token_manager::is_refresh_transport_error(&error) =>
                        {
                            return Err(
                                error.context(format!("凭据 #{} token 强制刷新传输失败", ctx.id))
                            );
                        }
                        Err(error) => {
                            tracing::warn!(
                                "凭据 #{} token 强制刷新失败，计入失败: {:#}",
                                ctx.id,
                                error
                            );
                        }
                    }
                }

                let has_available = self.token_manager.report_failure(ctx.id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {} {}",
                        api_type,
                        status,
                        body
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                if non_link_budget_exhausted {
                    break;
                }
                continue;
            }

            // 429/408/5xx - 瞬态上游错误：重试但不禁用或切换凭据
            // （避免 429 high traffic / 502 high load 等瞬态错误把所有凭据锁死）
            if is_transient_status(status) {
                tracing::warn!(
                    "API 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                if non_link_budget_exhausted {
                    break;
                }
                sleep(Self::retry_delay(attempt)).await;
                continue;
            }

            // 其他 4xx - 通常为请求/配置问题：直接返回，不计入凭据失败
            if status.is_client_error() {
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 兜底：当作可重试的瞬态错误处理（不切换凭据）
            tracing::warn!(
                "API 请求失败（未知错误，尝试 {}/{}）: {} {}",
                attempt + 1,
                max_retries,
                status,
                body
            );
            last_error = Some(anyhow::anyhow!(
                "{} API 请求失败: {} {}",
                api_type,
                status,
                body
            ));
            if non_link_budget_exhausted {
                break;
            }
            sleep(Self::retry_delay(attempt)).await;
        }

        // 所有重试都失败
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "{} API 请求失败：已达到最大重试次数（{}次）",
                api_type,
                max_retries
            )
        }))
    }

    /// 从请求体中提取模型信息
    ///
    /// 尝试解析 JSON 请求体，提取 conversationState.currentMessage.userInputMessage.modelId
    fn extract_model_from_request(request_body: &str) -> Option<String> {
        use serde_json::Value;

        let json: Value = serde_json::from_str(request_body).ok()?;

        json.get("conversationState")?
            .get("currentMessage")?
            .get("userInputMessage")?
            .get("modelId")?
            .as_str()
            .map(|s| s.to_string())
    }

    fn is_thinking_signature_invalid(status: reqwest::StatusCode, body: &str) -> bool {
        status.is_client_error() && body.contains("THINKING_SIGNATURE_INVALID")
    }

    pub(super) fn retry_delay(attempt: usize) -> Duration {
        // 指数退避 + 少量抖动，避免上游抖动时放大故障
        const BASE_MS: u64 = 200;
        const MAX_DELAY_MS: u64 = 30_000;
        let exp = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.min(18) as u32));
        let backoff = exp.min(MAX_DELAY_MS);
        let jitter_max = (backoff / 4).min(MAX_DELAY_MS - backoff);
        let jitter = fastrand::u64(0..=jitter_max);
        Duration::from_millis(backoff.saturating_add(jitter))
    }

    /// 链路级重试退避：1s / 2s / 4s / 8s（每次 +0..200ms 抖动）
    ///
    /// 链路故障（DNS / 连接 / TLS / 超时）恢复时间通常是秒级，用基础预算的
    /// 200/400ms 退避来不及等到链路恢复，等于没重试。
    pub(super) fn link_retry_delay(attempt: usize) -> Duration {
        let exp = LINK_RETRY_BASE_MS.saturating_mul(2u64.saturating_pow(attempt.min(8) as u32));
        let backoff = exp.min(LINK_RETRY_MAX_DELAY_MS);
        let jitter = fastrand::u64(0..=200);
        Duration::from_millis(backoff.saturating_add(jitter))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KiroProvider, LINK_RETRY_LIMIT, MAX_RETRIES_PER_CREDENTIAL, MAX_TOTAL_RETRIES,
        StreamRetryBudget, read_error_body,
    };
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn stream_retry_budget_limits_each_credential_and_total() {
        // Given: three credentials share one downstream streaming request budget.
        let mut budget = StreamRetryBudget::new(3);

        // When: each credential consumes its full allowance.
        for credential_id in 1..=3 {
            for _ in 0..MAX_RETRIES_PER_CREDENTIAL {
                assert!(budget.try_record(credential_id));
            }
            assert!(!budget.try_record(credential_id));
        }

        // Then: no credential exceeds three requests and the global cap is nine.
        assert_eq!(budget.total_attempts(), MAX_TOTAL_RETRIES);
        assert!(!budget.try_record(4));
    }

    #[test]
    fn link_level_failure_refunds_stream_retry_budget() {
        // Given: one credential that already burned its whole per-credential allowance
        let mut budget = StreamRetryBudget::new(1);
        for _ in 0..MAX_RETRIES_PER_CREDENTIAL {
            assert!(budget.try_record(1));
        }
        assert_eq!(budget.excluded_credentials(), HashSet::from([1]));

        // When: a request that never reached upstream is refunded
        budget.release(1);

        // Then: the credential is usable again instead of looking exhausted
        assert!(budget.excluded_credentials().is_empty());
        assert!(budget.can_retry());
        assert!(budget.try_record(1));
        assert_eq!(budget.total_attempts(), MAX_RETRIES_PER_CREDENTIAL);
    }

    #[test]
    fn link_retry_backoff_spans_about_fifteen_seconds() {
        let total: u64 = (0..LINK_RETRY_LIMIT)
            .map(|attempt| KiroProvider::link_retry_delay(attempt).as_millis() as u64)
            .sum();
        let capped = KiroProvider::link_retry_delay(usize::MAX).as_millis() as u64;

        assert!((15_000..=15_800).contains(&total), "total={total}");
        assert!((8_000..=8_200).contains(&capped), "capped={capped}");
    }

    #[tokio::test]
    async fn failed_response_body_transport_error_is_returned_for_retry() {
        // Given: an upstream sends a 503 header and closes before the declared body length.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("client should connect");
            let mut request = [0_u8; 1024];
            let _ = socket
                .read(&mut request)
                .await
                .expect("request should be readable");
            socket
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 32\r\nConnection: close\r\n\r\nshort",
                )
                .await
                .expect("response header should be writable");
        });
        let response = reqwest::get(format!("http://{address}/"))
            .await
            .expect("response headers should arrive");

        // When: the retry path reads the failed response body.
        let error = read_error_body(response)
            .await
            .expect_err("truncated body must remain a transport error");
        server.await.expect("test server should finish");

        // Then: callers can retain the error and continue within their retry budget.
        assert!(error.to_string().contains("读取上游错误响应体失败"));
    }

    #[test]
    fn retry_delay_at_high_attempts_is_capped_at_30_seconds() {
        let delay = KiroProvider::retry_delay(usize::MAX);

        assert_eq!(delay, Duration::from_secs(30));
    }

    #[test]
    fn thinking_signature_invalid_requires_a_client_error_marker() {
        assert!(KiroProvider::is_thinking_signature_invalid(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"code":"THINKING_SIGNATURE_INVALID"}"#,
        ));
        assert!(!KiroProvider::is_thinking_signature_invalid(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"code":"OTHER_ERROR"}"#,
        ));
        assert!(!KiroProvider::is_thinking_signature_invalid(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "THINKING_SIGNATURE_INVALID",
        ));
    }
}
