//! Admin API 业务逻辑服务（直接调 CredentialPool admin 方法，无字符串匹配）

use std::collections::HashSet;
use std::sync::Arc;

use crate::domain::credential::Credential;
use crate::infra::storage::BalanceCacheStore;
use crate::service::credential_pool::CredentialPool;

use super::error::AdminServiceError;
use crate::interface::http::admin::dto::{
    AddCredentialRequest, AddCredentialResponse, BalanceResponse, CredentialStatusItem,
    CredentialsStatusResponse, LoadBalancingModeResponse, SetLoadBalancingModeRequest,
};

/// Admin 服务
///
/// 封装 Admin API 业务逻辑：DTO 校验 + 调 CredentialPool admin 方法 + balance 缓存。
/// 错误通过 [`AdminServiceError`] 表达，借助 `From<AdminPoolError>` 一对一映射，
/// 不再依赖错误消息字符串匹配。
pub struct AdminService {
    pool: Arc<CredentialPool>,
    balance_cache: Arc<BalanceCacheStore>,
    /// 已注册端点名（用于 add_credential 校验）
    known_endpoints: HashSet<String>,
    /// 默认端点名（用于 get_all_credentials 缺省回退）
    default_endpoint: String,
}

impl AdminService {
    pub fn new(
        pool: Arc<CredentialPool>,
        balance_cache: Arc<BalanceCacheStore>,
        known_endpoints: impl IntoIterator<Item = String>,
    ) -> Self {
        let default_endpoint = pool.config().endpoint.default_endpoint.clone();
        Self {
            pool,
            balance_cache,
            known_endpoints: known_endpoints.into_iter().collect(),
            default_endpoint,
        }
    }

    /// 获取所有凭据状态
    pub fn get_all_credentials(&self) -> CredentialsStatusResponse {
        let snapshot = self.pool.admin_snapshot();
        let default_endpoint = self.default_endpoint.clone();

        let mut credentials: Vec<CredentialStatusItem> = snapshot
            .entries
            .into_iter()
            .map(|entry| CredentialStatusItem {
                id: entry.id,
                priority: entry.priority,
                disabled: entry.disabled,
                failure_count: entry.failure_count,
                is_current: entry.id == snapshot.current_id,
                expires_at: entry.expires_at,
                auth_method: entry.auth_method,
                has_profile_arn: entry.has_profile_arn,
                refresh_token_hash: entry.refresh_token_hash,
                api_key_hash: entry.api_key_hash,
                masked_api_key: entry.masked_api_key,
                email: entry.email,
                success_count: entry.success_count,
                last_used_at: entry.last_used_at.clone(),
                has_proxy: entry.has_proxy,
                proxy_url: entry.proxy_url,
                refresh_failure_count: entry.refresh_failure_count,
                disabled_reason: entry.disabled_reason,
                endpoint: entry.endpoint.unwrap_or_else(|| default_endpoint.clone()),
            })
            .collect();

        credentials.sort_by_key(|c| c.priority);

        CredentialsStatusResponse {
            total: snapshot.total,
            available: snapshot.available,
            current_id: snapshot.current_id,
            credentials,
        }
    }

    /// 设置凭据禁用状态
    pub fn set_disabled(&self, id: u64, disabled: bool) -> Result<(), AdminServiceError> {
        let snapshot = self.pool.admin_snapshot();
        let current_id = snapshot.current_id;

        self.pool.set_disabled(id, disabled)?;

        // 禁用当前凭据时主动切换（priority 模式有效）
        if disabled && id == current_id {
            self.pool.switch_to_next();
        }
        Ok(())
    }

    /// 设置凭据优先级
    pub fn set_priority(&self, id: u64, priority: u32) -> Result<(), AdminServiceError> {
        self.pool.set_priority(id, priority)?;
        Ok(())
    }

    /// 重置失败计数并启用
    pub fn reset_and_enable(&self, id: u64) -> Result<(), AdminServiceError> {
        self.pool.reset_and_enable(id)?;
        Ok(())
    }

    /// 获取凭据余额（带 5 分钟缓存）
    pub async fn get_balance(&self, id: u64) -> Result<BalanceResponse, AdminServiceError> {
        if let Some(cached) = self.balance_cache.get(id)
            && let Ok(balance) = serde_json::from_value::<BalanceResponse>(cached)
        {
            tracing::debug!("凭据 #{} 余额命中缓存", id);
            return Ok(balance);
        }

        let usage = self.pool.get_usage_limits_for(id).await?;

        let current_usage = usage.current_usage();
        let usage_limit = usage.usage_limit();
        let remaining = (usage_limit - current_usage).max(0.0);
        let usage_percentage = if usage_limit > 0.0 {
            (current_usage / usage_limit * 100.0).min(100.0)
        } else {
            0.0
        };

        let balance = BalanceResponse {
            id,
            subscription_title: usage.subscription_title().map(|s| s.to_string()),
            current_usage,
            usage_limit,
            remaining,
            usage_percentage,
            next_reset_at: usage.next_date_reset,
        };

        if let Ok(value) = serde_json::to_value(&balance) {
            self.balance_cache.put(id, value);
        }

        Ok(balance)
    }

    /// 添加新凭据
    pub async fn add_credential(
        &self,
        req: AddCredentialRequest,
    ) -> Result<AddCredentialResponse, AdminServiceError> {
        if let Some(ref name) = req.endpoint
            && !self.known_endpoints.contains(name)
        {
            let mut known: Vec<&str> = self.known_endpoints.iter().map(|s| s.as_str()).collect();
            known.sort();
            return Err(AdminServiceError::InvalidRequest(format!(
                "未知端点 \"{}\"，已注册端点: {:?}",
                name, known
            )));
        }

        // proxy URL scheme 白名单：仅允许 http/https/socks4/socks5/socks5h/direct
        if let Some(ref proxy) = req.proxy_url {
            validate_proxy_scheme(proxy)?;
        }

        let email = req.email.clone();
        let new_cred = Credential {
            id: None,
            access_token: None,
            refresh_token: req.refresh_token,
            profile_arn: None,
            expires_at: None,
            auth_method: Some(req.auth_method),
            client_id: req.client_id,
            client_secret: req.client_secret,
            priority: req.priority,
            region: req.region,
            auth_region: req.auth_region,
            api_region: req.api_region,
            machine_id: req.machine_id,
            email: req.email,
            subscription_title: None,
            proxy_url: req.proxy_url,
            proxy_username: req.proxy_username,
            proxy_password: req.proxy_password,
            disabled: false,
            kiro_api_key: req.kiro_api_key,
            endpoint: req.endpoint,
        };

        let credential_id = self.pool.add_credential(new_cred).await?;

        // 主动获取订阅等级，避免首次请求时 Free 账号绕过 Opus 模型过滤；失败不阻断
        if let Err(e) = self.pool.get_usage_limits_for(credential_id).await {
            tracing::warn!("添加凭据后获取订阅等级失败（不影响凭据添加）: {}", e);
        }

        Ok(AddCredentialResponse {
            success: true,
            message: format!("凭据添加成功，ID: {}", credential_id),
            credential_id,
            email,
        })
    }

    /// 删除凭据
    pub fn delete_credential(&self, id: u64) -> Result<(), AdminServiceError> {
        self.pool.delete_credential(id)?;
        // 清理已删除凭据的余额缓存
        self.balance_cache.invalidate(id);
        Ok(())
    }

    /// 获取负载均衡模式
    pub fn get_load_balancing_mode(&self) -> LoadBalancingModeResponse {
        LoadBalancingModeResponse {
            mode: self.pool.get_load_balancing_mode(),
        }
    }

    /// 设置负载均衡模式
    pub fn set_load_balancing_mode(
        &self,
        req: SetLoadBalancingModeRequest,
    ) -> Result<LoadBalancingModeResponse, AdminServiceError> {
        if req.mode != "priority" && req.mode != "balanced" {
            return Err(AdminServiceError::InvalidRequest(
                "mode 必须是 'priority' 或 'balanced'".to_string(),
            ));
        }
        self.pool
            .set_load_balancing_mode(&req.mode)
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
        Ok(LoadBalancingModeResponse { mode: req.mode })
    }

    /// 强制刷新指定凭据 Token
    pub async fn force_refresh_token(&self, id: u64) -> Result<(), AdminServiceError> {
        self.pool.force_refresh_token_for(id).await?;
        Ok(())
    }
}

/// proxy URL scheme 白名单校验（防止 SSRF 通过 file:// / unix:// / 攻击者控制的 socks 等）
fn validate_proxy_scheme(url: &str) -> Result<(), AdminServiceError> {
    let lower = url.to_ascii_lowercase();
    if lower == Credential::PROXY_DIRECT
        || lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("socks5://")
        || lower.starts_with("socks5h://")
        || lower.starts_with("socks4://")
    {
        Ok(())
    } else {
        Err(AdminServiceError::InvalidRequest(format!(
            "代理 URL scheme 非法: {url}（仅允许 http/https/socks4/socks5/socks5h/direct）"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_proxy_scheme_accepts_common_schemes() {
        for ok in [
            "http://127.0.0.1:7890",
            "HTTPS://example.com:443",
            "socks5://host:1080",
            "socks5h://host:1080",
            "socks4://host:1080",
            "direct",
        ] {
            assert!(validate_proxy_scheme(ok).is_ok(), "expected ok: {ok}");
        }
    }

    #[test]
    fn validate_proxy_scheme_rejects_dangerous_schemes() {
        for bad in [
            "file:///etc/passwd",
            "unix:///run/docker.sock",
            "ftp://example.com",
            "javascript:alert(1)",
            "data:text/html,foo",
        ] {
            assert!(validate_proxy_scheme(bad).is_err(), "expected err: {bad}");
        }
    }
}
