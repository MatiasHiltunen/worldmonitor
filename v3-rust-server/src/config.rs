use std::{env, net::SocketAddr};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub valid_keys: Vec<String>,
    pub runtime_env: String,
    pub groq_api_key: Option<String>,
    pub acled_access_token: Option<String>,
    pub request_timeout_ms: u64,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("WM_SERVER_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
            .parse::<SocketAddr>()
            .context("WM_SERVER_ADDR must be a valid socket address")?;

        let valid_keys = env::var("WORLDMONITOR_VALID_KEYS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        let runtime_env = env::var("VERCEL_ENV")
            .or_else(|_| env::var("NODE_ENV"))
            .unwrap_or_else(|_| "development".to_string());

        let groq_api_key = env::var("GROQ_API_KEY")
            .ok()
            .filter(|value| !value.is_empty());
        let acled_access_token = env::var("ACLED_ACCESS_TOKEN")
            .ok()
            .filter(|value| !value.is_empty());

        let request_timeout_ms = env::var("WM_UPSTREAM_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(15_000);

        Ok(Self {
            bind_addr,
            valid_keys,
            runtime_env,
            groq_api_key,
            acled_access_token,
            request_timeout_ms,
        })
    }
}
