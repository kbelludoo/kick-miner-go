use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read '{0}': {1}")]
    Io(String, #[source] std::io::Error),
    #[error("failed to parse JSON from '{0}': {1}")]
    Json(String, #[source] serde_json::Error),
    #[error("no accounts configured in '{0}'. Add at least one account with bearerToken")]
    NoAccounts(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub accounts: Vec<AccountConfig>,
    #[serde(default)]
    pub settings: SettingsConfig,
    #[serde(default)]
    pub chatter: ChatterConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountConfig {
    pub username: String,
    pub bearer_token: String,
    /// Legacy flat list of streamers
    #[serde(default)]
    pub streamers: Vec<String>,
    /// Legacy max concurrent
    #[serde(default)]
    pub max_concurrent_streams: i32,
    /// New: independent groups
    #[serde(default)]
    pub groups: Vec<StreamerGroup>,
}

impl AccountConfig {
    /// Returns the groups to use. If `groups` is set, uses those.
    /// Otherwise wraps the legacy flat list into a single "default" group.
    pub fn effective_groups(&self) -> Vec<StreamerGroup> {
        if !self.groups.is_empty() {
            let mut groups = self.groups.clone();
            for g in &mut groups {
                if g.max_concurrent <= 0 {
                    g.max_concurrent = 2;
                }
            }
            return groups;
        }
        let max = if self.max_concurrent_streams > 0 {
            self.max_concurrent_streams
        } else {
            2
        };
        vec![StreamerGroup {
            name: "default".to_string(),
            streamers: self.streamers.clone(),
            max_concurrent: max,
        }]
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamerGroup {
    pub name: String,
    #[serde(default)]
    pub streamers: Vec<String>,
    #[serde(default)]
    pub max_concurrent: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsConfig {
    #[serde(default)]
    pub check_interval_seconds: u64,
    #[serde(default)]
    pub ping_interval_seconds: u64,
    #[serde(default)]
    pub tracking_interval_seconds: u64,
    #[serde(default)]
    pub points_log_interval_minutes: u64,
    #[serde(default)]
    pub web_dashboard_port: u16,
    #[serde(default)]
    pub auto_fill_enabled: bool,
    #[serde(default)]
    pub auto_fill_target: i32,
    pub daily_claim_enabled: Option<bool>,
    #[serde(default)]
    pub daily_claim_check_minutes: u64,
}

impl Default for SettingsConfig {
    fn default() -> Self {
        Self {
            check_interval_seconds: 0,
            ping_interval_seconds: 0,
            tracking_interval_seconds: 0,
            points_log_interval_minutes: 0,
            web_dashboard_port: 0,
            auto_fill_enabled: false,
            auto_fill_target: 0,
            daily_claim_enabled: None,
            daily_claim_check_minutes: 0,
        }
    }
}

impl SettingsConfig {
    pub fn is_daily_claim_enabled(&self) -> bool {
        self.daily_claim_enabled.unwrap_or(true)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatterConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub llm_api_key: String,
    #[serde(default)]
    pub llm_model: String,
    #[serde(default)]
    pub llm_base_url: String,
    #[serde(default)]
    pub min_interval_minutes: u64,
    #[serde(default)]
    pub max_interval_minutes: u64,
    #[serde(default)]
    pub streamers: Vec<String>,
}

impl Default for ChatterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            llm_api_key: String::new(),
            llm_model: "free/gemini-3.8-flash".to_string(),
            llm_base_url: "https://apinex.io/v1".to_string(),
            min_interval_minutes: 0,
            max_interval_minutes: 0,
            streamers: vec![],
        }
    }
}

pub fn load_config(path: &str) -> Result<Config, ConfigError> {
    let data = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::Io(path.to_string(), e))?;

    let mut cfg: Config = serde_json::from_str(&data)
        .map_err(|e| ConfigError::Json(path.to_string(), e))?;

    // Apply defaults
    if cfg.settings.check_interval_seconds == 0 {
        cfg.settings.check_interval_seconds = 45;
    }
    if cfg.settings.ping_interval_seconds == 0 {
        cfg.settings.ping_interval_seconds = 30;
    }
    if cfg.settings.tracking_interval_seconds == 0 {
        cfg.settings.tracking_interval_seconds = 10;
    }
    if cfg.settings.points_log_interval_minutes == 0 {
        cfg.settings.points_log_interval_minutes = 5;
    }
    if cfg.settings.web_dashboard_port == 0 {
        cfg.settings.web_dashboard_port = 3009;
    }
    if cfg.settings.auto_fill_target <= 0 {
        cfg.settings.auto_fill_target = 12;
    }
    if cfg.settings.daily_claim_check_minutes == 0 {
        cfg.settings.daily_claim_check_minutes = 15;
    }
    if cfg.chatter.min_interval_minutes == 0 {
        cfg.chatter.min_interval_minutes = 13;
    }
    if cfg.chatter.max_interval_minutes == 0 {
        cfg.chatter.max_interval_minutes = 19;
    }
    for acc in &mut cfg.accounts {
        if acc.max_concurrent_streams <= 0 {
            acc.max_concurrent_streams = 2;
        }
    }

    if cfg.accounts.is_empty() {
        return Err(ConfigError::NoAccounts(path.to_string()));
    }

    Ok(cfg)
}

/// Build favorites set (all streamers across all groups) keyed by username
pub fn build_favorites(accounts: &[AccountConfig]) -> HashMap<String, std::collections::HashSet<String>> {
    let mut map = HashMap::new();
    for acc in accounts {
        let mut set = std::collections::HashSet::new();
        for g in acc.effective_groups() {
            for s in g.streamers {
                let clean = s.trim().to_lowercase();
                if !clean.is_empty() {
                    set.insert(clean);
                }
            }
        }
        map.insert(acc.username.clone(), set);
    }
    map
}
