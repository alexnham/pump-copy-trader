use std::{collections::HashSet, fs, path::Path};

use serde::{Deserialize, Deserializer, de::Error as DeError};
use solana_sdk::pubkey::Pubkey;
use url::Url;

use crate::error::{CopyTraderError, Result};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub signal: SignalConfig,
    pub mainnet: Option<MainnetConfig>,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(default)]
    pub http: HttpConfig,
    pub storage: StorageConfig,
    pub execution: ExecutionConfig,
    pub sizing: SizingConfig,
    #[serde(default)]
    pub token_policy: TokenPolicyConfig,
    #[serde(default)]
    pub tokens: Vec<TokenRule>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    #[serde(alias = "race_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self { timeout_ms: 2_000 }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    pub tcp_keepalive_seconds: u64,
    pub http2_keepalive_seconds: u64,
    pub http2_keepalive_timeout_seconds: u64,
    pub pool_idle_timeout_seconds: u64,
    pub max_idle_connections_per_host: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            tcp_keepalive_seconds: 30,
            http2_keepalive_seconds: 20,
            http2_keepalive_timeout_seconds: 5,
            pool_idle_timeout_seconds: 90,
            max_idle_connections_per_host: 16,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalConfig {
    #[serde(deserialize_with = "deserialize_pubkey")]
    pub wallet: Pubkey,
    pub laserstream_url: Url,
    pub http_url: Url,
    #[serde(default = "default_commitment")]
    pub commitment: String,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default)]
    pub preconfirmations: PreconfirmationsConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PreconfirmationsConfig {
    pub enabled: bool,
    pub websocket_url: Url,
}

impl Default for PreconfirmationsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            websocket_url: Url::parse("wss://beta.helius-rpc.com/").expect("static URL"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MainnetConfig {
    #[serde(default)]
    pub fanout: FanoutConfig,
    #[serde(
        default = "default_source_direct",
        rename = "skip",
        alias = "source_direct"
    )]
    pub source_direct: bool,
    #[serde(default)]
    pub fixed_priority_fee_micro_lamports: Option<u64>,
    pub sender_url: Url,
    #[serde(default = "default_sender_tip_lamports")]
    pub tip_lamports: u64,
    #[serde(default = "default_priority_level")]
    pub priority_level: String,
    #[serde(default = "default_max_priority_fee")]
    pub max_priority_fee_micro_lamports: u64,
}

impl MainnetConfig {
    /// Resolve recognized Helius RPC and Sender endpoints. Other providers keep
    /// their own authentication, and explicit real keys take precedence.
    pub fn with_helius_api_key(&self, api_key: &str) -> Result<Self> {
        validate_api_key(api_key)?;
        let mut config = self.clone();
        inject_sender_api_key(&mut config.sender_url, api_key);
        for route in &mut config.fanout.routes {
            inject_sender_api_key(&mut route.url, api_key);
        }
        if config.fanout.enabled
            && config
                .fanout
                .routes
                .iter()
                .any(|r| r.provider == FanoutProvider::Blockrazor)
        {
            crate::mainnet::blockrazor::api_key()?;
        }
        if config.fanout.enabled
            && config
                .fanout
                .routes
                .iter()
                .any(|r| r.provider == FanoutProvider::Nextblock)
        {
            crate::mainnet::nextblock::api_key()?;
        }
        if config.fanout.enabled
            && config
                .fanout
                .routes
                .iter()
                .any(|r| r.provider == FanoutProvider::Astralane)
        {
            crate::mainnet::astralane::api_key()?;
        }
        Ok(config)
    }

    pub fn execution_tip_budget(&self) -> u64 {
        if self.fanout.enabled {
            self.fanout
                .routes
                .iter()
                .map(|r| r.tip_lamports)
                .max()
                .unwrap_or(0)
        } else {
            self.tip_lamports
        }
    }
    pub fn execution_priority_fee(&self) -> u64 {
        if self.fanout.enabled {
            self.fanout
                .routes
                .iter()
                .map(|r| r.priority_fee_micro_lamports)
                .max()
                .unwrap_or(0)
        } else {
            self.fixed_priority_fee_micro_lamports
                .unwrap_or(self.max_priority_fee_micro_lamports)
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FanoutConfig {
    pub enabled: bool,
    pub nonce_accounts: Vec<String>,
    pub routes: Vec<FanoutRouteConfig>,
    pub submit_timeout_ms: u64,
}
impl Default for FanoutConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            nonce_accounts: vec![],
            routes: vec![],
            submit_timeout_ms: 1500,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FanoutProvider {
    #[default]
    JsonRpc,
    Blockrazor,
    Nextblock,
    Astralane,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FanoutRouteConfig {
    #[serde(default)]
    pub provider: FanoutProvider,
    pub name: String,
    pub url: Url,
    pub tip_account: String,
    pub tip_lamports: u64,
    pub priority_fee_micro_lamports: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub database_url: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub target: ExecutionTarget,
    #[serde(default)]
    pub allow_live_mainnet: bool,
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u16,
    #[serde(default = "default_max_signal_age")]
    pub max_signal_age_seconds: u64,
    #[serde(default = "default_confirmation_timeout")]
    pub confirmation_timeout_seconds: u64,
    /// Maximum concurrent submission calls; settlement has a separate 64-copy bound.
    #[serde(default = "default_max_concurrent_sends")]
    pub max_concurrent_sends: usize,
    #[serde(default = "default_preparation_workers")]
    pub preparation_workers: usize,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTarget {
    #[default]
    Mainnet,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SizingConfig {
    Percent { percent_bps: u16 },
    Fixed { amount: String },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenRule {
    #[serde(deserialize_with = "deserialize_pubkey")]
    pub mint: Pubkey,
    pub minimum_input: String,
    pub maximum_input: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum TokenPolicyConfig {
    #[default]
    Allowlist,
    All {
        minimum_input: String,
        maximum_input: String,
    },
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path).map_err(|error| {
            CopyTraderError::Configuration(format!("cannot read {}: {error}", path.display()))
        })?;
        let config: Self = toml::from_str(&raw).map_err(|error| {
            CopyTraderError::Configuration(format!("invalid {}: {error}", path.display()))
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn require_live_execution(&self) -> Result<()> {
        if !self.execution.allow_live_mainnet {
            return Err(CopyTraderError::Configuration(
                "mainnet execution requires execution.allow_live_mainnet = true".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.signal.preconfirmations.enabled
            && self.signal.preconfirmations.websocket_url.scheme() != "wss"
        {
            return Err(CopyTraderError::Configuration(
                "signal.preconfirmations.websocket_url must use wss".to_owned(),
            ));
        }
        if !matches!(self.signal.commitment.as_str(), "processed" | "confirmed") {
            return Err(CopyTraderError::Configuration(
                "signal.commitment must be \"processed\" or \"confirmed\"".to_owned(),
            ));
        }
        if self.signal.queue_capacity == 0 {
            return Err(CopyTraderError::Configuration(
                "signal.queue_capacity must be greater than zero".to_owned(),
            ));
        }
        if !(1..=16).contains(&self.execution.preparation_workers) {
            return Err(CopyTraderError::Configuration(
                "execution.preparation_workers must be between 1 and 16".into(),
            ));
        }
        if !(1..=64).contains(&self.execution.max_concurrent_sends) {
            return Err(CopyTraderError::Configuration(
                "execution.max_concurrent_sends must be between 1 and 64".to_owned(),
            ));
        }
        if self.execution.slippage_bps > 10_000 {
            return Err(CopyTraderError::Configuration(
                "execution.slippage_bps must not exceed 10000".to_owned(),
            ));
        }
        if self.routing.timeout_ms == 0 {
            return Err(CopyTraderError::Configuration(
                "routing.timeout_ms must be greater than zero".to_owned(),
            ));
        }
        if self.http.tcp_keepalive_seconds == 0
            || self.http.http2_keepalive_seconds == 0
            || self.http.http2_keepalive_timeout_seconds == 0
            || self.http.pool_idle_timeout_seconds == 0
            || self.http.max_idle_connections_per_host == 0
        {
            return Err(CopyTraderError::Configuration(
                "HTTP keepalive and pool settings must be greater than zero".to_owned(),
            ));
        }
        match self.execution.target {
            ExecutionTarget::Mainnet => {
                let mainnet = self.mainnet.as_ref().ok_or_else(|| {
                    CopyTraderError::Configuration(
                        "execution.target = \"mainnet\" requires a [mainnet] section".to_owned(),
                    )
                })?;
                validate_sender(mainnet)?;
            }
        }
        match &self.sizing {
            SizingConfig::Percent { percent_bps } if *percent_bps == 0 || *percent_bps > 10_000 => {
                return Err(CopyTraderError::Configuration(
                    "sizing.percent_bps must be between 1 and 10000".to_owned(),
                ));
            }
            SizingConfig::Fixed { amount } if amount.trim().is_empty() => {
                return Err(CopyTraderError::Configuration(
                    "sizing.amount cannot be empty".to_owned(),
                ));
            }
            _ => {}
        }
        if matches!(self.token_policy, TokenPolicyConfig::Allowlist) && self.tokens.is_empty() {
            return Err(CopyTraderError::Configuration(
                "token_policy.mode = \"allowlist\" requires at least one [[tokens]] rule"
                    .to_owned(),
            ));
        }
        if let TokenPolicyConfig::All {
            minimum_input,
            maximum_input,
        } = &self.token_policy
        {
            validate_decimal(minimum_input, "token_policy.minimum_input")?;
            validate_decimal(maximum_input, "token_policy.maximum_input")?;
        }
        let mut seen = HashSet::new();
        for token in &self.tokens {
            if !seen.insert(token.mint) {
                return Err(CopyTraderError::Configuration(format!(
                    "duplicate token rule for {}",
                    token.mint
                )));
            }
            validate_decimal(&token.minimum_input, "minimum_input")?;
            validate_decimal(&token.maximum_input, "maximum_input")?;
        }
        Ok(())
    }

    pub fn token_rule(&self, mint: Pubkey) -> Option<TokenRule> {
        if let Some(rule) = self.tokens.iter().find(|rule| rule.mint == mint) {
            return Some(rule.clone());
        }
        match &self.token_policy {
            TokenPolicyConfig::Allowlist => None,
            TokenPolicyConfig::All {
                minimum_input,
                maximum_input,
            } => Some(TokenRule {
                mint,
                minimum_input: minimum_input.clone(),
                maximum_input: maximum_input.clone(),
            }),
        }
    }

    pub const fn allows_all_tokens(&self) -> bool {
        matches!(self.token_policy, TokenPolicyConfig::All { .. })
    }

    pub fn laserstream_endpoint(&self, api_key: &str) -> Result<String> {
        validate_api_key(api_key)?;
        if self.signal.laserstream_url.scheme() != "https" {
            return Err(CopyTraderError::Configuration(
                "signal.laserstream_url must use https".to_owned(),
            ));
        }
        Ok(self.signal.laserstream_url.to_string())
    }

    pub fn helius_http_url(&self, api_key: &str) -> Result<Url> {
        with_api_key(self.signal.http_url.clone(), api_key)
    }
}

impl ExecutionTarget {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
        }
    }
}

fn validate_sender(config: &MainnetConfig) -> Result<()> {
    let fanout = &config.fanout;
    if fanout.enabled {
        use std::{collections::HashSet, str::FromStr};
        if fanout.nonce_accounts.is_empty()
            || fanout.routes.is_empty()
            || fanout.routes.len() > 32
            || !(1..=10_000).contains(&fanout.submit_timeout_ms)
        {
            return Err(CopyTraderError::Configuration(
                "fanout requires nonce accounts, 1–32 routes, and submit_timeout_ms in 1–10000"
                    .into(),
            ));
        }
        let mut accounts = HashSet::new();
        for account in &fanout.nonce_accounts {
            let key = Pubkey::from_str(account).map_err(|_| {
                CopyTraderError::Configuration("invalid fanout nonce account".into())
            })?;
            if !accounts.insert(key) {
                return Err(CopyTraderError::Configuration(
                    "duplicate fanout nonce account".into(),
                ));
            }
        }
        let mut names = HashSet::new();
        for route in &fanout.routes {
            if route.name.is_empty()
                || !names.insert(&route.name)
                || !matches!(route.url.scheme(), "http" | "https")
                || route.url.host_str().is_none()
                || route.priority_fee_micro_lamports > config.max_priority_fee_micro_lamports
                || Pubkey::from_str(&route.tip_account).is_err()
            {
                return Err(CopyTraderError::Configuration("invalid fanout route: use unique names, HTTP(S) URLs, valid tip accounts, and priority fees within the maximum".into()));
            }
            if route.provider == FanoutProvider::Astralane {
                let host = route.url.host_str().unwrap_or_default();
                if !(host == "edge.astralane.io"
                    || [
                        "ny", "fr", "fr2", "la", "jp", "ams", "ams2", "lim", "sg", "lit", "lon",
                    ]
                    .iter()
                    .any(|region| host == format!("{region}.gateway.astralane.io")))
                    || route.url.scheme() != "https"
                    || route.url.path() != "/iris"
                    || route.url.query().is_some()
                    || !route.url.username().is_empty()
                    || route.url.password().is_some()
                    || route.tip_lamports < 10_000
                    || !crate::mainnet::astralane::TIP_ACCOUNTS
                        .contains(&route.tip_account.as_str())
                {
                    return Err(CopyTraderError::Configuration("Astralane requires an official HTTPS /iris endpoint, no URL credentials, an Astralane tip account and at least 10000 tip lamports (actual minimum depends on tier)".into()));
                }
            }
            if route.provider == FanoutProvider::Nextblock {
                let host = route.url.host_str().unwrap_or_default();
                if !host.ends_with(".nextblock.io")
                    || route.url.path() != "/api/v2/submit"
                    || route.url.query().is_some()
                    || !route.url.username().is_empty()
                    || route.url.password().is_some()
                    || route.tip_lamports < 1_000_000
                    || !crate::mainnet::nextblock::TIP_ACCOUNTS
                        .contains(&route.tip_account.as_str())
                {
                    return Err(CopyTraderError::Configuration("NextBlock routes require an official /api/v2/submit endpoint, no URL credentials, a NextBlock tip account and at least 1000000 tip lamports".into()));
                }
            }
            if route.provider == FanoutProvider::Blockrazor {
                let host = route.url.host_str().unwrap_or_default();
                if !(host.ends_with(".solana.blockrazor.xyz")
                    || host.ends_with(".solana.blockrazor.io"))
                    || route.url.path() != "/sendTransaction"
                    || route.url.query().is_some()
                    || !route.url.username().is_empty()
                    || route.url.password().is_some()
                    || route.tip_lamports < 100_000
                    || !crate::mainnet::blockrazor::TIP_ACCOUNTS
                        .contains(&route.tip_account.as_str())
                {
                    return Err(CopyTraderError::Configuration("BlockRazor routes require an official Solana /sendTransaction endpoint, no URL credentials, a BlockRazor tip account and at least 100000 tip lamports".into()));
                }
            }
            if route.url.host_str().is_some_and(|host| {
                host == "sender.helius-rpc.com" || host.ends_with("-sender.helius-rpc.com")
            }) {
                let mut sender = config.clone();
                sender.fanout.enabled = false;
                sender.sender_url = route.url.clone();
                sender.tip_lamports = route.tip_lamports;
                validate_sender(&sender)?;
                if !crate::mainnet::MainnetClient::is_sender_tip_account(&route.tip_account) {
                    return Err(CopyTraderError::Configuration(
                        "Helius fanout routes require a Helius Sender tip account".into(),
                    ));
                }
            }
        }
    }

    if !config.source_direct {
        return Err(CopyTraderError::Configuration(
            "this trader builds only from source instructions; mainnet.skip must be true"
                .to_owned(),
        ));
    }

    if config.source_direct && config.fixed_priority_fee_micro_lamports.is_none() {
        return Err(CopyTraderError::Configuration(
            "source instruction execution requires fixed_priority_fee_micro_lamports".to_owned(),
        ));
    }
    if config
        .fixed_priority_fee_micro_lamports
        .is_some_and(|fee| fee > config.max_priority_fee_micro_lamports)
    {
        return Err(CopyTraderError::Configuration(
            "fixed priority fee exceeds configured maximum".to_owned(),
        ));
    }

    let regional = matches!(
        config.sender_url.host_str(),
        Some(
            "slc-sender.helius-rpc.com"
                | "ewr-sender.helius-rpc.com"
                | "lon-sender.helius-rpc.com"
                | "fra-sender.helius-rpc.com"
                | "ams-sender.helius-rpc.com"
                | "sg-sender.helius-rpc.com"
                | "tyo-sender.helius-rpc.com"
        )
    );
    let global = config.sender_url.host_str() == Some("sender.helius-rpc.com")
        && config.sender_url.scheme() == "https";
    if !(global || regional && matches!(config.sender_url.scheme(), "http" | "https")) {
        return Err(CopyTraderError::Configuration(
            "mainnet.sender_url must use the global HTTPS or a supported regional Helius Sender endpoint".to_owned(),
        ));
    }
    let swqos_only = config
        .sender_url
        .query_pairs()
        .any(|(key, value)| key == "swqos_only" && value == "true");
    let minimum_tip = if swqos_only { 5_000 } else { 1_000_000 };
    if config.tip_lamports < minimum_tip {
        return Err(CopyTraderError::Configuration(format!(
            "mainnet.tip_lamports must be at least {minimum_tip} for the configured Sender tier"
        )));
    }
    if canonical_priority_level(&config.priority_level).is_none() {
        return Err(CopyTraderError::Configuration(
            "mainnet.priority_level must be min, low, medium, high, or veryHigh".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn canonical_priority_level(value: &str) -> Option<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "min" => Some("Min"),
        "low" => Some("Low"),
        "medium" => Some("Medium"),
        "high" => Some("High"),
        "veryhigh" => Some("VeryHigh"),
        _ => None,
    }
}

fn inject_sender_api_key(url: &mut Url, api_key: &str) {
    let recognized = matches!(
        url.host_str(),
        Some(
            "sender.helius-rpc.com"
                | "mainnet.helius-rpc.com"
                | "slc-sender.helius-rpc.com"
                | "ewr-sender.helius-rpc.com"
                | "lon-sender.helius-rpc.com"
                | "fra-sender.helius-rpc.com"
                | "ams-sender.helius-rpc.com"
                | "sg-sender.helius-rpc.com"
                | "tyo-sender.helius-rpc.com"
        )
    );
    if !recognized || !matches!(url.scheme(), "http" | "https") {
        return;
    }
    let pairs = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if pairs.iter().any(|(key, value)| {
        key == "api-key"
            && !value.trim().is_empty()
            && !matches!(value.as_str(), "YOUR_API_KEY" | "YOUR_HELIUS_API_KEY")
    }) {
        return;
    }
    url.set_query(None);
    let mut query = url.query_pairs_mut();
    for (key, value) in pairs {
        if key != "api-key" {
            query.append_pair(&key, &value);
        }
    }
    query.append_pair("api-key", api_key.trim());
}

fn with_api_key(mut url: Url, api_key: &str) -> Result<Url> {
    validate_api_key(api_key)?;
    url.query_pairs_mut().append_pair("api-key", api_key.trim());
    Ok(url)
}

fn validate_api_key(api_key: &str) -> Result<()> {
    if api_key.trim().is_empty() {
        return Err(CopyTraderError::Configuration(
            "HELIUS_API_KEY cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn deserialize_pubkey<'de, D>(deserializer: D) -> std::result::Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse::<Pubkey>().map_err(D::Error::custom)
}

fn validate_decimal(value: &str, field: &str) -> Result<()> {
    let trimmed = value.trim();
    let mut parts = trimmed.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    let valid = !whole.is_empty()
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && fraction
            .is_none_or(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
        && parts.next().is_none();
    if !valid {
        return Err(CopyTraderError::Configuration(format!(
            "{field} must be a non-negative decimal string"
        )));
    }
    Ok(())
}

fn default_commitment() -> String {
    "processed".to_owned()
}

const fn default_queue_capacity() -> usize {
    256
}

const fn default_slippage_bps() -> u16 {
    100
}

const fn default_max_signal_age() -> u64 {
    30
}

const fn default_confirmation_timeout() -> u64 {
    30
}

const fn default_sender_tip_lamports() -> u64 {
    5_000
}

fn default_priority_level() -> String {
    "high".to_owned()
}

const fn default_max_priority_fee() -> u64 {
    5_000_000
}

const fn default_source_direct() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nextblock_routes_validate_endpoint_and_tip_wallet() {
        let mut config = AppConfig::load(Path::new("config.example.toml"))
            .unwrap()
            .mainnet
            .unwrap();
        config.fanout.enabled = true;
        config.fanout.nonce_accounts = vec![Pubkey::new_unique().to_string()];
        let route = FanoutRouteConfig {
            provider: FanoutProvider::Nextblock,
            name: "nextblock".into(),
            url: Url::parse("https://ny.nextblock.io/api/v2/submit").unwrap(),
            tip_account: crate::mainnet::nextblock::TIP_ACCOUNTS[0].into(),
            tip_lamports: 1000000,
            priority_fee_micro_lamports: 100000,
        };
        config.fanout.routes = vec![route.clone()];
        validate_sender(&config).unwrap();
        for endpoint in [
            "https://evil.example/api/v2/submit",
            "https://ny.nextblock.io/other",
            "https://ny.nextblock.io/api/v2/submit?key=secret",
        ] {
            config.fanout.routes[0].url = Url::parse(endpoint).unwrap();
            assert!(validate_sender(&config).is_err());
        }
        config.fanout.routes[0] = route;
        config.fanout.routes[0].tip_account = Pubkey::new_unique().to_string();
        assert!(validate_sender(&config).is_err());
    }

    #[test]
    fn blockrazor_routes_validate_endpoints_tips_and_legacy_provider() {
        let mut config = AppConfig::load(Path::new("config.example.toml"))
            .unwrap()
            .mainnet
            .unwrap();
        config.fanout.enabled = true;
        config.fanout.nonce_accounts = vec![Pubkey::new_unique().to_string()];
        let route = FanoutRouteConfig {
            provider: FanoutProvider::Blockrazor,
            name: "blockrazor".into(),
            url: Url::parse("https://newyork.solana.blockrazor.io/sendTransaction").unwrap(),
            tip_account: crate::mainnet::blockrazor::TIP_ACCOUNTS[0].into(),
            tip_lamports: 100000,
            priority_fee_micro_lamports: 100000,
        };
        config.fanout.routes = vec![route.clone()];
        validate_sender(&config).unwrap();
        for endpoint in [
            "https://evil.example/sendTransaction",
            "https://newyork.solana.blockrazor.io/other",
            "https://newyork.solana.blockrazor.io/sendTransaction?api-key=secret",
            "https://user:secret@newyork.solana.blockrazor.io/sendTransaction",
        ] {
            config.fanout.routes[0].url = Url::parse(endpoint).unwrap();
            assert!(validate_sender(&config).is_err());
        }
        config.fanout.routes[0] = route.clone();
        config.fanout.routes[0].tip_lamports = 99999;
        assert!(validate_sender(&config).is_err());
        config.fanout.routes[0] = route;
        config.fanout.routes[0].tip_account = Pubkey::new_unique().to_string();
        assert!(validate_sender(&config).is_err());
        let legacy: FanoutRouteConfig = toml::from_str("name = 'old'\nurl = 'https://sender.helius-rpc.com/fast'\ntip_account = '11111111111111111111111111111111'\ntip_lamports = 5000\npriority_fee_micro_lamports = 100000").unwrap();
        assert_eq!(legacy.provider, FanoutProvider::JsonRpc);
    }

    #[test]
    fn preparation_worker_limits_and_legacy_defaults() {
        let text = std::fs::read_to_string("config.example.toml").unwrap();
        let legacy = text
            .lines()
            .filter(|line| {
                !line.starts_with("preparation_workers =")
                    && !line.starts_with("max_concurrent_sends =")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let mut config: AppConfig = toml::from_str(&legacy).unwrap();
        assert_eq!(config.execution.preparation_workers, 4);
        assert_eq!(config.execution.max_concurrent_sends, 8);
        for count in [1, 4, 16] {
            config.execution.preparation_workers = count;
            config.validate().unwrap();
        }
        for count in [0, 17, usize::MAX] {
            config.execution.preparation_workers = count;
            assert!(
                config
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("preparation_workers")
            );
        }
    }

    #[test]
    fn concurrent_submission_limits_are_bounded() {
        let mut config = AppConfig::load(Path::new("config.example.toml")).unwrap();
        for limit in [1, 8, 64] {
            config.execution.max_concurrent_sends = limit;
            config.validate().expect("supported concurrency");
        }
        for limit in [0, 65, usize::MAX] {
            config.execution.max_concurrent_sends = limit;
            assert!(
                config
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("max_concurrent_sends")
            );
        }
    }

    #[test]
    fn sender_urls_use_environment_key_without_leaking_it_to_other_hosts() {
        let base = AppConfig::load(Path::new("config.example.toml"))
            .unwrap()
            .mainnet
            .unwrap();
        for endpoint in [
            "https://mainnet.helius-rpc.com/",
            "https://sender.helius-rpc.com/fast?swqos_only=true",
            "http://ewr-sender.helius-rpc.com/fast?api-key=YOUR_HELIUS_API_KEY&swqos_only=true",
            "https://tyo-sender.helius-rpc.com/fast?api-key=",
        ] {
            let mut config = base.clone();
            config.sender_url = Url::parse(endpoint).unwrap();
            config.fanout.routes = vec![FanoutRouteConfig {
                provider: crate::config::FanoutProvider::JsonRpc,
                name: "route".into(),
                url: config.sender_url.clone(),
                tip_account: Pubkey::new_unique().to_string(),
                tip_lamports: 5000,
                priority_fee_micro_lamports: 100000,
            }];
            let resolved = config.with_helius_api_key(" env+key&value ").unwrap();
            for url in [&resolved.sender_url, &resolved.fanout.routes[0].url] {
                let keys = url
                    .query_pairs()
                    .filter(|(key, _)| key == "api-key")
                    .map(|(_, value)| value.into_owned())
                    .collect::<Vec<_>>();
                assert_eq!(keys, vec!["env+key&value"]);
                assert_eq!(url.path(), Url::parse(endpoint).unwrap().path());
                if endpoint.contains("swqos_only") {
                    assert!(
                        url.query_pairs()
                            .any(|(key, value)| key == "swqos_only" && value == "true")
                    );
                }
            }
            assert_eq!(
                config.sender_url.as_str(),
                endpoint,
                "saved config is unchanged"
            );
        }
        for endpoint in [
            "https://sender.helius-rpc.com/fast?api-key=explicit-key",
            "https://relay.example.com/fast",
            "https://sender.helius-rpc.com.evil.example/fast",
            "https://unknown-sender.helius-rpc.com/fast",
        ] {
            let mut config = base.clone();
            config.sender_url = Url::parse(endpoint).unwrap();
            config.fanout.routes = vec![FanoutRouteConfig {
                provider: crate::config::FanoutProvider::JsonRpc,
                name: "route".into(),
                url: config.sender_url.clone(),
                tip_account: Pubkey::new_unique().to_string(),
                tip_lamports: 5000,
                priority_fee_micro_lamports: 100000,
            }];
            let resolved = config.with_helius_api_key("env-key").unwrap();
            assert_eq!(resolved.sender_url.as_str(), endpoint);
            assert_eq!(resolved.fanout.routes[0].url.as_str(), endpoint);
        }
        assert!(base.with_helius_api_key(" ").is_err());
    }

    #[test]
    fn fanout_validation_and_fee_budgets() {
        let mut config = AppConfig::load(Path::new("config.example.toml")).unwrap();
        let mainnet = config.mainnet.as_mut().unwrap();
        mainnet.fanout.enabled = true;
        assert!(validate_sender(mainnet).is_err());
        mainnet.fanout.nonce_accounts = vec![Pubkey::new_unique().to_string()];
        mainnet.fanout.routes = vec![FanoutRouteConfig {
            provider: crate::config::FanoutProvider::JsonRpc,
            name: "sender".into(),
            url: mainnet.sender_url.clone(),
            tip_account: "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE".into(),
            tip_lamports: 6000,
            priority_fee_micro_lamports: 200000,
        }];
        assert!(validate_sender(mainnet).is_ok());
        assert_eq!(mainnet.execution_tip_budget(), 6000);
        assert_eq!(mainnet.execution_priority_fee(), 200000);
        mainnet.fanout.routes[0].tip_lamports = 1;
        assert!(validate_sender(mainnet).is_err());
        mainnet.fanout.routes[0].tip_lamports = 6000;
        mainnet.fanout.routes[0].priority_fee_micro_lamports =
            mainnet.max_priority_fee_micro_lamports + 1;
        assert!(validate_sender(mainnet).is_err());
        mainnet.fanout.routes[0].priority_fee_micro_lamports = 200000;
        mainnet
            .fanout
            .nonce_accounts
            .push(mainnet.fanout.nonce_accounts[0].clone());
        assert!(validate_sender(mainnet).is_err());
        mainnet.fanout.nonce_accounts.pop();
        mainnet.fanout.routes.push(mainnet.fanout.routes[0].clone());
        assert!(validate_sender(mainnet).is_err());
    }

    #[test]
    fn decimal_validation_rejects_ambiguous_values() {
        assert!(validate_decimal("1.25", "value").is_ok());
        assert!(validate_decimal(".25", "value").is_err());
        assert!(validate_decimal("1e9", "value").is_err());
        assert!(validate_decimal("-1", "value").is_err());
    }

    #[test]
    fn pubkey_deserialization_is_supported() {
        assert!(
            "So11111111111111111111111111111111111111112"
                .parse::<Pubkey>()
                .is_ok()
        );
    }

    #[test]
    fn example_configuration_is_valid() {
        let config = toml::from_str::<AppConfig>(include_str!("../config.example.toml"));
        assert!(config.is_ok());
        if let Ok(config) = config {
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn preconfirmations_can_be_disabled_and_default_to_enabled() {
        let enabled: PreconfirmationsConfig = toml::from_str("").expect("defaults");
        assert!(enabled.enabled);
        let disabled: PreconfirmationsConfig = toml::from_str("enabled = false").expect("disabled");
        assert!(!disabled.enabled);
        let mut legacy: toml::Value =
            toml::from_str(include_str!("../config.example.toml")).expect("example");
        legacy["signal"]
            .as_table_mut()
            .expect("signal")
            .remove("preconfirmations");
        let config: AppConfig = legacy.try_into().expect("legacy config");
        assert!(config.signal.preconfirmations.enabled);
    }

    #[test]
    fn enabled_preconfirmation_endpoints_require_tls() {
        let mut config: AppConfig = toml::from_str(include_str!("../config.example.toml")).unwrap();
        config.signal.preconfirmations.websocket_url = Url::parse("ws://localhost/").unwrap();
        assert!(config.validate().is_err());
        config.signal.preconfirmations.enabled = false;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn mainnet_configuration_can_be_checked_with_trading_disabled() {
        let config = toml::from_str::<AppConfig>(include_str!("../config.example.toml"));
        assert!(config.is_ok());
        let Ok(mut config) = config else { return };
        config.execution.target = ExecutionTarget::Mainnet;
        config.execution.allow_live_mainnet = false;
        config.signal.commitment = "processed".to_owned();
        assert!(config.validate().is_ok());
        assert!(config.require_live_execution().is_err());
        config.execution.allow_live_mainnet = true;
        assert!(config.validate().is_ok());
        assert!(config.require_live_execution().is_ok());
    }

    #[test]
    fn all_token_mode_supplies_a_rule_for_unlisted_mints() {
        let config = toml::from_str::<AppConfig>(include_str!("../config.example.toml"));
        assert!(config.is_ok());
        let Ok(mut config) = config else { return };
        config.tokens.clear();
        config.token_policy = TokenPolicyConfig::All {
            minimum_input: "0.001".to_owned(),
            maximum_input: "2.5".to_owned(),
        };
        let mint = Pubkey::new_unique();
        assert!(config.validate().is_ok());
        let rule = config.token_rule(mint);
        assert_eq!(rule.as_ref().map(|rule| rule.mint), Some(mint));
        assert_eq!(
            rule.as_ref().map(|rule| rule.maximum_input.as_str()),
            Some("2.5")
        );
    }

    #[test]
    fn all_token_mode_deserializes_from_toml() {
        let policy = toml::from_str::<TokenPolicyConfig>(
            r#"
            mode = "all"
            minimum_input = "0.001"
            maximum_input = "1"
            "#,
        );
        assert!(matches!(policy, Ok(TokenPolicyConfig::All { .. })));
    }

    #[test]
    fn allowlist_mode_rejects_unlisted_mints() {
        let config = toml::from_str::<AppConfig>(include_str!("../config.example.toml"));
        assert!(config.is_ok());
        let Ok(config) = config else { return };
        assert!(config.token_rule(Pubkey::new_unique()).is_none());
    }

    #[test]
    fn sender_tiers_enforce_their_tip_minimums() {
        let swqos_url = Url::parse("https://sender.helius-rpc.com/fast?swqos_only=true");
        let max_url = Url::parse("https://sender.helius-rpc.com/fast");
        assert!(swqos_url.is_ok() && max_url.is_ok());
        let (Ok(swqos_url), Ok(max_url)) = (swqos_url, max_url) else {
            return;
        };
        let mut config = MainnetConfig {
            fanout: Default::default(),
            source_direct: true,
            fixed_priority_fee_micro_lamports: Some(100),
            sender_url: swqos_url,
            tip_lamports: 4_999,
            priority_level: "high".to_owned(),
            max_priority_fee_micro_lamports: 1_000_000,
        };
        assert!(validate_sender(&config).is_err());
        config.tip_lamports = 5_000;
        config.fixed_priority_fee_micro_lamports = Some(100);
        config.source_direct = true;
        assert!(validate_sender(&config).is_ok());
        config.sender_url = max_url;
        config.tip_lamports = 999_999;
        assert!(validate_sender(&config).is_err());
        config.tip_lamports = 1_000_000;
        assert!(validate_sender(&config).is_ok());
    }

    #[test]
    fn sender_accepts_documented_regions_and_rejects_other_hosts() {
        let mut config = MainnetConfig {
            fanout: Default::default(),
            source_direct: true,
            fixed_priority_fee_micro_lamports: Some(100),
            sender_url: Url::parse("https://sender.helius-rpc.com/fast").expect("url"),
            tip_lamports: 1_000_000,
            priority_level: "high".into(),
            max_priority_fee_micro_lamports: 1_000_000,
        };
        for region in ["slc", "ewr", "lon", "fra", "ams", "sg", "tyo"] {
            config.sender_url = Url::parse(&format!(
                "http://{region}-sender.helius-rpc.com/fast?api-key=fixture"
            ))
            .expect("regional url");
            assert!(validate_sender(&config).is_ok(), "{region}");
            config
                .sender_url
                .query_pairs_mut()
                .append_pair("swqos_only", "true");
            config.tip_lamports = 5_000;
            assert!(validate_sender(&config).is_ok());
            config.tip_lamports = 1_000_000;
        }
        for endpoint in [
            "http://sender.helius-rpc.com/fast",
            "http://unknown-sender.helius-rpc.com/fast",
            "http://ewr-sender.helius-rpc.com.example.com/fast",
            "ftp://ewr-sender.helius-rpc.com/fast",
        ] {
            config.sender_url = Url::parse(endpoint).expect("url");
            assert!(validate_sender(&config).is_err(), "{endpoint}");
        }
    }

    #[test]
    fn sender_rejects_unknown_priority_level() {
        let sender_url = Url::parse("https://sender.helius-rpc.com/fast?swqos_only=true");
        assert!(sender_url.is_ok());
        let Ok(sender_url) = sender_url else { return };
        let config = MainnetConfig {
            fanout: Default::default(),
            source_direct: true,
            fixed_priority_fee_micro_lamports: Some(100),
            sender_url,
            tip_lamports: 5_000,
            priority_level: "unsafeMax".to_owned(),
            max_priority_fee_micro_lamports: 1_000_000,
        };
        assert!(validate_sender(&config).is_err());
    }

    #[test]
    fn source_direct_requires_an_explicit_fee_within_the_cap() {
        let config: AppConfig =
            toml::from_str(include_str!("../config.example.toml")).expect("config");
        let mut mainnet = config.mainnet.expect("mainnet");
        assert!(mainnet.source_direct);
        mainnet.source_direct = true;
        mainnet.fixed_priority_fee_micro_lamports = None;
        assert!(validate_sender(&mainnet).is_err());
        mainnet.fixed_priority_fee_micro_lamports =
            Some(mainnet.max_priority_fee_micro_lamports + 1);
        assert!(validate_sender(&mainnet).is_err());
        mainnet.fixed_priority_fee_micro_lamports = Some(100000);
        assert!(validate_sender(&mainnet).is_ok());
    }

    #[test]
    fn priority_levels_are_normalized_for_helius() {
        assert_eq!(canonical_priority_level("high"), Some("High"));
        assert_eq!(canonical_priority_level("High"), Some("High"));
        assert_eq!(canonical_priority_level("veryHigh"), Some("VeryHigh"));
        assert_eq!(canonical_priority_level("unsafeMax"), None);
    }
    #[test]
    fn source_only_configuration_rejects_quote_mode_and_pool_settings() {
        let example = include_str!("../config.example.toml");
        let config: AppConfig = toml::from_str(example).expect("example");
        assert!(config.mainnet.as_ref().expect("mainnet").source_direct);
        assert!(AppConfig::load(std::path::Path::new("config.example.toml")).is_ok());
        let disabled: AppConfig = toml::from_str(&example.replace("skip = true", "skip = false"))
            .expect("parse old mode");
        assert!(
            disabled
                .validate()
                .expect_err("quote mode removed")
                .to_string()
                .contains("source instructions")
        );
        assert!(
            toml::from_str::<AppConfig>(&example.replace(
                "timeout_ms = 2000",
                "timeout_ms = 2000\npool_refresh_seconds = 60"
            ))
            .is_err()
        );
        assert!(
            toml::from_str::<AppConfig>(&example.replace(
                "timeout_ms = 2000",
                "timeout_ms = 2000\nmax_pools_per_dex = 4"
            ))
            .is_err()
        );
        let old_timeout: AppConfig =
            toml::from_str(&example.replace("timeout_ms = 2000", "race_timeout_ms = 2000"))
                .expect("timeout alias");
        assert_eq!(old_timeout.routing.timeout_ms, 2000);
    }

    #[test]
    fn removed_backends_and_seeding_configuration_are_rejected() {
        let example = include_str!("../config.example.toml");
        assert!(
            toml::from_str::<AppConfig>(
                &example.replace("target = \"mainnet\"", "target = \"surfpool\"")
            )
            .is_err()
        );
        assert!(
            toml::from_str::<AppConfig>(&format!(
                "{example}\n[surfpool]\nrpc_url = \"http://localhost:8899\"\n"
            ))
            .is_err()
        );
    }
}

const fn default_max_concurrent_sends() -> usize {
    8
}

const fn default_preparation_workers() -> usize {
    4
}
