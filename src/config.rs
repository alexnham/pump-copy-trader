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
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MainnetConfig {
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
            toml::from_str(&example.replace("timeout_ms =", "race_timeout_ms ="))
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
