use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    ObservedLive,
    MissedOffline,
    Decoded,
    Skipped,
    Unsupported,
    Prepared,
    Submitting,
    Landed,
    Failed,
    Unknown,
}

impl AttemptStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ObservedLive => "observed_live",
            Self::MissedOffline => "missed_offline",
            Self::Decoded => "decoded",
            Self::Skipped => "skipped",
            Self::Unsupported => "unsupported",
            Self::Prepared => "prepared",
            Self::Submitting => "submitting",
            Self::Landed => "landed",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    RecoveredOffline,
    Duplicate,
    FailedSource,
    SourceWalletNotSigner,
    NoWalletDebit,
    MultipleSwaps,
    UnsupportedDex,
    UnsupportedInstruction,
    ExactOutput,
    MultiHop,
    AmbiguousAttribution,
    MintNotAllowed,
    BelowMinimum,
    AboveMaximum,
    InsufficientBalance,
    UnsupportedTokenExtension,
    StaleSignal,
    OutOfOrder,
    SimulationFailed,
}

impl SkipReason {
    pub fn as_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "\"serialization_error\"".to_owned())
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionRecord {
    pub source_signature: String,
    pub local_signature: Option<String>,
    pub status: AttemptStatus,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedReason {
    UnsupportedDex,
    UnsupportedInstruction,
    UnsupportedToken,
    AmbiguousTrade,
    MultiHop,
}
