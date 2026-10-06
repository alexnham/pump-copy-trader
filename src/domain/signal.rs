use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_sdk::{signature::Signature, transaction::VersionedTransaction};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalOrigin {
    Live,
    Recovery,
}

impl SignalOrigin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Recovery => "recovery",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObservedTransaction {
    pub signature: Signature,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub origin: SignalOrigin,
    pub transaction: VersionedTransaction,
    pub meta: TransactionMeta,
    pub raw_payload: String,
    pub received_bytes: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionMeta {
    pub err: Option<Value>,
    #[serde(default)]
    pub inner_instructions: Option<Vec<UiInnerInstructions>>,
    #[serde(default)]
    pub log_messages: Option<Vec<String>>,
    #[serde(default)]
    pub pre_token_balances: Option<Vec<UiTokenBalance>>,
    #[serde(default)]
    pub post_token_balances: Option<Vec<UiTokenBalance>>,
    #[serde(default)]
    pub loaded_addresses: Option<LoadedAddresses>,
    #[serde(default)]
    pub compute_units_consumed: Option<u64>,
    #[serde(default)]
    pub pre_balances: Vec<u64>,
    #[serde(default)]
    pub post_balances: Vec<u64>,
    #[serde(default)]
    pub fee: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UiInnerInstructions {
    pub index: u8,
    pub instructions: Vec<UiCompiledInstruction>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UiCompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
    #[serde(default)]
    pub stack_height: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LoadedAddresses {
    #[serde(default)]
    pub writable: Vec<String>,
    #[serde(default)]
    pub readonly: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UiTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: UiTokenAmount,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub program_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UiTokenAmount {
    pub amount: String,
    pub decimals: u8,
}
