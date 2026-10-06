use serde::{Deserialize, Serialize};
use solana_sdk::{instruction::Instruction, pubkey::Pubkey, signature::Signature};

pub const NATIVE_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "mint")]
pub enum AssetId {
    NativeSol,
    Token(Pubkey),
}

impl AssetId {
    pub fn routing_mint(self) -> Pubkey {
        match self {
            Self::NativeSol => Pubkey::from_str_const(NATIVE_MINT),
            Self::Token(mint) => mint,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TradeIntent {
    pub source_pool: Option<SourcePool>,
    pub source_instruction: Option<SourceInstruction>,
    pub source_signature: Signature,
    pub slot: u64,
    pub input_asset: AssetId,
    pub output_asset: AssetId,
    pub source_input_amount: u64,
    pub source_output_amount: u64,
}

#[derive(Clone, Debug)]
pub struct SourceInstruction {
    pub instruction: Instruction,
    pub source_wallet: Pubkey,
    pub wallet_token_accounts: Vec<(Pubkey, Pubkey, Pubkey)>,
}

/// An untrusted pool address extracted from one recognized source swap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourcePool {
    pub dex: DexKind,
    pub address: Pubkey,
}

#[derive(Clone, Debug)]
pub struct SizedTrade {
    pub intent: TradeIntent,
    pub input_amount: u64,
}

#[derive(Debug)]
pub struct PreparedRoute {
    pub dex: DexKind,
    pub pool: Pubkey,
    pub instructions: Vec<Instruction>,
    pub additional_signers: Vec<solana_sdk::signature::Keypair>,
    pub market_accounts: Vec<Pubkey>,
    pub expected_output: u64,
    pub minimum_output: u64,
    pub compute_unit_limit: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
// Excluded venue identities are retained only to reject mixed-venue sources.
pub enum DexKind {
    RaydiumCpmm,
    RaydiumClmm,
    OrcaWhirlpool,
    MeteoraDlmm,
    PumpSwap,
    PumpFun,
}

impl DexKind {
    pub const fn program_id(self) -> Pubkey {
        match self {
            Self::RaydiumCpmm => {
                solana_sdk::pubkey!("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C")
            }
            Self::RaydiumClmm => {
                solana_sdk::pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK")
            }
            Self::OrcaWhirlpool => {
                solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc")
            }
            Self::MeteoraDlmm => solana_sdk::pubkey!("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"),
            Self::PumpSwap => solana_sdk::pubkey!("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"),
            Self::PumpFun => solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RaydiumCpmm => "raydium_cpmm",
            Self::RaydiumClmm => "raydium_clmm",
            Self::OrcaWhirlpool => "orca_whirlpool",
            Self::MeteoraDlmm => "meteora_dlmm",
            Self::PumpSwap => "pump_swap",
            Self::PumpFun => "pump_fun",
        }
    }
}
