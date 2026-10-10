use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "pump-copy-trader",
    version,
    about = "Pump.fun and PumpSwap mainnet copy trader"
)]
pub struct Cli {
    #[arg(long, env = "COPY_TRADER_CONFIG", default_value = "config.toml")]
    pub config: PathBuf,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the live signal and execution pipeline.
    Run,
    /// Enrich completed copies in a separate process without starting trading.
    TransactionGaps,
    /// Observe and decode preconfirmation messages without trading or journaling observations.
    PreconfirmationDiagnostics {
        #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=300))]
        seconds: u64,
        #[arg(long)]
        wallet: Option<solana_sdk::pubkey::Pubkey>,
    },
    /// Validate configuration and external dependencies without trading.
    Doctor,
    /// Report receipt-to-send latency percentiles from recent attempted copies.
    Latency {
        #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(1..))]
        limit: u32,
    },
    /// Print recent source and copy-attempt records.
    Status {
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
}
