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
    /// Validate configuration and external dependencies without trading.
    Doctor,
    /// Print recent source and copy-attempt records.
    Status {
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
}
