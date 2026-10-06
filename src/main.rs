use clap::Parser;
use pump_copy_trader::{cli::Cli, run};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    pump_copy_trader::telemetry::init();
    run(Cli::parse()).await.map_err(anyhow::Error::from)
}
