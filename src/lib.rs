pub mod app;
pub mod cli;
pub mod config;
pub mod decode;
pub mod domain;
pub mod error;
pub mod execution;
pub mod http;
pub mod mainnet;
pub mod routing;
pub mod signal;
pub mod storage;
pub mod telemetry;
pub mod token;

pub use app::run;

#[cfg(test)]
mod test_rpc;
