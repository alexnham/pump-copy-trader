use crate::{config::ExecutionTarget, error::Result, mainnet::MainnetClient};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::Transaction};
use std::sync::Arc;

pub enum ExecutionBackend {
    Mainnet(Arc<MainnetClient>),
}
impl ExecutionBackend {
    pub const fn target(&self) -> ExecutionTarget {
        ExecutionTarget::Mainnet
    }
    pub const fn label(&self) -> &'static str {
        "mainnet"
    }
    pub fn rpc(&self) -> &RpcClient {
        match self {
            Self::Mainnet(client) => &client.rpc,
        }
    }
    pub async fn prepare_market(&self, _slot: u64, _accounts: &[Pubkey]) -> Result<()> {
        Ok(())
    }
    pub async fn send(&self, transaction: &Transaction) -> Result<Signature> {
        match self {
            Self::Mainnet(client) => client.send(transaction).await,
        }
    }
    pub fn mainnet(&self) -> Option<&MainnetClient> {
        match self {
            Self::Mainnet(client) => Some(client),
        }
    }
}
