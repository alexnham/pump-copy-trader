use crate::{config::ExecutionTarget, error::Result, mainnet::MainnetClient};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Signature, transaction::Transaction};
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
