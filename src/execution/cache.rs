use std::{collections::HashMap, sync::Arc, time::Duration};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock;

use crate::token::accounts::associated_token_address;

#[derive(Clone, Default)]
pub struct WalletBalanceCache {
    values: Arc<RwLock<HashMap<Pubkey, u64>>>,
}

impl WalletBalanceCache {
    pub async fn get(&self, address: &Pubkey) -> Option<u64> {
        self.values.read().await.get(address).copied()
    }

    pub async fn refresh(&self, rpc: &RpcClient, owner: Pubkey, mints: &[Pubkey]) {
        if let Ok(balance) = rpc.get_balance(&owner).await {
            self.values.write().await.insert(owner, balance);
        }
        for mint in mints {
            for program in [spl_token::id(), spl_token_2022::id()] {
                let account = associated_token_address(&owner, mint, &program);
                if let Ok(balance) = rpc.get_token_account_balance(&account).await
                    && let Ok(amount) = balance.amount.parse::<u64>()
                {
                    self.values.write().await.insert(account, amount);
                }
            }
        }
    }

    pub async fn run(self, rpc: Arc<RpcClient>, owner: Pubkey, mints: Vec<Pubkey>) {
        loop {
            self.refresh(&rpc, owner, &mints).await;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}
