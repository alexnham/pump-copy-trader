use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::{
    extension::{BaseStateWithExtensions, ExtensionType, StateWithExtensionsOwned},
    state::Mint,
};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::{
    error::{CopyTraderError, Result},
    http::HttpTransport,
};

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

#[derive(Clone, Debug)]
pub struct MintInfo {
    pub decimals: u8,
    pub token_program: Pubkey,
    pub has_transfer_fee: bool,
}

#[derive(Clone)]
pub struct TokenSafetyClient {
    rpc: Arc<RpcClient>,
    cache: Arc<Mutex<HashMap<Pubkey, (Instant, MintInfo)>>>,
    background: Arc<Mutex<HashMap<Pubkey, BackgroundMint>>>,
}

struct BackgroundMint {
    started: Instant,
    result: Option<std::result::Result<MintInfo, String>>,
}

pub fn native_mint_info() -> MintInfo {
    MintInfo {
        decimals: 9,
        token_program: spl_token::id(),
        has_transfer_fee: false,
    }
}

fn is_native_mint(mint: &Pubkey) -> bool {
    *mint == Pubkey::from_str_const(crate::domain::NATIVE_MINT)
}

impl TokenSafetyClient {
    pub fn new(rpc_url: &url::Url, transport: &HttpTransport) -> Self {
        Self {
            rpc: Arc::new(transport.solana_rpc(rpc_url)),
            background: Default::default(),
            cache: Default::default(),
        }
    }

    pub async fn inspect_mint(&self, mint: &Pubkey) -> Result<MintInfo> {
        if is_native_mint(mint) {
            return Ok(native_mint_info());
        }
        Ok(self.inspect_pair(*mint, *mint).await?.0)
    }

    pub async fn inspect_pair(
        &self,
        input: Pubkey,
        output: Pubkey,
    ) -> Result<(MintInfo, MintInfo)> {
        const TTL: Duration = Duration::from_secs(60);
        const MAX_ENTRIES: usize = 4096;
        let mut cache = self.cache.lock().await;
        cache.retain(|_, (fetched, _)| fetched.elapsed() < TTL);
        let mut missing = Vec::with_capacity(2);
        for mint in [input, output] {
            if !cache.contains_key(&mint) && !missing.contains(&mint) {
                missing.push(mint);
            }
        }
        if !missing.is_empty() {
            let accounts = self
                .rpc
                .get_multiple_accounts(&missing)
                .await
                .map_err(|error| {
                    CopyTraderError::Execution(format!("failed to fetch trade mints: {error}"))
                })?;
            if accounts.len() != missing.len() {
                return Err(CopyTraderError::Execution(
                    "incomplete mint response".to_owned(),
                ));
            }
            let mut validated = Vec::with_capacity(missing.len());
            for (mint, account) in missing.iter().zip(accounts) {
                let account = account.ok_or_else(|| {
                    CopyTraderError::Execution(format!("mint {mint} does not exist"))
                })?;
                validated.push((*mint, Self::validate_mint(mint, account)?));
            }
            for (mint, info) in validated {
                if cache.len() >= MAX_ENTRIES {
                    let victim = cache
                        .keys()
                        .copied()
                        .find(|key| *key != input && *key != output);
                    if let Some(victim) = victim {
                        cache.remove(&victim);
                    }
                }
                cache.insert(mint, (Instant::now(), info));
            }
        }
        let get = |mint| {
            cache
                .get(&mint)
                .map(|(_, info)| info.clone())
                .ok_or_else(|| CopyTraderError::Execution(format!("missing validated mint {mint}")))
        };
        Ok((get(input)?, get(output)?))
    }

    pub async fn source_info(&self, mint: Pubkey, assumed: MintInfo) -> Result<MintInfo> {
        if is_native_mint(&mint) {
            return Ok(native_mint_info());
        }
        let mut cache = self.background.lock().await;
        cache.retain(|_, entry| entry.started.elapsed() < Duration::from_secs(60));
        if let Some(entry) = cache.get(&mint) {
            return match &entry.result {
                Some(Ok(info))
                    if info.decimals != assumed.decimals
                        || info.token_program != assumed.token_program =>
                {
                    Err(CopyTraderError::Execution(format!(
                        "source mint metadata disagrees with inspected mint {mint}"
                    )))
                }
                Some(Ok(info)) => Ok(info.clone()),
                Some(Err(error)) => Err(CopyTraderError::OutOfScope(
                    crate::domain::UnsupportedReason::UnsupportedToken,
                    error.clone(),
                )),
                None => Ok(assumed),
            };
        }
        let started = Instant::now();
        cache.insert(
            mint,
            BackgroundMint {
                started,
                result: None,
            },
        );
        let client = self.clone();
        tokio::spawn(async move {
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                let account = client.rpc.get_account(&mint).await.map_err(|error| {
                    CopyTraderError::Execution(format!("failed to fetch mint {mint}: {error}"))
                })?;
                Self::validate_mint(&mint, account)
            })
            .await;
            let mut cache = client.background.lock().await;
            if !cache
                .get(&mint)
                .is_some_and(|entry| entry.started == started)
            {
                return;
            }
            match result {
                Ok(Ok(info)) => {
                    cache.insert(
                        mint,
                        BackgroundMint {
                            started,
                            result: Some(Ok(info)),
                        },
                    );
                }
                Ok(Err(
                    error @ (CopyTraderError::Unsupported(_) | CopyTraderError::OutOfScope(_, _)),
                )) => {
                    warn!(%mint, %error, "background mint inspection rejected mint");
                    cache.insert(
                        mint,
                        BackgroundMint {
                            started,
                            result: Some(Err(error.to_string())),
                        },
                    );
                }
                _ => {
                    warn!(%mint, "background mint inspection failed; retry on next source trade");
                    cache.remove(&mint);
                }
            }
        });
        Ok(assumed)
    }

    fn validate_mint(mint: &Pubkey, account: solana_sdk::account::Account) -> Result<MintInfo> {
        let classic_program: Pubkey = TOKEN_PROGRAM.parse().map_err(|error| {
            CopyTraderError::Configuration(format!("invalid token program constant: {error}"))
        })?;
        let token_2022_program: Pubkey = TOKEN_2022_PROGRAM.parse().map_err(|error| {
            CopyTraderError::Configuration(format!("invalid Token-2022 program constant: {error}"))
        })?;
        if account.owner == classic_program {
            let state =
                StateWithExtensionsOwned::<Mint>::unpack(account.data).map_err(|error| {
                    CopyTraderError::Execution(format!("invalid classic mint {mint}: {error}"))
                })?;
            let info = MintInfo {
                decimals: state.base.decimals,
                token_program: classic_program,
                has_transfer_fee: false,
            };
            debug!(%mint, decimals = info.decimals, program = "spl-token", "mint inspected");
            return Ok(info);
        }
        if account.owner != token_2022_program {
            return Err(CopyTraderError::OutOfScope(
                crate::domain::UnsupportedReason::UnsupportedToken,
                format!("mint {mint} is not owned by a supported token program"),
            ));
        }
        let state = StateWithExtensionsOwned::<Mint>::unpack(account.data).map_err(|error| {
            CopyTraderError::Execution(format!("invalid Token-2022 mint {mint}: {error}"))
        })?;
        let extensions = state.get_extension_types().map_err(|error| {
            CopyTraderError::Execution(format!("cannot inspect Token-2022 mint {mint}: {error}"))
        })?;
        let extension_count = extensions.len();
        let mut has_transfer_fee = false;
        for extension in extensions {
            match extension {
                ExtensionType::TransferFeeConfig => has_transfer_fee = true,
                ExtensionType::InterestBearingConfig
                | ExtensionType::MetadataPointer
                | ExtensionType::TokenMetadata
                | ExtensionType::GroupPointer
                | ExtensionType::TokenGroup
                | ExtensionType::GroupMemberPointer
                | ExtensionType::TokenGroupMember
                | ExtensionType::ScaledUiAmount
                | ExtensionType::Uninitialized => {}
                unsupported => {
                    return Err(CopyTraderError::OutOfScope(
                        crate::domain::UnsupportedReason::UnsupportedToken,
                        format!("Token-2022 extension {unsupported:?} is not enabled for v1"),
                    ));
                }
            }
        }
        let info = MintInfo {
            decimals: state.base.decimals,
            token_program: token_2022_program,
            has_transfer_fee,
        };
        debug!(%mint, decimals = info.decimals, program = "token-2022", extensions = extension_count, has_transfer_fee, "mint inspected");
        Ok(info)
    }
}

pub fn decimal_to_atomic(value: &str, decimals: u8) -> Result<u64> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > usize::from(decimals) {
        return Err(CopyTraderError::Configuration(format!(
            "{value} has more fractional digits than mint decimals {decimals}"
        )));
    }
    let whole = whole.parse::<u128>().map_err(|error| {
        CopyTraderError::Configuration(format!("invalid decimal amount {value}: {error}"))
    })?;
    let scale = 10_u128
        .checked_pow(u32::from(decimals))
        .ok_or_else(|| CopyTraderError::Configuration("mint decimal scale overflow".to_owned()))?;
    let padded_fraction = if fraction.is_empty() {
        0
    } else {
        let raw = fraction.parse::<u128>().map_err(|error| {
            CopyTraderError::Configuration(format!("invalid decimal amount {value}: {error}"))
        })?;
        let padding = u32::from(decimals)
            .checked_sub(u32::try_from(fraction.len()).map_err(|_| {
                CopyTraderError::Configuration("fraction length overflow".to_owned())
            })?)
            .ok_or_else(|| CopyTraderError::Configuration("fraction scale underflow".to_owned()))?;
        raw.checked_mul(
            10_u128.checked_pow(padding).ok_or_else(|| {
                CopyTraderError::Configuration("fraction scale overflow".to_owned())
            })?,
        )
        .ok_or_else(|| CopyTraderError::Configuration("fraction amount overflow".to_owned()))?
    };
    let atomic = whole
        .checked_mul(scale)
        .and_then(|amount| amount.checked_add(padded_fraction))
        .ok_or_else(|| CopyTraderError::Configuration("token amount overflow".to_owned()))?;
    u64::try_from(atomic)
        .map_err(|_| CopyTraderError::Configuration("token amount exceeds u64".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn source_metadata_does_not_wait_and_background_rejection_is_reused() {
        use crate::{
            config::HttpConfig,
            test_rpc::{TestRpc, mint_account},
        };
        use serde_json::json;
        let server = TestRpc::start(|request| {
            assert_eq!(request["method"], "getAccountInfo");
            let mut account = mint_account();
            account["owner"] = json!(Pubkey::default().to_string());
            json!({"test_delay_ms":150,"test_result":{"context":{"slot":42},"value":account}})
        })
        .await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let client = TokenSafetyClient::new(&server.url, &transport);
        let mint = Pubkey::new_unique();
        let info = MintInfo {
            decimals: 6,
            token_program: spl_token_2022::id(),
            has_transfer_fee: false,
        };
        tokio::time::timeout(
            Duration::from_millis(50),
            client.source_info(mint, info.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        client.source_info(mint, info.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if client
                    .background
                    .lock()
                    .await
                    .get(&mint)
                    .is_some_and(|entry| entry.result.is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(client.source_info(mint, info).await.is_err());
        assert_eq!(server.count("getAccountInfo"), 1);
    }

    #[tokio::test]
    async fn background_metadata_mismatch_is_rejected() {
        use crate::{
            config::HttpConfig,
            test_rpc::{TestRpc, mint_account},
        };
        use serde_json::json;
        let server =
            TestRpc::start(|_| json!({"context":{"slot":42},"value":mint_account()})).await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let client = TokenSafetyClient::new(&server.url, &transport);
        let mint = Pubkey::new_unique();
        let info = MintInfo {
            decimals: 9,
            token_program: spl_token::id(),
            has_transfer_fee: false,
        };
        client.source_info(mint, info.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if client
                    .background
                    .lock()
                    .await
                    .get(&mint)
                    .is_some_and(|entry| entry.result.is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            client
                .source_info(mint, info)
                .await
                .unwrap_err()
                .to_string()
                .contains("disagrees")
        );
    }

    #[tokio::test]
    async fn pair_reads_are_batched_and_validate_missing_or_unsupported_mints() {
        use crate::{
            config::HttpConfig,
            test_rpc::{TestRpc, mint_account},
        };
        use serde_json::json;
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();
        for scenario in 0..3 {
            let server = TestRpc::start(move |request| {
                let mut accounts = request["params"][0]
                    .as_array()
                    .expect("addresses")
                    .iter()
                    .map(|_| mint_account())
                    .collect::<Vec<_>>();
                if scenario == 1 {
                    accounts[0] = serde_json::Value::Null;
                }
                if scenario == 2 {
                    accounts[0]["owner"] = json!(Pubkey::default().to_string());
                }
                json!({"context":{"slot":42},"value":accounts})
            })
            .await;
            let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
            let client = TokenSafetyClient::new(&server.url, &transport);
            let result = client.inspect_pair(input, output).await;
            assert_eq!(result.is_ok(), scenario == 0);
            assert_eq!(server.count("getMultipleAccounts"), 1);
            assert_eq!(server.count("getAccountInfo"), 0);
            if scenario == 0 {
                assert_eq!(result.expect("mints").0.decimals, 6);
                client.inspect_pair(input, input).await.expect("same mint");
                client
                    .inspect_pair(output, input)
                    .await
                    .expect("reversed cached pair");
                assert_eq!(server.count("getMultipleAccounts"), 1);
                client
                    .inspect_pair(input, Pubkey::new_unique())
                    .await
                    .expect("one new mint");
                assert_eq!(server.count("getMultipleAccounts"), 2);
                let requests = server.requests.lock().expect("requests");
                assert_eq!(
                    requests.last().expect("request")["params"][0]
                        .as_array()
                        .expect("addresses")
                        .len(),
                    1
                );
            }
        }
    }

    #[tokio::test]
    async fn cached_token_2022_pair_refreshes_after_expiry() {
        use crate::{
            config::HttpConfig,
            test_rpc::{TestRpc, mint_account},
        };
        use serde_json::json;
        let server = TestRpc::start(|request| {
            let accounts: Vec<_> = request["params"][0]
                .as_array()
                .expect("mints")
                .iter()
                .map(|_| {
                    let mut account = mint_account();
                    account["owner"] = json!(spl_token_2022::id().to_string());
                    account
                })
                .collect();
            json!({"context":{"slot":42},"value":accounts})
        })
        .await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let client = TokenSafetyClient::new(&server.url, &transport);
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();
        let first = client
            .inspect_pair(input, output)
            .await
            .expect("first read");
        assert_eq!(first.1.token_program, spl_token_2022::id());
        client
            .inspect_pair(output, input)
            .await
            .expect("cached sell");
        client
            .inspect_mint(&output)
            .await
            .expect("cached single mint");
        assert_eq!(server.count("getMultipleAccounts"), 1);
        {
            let mut cache = client.cache.lock().await;
            cache.get_mut(&output).expect("cached output").0 =
                Instant::now() - Duration::from_secs(61);
        }
        client.inspect_pair(input, output).await.expect("refresh");
        assert_eq!(server.count("getMultipleAccounts"), 2);
        assert_eq!(
            server
                .requests
                .lock()
                .expect("requests")
                .last()
                .expect("request")["params"][0]
                .as_array()
                .expect("mints")
                .len(),
            1
        );
    }

    #[test]
    fn converts_decimal_without_floating_point() {
        assert_eq!(decimal_to_atomic("1.25", 6).ok(), Some(1_250_000));
        assert_eq!(decimal_to_atomic("1", 9).ok(), Some(1_000_000_000));
        assert!(decimal_to_atomic("0.0000001", 6).is_err());
    }
}
