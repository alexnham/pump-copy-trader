use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::{
    extension::{BaseStateWithExtensions, ExtensionType, StateWithExtensionsOwned},
    state::Mint,
};
use tracing::debug;

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

pub struct TokenSafetyClient {
    rpc: RpcClient,
}

impl TokenSafetyClient {
    pub fn new(rpc_url: &url::Url, transport: &HttpTransport) -> Self {
        Self {
            rpc: transport.solana_rpc(rpc_url),
        }
    }

    pub async fn inspect_mint(&self, mint: &Pubkey) -> Result<MintInfo> {
        let account = self.rpc.get_account(mint).await.map_err(|error| {
            CopyTraderError::Execution(format!("failed to fetch mint {mint}: {error}"))
        })?;
        Self::validate_mint(mint, account)
    }

    pub async fn inspect_pair(
        &self,
        input: Pubkey,
        output: Pubkey,
    ) -> Result<(MintInfo, MintInfo)> {
        let mints = if input == output {
            vec![input]
        } else {
            vec![input, output]
        };
        let accounts = self
            .rpc
            .get_multiple_accounts(&mints)
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("failed to fetch trade mints: {error}"))
            })?;
        let mut infos = Vec::with_capacity(mints.len());
        for (mint, account) in mints.iter().zip(accounts) {
            let account = account
                .ok_or_else(|| CopyTraderError::Execution(format!("mint {mint} does not exist")))?;
            infos.push(Self::validate_mint(mint, account)?);
        }
        let first = infos
            .first()
            .cloned()
            .ok_or_else(|| CopyTraderError::Execution("missing mint response".to_owned()))?;
        let second = if input == output {
            first.clone()
        } else {
            infos.get(1).cloned().ok_or_else(|| {
                CopyTraderError::Execution("missing output mint response".to_owned())
            })?
        };
        Ok((first, second))
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
    async fn batched_mints_reduce_controlled_network_latency() {
        use crate::{
            config::HttpConfig,
            test_rpc::{TestRpc, mint_account},
        };
        use serde_json::json;
        let server = TestRpc::start(|request| {
            let value = if request["method"] == "getMultipleAccounts" {
                json!([mint_account(), mint_account()])
            } else {
                mint_account()
            };
            json!({"test_delay_ms":75,"test_result":{"context":{"slot":42},"value":value}})
        })
        .await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let client = TokenSafetyClient::new(&server.url, &transport);
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();
        let started = std::time::Instant::now();
        client.inspect_mint(&input).await.expect("baseline input");
        client.inspect_mint(&output).await.expect("baseline output");
        let sequential = started.elapsed();
        let started = std::time::Instant::now();
        client
            .inspect_pair(input, output)
            .await
            .expect("batched pair");
        let batched = started.elapsed();
        assert_eq!(server.count("getAccountInfo"), 2);
        assert_eq!(server.count("getMultipleAccounts"), 1);
        assert!(
            batched < sequential,
            "batched={batched:?}, sequential={sequential:?}"
        );
        eprintln!(
            "controlled 75ms RPC fixture: sequential mint reads={sequential:?}, batched={batched:?}"
        );
    }

    #[test]
    fn converts_decimal_without_floating_point() {
        assert_eq!(decimal_to_atomic("1.25", 6).ok(), Some(1_250_000));
        assert_eq!(decimal_to_atomic("1", 9).ok(), Some(1_000_000_000));
        assert!(decimal_to_atomic("0.0000001", 6).is_err());
    }
}
