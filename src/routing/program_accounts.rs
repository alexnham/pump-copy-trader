use std::{collections::HashSet, fmt, str::FromStr, time::Duration};

use serde::Deserialize;
use serde_json::{Value, json};
use solana_account_decoder_client_types::{UiAccount, UiAccountEncoding};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::RpcFilterType,
};
use solana_sdk::{account::Account, pubkey::Pubkey};

/// Program-owned pool accounts are all larger than the 128-byte limit of the
/// legacy base58 RPC encoding. Always make the response encoding explicit.
pub(super) fn program_accounts_config(filters: Vec<RpcFilterType>) -> RpcProgramAccountsConfig {
    RpcProgramAccountsConfig {
        filters: Some(filters),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            ..RpcAccountInfoConfig::default()
        },
        ..RpcProgramAccountsConfig::default()
    }
}

const PAGE_LIMIT: u64 = 1_000;

#[derive(Debug)]
pub(super) struct ProgramAccountsV2Error {
    code: Option<i64>,
    message: String,
}

impl ProgramAccountsV2Error {
    pub(super) fn is_method_not_found(&self) -> bool {
        self.code == Some(-32601)
    }
}

impl fmt::Display for ProgramAccountsV2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code {
            Some(code) => write!(formatter, "RPC error {code}: {}", self.message),
            None => formatter.write_str(&self.message),
        }
    }
}

#[derive(Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProgramAccountsPage {
    accounts: Vec<KeyedUiAccount>,
    pagination_key: Option<String>,
}

#[derive(Deserialize)]
struct KeyedUiAccount {
    pubkey: String,
    account: UiAccount,
}

/// Fetch every matching account through Helius' cursor-based extension.
/// Callers may fall back to standard `getProgramAccounts` when the endpoint
/// reports that the V2 method is unavailable.
pub(super) async fn get_program_accounts_v2(
    rpc: &RpcClient,
    program_id: &Pubkey,
    config: &RpcProgramAccountsConfig,
) -> std::result::Result<Vec<(Pubkey, Account)>, ProgramAccountsV2Error> {
    let client = reqwest::Client::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut accounts = Vec::new();

    loop {
        let mut page_config =
            serde_json::to_value(config).map_err(|error| ProgramAccountsV2Error {
                code: None,
                message: format!("cannot encode program-account config: {error}"),
            })?;
        let Value::Object(fields) = &mut page_config else {
            unreachable!("RPC program-account config serializes as an object");
        };
        fields.insert("limit".to_owned(), Value::from(PAGE_LIMIT));
        if let Some(cursor) = &cursor {
            fields.insert("paginationKey".to_owned(), Value::String(cursor.clone()));
        }

        let response = client
            .post(rpc.url())
            .timeout(Duration::from_secs(30))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getProgramAccountsV2",
                "params": [program_id.to_string(), page_config],
            }))
            .send()
            .await
            .map_err(|error| ProgramAccountsV2Error {
                code: None,
                message: format!("getProgramAccountsV2 request failed: {error}"),
            })?
            .error_for_status()
            .map_err(|error| ProgramAccountsV2Error {
                code: None,
                message: format!("getProgramAccountsV2 HTTP failure: {error}"),
            })?
            .json::<RpcEnvelope<ProgramAccountsPage>>()
            .await
            .map_err(|error| ProgramAccountsV2Error {
                code: None,
                message: format!("invalid getProgramAccountsV2 response: {error}"),
            })?;

        if let Some(error) = response.error {
            return Err(ProgramAccountsV2Error {
                code: Some(error.code),
                message: error.message,
            });
        }
        let page = response.result.ok_or_else(|| ProgramAccountsV2Error {
            code: None,
            message: "getProgramAccountsV2 response omitted result".to_owned(),
        })?;
        for item in page.accounts {
            let address =
                Pubkey::from_str(&item.pubkey).map_err(|error| ProgramAccountsV2Error {
                    code: None,
                    message: format!("invalid program-account pubkey: {error}"),
                })?;
            let account =
                item.account
                    .decode::<Account>()
                    .ok_or_else(|| ProgramAccountsV2Error {
                        code: None,
                        message: format!("cannot decode program account {address}"),
                    })?;
            accounts.push((address, account));
        }

        let Some(next_cursor) = page.pagination_key else {
            break;
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err(ProgramAccountsV2Error {
                code: None,
                message: "getProgramAccountsV2 repeated a pagination key".to_owned(),
            });
        }
        cursor = Some(next_cursor);
    }

    Ok(accounts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_queries_explicitly_request_base64() {
        let value = serde_json::to_value(program_accounts_config(vec![])).unwrap();
        assert_eq!(value["encoding"], "base64");
    }
}
