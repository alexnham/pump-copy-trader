mod math;
pub(crate) mod pump_fun;
mod pump_swap;
mod timings;
pub use timings::RouteStages;
#[cfg(test)]
mod pump_fun_tests;
#[cfg(test)]
mod pump_swap_tests;
#[cfg(test)]
mod source_tests;

use crate::{
    domain::{AssetId, DexKind, NATIVE_MINT, PreparedRoute, SizedTrade},
    error::{CopyTraderError, Result},
    execution::ExecutionBackend,
    token::accounts::associated_token_address,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;
use tracing::info;

pub struct ExecutableRoute {
    pub route: PreparedRoute,
    pub transaction: Transaction,
    pub simulation_json: String,
    pub output_balance_before: Option<u64>,
}

#[derive(Default)]
pub struct RoutingTimings {
    pub stages: RouteStages,
}

pub struct Router {
    rpc: Arc<RpcClient>,
    timeout: Duration,
}

impl Router {
    pub fn new(rpc: Arc<RpcClient>, timeout: Duration) -> Self {
        Self { rpc, timeout }
    }

    pub async fn build(
        &self,
        trade: &SizedTrade,
        signer: &Keypair,
        backend: &ExecutionBackend,
        slippage_bps: u16,
        timings: &mut RoutingTimings,
    ) -> Result<ExecutableRoute> {
        timeout(self.timeout, async {
            let source = trade.intent.source_instruction.as_ref().ok_or_else(||
                CopyTraderError::Unsupported("a decoded Pump source instruction is required".to_owned()))?;
            if source.instruction.program_id != pump_fun::PROGRAM_ID
                && source.instruction.program_id != DexKind::PumpSwap.program_id() {
                return Err(CopyTraderError::OutOfScope(crate::domain::UnsupportedReason::UnsupportedDex,
                    "unsupported source program".to_owned()));
            }
            let (expected_output, minimum_output) = source_outputs(trade, slippage_bps)?;
            let mut route = {
                let _timer = timings.stages.start("route_instruction_build_ms");
                if source.instruction.program_id == pump_fun::PROGRAM_ID {
                    pump_fun::copy_source_instruction(source, trade, signer.pubkey(), expected_output, minimum_output)?
                } else {
                    pump_swap::copy_source_instruction(source, trade, signer.pubkey(), expected_output, minimum_output)?
                }
            };
            if route.dex == DexKind::PumpSwap {
                if trade.intent.source_pool.is_some_and(|hint|
                    hint.dex != DexKind::PumpSwap || hint.address != route.pool) {
                    return Err(CopyTraderError::Unsupported("PumpSwap source pool does not match its instruction".to_owned()));
                }
                if matches!((trade.intent.input_asset, trade.intent.output_asset),
                    (AssetId::NativeSol, _) | (_, AssetId::NativeSol)) {
                    let _timer = timings.stages.start("route_wsol_check_ms");
                    let wsol = associated_token_address(&signer.pubkey(), &Pubkey::from_str_const(NATIVE_MINT), &spl_token::id());
                    let existing = self.rpc.get_account_with_commitment(&wsol, CommitmentConfig::confirmed())
                        .await.map_err(|error| CopyTraderError::Execution(format!("cannot inspect copier WSOL account: {error}")))?;
                    if existing.value.is_some() {
                        return Err(CopyTraderError::OutOfScope(crate::domain::UnsupportedReason::UnsupportedToken,
                            "native PumpSwap source copies require no persistent copier WSOL account".to_owned()));
                    }
                }
            }
            let mainnet = backend.mainnet().ok_or_else(|| CopyTraderError::Execution("mainnet backend is required".to_owned()))?;
            let blockhash = {
                let _timer = timings.stages.start("route_shared_preparation_ms");
                mainnet.cached_blockhash().ok_or_else(|| CopyTraderError::Execution("background blockhash cache is not ready".to_owned()))?
            };
            let instructions = mainnet.finalize_instructions(&signer.pubkey(), &trade.intent.source_signature,
                route.compute_unit_limit, route.instructions.clone()).await?;
            let mut signers: Vec<&dyn Signer> = vec![signer];
            signers.extend(route.additional_signers.iter().map(|signer| signer as &dyn Signer));
            let transaction = Transaction::new_signed_with_payer(&instructions, Some(&signer.pubkey()), &signers, blockhash);
            route.instructions = instructions;
            info!(dex = route.dex.as_str(), "source instruction copied");
            Ok(ExecutableRoute { route, transaction,
                simulation_json: "{\"skipped\":true,\"reason\":\"hot_path_no_simulation\"}".to_owned(),
                output_balance_before: None })
        }).await.map_err(|_| CopyTraderError::Execution("source instruction build timed out".to_owned()))?
    }
}

fn source_outputs(trade: &SizedTrade, slippage_bps: u16) -> Result<(u64, u64)> {
    let expected = u128::from(trade.input_amount)
        .checked_mul(u128::from(trade.intent.source_output_amount))
        .and_then(|value| value.checked_div(u128::from(trade.intent.source_input_amount)))
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            CopyTraderError::Execution("invalid source-price output estimate".to_owned())
        })?;
    Ok((expected, math::apply_slippage(expected, slippage_bps)?))
}
