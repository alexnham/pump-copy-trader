mod math;
pub(crate) mod pump_fun;
pub(crate) mod pump_swap;
mod timings;
pub use timings::RouteStages;
#[cfg(test)]
mod pump_fun_tests;
#[cfg(test)]
mod pump_swap_tests;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
use crate::{
    domain::{AssetId, NATIVE_MINT},
    token::accounts::associated_token_address,
};
#[cfg(test)]
use solana_sdk::pubkey::Pubkey;

use crate::{
    domain::{DexKind, PreparedRoute, SizedTrade},
    error::{CopyTraderError, Result},
    execution::ExecutionBackend,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;
use tracing::info;

pub struct ExecutableRoute {
    pub route: PreparedRoute,
    pub transaction: Transaction,
    pub variants: Vec<Transaction>,
    pub variant_route_indices: Vec<usize>,
    pub nonce: Option<crate::mainnet::nonce::NonceLease>,
    pub simulation_json: String,
}

#[derive(Default)]
pub struct RoutingTimings {
    pub stages: RouteStages,
}

pub struct Router {
    timeout: Duration,
}

impl Router {
    pub fn new(_rpc: Arc<RpcClient>, timeout: Duration) -> Self {
        Self { timeout }
    }

    pub async fn build(
        &self,
        trade: &SizedTrade,
        signer: &Keypair,
        backend: &ExecutionBackend,
        slippage_bps: u16,
        timings: &mut RoutingTimings,
    ) -> Result<ExecutableRoute> {
        self.build_with_wsol(trade, signer, backend, slippage_bps, timings, 0)
            .await
    }

    pub async fn build_with_wsol(
        &self,
        trade: &SizedTrade,
        signer: &Keypair,
        backend: &ExecutionBackend,
        slippage_bps: u16,
        timings: &mut RoutingTimings,
        cached_wsol: u64,
    ) -> Result<ExecutableRoute> {
        timeout(self.timeout, async {
            let source = trade.intent.source_instruction.as_ref().ok_or_else(|| {
                CopyTraderError::Unsupported(
                    "a decoded Pump source instruction is required".to_owned(),
                )
            })?;
            if source.instruction.program_id != pump_fun::PROGRAM_ID
                && source.instruction.program_id != DexKind::PumpSwap.program_id()
            {
                return Err(CopyTraderError::OutOfScope(
                    crate::domain::UnsupportedReason::UnsupportedDex,
                    "unsupported source program".to_owned(),
                ));
            }
            let (expected_output, minimum_output) = source_outputs(trade, slippage_bps)?;
            let mut route = {
                let _timer = timings.stages.start("route_instruction_build_ms");
                if source.instruction.program_id == pump_fun::PROGRAM_ID {
                    pump_fun::copy_source_instruction(
                        source,
                        trade,
                        signer.pubkey(),
                        expected_output,
                        minimum_output,
                    )?
                } else {
                    pump_swap::copy_source_instruction_with_wsol(
                        source,
                        trade,
                        signer.pubkey(),
                        expected_output,
                        minimum_output,
                        cached_wsol,
                    )?
                }
            };
            if route.dex == DexKind::PumpSwap
                && trade
                    .intent
                    .source_pool
                    .is_some_and(|hint| hint.dex != DexKind::PumpSwap || hint.address != route.pool)
            {
                return Err(CopyTraderError::Unsupported(
                    "PumpSwap source pool does not match its instruction".to_owned(),
                ));
            }
            let mainnet = backend.mainnet().ok_or_else(|| {
                CopyTraderError::Execution("mainnet backend is required".to_owned())
            })?;
            let nonce = if mainnet.fanout.enabled {
                Some(mainnet.nonce_pool.reserve()?)
            } else {
                None
            };
            let blockhash = if let Some(lease) = &nonce {
                lease.hash
            } else {
                let _timer = timings.stages.start("route_shared_preparation_ms");
                mainnet.cached_blockhash().ok_or_else(|| {
                    CopyTraderError::Execution("background blockhash cache is not ready".to_owned())
                })?
            };
            const BUILD_KEYS: [&str; 32] = [
                "variant_0_build_us",
                "variant_1_build_us",
                "variant_2_build_us",
                "variant_3_build_us",
                "variant_4_build_us",
                "variant_5_build_us",
                "variant_6_build_us",
                "variant_7_build_us",
                "variant_8_build_us",
                "variant_9_build_us",
                "variant_10_build_us",
                "variant_11_build_us",
                "variant_12_build_us",
                "variant_13_build_us",
                "variant_14_build_us",
                "variant_15_build_us",
                "variant_16_build_us",
                "variant_17_build_us",
                "variant_18_build_us",
                "variant_19_build_us",
                "variant_20_build_us",
                "variant_21_build_us",
                "variant_22_build_us",
                "variant_23_build_us",
                "variant_24_build_us",
                "variant_25_build_us",
                "variant_26_build_us",
                "variant_27_build_us",
                "variant_28_build_us",
                "variant_29_build_us",
                "variant_30_build_us",
                "variant_31_build_us",
            ];
            const SIGN_KEYS: [&str; 32] = [
                "variant_0_sign_us",
                "variant_1_sign_us",
                "variant_2_sign_us",
                "variant_3_sign_us",
                "variant_4_sign_us",
                "variant_5_sign_us",
                "variant_6_sign_us",
                "variant_7_sign_us",
                "variant_8_sign_us",
                "variant_9_sign_us",
                "variant_10_sign_us",
                "variant_11_sign_us",
                "variant_12_sign_us",
                "variant_13_sign_us",
                "variant_14_sign_us",
                "variant_15_sign_us",
                "variant_16_sign_us",
                "variant_17_sign_us",
                "variant_18_sign_us",
                "variant_19_sign_us",
                "variant_20_sign_us",
                "variant_21_sign_us",
                "variant_22_sign_us",
                "variant_23_sign_us",
                "variant_24_sign_us",
                "variant_25_sign_us",
                "variant_26_sign_us",
                "variant_27_sign_us",
                "variant_28_sign_us",
                "variant_29_sign_us",
                "variant_30_sign_us",
                "variant_31_sign_us",
            ];
            let build_started = std::time::Instant::now();
            let build_timer = timings.stages.start("transaction_build_ms");
            let instructions = if let Some(lease) = &nonce {
                mainnet.fanout_instructions(
                    &signer.pubkey(),
                    route.compute_unit_limit,
                    &route.instructions,
                    lease,
                    &mainnet.fanout.routes[0],
                )?
            } else {
                mainnet
                    .finalize_instructions(
                        &signer.pubkey(),
                        &trade.intent.source_signature,
                        route.compute_unit_limit,
                        route.instructions.clone(),
                    )
                    .await?
            };
            let mut signers: Vec<&dyn Signer> = vec![signer];
            signers.extend(
                route
                    .additional_signers
                    .iter()
                    .map(|signer| signer as &dyn Signer),
            );
            let message = solana_sdk::message::Message::new_with_blockhash(
                &instructions,
                Some(&signer.pubkey()),
                &blockhash,
            );
            let mut transaction = Transaction::new_unsigned(message);
            drop(build_timer);
            let elapsed = build_started.elapsed();
            timings.stages.record_us(BUILD_KEYS[0], elapsed);
            timings.stages.record_us("variant_build_us", elapsed);
            let sign_started = std::time::Instant::now();
            let signing_timer = timings.stages.start("transaction_sign_ms");
            transaction.try_sign(&signers, blockhash).map_err(|error| {
                CopyTraderError::Execution(format!("cannot sign source transaction: {error}"))
            })?;
            let elapsed = sign_started.elapsed();
            timings.stages.record_us(SIGN_KEYS[0], elapsed);
            timings.stages.record_us("signing_only_us", elapsed);
            let mut variants = Vec::new();
            if let Some(lease) = &nonce {
                variants.push(transaction.clone());
                for (index, config) in mainnet.fanout.routes.iter().enumerate().skip(1) {
                    let variant_started = std::time::Instant::now();
                    let instructions = mainnet.fanout_instructions(
                        &signer.pubkey(),
                        route.compute_unit_limit,
                        &route.instructions,
                        lease,
                        config,
                    )?;
                    let mut variant = Transaction::new_unsigned(
                        solana_sdk::message::Message::new_with_blockhash(
                            &instructions,
                            Some(&signer.pubkey()),
                            &blockhash,
                        ),
                    );
                    let elapsed = variant_started.elapsed();
                    timings.stages.record_us(BUILD_KEYS[index], elapsed);
                    timings.stages.record_us("variant_build_us", elapsed);
                    let sign_started = std::time::Instant::now();
                    variant.try_sign(&signers, blockhash).map_err(|e| {
                        CopyTraderError::Execution(format!("cannot sign fanout variant: {e}"))
                    })?;
                    let elapsed = sign_started.elapsed();
                    timings.stages.record_us(SIGN_KEYS[index], elapsed);
                    timings.stages.record_us("signing_only_us", elapsed);
                    variants.push(variant);
                }
            }
            let size_started = std::time::Instant::now();
            let mut variant_route_indices = Vec::new();
            if nonce.is_some() {
                let mut fitting = Vec::new();
                for (index, tx) in variants.into_iter().enumerate() {
                    let size = transaction_size(&tx)?;
                    if size > 1232 {
                        tracing::warn!(route = %mainnet.fanout.routes[index].name, size, "skipping oversized fanout variant");
                    } else {
                        variant_route_indices.push(index);
                        fitting.push(tx);
                    }
                }
                variants = fitting;
                transaction = variants.first().cloned().ok_or_else(|| {
                    CopyTraderError::Execution("all fanout variants exceed Solana packet limit: 1232 bytes".into())
                })?;
            }

            timings
                .stages
                .record_us("variant_size_checks_us", size_started.elapsed());
            drop(signing_timer);
            route.instructions = instructions;
            info!(dex = route.dex.as_str(), "source instruction copied");
            Ok(ExecutableRoute {
                route,
                transaction,
                variants,
                variant_route_indices,
                nonce,
                simulation_json: "{\"skipped\":true,\"reason\":\"hot_path_no_simulation\"}"
                    .to_owned(),
            })
        })
        .await
        .map_err(|_| CopyTraderError::Execution("source instruction build timed out".to_owned()))?
    }
}

fn source_outputs(trade: &SizedTrade, slippage_bps: u16) -> Result<(u64, u64)> {
    if let Some(source) = &trade.intent.source_instruction
        && let Some(minimum) = source.minimum_output_override
    {
        if minimum != 1
            || trade.input_amount == 0
            || trade.intent.input_asset != crate::domain::AssetId::NativeSol
            || source.instruction.program_id != pump_fun::PROGRAM_ID
            || source.instruction.data.get(..8)
                != Some(pump_fun::BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR.as_slice())
        {
            return Err(CopyTraderError::Execution(
                "invalid early output floor override".into(),
            ));
        }
        // This is a source instruction floor, not a price quote or predicted fill.
        return Ok((minimum, minimum));
    }

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

fn transaction_size(transaction: &Transaction) -> Result<u64> {
    bincode::serialized_size(transaction)
        .map_err(|e| CopyTraderError::Execution(format!("cannot size transaction: {e}")))
}
