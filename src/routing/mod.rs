mod catalog;
mod math;
mod program_accounts;
pub(crate) mod pump_fun;
mod pump_swap;
mod source_pool;
mod timings;
pub use timings::RouteStages;
#[cfg(test)]
mod pump_fun_tests;
#[cfg(test)]
mod pump_swap_tests;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod streaming_tests;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::{
    FutureExt, Stream, StreamExt,
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_filter::RpcFilterType;
use solana_commitment_config::CommitmentConfig;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::{
    domain::{AssetId, DexKind, NATIVE_MINT, PreparedRoute, SizedTrade, SourcePool, TradeIntent},
    error::{CopyTraderError, Result},
    execution::ExecutionBackend,
    telemetry::compact_id,
    token::{
        accounts::{associated_token_address, create_associated_token_account_idempotent},
        extensions::MintInfo,
    },
};

pub use catalog::{PoolCatalog, PoolDescriptor};
use program_accounts::program_accounts_config;
use pump_swap::PumpSwapAdapter;

#[derive(Clone, Copy)]
pub struct RouteContext {
    pub copier: Pubkey,
    pub slippage_bps: u16,
    pub source_outputs: Option<(u64, u64)>,
}

#[async_trait]
pub trait RouteDexAdapter: Send + Sync {
    fn kind(&self) -> DexKind;
    async fn discover(&self, rpc: &RpcClient, mints: &[Pubkey]) -> Result<Vec<PoolDescriptor>>;
    async fn prepare(
        &self,
        rpc: &RpcClient,
        pool: &PoolDescriptor,
        trade: &SizedTrade,
        context: &RouteContext,
        pool_account: Option<&solana_sdk::account::Account>,
    ) -> Result<PreparedRoute>;
}

pub struct ExecutableRoute {
    pub route: PreparedRoute,
    pub transaction: Transaction,
    pub simulation_json: String,
    pub output_balance_before: Option<u64>,
}

#[derive(Default)]
pub struct RoutingTimings {
    pub discovery_ms: u64,
    pub source_route_ms: u64,
    pub stages: RouteStages,
}

pub(crate) struct PrefetchedSource {
    hint: SourcePool,
    slot: u64,
    pub(crate) account:
        Shared<BoxFuture<'static, std::result::Result<Arc<solana_sdk::account::Account>, String>>>,
}

pub(crate) struct RoutingInputs<'a> {
    pub(crate) mint_infos: (&'a MintInfo, &'a MintInfo),
    pub(crate) source: Option<PrefetchedSource>,
}

#[derive(Clone, Copy)]
struct CandidateContext<'a, 'b> {
    backend: &'a ExecutionBackend,
    trade: &'a SizedTrade,
    signer: &'a Keypair,
    route: RouteContext,
    shared: &'a SharedPreparation<'b>,
    stages: &'a RouteStages,
}

type SharedPreparation<'a> =
    Shared<BoxFuture<'a, std::result::Result<Option<Arc<MainnetRouteContext>>, String>>>;

struct MainnetRouteContext {
    blockhash: solana_sdk::hash::Hash,
    missing_atas: Vec<(Pubkey, Pubkey, Pubkey)>,
}

pub struct Router {
    adapters: Vec<Arc<dyn RouteDexAdapter>>,
    catalog: PoolCatalog,
    discovery_rpc: Arc<RpcClient>,
    max_pools_per_dex: usize,
    race_timeout: Duration,
}

fn native_pool_layout(dex: DexKind) -> (Option<u64>, &'static [usize]) {
    match dex {
        DexKind::PumpSwap => (None, &[43, 75]),
        _ => (None, &[]),
    }
}

fn native_pool_other_mint(data: &[u8], offsets: &[usize], native_offset: usize) -> Option<Pubkey> {
    let native = Pubkey::from_str_const(NATIVE_MINT);
    if data.get(native_offset..native_offset.checked_add(32)?)? != native.as_ref() {
        return None;
    }
    let other = offsets
        .iter()
        .copied()
        .find(|offset| *offset != native_offset)?;
    let bytes = data.get(other..other.checked_add(32)?)?;
    Some(Pubkey::new_from_array(<[u8; 32]>::try_from(bytes).ok()?))
}

impl Router {
    pub fn supported(
        discovery_rpc: Arc<RpcClient>,
        max_pools_per_dex: usize,
        race_timeout: Duration,
    ) -> Self {
        Self {
            adapters: vec![Arc::new(PumpSwapAdapter)],
            catalog: PoolCatalog::default(),
            discovery_rpc,
            max_pools_per_dex,
            race_timeout,
        }
    }

    pub async fn warm(&self, mints: &[Pubkey]) -> Result<()> {
        let mut refreshes = FuturesUnordered::new();
        for adapter in &self.adapters {
            refreshes
                .push(async move { (adapter, adapter.discover(&self.discovery_rpc, mints).await) });
        }

        let mut refreshed = 0_usize;
        while let Some((adapter, result)) = refreshes.next().await {
            match result {
                Ok(pools) => {
                    info!(
                        dex = adapter.kind().as_str(),
                        pools = pools.len(),
                        "route catalog refreshed"
                    );
                    self.catalog
                        .refresh(adapter.kind(), mints, pools.clone())
                        .await;
                    for pool in pools {
                        match self.discovery_rpc.get_account(&pool.address).await {
                            Ok(account) => self.catalog.cache_account(pool.address, account).await,
                            Err(error) => {
                                debug!(pool = %pool.address, %error, "pool account cache refresh failed")
                            }
                        }
                    }
                    refreshed = refreshed.saturating_add(1);
                }
                Err(error) => {
                    warn!(dex = adapter.kind().as_str(), %error, "route catalog refresh failed")
                }
            }
        }
        if refreshed == 0 {
            return Err(CopyTraderError::Execution(
                "all DEX catalog refreshes failed".to_owned(),
            ));
        }
        Ok(())
    }

    /// Scan and cache every pool whose base/quote side is canonical WSOL.
    pub async fn warm_sol_pools(&self) -> Result<()> {
        let started = Instant::now();
        info!(
            dexes = self.adapters.len(),
            "native WSOL pool warming started"
        );
        let mut scans = FuturesUnordered::new();
        for adapter in &self.adapters {
            let dex_kind = adapter.kind();
            let dex = dex_kind.as_str();
            let (size, offsets) = native_pool_layout(dex_kind);
            for &native_offset in offsets {
                info!(dex, native_offset, "native WSOL pool account scan started");
                let mut filters = vec![RpcFilterType::Memcmp(
                    solana_client::rpc_filter::Memcmp::new_base58_encoded(
                        native_offset,
                        Pubkey::from_str_const(NATIVE_MINT).as_ref(),
                    ),
                )];
                if let Some(size) = size {
                    filters.insert(0, RpcFilterType::DataSize(size));
                }
                let config = program_accounts_config(filters);
                let rpc = self.discovery_rpc.clone();
                let program_id = dex_kind.program_id();
                scans.push(async move {
                    let scan_started = Instant::now();
                    #[allow(deprecated)]
                    let result = timeout(
                        Duration::from_secs(15),
                        rpc.get_program_accounts_with_config(&program_id, config),
                    )
                    .await;
                    let elapsed_ms = scan_started.elapsed().as_millis() as u64;
                    match result {
                        Ok(Ok(accounts)) => {
                            info!(
                                dex = dex_kind.as_str(),
                                native_offset,
                                accounts = accounts.len(),
                                elapsed_ms,
                                "native WSOL pool account scan returned"
                            );
                            Ok((dex_kind, native_offset, accounts))
                        }
                        Ok(Err(error)) => {
                            warn!(
                                dex = dex_kind.as_str(),
                                native_offset,
                                elapsed_ms,
                                %error,
                                "native WSOL pool account scan failed"
                            );
                            Err((dex_kind, native_offset, format!("{error}")))
                        }
                        Err(_) => {
                            warn!(
                                dex = dex_kind.as_str(),
                                native_offset,
                                elapsed_ms,
                                timeout_seconds = 15,
                                "native WSOL pool account scan timed out"
                            );
                            Err((dex_kind, native_offset, "RPC request timed out".to_owned()))
                        }
                    }
                });
            }
        }

        let mut pools_by_dex = std::collections::HashMap::<DexKind, Vec<PoolDescriptor>>::new();
        let mut successful_scans = 0_usize;
        while let Some(result) = scans.next().await {
            let (dex_kind, native_offset, accounts) = match result {
                Ok(result) => {
                    successful_scans = successful_scans.saturating_add(1);
                    result
                }
                Err((dex_kind, native_offset, error)) => {
                    debug!(dex = dex_kind.as_str(), native_offset, %error, "native WSOL scan unavailable");
                    continue;
                }
            };
            let (_, offsets) = native_pool_layout(dex_kind);
            let pools = pools_by_dex.entry(dex_kind).or_default();
            for (address, account) in accounts {
                let Some(other_mint) =
                    native_pool_other_mint(&account.data, offsets, native_offset)
                else {
                    continue;
                };
                if pools.iter().any(|pool| pool.address == address) {
                    continue;
                }
                pools.push(PoolDescriptor {
                    dex: dex_kind,
                    address,
                    mint_a: Pubkey::from_str_const(NATIVE_MINT),
                    mint_b: other_mint,
                    liquidity_hint: 0,
                });
                self.catalog.cache_account(address, account).await;
            }
        }

        let mut warmed = 0_usize;
        for adapter in &self.adapters {
            let dex_kind = adapter.kind();
            let pools = pools_by_dex.remove(&dex_kind).unwrap_or_default();
            info!(
                dex = dex_kind.as_str(),
                pools = pools.len(),
                "native WSOL pool scan complete"
            );
            warmed = warmed.saturating_add(pools.len());
            info!(
                dex = dex_kind.as_str(),
                total_pools = warmed,
                "native WSOL pool warming progress"
            );
            self.catalog.merge_pair(dex_kind, pools).await;
        }
        if warmed == 0 {
            warn!(
                elapsed_ms = started.elapsed().as_millis() as u64,
                successful_scans, "native WSOL pool warming finished without pools"
            );
            return Err(CopyTraderError::Unsupported(
                "no native WSOL pools found".to_owned(),
            ));
        }
        info!(
            pools = warmed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "native WSOL pool catalog warmed"
        );
        Ok(())
    }

    pub async fn ensure_pair(&self, input_mint: Pubkey, output_mint: Pubkey) -> Result<()> {
        if self.catalog.contains_pair(input_mint, output_mint).await {
            return Ok(());
        }
        self.discover_pair(input_mint, output_mint).await
    }

    async fn discover_pair(&self, input_mint: Pubkey, output_mint: Pubkey) -> Result<()> {
        let mints = [input_mint, output_mint];
        let mut discoveries = FuturesUnordered::new();
        for adapter in &self.adapters {
            discoveries.push(async move {
                (adapter, adapter.discover(&self.discovery_rpc, &mints).await)
            });
        }
        let mut successful_discoveries = 0_usize;
        let mut discovered_pools = 0_usize;
        while let Some((adapter, result)) = discoveries.next().await {
            match result {
                Ok(pools) => {
                    discovered_pools = discovered_pools.saturating_add(pools.len());
                    successful_discoveries = successful_discoveries.saturating_add(1);
                    self.catalog.merge_pair(adapter.kind(), pools).await;
                }
                Err(error) => {
                    debug!(dex = adapter.kind().as_str(), %error, "on-demand pool discovery failed")
                }
            }
        }
        if successful_discoveries == 0 {
            return Err(CopyTraderError::Execution(
                "all on-demand DEX discoveries failed".to_owned(),
            ));
        }
        if discovered_pools == 0 {
            return Err(CopyTraderError::Unsupported(
                "no direct pool exists for the observed token pair".to_owned(),
            ));
        }
        info!(%input_mint, %output_mint, pools = discovered_pools, "on-demand token pair cataloged");
        Ok(())
    }

    pub async fn race(
        &self,
        trade: &SizedTrade,
        signer: &Keypair,
        backend: &ExecutionBackend,
        slippage_bps: u16,
        mint_infos: (&MintInfo, &MintInfo),
    ) -> Result<ExecutableRoute> {
        self.race_with_timings(
            trade,
            signer,
            backend,
            slippage_bps,
            mint_infos,
            &mut RoutingTimings::default(),
        )
        .await
    }

    pub(crate) fn prefetch_source(
        &self,
        intent: &TradeIntent,
        stages: &RouteStages,
    ) -> Option<PrefetchedSource> {
        let hint = intent.source_pool?;
        let slot = intent.slot;
        let stages = stages.clone();
        let result = self.catalog.cached_account(hint.address);
        let account = async move {
            let _timer = stages.start("cache_lookup_ms");
            result.ok_or_else(|| "source pool is not present in the background cache".to_owned())
        }
        .boxed()
        .shared();
        Some(PrefetchedSource {
            hint,
            slot,
            account,
        })
    }

    pub async fn race_with_timings(
        &self,
        trade: &SizedTrade,
        signer: &Keypair,
        backend: &ExecutionBackend,
        slippage_bps: u16,
        mint_infos: (&MintInfo, &MintInfo),
        timings: &mut RoutingTimings,
    ) -> Result<ExecutableRoute> {
        self.race_prepared(
            trade,
            signer,
            backend,
            slippage_bps,
            RoutingInputs {
                mint_infos,
                source: None,
            },
            timings,
        )
        .await
    }

    pub(crate) async fn race_prepared(
        &self,
        trade: &SizedTrade,
        signer: &Keypair,
        backend: &ExecutionBackend,
        slippage_bps: u16,
        inputs: RoutingInputs<'_>,
        timings: &mut RoutingTimings,
    ) -> Result<ExecutableRoute> {
        let direct = backend
            .mainnet()
            .is_some_and(|client| client.source_direct());
        if trade
            .intent
            .source_instruction
            .as_ref()
            .is_some_and(|source| source.instruction.program_id == pump_fun::PROGRAM_ID)
            && !direct
        {
            return Err(CopyTraderError::Unsupported(
                "Pump.fun bonding curves require mainnet.skip = true".to_owned(),
            ));
        }
        let source_outputs = if direct
            && (trade.intent.source_pool.is_some() || trade.intent.source_instruction.is_some())
        {
            Some(source_outputs(trade, slippage_bps)?)
        } else {
            None
        };
        let mint_infos = inputs.mint_infos;
        let stages = timings.stages.clone();
        let shared = async {
            let Some(mainnet) = backend.mainnet() else {
                return Ok(None);
            };
            let _timer = stages.start("route_shared_preparation_ms");
            let blockhash = mainnet
                .cached_blockhash()
                .ok_or_else(|| "background blockhash cache is not ready".to_owned())?;
            Ok(Some(Arc::new(MainnetRouteContext {
                blockhash,
                missing_atas: Vec::new(),
            })))
        }
        .boxed()
        .shared();
        let context = CandidateContext {
            backend,
            trade,
            signer,
            route: RouteContext {
                copier: signer.pubkey(),
                slippage_bps,
                source_outputs,
            },
            shared: &shared,
            stages: &stages,
        };
        timeout(self.race_timeout, async {
            let routing = async {
                let mut last_error = None;
                if (trade.intent.source_pool.is_none()
                    || trade
                        .intent
                        .source_instruction
                        .as_ref()
                        .is_some_and(|source| source.instruction.program_id == pump_fun::PROGRAM_ID))
                    && trade.intent.source_instruction.is_some()
                    && trade.intent.source_instruction.as_ref().is_some_and(|source| {
                        matches!(
                            source.instruction.program_id,
                            id if id == DexKind::PumpSwap.program_id() || id == pump_fun::PROGRAM_ID
                        )
                    })
                    && let Some(source_instruction) =
                        trade.intent.source_instruction.as_ref()
                    && let Some((expected_output, minimum_output)) = context.route.source_outputs
                {
                    let copied = if source_instruction.instruction.program_id == pump_fun::PROGRAM_ID {
                        pump_fun::copy_source_instruction(
                            source_instruction, trade, context.route.copier, expected_output, minimum_output,
                        )
                    } else {
                        pump_swap::copy_source_instruction(
                            source_instruction, trade, context.route.copier, expected_output, minimum_output,
                        )
                    };
                    match copied {
                        Ok(route) => {
                            let shared = context
                                .shared
                                .clone()
                                .await
                                .map_err(CopyTraderError::Execution)?;
                            info!(dex = route.dex.as_str(), "source instruction copied without pool discovery");
                            return finish_route(route, shared, context).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if let Some(hint) = trade.intent.source_pool {
                    // Leave half the total budget for alternatives if the source pool stalls.
                    let result = timeout(self.race_timeout / 2, async {
                        let _timer = ElapsedTimer::new(&mut timings.source_route_ms);
                        let (pool, account) = {
                            let _timer = stages.start("route_source_pool_fetch_ms");
                            let Some(prefetched) = &inputs.source else {
                                return Err(CopyTraderError::Execution(
                                    "source pool cache was not prefetched".to_owned(),
                                ));
                            };
                            if prefetched.hint != hint || prefetched.slot != trade.intent.slot {
                                return Err(CopyTraderError::Execution(
                                    "source prefetch does not match trade".to_owned(),
                                ));
                            }
                            let account = prefetched
                                .account
                                .clone()
                                .await
                                .map_err(CopyTraderError::Execution)?;
                            let pool = source_pool::validate(
                                hint,
                                &account,
                                trade.intent.input_asset.routing_mint(),
                                trade.intent.output_asset.routing_mint(),
                                mint_infos,
                            )?;
                            (pool, account)
                        };
                        let native_source_copy = matches!(
                            (trade.intent.input_asset, trade.intent.output_asset),
                            (AssetId::NativeSol, _) | (_, AssetId::NativeSol)
                        );
                        let persistent_wsol = if hint.dex == DexKind::PumpSwap
                            && native_source_copy
                            && trade.intent.source_instruction.is_some()
                        {
                            let wsol = associated_token_address(
                                &context.route.copier,
                                &Pubkey::from_str_const(NATIVE_MINT),
                                &spl_token::id(),
                            );
                            match self
                                .discovery_rpc
                                .get_account_with_commitment(
                                    &wsol,
                                    CommitmentConfig::confirmed(),
                                )
                                .await
                            {
                                Ok(response) => response.value.is_some(),
                                Err(error) => {
                                    debug!(%error, "cannot inspect copier WSOL ATA; using cached route");
                                    true
                                }
                            }
                        } else {
                            false
                        };
                        if hint.dex == DexKind::PumpSwap
                            && context.route.source_outputs.is_some()
                            && !persistent_wsol
                            && let Some(source_instruction) =
                                trade.intent.source_instruction.as_ref()
                        {
                                match pump_swap::copy_source_instruction(
                                    source_instruction,
                                    trade,
                                    context.route.copier,
                                    context.route.source_outputs
                                        .ok_or_else(|| {
                                            CopyTraderError::Unsupported(
                                                "source output estimate is unavailable".to_owned(),
                                            )
                                        })?
                                        .0,
                                    context.route.source_outputs
                                        .ok_or_else(|| {
                                            CopyTraderError::Unsupported(
                                                "source output estimate is unavailable".to_owned(),
                                            )
                                        })?
                                        .1,
                                ) {
                                    Ok(route) => {
                                        let shared = context
                                            .shared
                                            .clone()
                                            .await
                                            .map_err(CopyTraderError::Execution)?;
                                        return finish_route(route, shared, context).await;
                                    }
                                                Err(error) => return Err(error),
                            }
                        }
                        let winner = self
                            .race_pools(context, vec![pool.clone()], Some(&account))
                            .await?;
                        self.catalog.remember(pool, self.max_pools_per_dex).await;
                        Ok::<_, CopyTraderError>(winner)
                    })
                    .await;
                    match result {
                        Ok(Ok(winner)) => {
                            info!(
                                dex = hint.dex.as_str(),
                                elapsed_ms = timings.source_route_ms,
                                "source pool route reused"
                            );
                            return Ok(winner);
                        }
                        Ok(Err(error)) => {
                            return Err(error);
                        }
                        Err(_) if direct => {
                            return Err(CopyTraderError::Execution(
                                "source-direct route timed out".to_owned(),
                            ));
                        }
                        Err(_) => debug!("source pool route timed out; trying alternatives"),
                    }
                }

                if trade.intent.source_pool.is_some() {
                    return Err(last_error.unwrap_or_else(|| {
                        CopyTraderError::Execution(
                            "cached source pool route was not executable".to_owned(),
                        )
                    }));
                }
                let candidates = self.cached_candidates(trade).await;
                let attempted = candidates
                    .iter()
                    .map(|pool| pool.address)
                    .collect::<Vec<_>>();
                if !candidates.is_empty() {
                    match self.race_pools(context, candidates, None).await {
                        Ok(winner) => return Ok(winner),
                        Err(error) => {
                            debug!(%error, "cached routes failed; refreshing pair");
                            last_error = Some(error);
                        }
                    }
                }
                self.discover_and_race(context, &attempted, last_error, &mut timings.discovery_ms)
                    .await
            };
            tokio::pin!(routing);
            // Drive shared reads while source validation or discovery runs.
            let preparation = async {
                let _ = shared.clone().await;
            };
            tokio::select! {
                result = &mut routing => result,
                () = preparation => routing.await,
            }
        })
        .await
        .map_err(|_| CopyTraderError::Execution("DEX race timed out".to_owned()))?
    }

    async fn cached_candidates(&self, trade: &SizedTrade) -> Vec<PoolDescriptor> {
        let mut candidates = Vec::new();
        for adapter in &self.adapters {
            candidates.extend(
                self.catalog
                    .candidates(
                        adapter.kind(),
                        trade.intent.input_asset.routing_mint(),
                        trade.intent.output_asset.routing_mint(),
                        self.max_pools_per_dex,
                    )
                    .await
                    .into_iter()
                    .filter(|pool| {
                        !trade.intent.source_pool.is_some_and(|hint| {
                            hint.dex == pool.dex && hint.address == pool.address
                        })
                    }),
            );
        }
        candidates
    }

    async fn discover_and_race(
        &self,
        context: CandidateContext<'_, '_>,
        attempted: &[Pubkey],
        mut last_error: Option<CopyTraderError>,
        discovery_ms: &mut u64,
    ) -> Result<ExecutableRoute> {
        let input = context.trade.intent.input_asset.routing_mint();
        let output = context.trade.intent.output_asset.routing_mint();
        let mints = [input, output];
        let mut discoveries = FuturesUnordered::new();
        for adapter in &self.adapters {
            discoveries.push(async move {
                (
                    adapter.kind(),
                    adapter.discover(&self.discovery_rpc, &mints).await,
                )
            });
        }
        let mut races = FuturesUnordered::new();
        let mut successful_discoveries = 0_usize;
        while !discoveries.is_empty() || !races.is_empty() {
            // Only discovery-only waiting is subtracted from route wall time.
            let _timer = races.is_empty().then(|| ElapsedTimer::new(discovery_ms));
            tokio::select! {
                result = races.next(), if !races.is_empty() => {
                    match result {
                        Some(Ok(winner)) => return Ok(winner),
                        Some(Err(error)) => retain_route_error(&mut last_error, error),
                        None => {}
                    }
                }
                result = discoveries.next(), if !discoveries.is_empty() => {
                    if let Some((dex, result)) = result {
                        match result {
                            Ok(pools) => {
                                successful_discoveries = successful_discoveries.saturating_add(1);
                                self.catalog.merge_pair(dex, pools).await;
                                let pools = self.catalog.candidates(dex, input, output, self.max_pools_per_dex).await
                                    .into_iter().filter(|pool| {
                                        !attempted.contains(&pool.address)
                                            && !context.trade.intent.source_pool.is_some_and(|hint| hint.dex == pool.dex && hint.address == pool.address)
                                    }).collect::<Vec<_>>();
                                if !pools.is_empty() {
                                    races.push(self.race_pools(context, pools, None));
                                }
                            }
                            Err(error) => {
                                debug!(dex = dex.as_str(), %error, "on-demand pool discovery failed");
                                retain_route_error(&mut last_error, error);
                            }
                        }
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            if successful_discoveries == 0 {
                CopyTraderError::Execution("all on-demand DEX discoveries failed".to_owned())
            } else {
                CopyTraderError::Unsupported(
                    "no direct pool exists for the observed token pair".to_owned(),
                )
            }
        }))
    }

    async fn race_pools(
        &self,
        context: CandidateContext<'_, '_>,
        pools: Vec<PoolDescriptor>,
        account: Option<&solana_sdk::account::Account>,
    ) -> Result<ExecutableRoute> {
        let futures = FuturesUnordered::new();
        let mut candidate_count = 0_usize;
        for adapter in &self.adapters {
            let candidates = pools
                .iter()
                .filter(|pool| pool.dex == adapter.kind())
                .cloned();
            for pool in candidates {
                candidate_count = candidate_count.saturating_add(1);
                futures.push(route_candidate(
                    adapter.as_ref(),
                    self.discovery_rpc.as_ref(),
                    pool,
                    context,
                    account,
                ));
            }
        }
        debug!(
            candidates = candidate_count,
            timeout_ms = self.race_timeout.as_millis(),
            "route race started"
        );
        let winner = first_successful(futures, self.race_timeout).await?;
        info!(dex = winner.route.dex.as_str(), pool = %compact_id(winner.route.pool.to_string()), expected_output = winner.route.expected_output, minimum_output = winner.route.minimum_output, "route selected");
        Ok(winner)
    }
}

struct ElapsedTimer<'a> {
    value: &'a mut u64,
    started: std::time::Instant,
}

impl<'a> ElapsedTimer<'a> {
    fn new(value: &'a mut u64) -> Self {
        Self {
            value,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for ElapsedTimer<'_> {
    fn drop(&mut self) {
        *self.value = self
            .value
            .saturating_add(u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX));
    }
}

fn retain_route_error(last: &mut Option<CopyTraderError>, next: CopyTraderError) {
    let unsupported = |error: &CopyTraderError| {
        matches!(
            error,
            CopyTraderError::Unsupported(_) | CopyTraderError::OutOfScope(_, _)
        )
    };
    if !unsupported(&next) || last.as_ref().is_none_or(unsupported) {
        *last = Some(next);
    }
}

async fn first_successful<S, T>(mut candidates: S, deadline: Duration) -> Result<T>
where
    S: Stream<Item = Result<T>> + Unpin,
{
    let mut last_error = None;
    let mut operational_error = None;
    let winner = timeout(deadline, async {
        while let Some(result) = candidates.next().await {
            match result {
                Ok(candidate) => return Some(candidate),
                Err(error) => {
                    debug!(%error, "route candidate rejected");
                    if matches!(
                        error,
                        CopyTraderError::Unsupported(_) | CopyTraderError::OutOfScope(_, _)
                    ) {
                        last_error = Some(error);
                    } else {
                        operational_error = Some(error);
                    }
                }
            }
        }
        None
    })
    .await
    .map_err(|_| CopyTraderError::Execution("DEX race timed out".to_owned()))?;
    winner.ok_or_else(|| {
        operational_error.or(last_error).unwrap_or_else(|| {
            CopyTraderError::Unsupported("no direct route candidates were cataloged".to_owned())
        })
    })
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
    let minimum = math::apply_slippage(expected, slippage_bps)?;
    Ok((expected, minimum))
}

async fn route_candidate(
    adapter: &dyn RouteDexAdapter,
    quote_rpc: &RpcClient,
    pool: PoolDescriptor,
    context: CandidateContext<'_, '_>,
    account: Option<&solana_sdk::account::Account>,
) -> Result<ExecutableRoute> {
    let (route, shared) = tokio::try_join!(
        async {
            let _timer = context
                .stages
                .start(if context.route.source_outputs.is_some() {
                    "route_instruction_build_ms"
                } else {
                    "route_quote_ms"
                });
            adapter
                .prepare(quote_rpc, &pool, context.trade, &context.route, account)
                .await
        },
        async {
            context
                .shared
                .clone()
                .await
                .map_err(CopyTraderError::Execution)
        }
    )?;
    finish_route(route, shared, context).await
}

async fn finish_route(
    mut route: PreparedRoute,
    shared: Option<Arc<MainnetRouteContext>>,
    context: CandidateContext<'_, '_>,
) -> Result<ExecutableRoute> {
    if let Some(shared) = &shared {
        add_missing_atas(
            context.route.copier,
            &shared.missing_atas,
            &mut route.instructions,
        );
    }
    context
        .backend
        .prepare_market(context.trade.intent.slot, &route.market_accounts)
        .await?;
    let blockhash = if let Some(shared) = &shared {
        shared.blockhash
    } else {
        context
            .backend
            .rpc()
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("cannot fetch route blockhash: {error}"))
            })?
            .0
    };
    let instructions = if let Some(mainnet) = context.backend.mainnet() {
        let mut provisional_instructions = Vec::with_capacity(route.instructions.len() + 1);
        provisional_instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            route.compute_unit_limit,
        ));
        provisional_instructions.extend(route.instructions.iter().cloned());
        let mut provisional =
            Transaction::new_with_payer(&provisional_instructions, Some(&context.signer.pubkey()));
        provisional.message.recent_blockhash = blockhash;
        mainnet
            .finalize_instructions(
                &context.signer.pubkey(),
                &context.trade.intent.source_signature,
                route.compute_unit_limit,
                route.instructions.clone(),
                &provisional,
            )
            .await?
    } else {
        let mut instructions = Vec::with_capacity(route.instructions.len() + 1);
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            route.compute_unit_limit,
        ));
        instructions.extend(route.instructions.clone());
        instructions
    };
    let transaction = {
        let mut signers: Vec<&dyn Signer> = vec![context.signer];
        signers.extend(
            route
                .additional_signers
                .iter()
                .map(|additional| additional as &dyn Signer),
        );
        Transaction::new_signed_with_payer(
            &instructions,
            Some(&context.signer.pubkey()),
            &signers,
            blockhash,
        )
    };
    let (simulation_json, output_balance_before) = tokio::try_join!(
        async {
            Ok::<_, CopyTraderError>(
                "{\"skipped\":true,\"reason\":\"hot_path_no_simulation\"}".to_owned(),
            )
        },
        async {
            let Some(mainnet) = context.backend.mainnet() else {
                return Ok(None);
            };
            let _ = mainnet;
            Ok(None)
        }
    )?;
    route.instructions = instructions;
    Ok(ExecutableRoute {
        route,
        transaction,
        simulation_json,
        output_balance_before,
    })
}

#[allow(dead_code)]
async fn missing_mainnet_atas(
    rpc: &RpcClient,
    owner: Pubkey,
    trade: &SizedTrade,
    mint_infos: (&MintInfo, &MintInfo),
) -> Result<Vec<(Pubkey, Pubkey, Pubkey)>> {
    let mut candidates = Vec::new();
    for (asset, info) in [
        (trade.intent.input_asset, mint_infos.0),
        (trade.intent.output_asset, mint_infos.1),
    ] {
        if let AssetId::Token(mint) = asset
            && !candidates.iter().any(|(existing, _, _)| *existing == mint)
        {
            candidates.push((
                mint,
                info.token_program,
                associated_token_address(&owner, &mint, &info.token_program),
            ));
        }
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let addresses = candidates
        .iter()
        .map(|(_, _, ata)| *ata)
        .collect::<Vec<_>>();
    let accounts = rpc
        .get_multiple_accounts(&addresses)
        .await
        .map_err(|error| {
            CopyTraderError::Execution(format!("cannot inspect mainnet token accounts: {error}"))
        })?;
    if accounts.len() != candidates.len() {
        return Err(CopyTraderError::Execution(
            "incomplete token account response".to_owned(),
        ));
    }
    Ok(candidates
        .into_iter()
        .zip(accounts)
        .filter_map(|(candidate, account)| account.is_none().then_some(candidate))
        .collect())
}

fn add_missing_atas(
    owner: Pubkey,
    missing: &[(Pubkey, Pubkey, Pubkey)],
    instructions: &mut Vec<solana_sdk::instruction::Instruction>,
) {
    let creates = missing
        .iter()
        .filter(|(_, _, ata)| !has_associated_token_create(instructions, ata))
        .map(|(mint, program, _)| {
            create_associated_token_account_idempotent(&owner, &owner, mint, program)
        })
        .collect::<Vec<_>>();
    instructions.splice(0..0, creates);
}

fn has_associated_token_create(
    instructions: &[solana_sdk::instruction::Instruction],
    ata: &Pubkey,
) -> bool {
    instructions.iter().any(|instruction| {
        instruction.program_id == spl_associated_token_account::id()
            && instruction.accounts.get(1).map(|account| account.pubkey) == Some(*ata)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use futures_util::{FutureExt, future::BoxFuture};

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct TestAdapter;
    #[async_trait]
    impl RouteDexAdapter for TestAdapter {
        fn kind(&self) -> DexKind {
            DexKind::PumpSwap
        }
        async fn discover(&self, _: &RpcClient, _: &[Pubkey]) -> Result<Vec<PoolDescriptor>> {
            Ok(Vec::new())
        }
        async fn prepare(
            &self,
            _: &RpcClient,
            pool: &PoolDescriptor,
            _: &SizedTrade,
            _: &RouteContext,
            _: Option<&solana_sdk::account::Account>,
        ) -> Result<PreparedRoute> {
            Ok(PreparedRoute {
                dex: self.kind(),
                pool: pool.address,
                instructions: Vec::new(),
                additional_signers: Vec::new(),
                market_accounts: Vec::new(),
                expected_output: 100,
                minimum_output: 90,
                compute_unit_limit: 100_000,
            })
        }
    }

    #[tokio::test]
    async fn mainnet_hot_path_requires_cached_blockhash_and_skips_fee_and_simulation_rpc() {
        use crate::{
            config::{HttpConfig, MainnetConfig},
            domain::TradeIntent,
            http::HttpTransport,
            mainnet::MainnetClient,
            test_rpc::TestRpc,
        };
        use serde_json::json;
        for count in [1, 4] {
            let hash = solana_sdk::hash::Hash::new_unique();
            let server = TestRpc::start(move |request| match request["method"].as_str().expect("method") {
                "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":hash.to_string(),"lastValidBlockHeight":1000}}),
                "getBlockHeight" => json!(100),
                method => panic!("unexpected hot-path RPC {method}"),
            }).await;
            let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
            let client = Arc::new(MainnetClient::new(
                server.url.clone(),
                &MainnetConfig {
                    source_direct: false,
                    fixed_priority_fee_micro_lamports: Some(10),
                    sender_url: server.url.clone(),
                    tip_lamports: 5000,
                    priority_level: "High".to_owned(),
                    max_priority_fee_micro_lamports: 100,
                },
                transport.clone(),
            ));
            let backend = ExecutionBackend::Mainnet(client.clone());
            let router = Router {
                adapters: vec![Arc::new(TestAdapter)],
                catalog: PoolCatalog::default(),
                discovery_rpc: Arc::new(transport.solana_rpc(&server.url)),
                max_pools_per_dex: 4,
                race_timeout: Duration::from_secs(2),
            };
            let input = Pubkey::new_unique();
            let output = Pubkey::new_unique();
            router
                .catalog
                .replace(
                    DexKind::PumpSwap,
                    (0..count)
                        .map(|_| PoolDescriptor {
                            dex: DexKind::PumpSwap,
                            address: Pubkey::new_unique(),
                            mint_a: input,
                            mint_b: output,
                            liquidity_hint: 1,
                        })
                        .collect(),
                )
                .await;
            let trade = SizedTrade {
                intent: TradeIntent {
                    source_pool: None,
                    source_instruction: None,
                    source_signature: Default::default(),
                    slot: 42,
                    input_asset: AssetId::Token(input),
                    output_asset: AssetId::Token(output),
                    source_input_amount: 100,
                    source_output_amount: 100,
                },
                input_amount: 100,
            };
            let info = MintInfo {
                decimals: 6,
                token_program: spl_token::id(),
                has_transfer_fee: false,
            };
            let cold = router
                .race(&trade, &Keypair::new(), &backend, 100, (&info, &info))
                .await;
            assert!(
                matches!(cold, Err(CopyTraderError::Execution(message)) if message.contains("blockhash cache"))
            );
            client.latest_blockhash().await.expect("warm cache");
            let winner = router
                .race(&trade, &Keypair::new(), &backend, 100, (&info, &info))
                .await
                .expect("warm winner");
            assert!(winner.transaction.verify().is_ok());
            assert_eq!(winner.output_balance_before, None);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&winner.simulation_json)
                    .expect("simulation marker")["reason"],
                "hot_path_no_simulation"
            );
            assert_eq!(server.count("getLatestBlockhash"), 1);
            assert_eq!(server.count("getBlockHeight"), 1);
            for method in [
                "sendTransaction",
                "simulateTransaction",
                "getMultipleAccounts",
                "getFeeForMessage",
                "getPriorityFeeEstimate",
            ] {
                assert_eq!(server.count(method), 0);
            }
        }
    }

    #[tokio::test]
    async fn ata_lookup_excludes_native_sol_and_deduplicates_tokens() {
        use crate::{
            config::HttpConfig, domain::TradeIntent, http::HttpTransport, test_rpc::TestRpc,
        };
        use serde_json::json;
        let server = TestRpc::start(|request| {
            assert_eq!(request["params"][0].as_array().expect("addresses").len(), 1);
            json!({"context":{"slot":42},"value":[null]})
        })
        .await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let rpc = transport.solana_rpc(&server.url);
        let mint = Pubkey::new_unique();
        let info = MintInfo {
            decimals: 6,
            token_program: spl_token::id(),
            has_transfer_fee: false,
        };
        let mut trade = SizedTrade {
            intent: TradeIntent {
                source_pool: None,
                source_instruction: None,
                source_signature: solana_sdk::signature::Signature::default(),
                slot: 42,
                input_asset: AssetId::NativeSol,
                output_asset: AssetId::Token(mint),
                source_input_amount: 100,
                source_output_amount: 100,
            },
            input_amount: 100,
        };
        let owner = Pubkey::new_unique();
        let missing = missing_mainnet_atas(&rpc, owner, &trade, (&info, &info))
            .await
            .expect("native pair");
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].0, mint);
        trade.intent.input_asset = AssetId::Token(mint);
        assert_eq!(
            missing_mainnet_atas(&rpc, owner, &trade, (&info, &info))
                .await
                .expect("same mint")
                .len(),
            1
        );
        trade.intent.input_asset = AssetId::NativeSol;
        trade.intent.output_asset = AssetId::NativeSol;
        assert!(
            missing_mainnet_atas(&rpc, owner, &trade, (&info, &info))
                .await
                .expect("native only")
                .is_empty()
        );
        assert_eq!(server.count("getMultipleAccounts"), 2);
    }

    #[test]
    fn missing_ata_creates_preserve_existing_instructions() {
        let owner = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let program = spl_token::id();
        let ata = associated_token_address(&owner, &mint, &program);
        let mut instructions = Vec::new();
        add_missing_atas(owner, &[(mint, program, ata)], &mut instructions);
        let original = instructions.clone();
        add_missing_atas(owner, &[(mint, program, ata)], &mut instructions);
        assert_eq!(instructions, original);
    }

    #[test]
    fn recognizes_an_idempotent_ata_create() {
        let owner = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let token_program = spl_token::id();
        let ata = associated_token_address(&owner, &mint, &token_program);
        let instruction =
            create_associated_token_account_idempotent(&owner, &owner, &mint, &token_program);

        assert!(has_associated_token_create(&[instruction], &ata));
        assert!(!has_associated_token_create(&[], &ata));
    }

    #[tokio::test]
    async fn race_continues_after_failure_and_cancels_loser() {
        let started = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        let candidates: FuturesUnordered<BoxFuture<'static, Result<u8>>> = FuturesUnordered::new();
        let failed_started = started.clone();
        candidates.push(
            async move {
                failed_started.fetch_add(1, Ordering::SeqCst);
                Err(CopyTraderError::Execution("quote failed".to_owned()))
            }
            .boxed(),
        );
        let winner_started = started.clone();
        candidates.push(
            async move {
                winner_started.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok(7)
            }
            .boxed(),
        );
        let loser_started = started.clone();
        let loser_cancelled = cancelled.clone();
        candidates.push(
            async move {
                loser_started.fetch_add(1, Ordering::SeqCst);
                let _flag = DropFlag(loser_cancelled);
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(9)
            }
            .boxed(),
        );

        let result = first_successful(candidates, Duration::from_millis(100)).await;
        assert_eq!(result.ok(), Some(7));
        assert_eq!(started.load(Ordering::SeqCst), 3);
        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn race_enforces_deadline() {
        let candidates: FuturesUnordered<BoxFuture<'static, Result<u8>>> = FuturesUnordered::new();
        candidates.push(
            async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(1)
            }
            .boxed(),
        );
        let result = first_successful(candidates, Duration::from_millis(5)).await;
        assert!(
            matches!(result, Err(CopyTraderError::Execution(message)) if message == "DEX race timed out")
        );
    }

    #[tokio::test]
    async fn race_reports_candidate_rejection_reasons() {
        let candidates: FuturesUnordered<BoxFuture<'static, Result<u8>>> = FuturesUnordered::new();
        candidates.push(
            async move {
                Err(CopyTraderError::Execution(
                    "simulation failed: insufficient funds".to_owned(),
                ))
            }
            .boxed(),
        );
        candidates.push(
            async move {
                Err(CopyTraderError::Execution(
                    "priority fee request timed out".to_owned(),
                ))
            }
            .boxed(),
        );

        let result = first_successful(candidates, Duration::from_millis(100)).await;
        assert!(matches!(
            result,
            Err(CopyTraderError::Execution(message))
                if message.contains("priority fee request timed out")
        ));
    }
}
