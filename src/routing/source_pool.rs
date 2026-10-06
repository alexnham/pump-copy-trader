use solana_sdk::{account::Account, pubkey::Pubkey};

use crate::{
    domain::{DexKind, SourcePool},
    error::{CopyTraderError, Result},
    token::extensions::MintInfo,
};

use super::PoolDescriptor;

pub(super) fn validate(
    hint: SourcePool,
    account: &Account,
    input: Pubkey,
    output: Pubkey,
    _mint_infos: (&MintInfo, &MintInfo),
) -> Result<PoolDescriptor> {
    if account.owner != hint.dex.program_id() || account.executable {
        return Err(CopyTraderError::Decode(
            "invalid source pool owner".to_owned(),
        ));
    }
    let data = &account.data;
    let (mint_a, mint_b) = match hint.dex {
        DexKind::PumpSwap => {
            let pool =
                pump_rust_client::accounts::pump_amm::decode_pool(data).map_err(|error| {
                    CopyTraderError::Decode(format!("invalid source PumpSwap pool: {error}"))
                })?;
            (pool.base_mint, pool.quote_mint)
        }
        _ => {
            return Err(CopyTraderError::Unsupported(
                "unsupported pool program".to_owned(),
            ));
        }
    };
    if input == output
        || !((mint_a == input && mint_b == output) || (mint_a == output && mint_b == input))
    {
        return Err(CopyTraderError::Unsupported(
            "source pool does not match the observed pair".to_owned(),
        ));
    }
    Ok(PoolDescriptor {
        dex: hint.dex,
        address: hint.address,
        mint_a,
        mint_b,
        liquidity_hint: 0,
    })
}
