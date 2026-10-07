use solana_sdk::{instruction::Instruction, pubkey::Pubkey};

pub(crate) fn user_volume_address(program: &Pubkey, owner: &Pubkey) -> Pubkey {
    use std::{
        collections::HashMap,
        sync::{Mutex, OnceLock},
    };
    static CACHE: OnceLock<Mutex<HashMap<(Pubkey, Pubkey), Pubkey>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (*program, *owner);
    if let Some(address) = cache
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&key)
        .copied()
    {
        return address;
    }
    let address =
        Pubkey::find_program_address(&[b"user_volume_accumulator", owner.as_ref()], program).0;
    let mut cache = cache.lock().unwrap_or_else(|error| error.into_inner());
    if cache.len() < 8192 {
        cache.insert(key, address);
    }
    address
}

pub fn associated_token_address(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    use std::{
        collections::HashMap,
        sync::{Mutex, OnceLock},
    };
    type Key = (Pubkey, Pubkey, Pubkey);
    static CACHE: OnceLock<Mutex<HashMap<Key, Pubkey>>> = OnceLock::new();
    const MAX_ENTRIES: usize = 8192;
    let key = (*owner, *mint, *token_program);
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(address) = cache
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&key)
        .copied()
    {
        return address;
    }
    let address = spl_associated_token_account::get_associated_token_address_with_program_id(
        owner,
        mint,
        token_program,
    );
    let mut cache = cache.lock().unwrap_or_else(|error| error.into_inner());
    if cache.len() >= MAX_ENTRIES {
        cache.clear();
    }
    cache.insert(key, address);
    address
}

pub fn create_associated_token_account_idempotent(
    payer: &Pubkey,
    owner: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> Instruction {
    spl_associated_token_account::instruction::create_associated_token_account_idempotent(
        payer,
        owner,
        mint,
        token_program,
    )
}

pub(crate) fn validate_wsol_account(
    account: &solana_sdk::account::Account,
    owner: &Pubkey,
) -> crate::error::Result<()> {
    use solana_sdk::program_pack::Pack;
    let invalid = || {
        crate::error::CopyTraderError::Execution(
            "copier WSOL account has invalid owner, mint, authority, or native state".to_owned(),
        )
    };
    if account.owner != spl_token::id() {
        return Err(invalid());
    }
    let token = spl_token::state::Account::unpack(&account.data).map_err(|_| invalid())?;
    if token.mint != spl_token::native_mint::id()
        || token.owner != *owner
        || token.state != spl_token::state::AccountState::Initialized
        || token.is_native.is_none()
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn cached_volume_addresses_match_both_programs_and_owners() {
        for owner in [Pubkey::new_unique(), Pubkey::new_unique()] {
            for program in [
                crate::domain::DexKind::PumpSwap.program_id(),
                crate::routing::pump_fun::PROGRAM_ID,
            ] {
                let expected = Pubkey::find_program_address(
                    &[b"user_volume_accumulator", owner.as_ref()],
                    &program,
                )
                .0;
                assert_eq!(super::user_volume_address(&program, &owner), expected);
                assert_eq!(super::user_volume_address(&program, &owner), expected);
            }
        }
    }

    use super::*;
    use solana_sdk::{program_option::COption, program_pack::Pack};
    #[test]
    fn cached_addresses_keep_owner_mint_and_program_distinct() {
        let owner = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        for (owner, mint, program) in [
            (owner, mint, spl_token::id()),
            (owner, mint, spl_token_2022::id()),
            (Pubkey::new_unique(), mint, spl_token::id()),
            (owner, Pubkey::new_unique(), spl_token::id()),
        ] {
            let expected =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &owner, &mint, &program,
                );
            assert_eq!(associated_token_address(&owner, &mint, &program), expected);
            assert_eq!(associated_token_address(&owner, &mint, &program), expected);
        }
    }

    #[test]
    fn validates_persistent_wsol_account() {
        let owner = Pubkey::new_unique();
        let mut token = spl_token::state::Account {
            mint: spl_token::native_mint::id(),
            owner,
            state: spl_token::state::AccountState::Initialized,
            is_native: COption::Some(2039280),
            ..Default::default()
        };
        let mut account = solana_sdk::account::Account {
            owner: spl_token::id(),
            data: vec![0; spl_token::state::Account::LEN],
            ..Default::default()
        };
        spl_token::state::Account::pack(token, &mut account.data).expect("pack");
        assert!(validate_wsol_account(&account, &owner).is_ok());
        assert!(validate_wsol_account(&account, &Pubkey::new_unique()).is_err());
        token.is_native = COption::None;
        spl_token::state::Account::pack(token, &mut account.data).expect("pack");
        assert!(validate_wsol_account(&account, &owner).is_err());
        account.owner = Pubkey::new_unique();
        assert!(validate_wsol_account(&account, &owner).is_err());
    }
}
