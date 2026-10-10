//! Narrow, empirically verified Terminal single-route Pump.fun exact-input buy.
use super::transaction::{DecodeContext, full_account_keys};
use crate::{
    domain::ObservedTransaction,
    error::{CopyTraderError, Result},
    routing::pump_fun,
    token::accounts::{associated_token_address, user_volume_address},
};
use solana_sdk::{message::compiled_instruction::CompiledInstruction, pubkey::Pubkey};

pub(crate) const PROGRAM: Pubkey =
    Pubkey::from_str_const("term9YPb9mzAsABaqN71A4xdbxHmpBNZavpBiQKZzN3");
const MAP: [usize; 28] = [
    12, 9, 7, 4, 3, 5, 17, 18, 19, 20, 11, 21, 22, 0, 8, 26, 23, 24, 25, 33, 27, 28, 29, 30, 2, 31,
    10, 32,
];
const TEMPLATE: [u8; 60] = [
    0xe5, 0x17, 0xcb, 0x97, 0x7a, 0xe3, 0xad, 0x2a, 2, 0, 0, 0, 1, 0, 0x80, 0x33, 2, 0x3b, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 5, 0x18, 1, 0x80, 0x33, 2, 0x3b, 0, 0, 0, 0, 0x86,
    0x6b, 0xc5, 0x27, 0x0a, 0x16, 0, 0, 0x64, 0, 1, 0, 0,
];
fn reject(reason: &str) -> CopyTraderError {
    CopyTraderError::Unsupported(format!("Terminal preconfirmation: {reason}"))
}
pub(crate) fn present(observed: &ObservedTransaction) -> bool {
    full_account_keys(observed).is_ok_and(|keys| {
        observed
            .transaction
            .message
            .instructions()
            .iter()
            .any(|ix| keys.get(ix.program_id_index as usize) == Some(&PROGRAM))
    })
}
pub(super) fn context(
    observed: &ObservedTransaction,
    wallet: Pubkey,
) -> Result<Option<DecodeContext>> {
    let keys = full_account_keys(observed)?;
    let instructions = observed.transaction.message.instructions();
    if !instructions
        .iter()
        .any(|ix| keys.get(ix.program_id_index as usize) == Some(&PROGRAM))
    {
        return Ok(None);
    }
    let mut buy = None;
    let mut setup = false;
    let mut normalized = Vec::new();
    for ix in instructions {
        let program = keys
            .get(ix.program_id_index as usize)
            .ok_or_else(|| reject("unresolved program"))?;
        if *program != PROGRAM {
            if *program == pump_fun::PROGRAM_ID
                || *program == crate::domain::DexKind::PumpSwap.program_id()
            {
                return Err(reject("additional trade"));
            }
            normalized.push(ix.clone());
            continue;
        }
        if ix.data.get(..8) == Some(&[0xde, 0x44, 0xee, 0x3b, 0xb9, 0xb6, 0x3f, 0x88]) {
            if setup
                || buy.is_some()
                || ix.data.len() != 10
                || ix.accounts.len() != 2
                || keys.get(ix.accounts[1] as usize) != Some(&wallet)
                || keys.get(ix.accounts[0] as usize)
                    != Some(&Pubkey::from_str_const(
                        "8EZSSGQJV3EyukvwTRyaPR9DbbKpyF8BLUAhHbDdCDq",
                    ))
            {
                return Err(reject("unsupported setup"));
            }
            setup = true;
            continue;
        }
        if buy.is_some() {
            return Err(reject("multiple Terminal calls"));
        }
        if ix.data.len() != 60 || ix.accounts.len() != 39 {
            return Err(reject("unsupported layout length"));
        }
        if ix.accounts.iter().any(|i| keys.get(*i as usize).is_none()) {
            return Err(reject("unresolved account"));
        }
        for (i, byte) in ix.data.iter().enumerate() {
            if !(14..22).contains(&i) && !(39..55).contains(&i) && *byte != TEMPLATE[i] {
                return Err(reject("unverified flags or fee"));
            }
        }
        let gross = u64::from_le_bytes(ix.data[14..22].try_into().unwrap());
        if gross == 0 || gross != u64::from_le_bytes(ix.data[39..47].try_into().unwrap()) {
            return Err(reject("invalid or inconsistent input"));
        }
        if gross % 100 != 0 {
            return Err(reject("unverified fee rounding"));
        }
        let net = gross
            .checked_sub(gross / 100)
            .ok_or_else(|| reject("fee arithmetic"))?;
        let accounts: Vec<u8> = MAP.iter().map(|i| ix.accounts[*i]).collect();
        let account = |i: usize| {
            keys.get(accounts[i] as usize)
                .copied()
                .ok_or_else(|| reject("unresolved account"))
        };
        let quote = spl_token::native_mint::id();
        let base_program = account(3)?;
        if account(13)? != wallet
            || ![spl_token::id(), spl_token_2022::id()].contains(&base_program)
            || account(2)? != quote
            || account(4)? != spl_token::id()
            || account(5)? != spl_associated_token_account::id()
            || account(24)? != Pubkey::default()
            || account(26)? != pump_fun::PROGRAM_ID
            || account(14)? != associated_token_address(&wallet, &account(1)?, &base_program)
            || account(15)? != associated_token_address(&wallet, &quote, &spl_token::id())
            || account(20)? != user_volume_address(&pump_fun::PROGRAM_ID, &wallet)
            || account(21)? != associated_token_address(&account(20)?, &quote, &spl_token::id())
        {
            return Err(reject("invalid wallet, programs, or derived accounts"));
        }
        let mut data = pump_fun::BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR.to_vec();
        data.extend_from_slice(&net.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(1);
        buy = Some(CompiledInstruction {
            program_id_index: accounts[26],
            accounts,
            data,
        });
    }
    normalized.push(buy.ok_or_else(|| reject("no supported buy"))?);
    Ok(Some(DecodeContext {
        keys,
        instructions: normalized,
    }))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::domain::{SignalOrigin, TransactionMeta};
    use solana_sdk::{
        hash::Hash,
        message::{Message, MessageHeader, VersionedMessage},
        signature::Signature,
        transaction::VersionedTransaction,
    };
    pub(crate) fn fixtures() -> Vec<serde_json::Value> {
        serde_json::from_str(include_str!("../../tests/fixtures/terminal_buys.json")).unwrap()
    }
    pub(crate) fn observation(f: &serde_json::Value) -> (ObservedTransaction, Pubkey) {
        let m = &f["message"];
        let mut keys: Vec<Pubkey> = m["accountKeys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().parse().unwrap())
            .collect();
        for field in ["writable", "readonly"] {
            if let Some(a) = f["loaded"][field].as_array() {
                keys.extend(
                    a.iter()
                        .map(|v| v.as_str().unwrap().parse::<Pubkey>().unwrap()),
                );
            }
        }
        let header = MessageHeader {
            num_required_signatures: m["header"]["numRequiredSignatures"].as_u64().unwrap() as u8,
            num_readonly_signed_accounts: m["header"]["numReadonlySignedAccounts"].as_u64().unwrap()
                as u8,
            num_readonly_unsigned_accounts: m["header"]["numReadonlyUnsignedAccounts"]
                .as_u64()
                .unwrap() as u8,
        };
        let instructions = m["instructions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| CompiledInstruction {
                program_id_index: v["programIdIndex"].as_u64().unwrap() as u8,
                accounts: v["accounts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8)
                    .collect(),
                data: bs58::decode(v["data"].as_str().unwrap())
                    .into_vec()
                    .unwrap(),
            })
            .collect();
        let wallet = keys[0];
        let signature: Signature = f["signature"].as_str().unwrap().parse().unwrap();
        (
            ObservedTransaction {
                signature,
                slot: f["slot"].as_u64().unwrap(),
                block_time: None,
                origin: SignalOrigin::Preconfirmation,
                transaction: VersionedTransaction {
                    signatures: vec![signature],
                    message: VersionedMessage::Legacy(Message {
                        header,
                        account_keys: keys,
                        recent_blockhash: Hash::default(),
                        instructions,
                    }),
                },
                meta: TransactionMeta::default(),
                raw_payload: String::new(),
                received_bytes: 0,
            },
            wallet,
        )
    }
    #[test]
    fn eleven_historical_buys_reconstruct_executed_instructions() {
        let all = fixtures();
        assert_eq!(all.len(), 11);
        for f in all {
            let (o, w) = observation(&f);
            let original = o.transaction.clone();
            let ctx = context(&o, w).unwrap().unwrap();
            let ix = ctx.instructions.last().unwrap();
            assert_eq!(
                ix.data,
                bs58::decode(f["pump"]["data"].as_str().unwrap())
                    .into_vec()
                    .unwrap()
            );
            assert_eq!(
                ix.accounts,
                f["pump"]["accounts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8)
                    .collect::<Vec<_>>()
            );
            for status in [1, 2] {
                let mut early = o.clone();
                early.meta.preconfirmation_status = Some(status);
                let intent = super::super::preconfirmation::decode(&early, w).unwrap();
                assert_eq!(
                    intent.source_input_amount,
                    u64::from_le_bytes(ix.data[8..16].try_into().unwrap())
                );
                assert_eq!(intent.source_instruction.unwrap().instruction.data, ix.data);
            }
            assert_eq!(o.transaction, original);
        }
    }
    fn buy_mut(o: &mut ObservedTransaction) -> &mut CompiledInstruction {
        let VersionedMessage::Legacy(m) = &mut o.transaction.message else {
            panic!()
        };
        m.instructions
            .iter_mut()
            .find(|ix| ix.data.len() == 60)
            .unwrap()
    }
    #[test]
    fn unfamiliar_layouts_and_invalid_accounts_defer() {
        let f = &fixtures()[0];
        for offset in [0, 8, 12, 22, 32, 36, 37, 38, 55, 57, 59] {
            let (mut o, w) = observation(f);
            buy_mut(&mut o).data[offset] ^= 1;
            assert!(
                super::super::preconfirmation::decode(&o, w).is_err(),
                "offset {offset}"
            );
        }
        for amount in [0, 101, u64::MAX] {
            let (mut o, w) = observation(f);
            let ix = buy_mut(&mut o);
            ix.data[14..22].copy_from_slice(&amount.to_le_bytes());
            ix.data[39..47].copy_from_slice(&amount.to_le_bytes());
            assert!(context(&o, w).is_err());
        }
        let (mut o, w) = observation(f);
        buy_mut(&mut o).data[14] ^= 1;
        assert!(context(&o, w).is_err());
        for i in [0, 3, 4, 5, 7, 8, 10, 26, 27, 28] {
            let (mut o, w) = observation(f);
            buy_mut(&mut o).accounts[i] = buy_mut(&mut o).accounts[9];
            assert!(context(&o, w).is_err(), "account {i}");
        }
        let (mut o, w) = observation(f);
        buy_mut(&mut o).data.pop();
        assert!(context(&o, w).is_err());
        let (mut o, w) = observation(f);
        buy_mut(&mut o).accounts.pop();
        assert!(context(&o, w).is_err());
        let (mut o, w) = observation(f);
        let extra = buy_mut(&mut o).clone();
        let VersionedMessage::Legacy(m) = &mut o.transaction.message else {
            panic!()
        };
        m.instructions.push(extra);
        assert!(context(&o, w).is_err());
        let (mut o, w) = observation(f);
        let VersionedMessage::Legacy(m) = &mut o.transaction.message else {
            panic!()
        };
        m.header.num_required_signatures = 0;
        assert!(super::super::preconfirmation::decode(&o, w).is_err());
    }
}
