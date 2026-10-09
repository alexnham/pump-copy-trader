use helius_laserstream::{
    grpc::{SubscribeUpdate, subscribe_update::UpdateOneof},
    solana::storage::confirmed_block as grpc_solana,
};
use prost::Message as _;
use serde_json::json;
use solana_sdk::{
    hash::Hash,
    message::{
        Message, MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction, v0,
    },
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};

use crate::{
    domain::{ObservedTransaction, SignalOrigin, TransactionMeta, UiTokenAmount, UiTokenBalance},
    error::{CopyTraderError, Result},
};

pub fn decode_update(update: SubscribeUpdate) -> Result<Option<ObservedTransaction>> {
    let received_bytes = update.encoded_len();
    let created_at = update
        .created_at
        .as_ref()
        .map(|timestamp| timestamp.seconds);
    let Some(UpdateOneof::Transaction(transaction_update)) = update.update_oneof else {
        return Ok(None);
    };
    let info = transaction_update.transaction.ok_or_else(|| {
        CopyTraderError::Decode("LaserStream transaction info is missing".to_owned())
    })?;
    // Avoid formatting the full protobuf on the ingestion hot path. The decoded
    // transaction and durable signature carry the information needed downstream.
    let raw_payload = String::new();
    let meta = info.meta.ok_or_else(|| {
        CopyTraderError::Decode("LaserStream transaction metadata is missing".to_owned())
    })?;
    if meta.err.is_some() {
        return Ok(None);
    }
    let signature = Signature::try_from(info.signature.as_slice()).map_err(|_| {
        CopyTraderError::Decode("invalid LaserStream transaction signature".to_owned())
    })?;
    let body = info.transaction.ok_or_else(|| {
        CopyTraderError::Decode("LaserStream transaction body is missing".to_owned())
    })?;
    let v1_config = body.message.as_ref().and_then(|message| message.config);
    if v1_config.is_some()
        && (!meta.loaded_writable_addresses.is_empty()
            || !meta.loaded_readonly_addresses.is_empty())
    {
        return Err(CopyTraderError::Decode(
            "v1 cannot contain loaded addresses".to_owned(),
        ));
    }
    let transaction = decode_transaction(body)?;
    let mut meta = decode_meta(meta)?;
    meta.source_v1_config = v1_config.map(|config| crate::domain::signal::SourceV1Config {
        priority_fee: config.priority_fee,
        compute_unit_limit: config.compute_unit_limit,
        loaded_accounts_data_size_limit: config.loaded_accounts_data_size_limit,
        heap_size: config.heap_size,
    });
    if transaction.signatures.first() != Some(&signature) {
        return Err(CopyTraderError::Decode(
            "LaserStream signature does not match transaction".to_owned(),
        ));
    }
    Ok(Some(ObservedTransaction {
        signature,
        slot: transaction_update.slot,
        block_time: created_at,
        origin: SignalOrigin::Live,
        transaction,
        meta,
        raw_payload,
        received_bytes,
    }))
}

fn decode_transaction(transaction: grpc_solana::Transaction) -> Result<VersionedTransaction> {
    let signatures = transaction
        .signatures
        .into_iter()
        .map(|signature| {
            Signature::try_from(signature.as_slice()).map_err(|_| {
                CopyTraderError::Decode("invalid signature in transaction body".to_owned())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let message = transaction
        .message
        .ok_or_else(|| CopyTraderError::Decode("transaction message is missing".to_owned()))?;
    let is_v1 = message.config.is_some();
    if is_v1
        && (!message.address_table_lookups.is_empty()
            || message.account_keys.len() > 64
            || message.instructions.len() > 64)
    {
        return Err(CopyTraderError::Decode(
            "invalid v1 account or instruction layout".to_owned(),
        ));
    }
    let header = message
        .header
        .ok_or_else(|| CopyTraderError::Decode("message header is missing".to_owned()))?;
    let header = MessageHeader {
        num_required_signatures: checked_u8(header.num_required_signatures, "required signatures")?,
        num_readonly_signed_accounts: checked_u8(
            header.num_readonly_signed_accounts,
            "readonly signed accounts",
        )?,
        num_readonly_unsigned_accounts: checked_u8(
            header.num_readonly_unsigned_accounts,
            "readonly unsigned accounts",
        )?,
    };
    let account_keys = message
        .account_keys
        .into_iter()
        .map(|key| decode_pubkey(&key, "message account key"))
        .collect::<Result<Vec<_>>>()?;
    let recent_blockhash_bytes: [u8; 32] = message
        .recent_blockhash
        .as_slice()
        .try_into()
        .map_err(|_| CopyTraderError::Decode("invalid recent blockhash".to_owned()))?;
    let recent_blockhash = Hash::new_from_array(recent_blockhash_bytes);
    let instructions = message
        .instructions
        .into_iter()
        .map(|instruction| {
            Ok(CompiledInstruction {
                program_id_index: checked_u8(instruction.program_id_index, "program id index")?,
                accounts: instruction.accounts,
                data: instruction.data,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if is_v1 {
        let required = usize::from(header.num_required_signatures);
        if required == 0
            || signatures.len() != required
            || required > account_keys.len()
            || usize::from(header.num_readonly_signed_accounts) >= required
            || usize::from(header.num_readonly_unsigned_accounts) > account_keys.len() - required
            || instructions.iter().any(|instruction| {
                usize::from(instruction.program_id_index) >= account_keys.len()
                    || instruction
                        .accounts
                        .iter()
                        .any(|index| usize::from(*index) >= account_keys.len())
            })
        {
            return Err(CopyTraderError::Decode(
                "invalid v1 header or account index".to_owned(),
            ));
        }
    }
    let message = if message.versioned && !is_v1 {
        let address_table_lookups = message
            .address_table_lookups
            .into_iter()
            .map(|lookup| {
                Ok(v0::MessageAddressTableLookup {
                    account_key: decode_pubkey(&lookup.account_key, "lookup table account")?,
                    writable_indexes: lookup.writable_indexes,
                    readonly_indexes: lookup.readonly_indexes,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        VersionedMessage::V0(v0::Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
        })
    } else {
        VersionedMessage::Legacy(Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        })
    };
    Ok(VersionedTransaction {
        signatures,
        message,
    })
}

fn decode_meta(meta: grpc_solana::TransactionStatusMeta) -> Result<TransactionMeta> {
    let err = meta
        .err
        .map(|error| json!({ "laserstream": bs58::encode(error.err).into_string() }));
    let live_inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        let mut instructions = Vec::with_capacity(
            meta.inner_instructions
                .iter()
                .map(|group| group.instructions.len())
                .sum(),
        );
        for group in meta.inner_instructions {
            checked_u8(group.index, "inner instruction group index")?;
            for instruction in group.instructions {
                instructions.push(CompiledInstruction {
                    program_id_index: checked_u8(
                        instruction.program_id_index,
                        "inner instruction program id index",
                    )?,
                    accounts: instruction.accounts,
                    data: instruction.data,
                });
            }
        }
        Some(instructions)
    };
    let log_messages = (!meta.log_messages_none).then_some(meta.log_messages);
    let pre_token_balances = decode_token_balances(meta.pre_token_balances)?;
    let post_token_balances = decode_token_balances(meta.post_token_balances)?;
    let live_loaded_addresses = meta
        .loaded_writable_addresses
        .into_iter()
        .chain(meta.loaded_readonly_addresses)
        .map(|address| decode_pubkey(&address, "loaded address"))
        .collect::<Result<Vec<_>>>()?;
    Ok(TransactionMeta {
        source_v1_config: None,
        err,
        inner_instructions: None,
        live_inner_instructions,
        live_loaded_addresses: Some(live_loaded_addresses),
        log_messages,
        pre_token_balances: Some(pre_token_balances),
        post_token_balances: Some(post_token_balances),
        loaded_addresses: None,
        compute_units_consumed: meta.compute_units_consumed,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        fee: meta.fee,
    })
}

fn decode_token_balances(balances: Vec<grpc_solana::TokenBalance>) -> Result<Vec<UiTokenBalance>> {
    balances
        .into_iter()
        .map(|balance| {
            let amount = balance.ui_token_amount.ok_or_else(|| {
                CopyTraderError::Decode("token balance amount is missing".to_owned())
            })?;
            Ok(UiTokenBalance {
                account_index: checked_u8(balance.account_index, "token account index")?,
                mint: balance.mint,
                ui_token_amount: UiTokenAmount {
                    amount: amount.amount,
                    decimals: checked_u8(amount.decimals, "token decimals")?,
                },
                owner: nonempty(balance.owner),
                program_id: nonempty(balance.program_id),
            })
        })
        .collect()
}

fn decode_pubkey(bytes: &[u8], field: &str) -> Result<Pubkey> {
    Pubkey::try_from(bytes).map_err(|_| CopyTraderError::Decode(format!("invalid {field}")))
}

fn checked_u8(value: u32, field: &str) -> Result<u8> {
    u8::try_from(value).map_err(|_| CopyTraderError::Decode(format!("{field} exceeds u8")))
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use helius_laserstream::grpc::{
        SubscribeUpdate, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        subscribe_update::UpdateOneof,
    };
    use solana_sdk::{
        hash::Hash,
        signature::{Keypair, Signer},
        transaction::{Transaction, VersionedTransaction},
    };

    use super::*;

    fn v1_fixture() -> grpc_solana::Transaction {
        grpc_solana::Transaction {
            signatures: vec![Signature::default().as_ref().to_vec()],
            message: Some(grpc_solana::Message {
                header: Some(grpc_solana::MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                }),
                account_keys: vec![
                    Pubkey::new_unique().to_bytes().to_vec(),
                    Pubkey::new_unique().to_bytes().to_vec(),
                ],
                recent_blockhash: Hash::new_unique().to_bytes().to_vec(),
                instructions: vec![grpc_solana::CompiledInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: vec![1, 2, 3],
                }],
                versioned: true,
                config: Some(grpc_solana::TransactionConfig {
                    priority_fee: Some(123),
                    compute_unit_limit: Some(200_000),
                    loaded_accounts_data_size_limit: Some(64 * 1024),
                    heap_size: None,
                }),
                address_table_lookups: vec![],
            }),
        }
    }

    #[test]
    fn v1_preserves_instruction_accounts_signers_and_config() {
        let body = v1_fixture();
        let mut legacy = body.clone();
        legacy.message.as_mut().expect("message").config = None;
        legacy.message.as_mut().expect("message").versioned = false;
        let expected = decode_transaction(legacy).expect("legacy");
        let decoded = decode_transaction(body.clone()).expect("v1");
        assert_eq!(decoded, expected);
        assert!(decoded.message.is_signer(0));
        assert!(!decoded.message.is_signer(1));
        assert!(decoded.message.is_maybe_writable(0, None));
        assert!(!decoded.message.is_maybe_writable(1, None));
        let mut v0_body = body.clone();
        v0_body.message.as_mut().expect("message").config = None;
        let v0 = decode_transaction(v0_body).expect("v0");
        assert!(matches!(v0.message, VersionedMessage::V0(_)));
        assert_eq!(v0.message.instructions(), decoded.message.instructions());
        assert_eq!(
            v0.message.static_account_keys(),
            decoded.message.static_account_keys()
        );
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: 123,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: Signature::default().as_ref().to_vec(),
                    transaction: Some(body),
                    meta: Some(grpc_solana::TransactionStatusMeta::default()),
                    ..Default::default()
                }),
            })),
            ..Default::default()
        };
        let observed = decode_update(update.clone())
            .expect("decode")
            .expect("observation");
        assert_eq!(
            observed.meta.source_v1_config.expect("config").priority_fee,
            Some(123)
        );
        let mut bad = update;
        if let Some(UpdateOneof::Transaction(ref mut tx)) = bad.update_oneof {
            tx.transaction
                .as_mut()
                .expect("info")
                .meta
                .as_mut()
                .expect("meta")
                .loaded_writable_addresses
                .push(Pubkey::new_unique().to_bytes().to_vec());
        }
        assert!(decode_update(bad).is_err());
    }

    #[test]
    fn v1_rejects_invalid_layouts() {
        for case in 0..7 {
            let mut body = v1_fixture();
            let message = body.message.as_mut().expect("message");
            match case {
                0 => message.address_table_lookups.push(Default::default()),
                1 => message.instructions[0].accounts.push(2),
                2 => message.instructions[0].program_id_index = 2,
                3 => {
                    message
                        .header
                        .as_mut()
                        .expect("header")
                        .num_required_signatures = 3
                }
                4 => message.account_keys = vec![Pubkey::new_unique().to_bytes().to_vec(); 65],
                5 => message.instructions = vec![message.instructions[0].clone(); 65],
                _ => {
                    message
                        .header
                        .as_mut()
                        .expect("header")
                        .num_readonly_unsigned_accounts = 2
                }
            }
            assert!(decode_transaction(body).is_err(), "case {case}");
        }
    }

    #[test]
    #[ignore = "manual release benchmark"]
    fn benchmark_v1_source_decode() {
        let mut body = v1_fixture();
        let message = body.message.as_mut().expect("message");
        message
            .account_keys
            .extend((2..64).map(|_| Pubkey::new_unique().to_bytes().to_vec()));
        message.instructions = (0..64)
            .map(|_| grpc_solana::CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0, 2, 3, 4],
                data: vec![7; 16],
            })
            .collect();
        let mut samples = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let input = body.clone();
            let start = std::time::Instant::now();
            std::hint::black_box(decode_transaction(input).expect("decode"));
            samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        eprintln!(
            "v1 source decode: p50={} ns p95={} ns p99={} ns",
            samples[5000], samples[9500], samples[9900]
        );
    }

    #[test]
    fn non_transaction_update_is_ignored() {
        assert!(decode_update(SubscribeUpdate::default()).is_ok_and(|value| value.is_none()));
    }

    #[test]
    fn full_grpc_transaction_decodes_without_follow_up_rpc() {
        let payer = Keypair::new();
        let transaction = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::new_unique(),
        ));
        let signature = transaction.signatures[0];
        let VersionedMessage::Legacy(message) = transaction.message.clone() else {
            return;
        };
        let grpc_transaction = grpc_solana::Transaction {
            signatures: transaction
                .signatures
                .iter()
                .map(|value| value.as_ref().to_vec())
                .collect(),
            message: Some(grpc_solana::Message {
                header: Some(grpc_solana::MessageHeader {
                    num_required_signatures: u32::from(message.header.num_required_signatures),
                    num_readonly_signed_accounts: u32::from(
                        message.header.num_readonly_signed_accounts,
                    ),
                    num_readonly_unsigned_accounts: u32::from(
                        message.header.num_readonly_unsigned_accounts,
                    ),
                }),
                account_keys: message
                    .account_keys
                    .iter()
                    .map(|value| value.as_ref().to_vec())
                    .collect(),
                recent_blockhash: message.recent_blockhash.as_ref().to_vec(),
                instructions: Vec::new(),
                versioned: false,
                address_table_lookups: Vec::new(),
                config: None,
            }),
        };
        let update = SubscribeUpdate {
            filters: vec!["copy-trader".to_owned()],
            created_at: None,
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: 123,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: signature.as_ref().to_vec(),
                    is_vote: false,
                    transaction: Some(grpc_transaction),
                    meta: Some(grpc_solana::TransactionStatusMeta::default()),
                    index: 0,
                }),
            })),
        };
        let observed = decode_update(update).ok().flatten();
        assert_eq!(observed.as_ref().map(|value| value.slot), Some(123));
        assert_eq!(observed.as_ref().map(|value| value.block_time), Some(None));
        assert_eq!(observed.map(|value| value.signature), Some(signature));
    }
    #[test]
    fn live_metadata_preserves_binary_instructions_and_address_order() {
        let writable = Pubkey::new_unique();
        let readonly = Pubkey::new_unique();
        let meta = grpc_solana::TransactionStatusMeta {
            loaded_writable_addresses: vec![writable.to_bytes().to_vec()],
            loaded_readonly_addresses: vec![readonly.to_bytes().to_vec()],
            inner_instructions: vec![grpc_solana::InnerInstructions {
                index: 3,
                instructions: vec![grpc_solana::InnerInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 1],
                    data: vec![7; 700],
                    stack_height: Some(2),
                }],
            }],
            ..Default::default()
        };
        let decoded = decode_meta(meta).expect("binary metadata");
        assert_eq!(
            decoded.live_loaded_addresses,
            Some(vec![writable, readonly])
        );
        let instructions = decoded.live_inner_instructions.expect("inner instructions");
        assert_eq!(instructions.len(), 1);
        assert_eq!(instructions[0].data, vec![7; 700]);
        assert_eq!(instructions[0].accounts, vec![0, 1]);
        assert!(decoded.inner_instructions.is_none());
        assert!(decoded.loaded_addresses.is_none());
        for bytes in [vec![], vec![0; 31], vec![0; 33]] {
            assert!(
                decode_meta(grpc_solana::TransactionStatusMeta {
                    loaded_writable_addresses: vec![bytes],
                    ..Default::default()
                })
                .is_err()
            );
        }
        assert!(
            decode_meta(grpc_solana::TransactionStatusMeta {
                inner_instructions: vec![grpc_solana::InnerInstructions {
                    index: 256,
                    instructions: vec![],
                }],
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    #[ignore = "manual decode conversion benchmark"]
    fn benchmark_live_metadata_conversion() {
        use std::{hint::black_box, time::Instant};
        let meta = grpc_solana::TransactionStatusMeta {
            loaded_writable_addresses: (0..20)
                .map(|_| Pubkey::new_unique().to_bytes().to_vec())
                .collect(),
            inner_instructions: vec![grpc_solana::InnerInstructions {
                index: 0,
                instructions: (0..12)
                    .map(|i| grpc_solana::InnerInstruction {
                        program_id_index: 1,
                        accounts: vec![0, 1, 2],
                        data: vec![i; if i == 0 { 700 } else { 32 }],
                        stack_height: Some(2),
                    })
                    .collect(),
            }],
            ..Default::default()
        };
        let rounds = 1000;
        let start = Instant::now();
        for _ in 0..rounds {
            let input = black_box(meta.clone());
            for group in &input.inner_instructions {
                for ix in &group.instructions {
                    let encoded = bs58::encode(&ix.data).into_string();
                    black_box(bs58::decode(encoded).into_vec().expect("legacy data"));
                }
            }
            for bytes in &input.loaded_writable_addresses {
                let key = Pubkey::try_from(bytes.as_slice()).expect("key");
                black_box(key.to_string().parse::<Pubkey>().expect("legacy address"));
            }
            black_box(decode_meta(input).expect("metadata"));
        }
        let legacy = start.elapsed();
        let start = Instant::now();
        for _ in 0..rounds {
            black_box(decode_meta(black_box(meta.clone())).expect("metadata"));
        }
        let binary = start.elapsed();
        eprintln!(
            "metadata fixture: legacy conversions + decode {:.2} us/op; binary decode {:.2} us/op",
            legacy.as_secs_f64() * 1e6 / f64::from(rounds),
            binary.as_secs_f64() * 1e6 / f64::from(rounds)
        );
    }
}
