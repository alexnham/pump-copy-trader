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
    domain::{
        LoadedAddresses, ObservedTransaction, SignalOrigin, TransactionMeta, UiCompiledInstruction,
        UiInnerInstructions, UiTokenAmount, UiTokenBalance,
    },
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
    let transaction = decode_transaction(info.transaction.ok_or_else(|| {
        CopyTraderError::Decode("LaserStream transaction body is missing".to_owned())
    })?)?;
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
        meta: decode_meta(meta)?,
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
    if message.config.is_some() {
        return Err(CopyTraderError::Unsupported(
            "transaction v1 messages are not supported".to_owned(),
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

    let message = if message.versioned {
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
    let inner_instructions = (!meta.inner_instructions_none)
        .then(|| {
            meta.inner_instructions
                .into_iter()
                .map(|group| {
                    Ok(UiInnerInstructions {
                        index: checked_u8(group.index, "inner instruction group index")?,
                        instructions: group
                            .instructions
                            .into_iter()
                            .map(|instruction| {
                                Ok(UiCompiledInstruction {
                                    program_id_index: checked_u8(
                                        instruction.program_id_index,
                                        "inner instruction program id index",
                                    )?,
                                    accounts: instruction.accounts,
                                    data: bs58::encode(instruction.data).into_string(),
                                    stack_height: instruction.stack_height,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let log_messages = (!meta.log_messages_none).then_some(meta.log_messages);
    let pre_token_balances = decode_token_balances(meta.pre_token_balances)?;
    let post_token_balances = decode_token_balances(meta.post_token_balances)?;
    let writable = meta
        .loaded_writable_addresses
        .into_iter()
        .map(|address| {
            decode_pubkey(&address, "loaded writable address").map(|key| key.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    let readonly = meta
        .loaded_readonly_addresses
        .into_iter()
        .map(|address| {
            decode_pubkey(&address, "loaded readonly address").map(|key| key.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(TransactionMeta {
        err,
        inner_instructions,
        log_messages,
        pre_token_balances: Some(pre_token_balances),
        post_token_balances: Some(post_token_balances),
        loaded_addresses: Some(LoadedAddresses { writable, readonly }),
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
}
