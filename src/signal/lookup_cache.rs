use crate::{
    domain::ObservedTransaction,
    error::{CopyTraderError, Result},
};
use solana_sdk::{message::VersionedMessage, pubkey::Pubkey};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Learn immutable ALT index mappings from processed transaction metadata.
/// A miss defers the trade; no RPC is made on the preconfirmation hot path.
#[derive(Clone, Default)]
pub(crate) struct LookupCache(Arc<Mutex<HashMap<Pubkey, Table>>>);
#[derive(Default)]
struct Table {
    indices: HashMap<u8, Pubkey>,
    slot: u64,
}
impl LookupCache {
    pub(crate) fn observe(&self, observed: &ObservedTransaction) -> Result<()> {
        let VersionedMessage::V0(message) = &observed.transaction.message else {
            return Ok(());
        };
        let Some(loaded) = &observed.meta.live_loaded_addresses else {
            return Ok(());
        };
        let expected = message
            .address_table_lookups
            .iter()
            .map(|l| l.writable_indexes.len() + l.readonly_indexes.len())
            .sum::<usize>();
        if loaded.len() != expected {
            return Ok(());
        }
        let mut cache = self
            .0
            .lock()
            .map_err(|_| CopyTraderError::Signal("lookup cache poisoned".into()))?;
        if cache.len() >= 4096 {
            cache.retain(|_, table| observed.slot.saturating_sub(table.slot) < 10_000);
            if cache.len() >= 4096 {
                cache.clear();
            }
        }
        let mut addresses = loaded.iter();
        // Solana loads all writable addresses first, then all readonly addresses.
        for writable in [true, false] {
            for lookup in &message.address_table_lookups {
                let table = cache.entry(lookup.account_key).or_default();
                table.slot = observed.slot;
                let indices = if writable {
                    &lookup.writable_indexes
                } else {
                    &lookup.readonly_indexes
                };
                for index in indices {
                    if let Some(address) = addresses.next() {
                        table.indices.insert(*index, *address);
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn resolve(&self, message: &VersionedMessage) -> Result<Option<Vec<Pubkey>>> {
        let VersionedMessage::V0(message) = message else {
            return Ok(Some(Vec::new()));
        };
        let cache = self
            .0
            .lock()
            .map_err(|_| CopyTraderError::Signal("lookup cache poisoned".into()))?;
        let mut addresses = Vec::new();
        for writable in [true, false] {
            for lookup in &message.address_table_lookups {
                let Some(table) = cache.get(&lookup.account_key) else {
                    return Ok(None);
                };
                for index in if writable {
                    &lookup.writable_indexes
                } else {
                    &lookup.readonly_indexes
                } {
                    let Some(address) = table.indices.get(index) else {
                        return Ok(None);
                    };
                    addresses.push(*address);
                }
            }
        }
        Ok(Some(addresses))
    }
}
