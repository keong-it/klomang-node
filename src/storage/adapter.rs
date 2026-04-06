//! Storage Adapter untuk Klomang Core
//!
//! Menyediakan implementasi Storage trait dari klomang-core yang menggunakan
//! RocksDB sebagai backend penyimpanan persisten untuk Verkle trees dan UTXO data.

use crate::storage::{KlomangStorage, StorageHandle};
use klomang_core::core::state::storage::Storage;
use std::sync::{Arc, RwLock};

/// RocksDB-backed storage adapter untuk klomang-core
/// Menggunakan CF_STATE untuk menyimpan Verkle tree data dan UTXO state
#[derive(Clone)]
pub struct RocksDBStorageAdapter {
    storage: StorageHandle,
}

impl RocksDBStorageAdapter {
    /// Create new adapter dengan storage handle
    pub fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }
}

impl Storage for RocksDBStorageAdapter {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let storage_read = match self.storage.read() {
            Ok(read) => read,
            Err(e) => {
                log::error!("Failed to acquire storage read lock: {}", e);
                return None;
            }
        };

        match storage_read.get_state(&String::from_utf8_lossy(key)) {
            Ok(Some(value)) => Some(value),
            Ok(None) => None,
            Err(e) => {
                log::error!("Failed to read from RocksDB storage: {}", e);
                None
            }
        }
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let mut storage_write = match self.storage.write() {
            Ok(write) => write,
            Err(e) => {
                log::error!("Failed to acquire storage write lock: {}", e);
                return;
            }
        };

        if let Err(e) = storage_write.put_state(&String::from_utf8_lossy(&key), &value) {
            log::error!("Failed to write to RocksDB storage: {}", e);
        }
    }

    fn delete(&mut self, _key: &[u8]) {
        // Underlying KlomangStorage currently does not expose a delete_state method.
        // For the adapter, we preserve the API but treat deletes as no-op.
        log::warn!(
            "RocksDBStorageAdapter.delete called, but delete_state is not supported by KlomangStorage"
        );
    }

    fn clear(&mut self) {
        // Note: RocksDB tidak memiliki operasi clear langsung untuk CF
        // Kita bisa implementasikan dengan menghapus semua keys yang dimulai dengan prefix tertentu
        // Untuk saat ini, log warning karena operasi ini mahal dan jarang digunakan
        log::warn!("Clear operation not implemented for RocksDB storage - use with caution");
    }
}

/// Helper untuk load/save UTXO set dari/ke storage
pub struct UtxoStorage {
    storage: StorageHandle,
}

impl UtxoStorage {
    pub fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }

    /// Load UTXO set dari storage saat startup
    pub fn load_utxo_set(
        &self,
    ) -> Result<klomang_core::core::state::utxo::UtxoSet, Box<dyn std::error::Error>> {
        let storage_read = self
            .storage
            .read()
            .map_err(|e| format!("Failed to acquire storage read lock: {}", e))?;

        // Load UTXO set dari state storage
        let utxo_data = storage_read.get_state("utxo_set")?;

        match utxo_data {
            Some(data) => {
                let utxo_set: klomang_core::core::state::utxo::UtxoSet =
                    bincode::deserialize(&data)
                        .map_err(|e| format!("Failed to deserialize UTXO set: {}", e))?;
                log::info!("Loaded UTXO set with {} entries", utxo_set.utxos.len());
                Ok(utxo_set)
            }
            None => {
                log::info!("No UTXO set found in storage, starting with empty set");
                Ok(klomang_core::core::state::utxo::UtxoSet::new())
            }
        }
    }

    /// Save UTXO set ke storage
    pub fn save_utxo_set(
        &self,
        utxo_set: &klomang_core::core::state::utxo::UtxoSet,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut storage_write = self
            .storage
            .write()
            .map_err(|e| format!("Failed to acquire storage write lock: {}", e))?;

        let utxo_data = bincode::serialize(utxo_set)
            .map_err(|e| format!("Failed to serialize UTXO set: {}", e))?;

        storage_write
            .put_state("utxo_set", &utxo_data)
            .map_err(|e| format!("Failed to save UTXO set: {}", e))?;

        log::debug!("Saved UTXO set with {} entries", utxo_set.utxos.len());
        Ok(())
    }
}
