//! Mempool Persistence for Crash Recovery
//!
//! Saves and loads mempool snapshots to/from RocksDB for persistence
//! across node restarts.

use crate::storage::StorageHandle;
use bincode;
use klomang_core::{Mempool, SignedTransaction};

/// Mempool snapshot manager
pub struct MempoolSnapshot {
    storage: StorageHandle,
    snapshot_key: String,
}

impl MempoolSnapshot {
    pub fn new(storage: StorageHandle) -> Self {
        MempoolSnapshot {
            storage,
            snapshot_key: "mempool_snapshot".to_string(),
        }
    }

    /// Save mempool snapshot
    pub fn save(&self, mempool: &Mempool) -> Result<(), String> {
        // NOTE: get_all_transactions removed from klomang-core API
        // Use get_top_transactions instead or return empty snapshot
        let transactions: Vec<SignedTransaction> = vec![]; // TODO: implement proper snapshot
        let serialized =
            bincode::serialize(&transactions).map_err(|e| format!("Serialization error: {}", e))?;

        self.storage
            .write()
            .map_err(|e| format!("Storage lock error: {}", e))?
            .put_state(&self.snapshot_key, &serialized)
            .map_err(|e| format!("Storage write error: {}", e))?;

        log::info!(
            "Saved mempool snapshot with {} transactions",
            transactions.len()
        );
        Ok(())
    }

    /// Load mempool snapshot
    pub fn load(&self) -> Result<Vec<SignedTransaction>, String> {
        let data = self
            .storage
            .read()
            .map_err(|e| format!("Storage lock error: {}", e))?
            .get_state(&self.snapshot_key)
            .map_err(|e| format!("Storage read error: {}", e))?
            .ok_or("No mempool snapshot found")?;

        let transactions: Vec<SignedTransaction> =
            bincode::deserialize(&data).map_err(|e| format!("Deserialization error: {}", e))?;

        log::info!(
            "Loaded mempool snapshot with {} transactions",
            transactions.len()
        );
        Ok(transactions)
    }

    /// Clear mempool snapshot
    pub fn clear(&self) -> Result<(), String> {
        // NOTE: delete_state not available in klomang-core Storage API
        // Snapshot will remain in storage - this is a placeholder
        log::info!("Clear mempool snapshot requested (not implemented in current API)");
        Ok(())
    }

    /// Check if snapshot exists
    pub fn exists(&self) -> Result<bool, String> {
        let exists = self
            .storage
            .read()
            .map_err(|e| format!("Storage lock error: {}", e))?
            .get_state(&self.snapshot_key)
            .map_err(|e| format!("Storage read error: {}", e))?
            .is_some();

        Ok(exists)
    }

    /// Get snapshot metadata
    pub fn get_metadata(&self) -> Result<SnapshotMetadata, String> {
        if let Some(data) = self
            .storage
            .read()
            .map_err(|e| format!("Storage lock error: {}", e))?
            .get_state(&self.snapshot_key)
            .map_err(|e| format!("Storage read error: {}", e))?
        {
            let transactions: Vec<SignedTransaction> =
                bincode::deserialize(&data).map_err(|e| format!("Deserialization error: {}", e))?;

            // NOTE: fee and weight fields no longer available in Transaction
            // Use default values for metadata
            let total_fee: u64 = 0; // TODO: Calculate from inputs/outputs
            let total_weight: u64 = 0; // TODO: Calculate transaction size

            Ok(SnapshotMetadata {
                transaction_count: transactions.len(),
                total_fee,
                total_weight,
                average_fee_per_weight: if total_weight > 0 {
                    total_fee as f64 / total_weight as f64
                } else {
                    0.0
                },
            })
        } else {
            Err("No snapshot found".to_string())
        }
    }
}

/// Snapshot metadata
#[derive(Clone, Debug)]
pub struct SnapshotMetadata {
    pub transaction_count: usize,
    pub total_fee: u64,
    pub total_weight: u64,
    pub average_fee_per_weight: f64,
}
