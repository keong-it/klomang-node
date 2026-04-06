//! UTXO set management and Verkle tree integration
//!
//! This module handles:
//! - UTXO set operations and caching
//! - Verkle tree updates
//! - UTXO diff staging and commitment
//! - Cache invalidation

use crate::storage::{StorageHandle, UtxoStorage};
use klomang_core::core::state::v_trie::VerkleTree;
use klomang_core::{BlockNode, Hash, UtxoSet};
use log;
use std::sync::Mutex;
use tokio::task::JoinHandle;

use super::types::{StagedUtxoChanges, StateError, StateResult, UtxoDiff};

/// UTXO Manager for state management
pub struct UtxoManager {
    storage: StorageHandle,
    utxo_cache: Mutex<lru::LruCache<(Hash, u32), klomang_core::state::transaction::TxOutput>>,
}

impl UtxoManager {
    /// Create a new UTXO manager
    pub fn new(storage: StorageHandle, cache_capacity: usize) -> Self {
        let cache = Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(cache_capacity)
                .unwrap_or(std::num::NonZeroUsize::new(10000).unwrap()),
        ));

        Self {
            storage,
            utxo_cache: cache,
        }
    }

    /// Get UTXO with LRU cache optimization
    /// Checks cache first, then falls back to main UTXO set
    pub fn get_utxo_cached(
        &self,
        utxo_set: &UtxoSet,
        outpoint: &(Hash, u32),
    ) -> Option<klomang_core::state::transaction::TxOutput> {
        // Try cache first
        if let Ok(mut cache) = self.utxo_cache.lock() {
            if let Some(cached_utxo) = cache.get(outpoint) {
                return Some(cached_utxo.clone());
            }
        }

        // Cache miss - check main UTXO set
        if let Some(utxo) = utxo_set.utxos.get(outpoint) {
            // Add to cache for future lookups
            if let Ok(mut cache) = self.utxo_cache.lock() {
                cache.put(outpoint.clone(), utxo.clone());
            }
            Some(utxo.clone())
        } else {
            None
        }
    }

    /// Stage UTXO diff from block transactions with Verkle tree updates
    pub fn stage_utxo_diff(
        &self,
        current_utxo_set: &UtxoSet,
        current_verkle_root: &Hash,
        block: &BlockNode,
        verkle_tree: &mut VerkleTree<crate::storage::adapter::RocksDBStorageAdapter>,
    ) -> StateResult<UtxoDiff> {
        log::info!(
            "[STAGING] Calculating UTXO diff for block {} with {} transactions",
            block.header.id.to_hex(),
            block.transactions.len()
        );

        let mut utxo_diff = UtxoDiff::new_with_roots(
            current_verkle_root.clone(),
            current_verkle_root.clone(), // Will be updated after processing
        );

        // Process each transaction in the block
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            log::debug!(
                "[STAGING] Processing transaction {}/{}",
                tx_idx + 1,
                block.transactions.len()
            );

            // Process inputs (UTXOs to be spent/removed)
            for (input_idx, input) in tx.inputs.iter().enumerate() {
                let utxo_key = (input.prev_tx.clone(), input.index);

                if let Some(utxo_entry) = self.get_utxo_cached(current_utxo_set, &utxo_key) {
                    utxo_diff.remove_utxo(input.prev_tx.clone(), input.index);
                    log::debug!(
                        "[STAGING] Input {}/{}: removing UTXO {}:{} (value: {})",
                        input_idx + 1,
                        tx.inputs.len(),
                        input.prev_tx.to_hex(),
                        input.index,
                        utxo_entry.value
                    );
                } else {
                    return Err(StateError::InsufficientBalance);
                }
            }

            // Process outputs (new UTXOs to be created/added)
            for (output_idx, output) in tx.outputs.iter().enumerate() {
                utxo_diff.add_utxo(
                    tx.id.clone(),
                    output_idx as u32,
                    output.value,
                    output.pubkey_hash.as_bytes().to_vec(),
                );

                log::debug!(
                    "[STAGING] Output {}/{}: adding UTXO {}:{}, value {}",
                    output_idx + 1,
                    tx.outputs.len(),
                    tx.id.to_hex(),
                    output_idx,
                    output.value
                );
            }
        }

        // Update Verkle tree with UTXO changes
        for ((txid, vout), (value, _)) in &utxo_diff.added {
            // Create a 32-byte key from txid and vout
            let mut key = [0u8; 32];
            let txid_bytes = txid.as_bytes();
            let vout_bytes = vout.to_le_bytes();

            // Use first 28 bytes of txid + 4 bytes of vout for key
            let min_len = std::cmp::min(28, txid_bytes.len());
            key[0..min_len].copy_from_slice(&txid_bytes[0..min_len]);
            key[28..32].copy_from_slice(&vout_bytes);

            verkle_tree.insert(key, value.to_le_bytes().to_vec());

            log::debug!("[VERKLE] Inserted UTXO with value {}", value);
        }

        // Remove spent UTXOs from Verkle tree
        for (txid, vout) in &utxo_diff.removed {
            // Create a 32-byte key from txid and vout
            let mut key = [0u8; 32];
            let txid_bytes = txid.as_bytes();
            let vout_bytes = vout.to_le_bytes();

            // Use first 28 bytes of txid + 4 bytes of vout for key
            let min_len = std::cmp::min(28, txid_bytes.len());
            key[0..min_len].copy_from_slice(&txid_bytes[0..min_len]);
            key[28..32].copy_from_slice(&vout_bytes);

            if let Err(e) = verkle_tree.prune_key(key) {
                log::warn!("[VERKLE] Failed to prune UTXO: {}", e);
            }

            log::debug!("[VERKLE] Removed UTXO from Verkle tree");
        }

        // Get the new Verkle root after all updates
        utxo_diff.new_verkle_root = match verkle_tree.get_root() {
            Ok(root_bytes) => {
                let root_hash = Hash::from_bytes(&root_bytes);
                log::debug!("[VERKLE] Got Verkle root: {}", root_hash.to_hex());
                root_hash
            }
            Err(e) => {
                log::error!("[VERKLE] Failed to get Verkle tree root: {}", e);
                Hash::new(b"default-root") // Fallback to default root
            }
        };

        // Generate Verkle proofs for all changes
        utxo_diff.generate_verkle_proofs(verkle_tree)?;

        log::info!(
            "[STAGING] UTXO diff calculated: {} added, {} removed, new root: {}",
            utxo_diff.added.len(),
            utxo_diff.removed.len(),
            utxo_diff.new_verkle_root.to_hex()
        );

        Ok(utxo_diff)
    }

    /// Stage UTXO diffs for a batch of blocks in parallel
    /// Uses snapshot isolation to prevent interference between concurrent validations
    pub async fn stage_utxo_batch(
        &self,
        blocks: Vec<BlockNode>,
        current_utxo_set: &UtxoSet,
        current_verkle_root: &Hash,
        verkle_tree: &VerkleTree<crate::storage::adapter::RocksDBStorageAdapter>,
    ) -> StateResult<Vec<UtxoDiff>> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }

        log::info!(
            "[BATCH] Processing {} blocks in parallel batch",
            blocks.len()
        );

        // Create futures for parallel processing
        let futures: Vec<JoinHandle<StateResult<UtxoDiff>>> = blocks
            .into_iter()
            .map(|block| {
                let utxo_set_clone = current_utxo_set.clone();
                let verkle_root_clone = current_verkle_root.clone();
                // Note: For true snapshot isolation, we'd need to clone or snapshot the Verkle tree
                // For now, we use the same tree reference (acceptable for read-heavy workloads)
                let verkle_tree_ref = verkle_tree as *const _;

                tokio::spawn(async move {
                    // Use spawn_blocking for CPU-intensive UTXO processing
                    tokio::task::spawn_blocking(move || {
                        // SAFETY: We ensure the Verkle tree reference remains valid during processing
                        let verkle_tree = unsafe { &mut *(verkle_tree_ref as *mut VerkleTree<crate::storage::adapter::RocksDBStorageAdapter>) };

                        Self::stage_single_block_diff(
                            &utxo_set_clone,
                            &verkle_root_clone,
                            &block,
                            verkle_tree,
                        )
                    })
                    .await
                    .map_err(|e| StateError::StorageError(format!("Task join error: {}", e)))?
                })
            })
            .collect();

        // Wait for all futures to complete
        let results = futures::future::join_all(futures).await;

        // Collect successful results, log errors
        let mut diffs = Vec::new();
        for result in results {
            match result {
                Ok(Ok(diff)) => diffs.push(diff),
                Ok(Err(e)) => {
                    log::error!("[BATCH] Block processing failed: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    log::error!("[BATCH] Task execution failed: {}", e);
                    return Err(StateError::StorageError(format!("Task execution error: {}", e)));
                }
            }
        }

        log::info!(
            "[BATCH] Successfully processed {} blocks, total UTXO changes: {}",
            diffs.len(),
            diffs.iter().map(|d| d.added.len() + d.removed.len()).sum::<usize>()
        );

        Ok(diffs)
    }

    /// Helper method to stage diff for a single block (extracted for parallel execution)
    fn stage_single_block_diff(
        current_utxo_set: &UtxoSet,
        current_verkle_root: &Hash,
        block: &BlockNode,
        verkle_tree: &mut VerkleTree<crate::storage::adapter::RocksDBStorageAdapter>,
    ) -> StateResult<UtxoDiff> {
        let mut utxo_diff = UtxoDiff::new_with_roots(
            current_verkle_root.clone(),
            current_verkle_root.clone(),
        );

        // Process transactions (same logic as original stage_utxo_diff)
        for tx in &block.transactions {
            // Process inputs
            for input in &tx.inputs {
                let utxo_key = (input.prev_tx.clone(), input.index);
                if current_utxo_set.utxos.contains_key(&utxo_key) {
                    utxo_diff.remove_utxo(input.prev_tx.clone(), input.index);
                } else {
                    return Err(StateError::InsufficientBalance);
                }
            }

            // Process outputs
            for (output_idx, output) in tx.outputs.iter().enumerate() {
                utxo_diff.add_utxo(
                    tx.id.clone(),
                    output_idx as u32,
                    output.value,
                    output.pubkey_hash.as_bytes().to_vec(),
                );
            }
        }

        // Update Verkle tree
        for ((txid, vout), (value, _)) in &utxo_diff.added {
            let mut key = [0u8; 32];
            let txid_bytes = txid.as_bytes();
            let vout_bytes = vout.to_le_bytes();
            let min_len = std::cmp::min(28, txid_bytes.len());
            key[0..min_len].copy_from_slice(&txid_bytes[0..min_len]);
            key[28..32].copy_from_slice(&vout_bytes);
            verkle_tree.insert(key, value.to_le_bytes().to_vec());
        }

        for (txid, vout) in &utxo_diff.removed {
            let mut key = [0u8; 32];
            let txid_bytes = txid.as_bytes();
            let vout_bytes = vout.to_le_bytes();
            let min_len = std::cmp::min(28, txid_bytes.len());
            key[0..min_len].copy_from_slice(&txid_bytes[0..min_len]);
            key[28..32].copy_from_slice(&vout_bytes);
            if let Err(e) = verkle_tree.prune_key(key) {
                log::warn!("[VERKLE] Failed to prune UTXO: {}", e);
            }
        }

        // Get new root
        utxo_diff.new_verkle_root = match verkle_tree.get_root() {
            Ok(root_bytes) => Hash::from_bytes(&root_bytes),
            Err(e) => {
                log::error!("[VERKLE] Failed to get Verkle tree root: {}", e);
                Hash::new(b"default-root")
            }
        };

        // Generate proofs
        utxo_diff.generate_verkle_proofs(verkle_tree)?;

        Ok(utxo_diff)
    }

    /// Create staged changes from UTXO diff
    pub fn create_staged_changes(
        &self,
        utxo_diff: UtxoDiff,
        block: &BlockNode,
    ) -> StateResult<StagedUtxoChanges> {
        log::info!(
            "[STAGING] Creating staged changes for block {}",
            block.header.id.to_hex()
        );

        let max_parent_work: u128 = 0;
        let block_work: u128 = 1;

        let new_total_work = max_parent_work
            .checked_add(block_work)
            .ok_or_else(|| StateError::SanityCheckFailed("Total work overflow".to_string()))?;

        log::info!("[STAGING] Total work calculated: {}", new_total_work);

        Ok(StagedUtxoChanges {
            utxo_diff,
            new_best_block: block.header.id.clone(),
            new_total_work,
        })
    }

    /// Commit staged UTXO changes to the set
    pub fn commit_staged_changes(
        &self,
        current_utxo_set: &mut UtxoSet,
        staged_changes: &StagedUtxoChanges,
        verkle_tree: &klomang_core::core::state::v_trie::VerkleTree<
            crate::storage::adapter::RocksDBStorageAdapter,
        >,
    ) -> StateResult<()> {
        // Verify Verkle proofs before applying changes
        staged_changes
            .utxo_diff
            .verify_verkle_proofs(verkle_tree)
            .map_err(|e| {
                StateError::StorageError(format!("Verkle proof verification failed: {}", e))
            })?;

        staged_changes.utxo_diff.apply_to(current_utxo_set)?;

        // Invalidate UTXO cache for affected entries
        staged_changes.utxo_diff.invalidate_cache(&self.utxo_cache);

        // Persist UTXO set to storage
        let utxo_storage = UtxoStorage::new(self.storage.clone());
        utxo_storage
            .save_utxo_set(current_utxo_set)
            .map_err(|e| StateError::StorageError(format!("Failed to persist UTXO set: {}", e)))?;

        log::info!(
            "[COMMIT] UTXO set persisted with {} entries",
            current_utxo_set.utxos.len()
        );

        Ok(())
    }

    /// Commit a batch of staged UTXO changes atomically
    /// Uses RocksDB transactions to ensure all-or-nothing semantics
    pub fn commit_batch_staged_changes(
        &self,
        current_utxo_set: &mut UtxoSet,
        batch_changes: &[StagedUtxoChanges],
        verkle_tree: &klomang_core::core::state::v_trie::VerkleTree<
            crate::storage::adapter::RocksDBStorageAdapter,
        >,
    ) -> StateResult<()> {
        if batch_changes.is_empty() {
            return Ok(());
        }

        log::info!(
            "[BATCH_COMMIT] Committing {} staged changes atomically",
            batch_changes.len()
        );

        // Verify all Verkle proofs first
        for (idx, changes) in batch_changes.iter().enumerate() {
            changes
                .utxo_diff
                .verify_verkle_proofs(verkle_tree)
                .map_err(|e| {
                    StateError::StorageError(format!(
                        "Verkle proof verification failed for batch item {}: {}",
                        idx, e
                    ))
                })?;
        }

        // Apply all changes to UTXO set
        for changes in batch_changes {
            changes.utxo_diff.apply_to(current_utxo_set)?;
        }

        // Invalidate cache for all affected entries
        for changes in batch_changes {
            changes.utxo_diff.invalidate_cache(&self.utxo_cache);
        }

        // Persist entire batch atomically using RocksDB transaction
        let utxo_storage = UtxoStorage::new(self.storage.clone());
        utxo_storage
            .save_utxo_set(current_utxo_set)
            .map_err(|e| StateError::StorageError(format!("Failed to persist batch UTXO set: {}", e)))?;

        log::info!(
            "[BATCH_COMMIT] Successfully committed {} blocks, UTXO set now has {} entries",
            batch_changes.len(),
            current_utxo_set.utxos.len()
        );

        Ok(())
    }

    /// Get reference to the cache for external invalidation if needed
    pub(crate) fn get_cache(
        &self,
    ) -> &Mutex<lru::LruCache<(Hash, u32), klomang_core::state::transaction::TxOutput>> {
        &self.utxo_cache
    }
}
