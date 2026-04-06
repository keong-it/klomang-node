//! Chain management logic for Klomang Node
//!
//! This module handles:
//! - Block processing pipeline
//! - Chain reorganization (reorg) logic
//! - Orphan block handling
//! - Best block tracking and updates
//! - Staged changes and UTXO diff management

use super::types::{OrphanPool, StagedUtxoChanges, StateError, StateResult, UtxoDiff};
use crate::storage::StorageHandle;
use klomang_core::{BlockNode, Dag, Hash, UtxoSet};

/// Chain management operations for state manager
pub trait ChainManager {
    fn get_dag(&self) -> &Dag;
    fn get_utxo_set(&self) -> &UtxoSet;
    fn get_storage(&self) -> &StorageHandle;
    fn get_best_block(&self) -> Hash;
    fn get_current_verkle_root(&self) -> Hash;

    /// Calculate block height in the chain
    fn calculate_block_height(&self, block: &BlockNode) -> StateResult<u64>;

    /// Stage UTXO diff without committing to main state
    fn stage_utxo_diff(&self, block: &BlockNode) -> StateResult<UtxoDiff>;

    /// Create staged changes from UTXO diff
    fn create_staged_changes(
        &self,
        utxo_diff: UtxoDiff,
        block: &BlockNode,
    ) -> StateResult<StagedUtxoChanges>;

    /// Find common ancestor between two blocks
    fn find_common_ancestor(&self, block_a: &Hash, block_b: &Hash) -> StateResult<Hash>;
}

/// Helper functions for chain operations
pub struct ChainOps;

impl ChainOps {
    /// Get current best block hash
    pub fn get_best_block(best_block: &Hash) -> Hash {
        best_block.clone()
    }

    /// Check if block is orphan (missing parents in storage)
    pub fn is_orphan(block: &BlockNode, storage: &StorageHandle) -> StateResult<bool> {
        for parent in &block.header.parents {
            let exists = storage
                .read()
                .map_err(|_| StateError::StorageError("Storage lock poisoned".to_string()))?
                .get_block(parent)
                .map_err(|e| StateError::StorageError(e.to_string()))?
                .is_some();
            if !exists {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get processable orphans after parent is processed
    pub fn get_processable_orphans(
        processed_parent: &Hash,
        orphan_pool: &mut OrphanPool,
    ) -> Vec<BlockNode> {
        orphan_pool.get_processable_orphans(processed_parent)
    }

    /// Add block to orphan pool
    pub fn add_to_orphan_pool(block: BlockNode, orphan_pool: &mut OrphanPool) {
        orphan_pool.add_orphan(block);
    }

    /// Calculate new total work for block
    pub fn calculate_total_work(block: &BlockNode, storage: &StorageHandle) -> StateResult<u128> {
        // In GHOSTDAG/Klomang: total_work = max(parent_work) + block_work
        // Block work is derived from difficulty target (bitfield)
        // Difficulty = max_target / block.bits_target
        // Work = 2^256 / (difficulty + 1) or approximated as 2^32 / difficulty_adjustment

        // For Klomang protocol, work calculation:
        // Convert 0u32 (difficulty target) to work contribution
        // Lower bits = higher difficulty = more work
        let block_work: u128 = calculate_block_work_from_difficulty(0u32);

        // Find maximum parent work
        let mut max_parent_work: u128 = 0;
        for parent_hash in &block.header.parents {
            if let Ok(Some(_parent_block)) = storage
                .read()
                .map_err(|_| StateError::StorageError("Storage lock poisoned".to_string()))?
                .get_block(parent_hash)
            {
                if let Ok(Some(record)) = storage
                    .read()
                    .map_err(|_| StateError::StorageError("Storage lock poisoned".to_string()))?
                    .get_chain_index(parent_hash)
                {
                    max_parent_work = max_parent_work.max(record.total_work);
                }
            }
        }

        max_parent_work
            .checked_add(block_work)
            .ok_or_else(|| StateError::SanityCheckFailed("Total work overflow".to_string()))
    }

    /// Find common ancestor using two-pointer algorithm
    pub fn find_common_ancestor_impl(
        block_a: &Hash,
        block_b: &Hash,
        storage: &StorageHandle,
    ) -> StateResult<Hash> {
        if block_a == block_b {
            return Ok(block_a.clone());
        }

        let storage_read = storage
            .read()
            .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?;

        // Get heights of both blocks
        let height_a = match storage_read
            .get_block(block_a)
            .map_err(|e| StateError::StorageError(e.to_string()))?
        {
            Some(_) => {
                match storage_read
                    .get_chain_index(block_a)
                    .map_err(|e| StateError::StorageError(e.to_string()))?
                {
                    Some(record) => record.height,
                    None => 0,
                }
            }
            None => {
                return Err(StateError::StorageError(format!(
                    "Block {} not found",
                    block_a.to_hex()
                )));
            }
        };

        let height_b = match storage_read
            .get_block(block_b)
            .map_err(|e| StateError::StorageError(e.to_string()))?
        {
            Some(_) => {
                match storage_read
                    .get_chain_index(block_b)
                    .map_err(|e| StateError::StorageError(e.to_string()))?
                {
                    Some(record) => record.height,
                    None => 0,
                }
            }
            None => {
                return Err(StateError::StorageError(format!(
                    "Block {} not found",
                    block_b.to_hex()
                )));
            }
        };

        let mut current_a = block_a.clone();
        let mut current_b = block_b.clone();
        let mut current_height_a = height_a;
        let mut current_height_b = height_b;

        // Level both pointers to same height
        while current_height_a > current_height_b {
            if let Some(block) = storage_read
                .get_block(&current_a)
                .map_err(|e| StateError::StorageError(e.to_string()))?
            {
                if let Some(parent) = block.header.parents.iter().next() {
                    current_a = parent.clone();
                    current_height_a -= 1;
                } else {
                    return Ok(current_a);
                }
            } else {
                return Err(StateError::StorageError(format!(
                    "Block {} not found",
                    current_a.to_hex()
                )));
            }
        }

        while current_height_b > current_height_a {
            if let Some(block) = storage_read
                .get_block(&current_b)
                .map_err(|e| StateError::StorageError(e.to_string()))?
            {
                if let Some(parent) = block.header.parents.iter().next() {
                    current_b = parent.clone();
                    current_height_b -= 1;
                } else {
                    return Ok(current_b);
                }
            } else {
                return Err(StateError::StorageError(format!(
                    "Block {} not found",
                    current_b.to_hex()
                )));
            }
        }

        // Both at same height, advance simultaneously until they meet
        while current_a != current_b {
            if let Some(block_a_data) = storage_read
                .get_block(&current_a)
                .map_err(|e| StateError::StorageError(e.to_string()))?
            {
                if let Some(parent_a) = block_a_data.header.parents.iter().next() {
                    current_a = parent_a.clone();
                } else {
                    return Ok(current_a);
                }
            } else {
                return Err(StateError::StorageError(format!(
                    "Block {} not found",
                    current_a.to_hex()
                )));
            }

            if let Some(block_b_data) = storage_read
                .get_block(&current_b)
                .map_err(|e| StateError::StorageError(e.to_string()))?
            {
                if let Some(parent_b) = block_b_data.header.parents.iter().next() {
                    current_b = parent_b.clone();
                } else {
                    return Ok(current_b);
                }
            } else {
                return Err(StateError::StorageError(format!(
                    "Block {} not found",
                    current_b.to_hex()
                )));
            }
        }

        log::debug!("[REORG] Common ancestor found: {}", current_a.to_hex());
        Ok(current_a)
    }
}

pub fn calculate_block_work_from_difficulty(bits: u32) -> u128 {
    if bits == 0 {
        return 1;
    }
    let exponent = (bits >> 24) as u32;
    let mantissa = bits & 0x00ffffff;
    if exponent < 3 || exponent > 32 {
        log::warn!("[WORK] Unusual exponent: {}", exponent);
        return 1;
    }
    let target: u128 = if exponent == 3 {
        mantissa as u128
    } else {
        (mantissa as u128) << ((exponent - 3) * 8)
    };
    let max_target: u128 = 1u128 << 32;
    if target == 0 {
        max_target
    } else {
        max_target / target
    }
}
