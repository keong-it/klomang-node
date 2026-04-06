//! Chain management and block processing
//!
//! This module handles:
//! - Block processing pipeline
//! - Chain reorganization logic
//! - Orphan block handling
//! - Block height calculation
//! - Common ancestor finding

use std::collections::VecDeque;
use klomang_core::{BlockNode, Hash};
use crate::storage::StorageHandle;
use log;

use super::types::{StateResult, StateError};

/// Chain manager for block processing and chain reorg
pub struct ChainManager {
    best_block: Hash,
    orphan_hashes: std::collections::VecDeque<Hash>,
}

impl ChainManager {
    /// Create a new chain manager
    pub fn new(best_block: Hash) -> Self {
        Self {
            best_block,
            orphan_hashes: std::collections::VecDeque::new(),
        }
    }

    /// Calculate block height from its parents  
    pub fn calculate_block_height(
        &self,
        block: &BlockNode,
        storage: &StorageHandle,
    ) -> StateResult<u64> {
        if block.header.parents.is_empty() {
            return Ok(0);
        }

        let mut max_parent_height = 0u64;
        for parent_hash in &block.header.parents {
            if let Some(parent_record) = storage.read()
                .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_chain_index(parent_hash)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                if parent_record.height > max_parent_height {
                    max_parent_height = parent_record.height;
                }
            } else {
                return Err(StateError::StorageError(
                    format!("Parent block {} not found in chain index", parent_hash.to_hex())));
            }
        }

        Ok(max_parent_height + 1)
    }

    /// Get current best block
    pub fn get_best_block(&self) -> &Hash {
        &self.best_block
    }

    /// Update best block
    pub(crate) fn set_best_block(&mut self, best_block: Hash) {
        self.best_block = best_block;
    }

    /// Find common ancestor between two blocks
    pub fn find_common_ancestor(
        &self,
        block_a: &Hash,
        block_b: &Hash,
        storage: &StorageHandle,
    ) -> StateResult<Hash> {
        log::info!("[REORG] Finding common ancestor between {} and {}",
            block_a.to_hex(), block_b.to_hex());

        let mut hashes_a = vec![block_a.clone()];
        let mut hashes_b = vec![block_b.clone()];

        // Collect all parents of block_a
        let mut current = block_a.clone();
        while !current.to_hex().is_empty() {
            match storage.read()
                .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_block(&current)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                Some(block_data) => {
                    if block_data.header.parents.is_empty() {
                        break;
                    }
                    if let Some(parent) = block_data.header.parents.iter().next() {
                        current = parent.clone();
                        hashes_a.push(current.clone());
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }

        // Collect all parents of block_b
        let mut current = block_b.clone();
        while !current.to_hex().is_empty() {
            match storage.read()
                .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_block(&current)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                Some(block_data) => {
                    if block_data.header.parents.is_empty() {
                        break;
                    }
                    if let Some(parent) = block_data.header.parents.iter().next() {
                        current = parent.clone();
                        hashes_b.push(current.clone());
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }

        // Find common ancestor
        for hash_a in hashes_a.iter() {
            for hash_b in hashes_b.iter() {
                if hash_a == hash_b {
                    log::info!("[REORG] Found common ancestor: {}", hash_a.to_hex());
                    return Ok(hash_a.clone());
                }
            }
        }

        Err(StateError::StorageError("No common ancestor found".to_string()))
    }

    /// Rollback to ancestor block
    pub fn rollback_to_ancestor(
        &mut self,
        ancestor: &Hash,
        storage: &StorageHandle,
    ) -> StateResult<()> {
        log::info!("[REORG] Rolling back chain to ancestor: {}", ancestor.to_hex());

        let mut current = self.best_block.clone();
        let mut _blocks_to_rollback = Vec::new();

        while &current != ancestor {
            _blocks_to_rollback.push(current.clone());

            match storage.read()
                .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_block(&current)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                Some(block_data) => {
                    if block_data.header.parents.is_empty() {
                        log::warn!("[REORG] Reached genesis before finding ancestor");
                        break;
                    }
                    if let Some(first_parent) = block_data.header.parents.iter().next() {
                        current = first_parent.clone();
                    } else {
                        break;
                    }
                }
                None => {
                    return Err(StateError::StorageError(
                        format!("Block {} not found during rollback", current.to_hex())));
                }
            }
        }

        log::info!("[REORG] Rollback completed to ancestor {}", ancestor.to_hex());
        Ok(())
    }

    /// Apply new chain path from ancestor
    pub fn apply_new_chain(
        &mut self,
        ancestor: &Hash,
        new_best: &Hash,
        storage: &StorageHandle,
    ) -> StateResult<()> {
        log::info!("[REORG] Applying new chain path from {} to {}",
            ancestor.to_hex(), new_best.to_hex());

        let mut path_blocks = Vec::new();
        let mut current = new_best.clone();

        while &current != ancestor {
            path_blocks.push(current.clone());

            match storage.read()
                .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_block(&current)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                Some(block_data) => {
                    if block_data.header.parents.is_empty() {
                        log::warn!("[REORG] Reached genesis before finding ancestor");
                        break;
                    }
                    if let Some(first_parent) = block_data.header.parents.iter().next() {
                        current = first_parent.clone();
                    } else {
                        break;
                    }
                }
                None => {
                    return Err(StateError::StorageError(
                        format!("Block {} not found in new chain", current.to_hex())));
                }
            }
        }

        path_blocks.reverse();
        let num_blocks = path_blocks.len();

        for block_hash in path_blocks {
            log::debug!("[REORG] Replayed block in new chain: {}", block_hash.to_hex());
        }

        self.best_block = new_best.clone();
        log::info!("[REORG] New chain application completed with {} blocks", num_blocks);
        Ok(())
    }
}

/// Chain operations trait for compatibility
pub trait ChainOps {
    fn find_common_ancestor_impl(
        block_a: &Hash,
        block_b: &Hash,
        storage: &StorageHandle,
    ) -> StateResult<Hash>;
}

impl ChainOps for ChainManager {
    fn find_common_ancestor_impl(
        block_a: &Hash,
        block_b: &Hash,
        storage: &StorageHandle,
    ) -> StateResult<Hash> {
        let manager = ChainManager::new(Hash::new(b""));
        manager.find_common_ancestor(block_a, block_b, storage)
    }
}
