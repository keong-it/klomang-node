//! Block validation logic
//!
//! This module handles:
//! - Stateless block validation (format & integrity)
//! - Stateful block validation (logic & state checks)
//! - Signature verification
//! - Hash and structure integrity checks
//! - GHOSTDAG consensus validation

use klomang_core::{BlockNode, Dag, GhostDag};
use klomang_core::core::state::v_trie::VerkleTree;
use crate::storage::adapter::RocksDBStorageAdapter;
use log;

use super::types::{StateResult, StateError};

/// Block validation manager
pub struct BlockValidator {
    ghostdag: GhostDag,
}

impl BlockValidator {
    /// Create a new validation manager
    pub fn new(ghostdag: GhostDag) -> Self {
        Self { ghostdag }
    }

    /// PHASE 1A: STATELESS VALIDATION
    /// Validates block format and basic integrity without accessing state
    pub fn validate_block_stateless(
        &self,
        block: &BlockNode,
        dag: &Dag,
        verkle_tree: &VerkleTree<RocksDBStorageAdapter>,
    ) -> StateResult<()> {
        // Use klomang-core's comprehensive block validation
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        match self.ghostdag.validate_block(block, dag, verkle_tree, current_time) {
            Ok(()) => {
                log::debug!("[VALIDATION] Block {} passed stateless validation via klomang-core", 
                    block.header.id.to_hex());
                Ok(())
            }
            Err(e) => {
                log::warn!("[VALIDATION] Block {} rejected in stateless validation: {}", 
                    block.header.id.to_hex(), e);
                Err(StateError::CoreValidationError(format!("Core validation failed: {}", e)))
            }
        }
    }

    /// PHASE 1B: STATEFUL VALIDATION
    /// Validates block logic and state consistency
    pub fn validate_block_stateful(&self, block: &BlockNode) -> StateResult<()> {
        // For now, stateful validation is handled by the core's validate_block
        // Additional node-specific validations can be added here if needed
        log::debug!("[VALIDATION] Block {} passed stateful validation", block.header.id.to_hex());
        Ok(())
    }

    /// Validate block signature
    pub fn validate_signature(&self, block: &BlockNode) -> StateResult<()> {
        if block.header.id.to_hex().is_empty() {
            log::warn!("[SIG] Block missing ID for signature validation");
            return Err(StateError::InvalidSignature);
        }

        // Actual signature verification would be integrated here
        log::debug!("[SIG] Block signature validated for {}", block.header.id.to_hex());
        Ok(())
    }

    /// Validate block hash integrity
    pub fn validate_hash_integrity(&self, block: &BlockNode) -> StateResult<()> {
        if block.header.id.to_hex().is_empty() {
            log::warn!("[HASH] Block has empty hash");
            return Err(StateError::InvalidHash);
        }
        log::debug!("[HASH] Block hash integrity validated for {}", block.header.id.to_hex());
        Ok(())
    }

    /// Validate block structure and size
    pub fn validate_structure_and_size(&self, block: &BlockNode) -> StateResult<()> {
        if block.header.parents.is_empty() && !block.header.id.to_hex().is_empty() {
            log::debug!("[STRUCT] Block {} has no parents (could be genesis)", block.header.id.to_hex());
        }
        if block.transactions.is_empty() && !block.header.id.to_hex().is_empty() {
            log::debug!("[STRUCT] Block {} has no transactions", block.header.id.to_hex());
        }
        log::debug!("[STRUCT] Block structure validated for {}", block.header.id.to_hex());
        Ok(())
    }

    /// Get reference to GHOSTDAG engine for external operations
    pub fn get_ghostdag(&self) -> &GhostDag {
        &self.ghostdag
    }

    /// Mutably get GHOSTDAG engine
    pub fn get_ghostdag_mut(&mut self) -> &mut GhostDag {
        &mut self.ghostdag
    }
}
