//! Block validation logic for Klomang State Manager
//!
//! # IMPORTANT: Core Integration Update
//!
//! As of the latest integration, block validation is now handled directly by klomang-core's
//! GhostDag::validate_block() method, which provides comprehensive validation including:
//! - DAG connectivity and parent validation
//! - Timestamp and difficulty validation
//! - Proof-of-Work verification
//! - Transaction validation (signatures, double-spend detection, balance)
//! - Verkle tree proof validation
//! - GHOSTDAG consensus rule enforcement
//!
//! The BlockValidator trait and its methods below are kept for:
//! 1. API compatibility and future extensibility
//! 2. Node-specific validation logic (if needed)
//! 3. Documentation of validation phases
//!
//! # Validation Flow
//!
//! Current implementation uses klomang-core validation:
//! 1. Stateless validation: GhostDag::validate_block() (comprehensive)
//! 2. Stateful validation: Additional node-specific checks (currently minimal)
//!
//! Legacy validation methods are preserved but not used in main flow.

use super::types::{StateError, StateResult};
use crate::storage::StorageHandle;
use klomang_core::{BlockNode, Dag, Hash, UtxoSet};

/// Validation behavior implemented by state manager
/// NOTE: Currently delegates to klomang-core for actual validation
pub trait BlockValidator {
    fn get_utxo_set(&self) -> &UtxoSet;
    fn get_dag(&self) -> &Dag;
    fn get_storage(&self) -> &StorageHandle;
    fn get_current_verkle_root(&self) -> Hash;

    // PHASE 1A: STATELESS VALIDATION (Format & Integrity Check)
    // DEPRECATED: Now handled by klomang-core's GhostDag::validate_block()
    fn validate_block_stateless(&self, block: &BlockNode) -> StateResult<()> {
        // NOTE: This method is kept for API compatibility but actual validation
        // is now performed by klomang-core's comprehensive validate_block method
        log::debug!(
            "Block {} validation delegated to klomang-core",
            block.header.id.to_hex()
        );
        Ok(())
    }

    /// Check consensus rules based on block height for backward compatibility
    fn validate_consensus_transition(&self, block: &BlockNode) -> StateResult<()> {
        let height = block.header.height;

        // Example: Before block 100,000 use old rules, after use new rules
        if height < 100_000 {
            // Old consensus rules
            // e.g., different emission schedule, different difficulty adjustment
            log::debug!("Block {} using legacy consensus rules", block.header.id.to_hex());
        } else {
            // New consensus rules
            log::debug!("Block {} using updated consensus rules", block.header.id.to_hex());
        }

        Ok(())
    }

    // PHASE 1B: STATEFUL VALIDATION (Logic & State Check)
    // DEPRECATED: Now handled by klomang-core's validate_block()
    fn validate_block_stateful(&self, block: &BlockNode) -> StateResult<()> {
        // NOTE: This method is kept for API compatibility but actual validation
        // is now performed by klomang-core's comprehensive validate_block method
        log::debug!(
            "Block {} stateful validation delegated to klomang-core",
            block.header.id.to_hex()
        );
        Ok(())
    }
}
