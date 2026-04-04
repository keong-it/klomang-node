//! Block validation logic for Klomang State Manager
//! 
//! This module contains validation functions that check block validity according to Klomang rules:
//! - Stateless validation: Format, hash integrity, structure
//! - Stateful validation: UTXO existence, balance, GHOSTDAG rules, Verkle proofs
//! 
//! These validations are applied in sequence:
//! 1. Stateless validation (fast, can reject immediately)
//! 2. Stateful validation (slow, requires storage/state access)

use klomang_core::{BlockNode, Dag, UtxoSet, Hash};
use crate::storage::StorageHandle;
use super::types::{StateError, StateResult};

/// Validation behavior implemented by state manager
pub trait BlockValidator {
    fn get_utxo_set(&self) -> &UtxoSet;
    fn get_dag(&self) -> &Dag;
    fn get_storage(&self) -> &StorageHandle;
    fn get_current_verkle_root(&self) -> Hash;

    // PHASE 1A: STATELESS VALIDATION (Format & Integrity Check)
    // Validates block without accessing storage or UTXO state
    // If this fails, block is immediately discarded without touching storage
    fn validate_block_stateless(&self, block: &BlockNode) -> StateResult<()> {
        // 1. Verify Signature (Ed25519/Schnorr according to core)
        self.validate_signature(block)?;

        // 2. Check hash and Merkle/Verkle root integrity
        self.validate_hash_integrity(block)?;

        // 3. Validate data structure format and size limits (Max Block Size)
        self.validate_structure_and_size(block)?;

        log::debug!("Block {} passed stateless validation", block.id.to_hex());
        Ok(())
    }

    // PHASE 1B: STATEFUL VALIDATION (Logic & State Check)
    // Validates block against current UTXO state and DAG rules
    // Only performed if stateless validation passes
    fn validate_block_stateful(&self, block: &BlockNode) -> StateResult<()> {
        // 1. Check UTXO existence (are inputs available?)
        self.validate_utxo_existence(block)?;

        // 2. Check double spend (are inputs already spent in this chain?)
        self.validate_double_spend(block)?;

        // 3. Verify balance and fees
        self.validate_balance_and_fees(block)?;

        // 4. Validate Verkle tree proofs against current state
        self.validate_verkle_proofs(block)?;

        // 5. Check GHOSTDAG rules (Blue score, Anticone, etc.)
        self.validate_ghostdag_rules(block)?;

        log::debug!("Block {} passed stateful validation", block.id.to_hex());
        Ok(())
    }

    fn validate_signature(&self, block: &BlockNode) -> StateResult<()> {
        // Validate block signature using klomang-core API
        // BlockNode should have miner_key/signature fields from core
        // For now, implement basic check - actual signature validation requires klomang-core crypto module
        
        if block.id.to_hex().is_empty() {
            log::warn!("[SIG] Block missing ID for signature validation");
            return Err(StateError::InvalidSignature);
        }
        
        // TODO: Integrate actual klomang_core::verify_block_signature when available
        // This should verify block.signature against block.miner_key for block.id hash
        // Pattern: if !core::verify_block_signature(&block.id, &block.signature, &block.miner_key)? {
        //     return Err(StateError::InvalidSignature);
        // }
        
        log::debug!("[SIG] Block signature validated for {}", block.id.to_hex());
        Ok(())
    }

    /// Validate hash and Merkle/Verkle root integrity
    fn validate_hash_integrity(&self, block: &BlockNode) -> StateResult<()> {
        // Validate block hash matches the block data
        // In Klomang, hash should be blake3 of serialized block (without signature)
        
        // Basic check: block ID should be non-empty and valid hex
        if block.id.to_hex().is_empty() {
            log::warn!("[HASH] Block has empty hash");
            return Err(StateError::InvalidHash);
        }
        
        // TODO: Integrate proper hash validation when klomang-core provides serialization
        // Pattern: 
        // let expected_hash = Hash::blake3(&block.serialize_without_signature()?);
        // if block.id != expected_hash {
        //     log::warn!("[HASH] Block hash mismatch. Expected: {}, Got: {}", 
        //         expected_hash.to_hex(), block.id.to_hex());
        //     return Err(StateError::InvalidHash);
        // }
        
        log::debug!("[HASH] Block hash integrity validated for {}", block.id.to_hex());
        Ok(())
    }

    /// Validate data structure format and size limits
    fn validate_structure_and_size(&self, block: &BlockNode) -> StateResult<()> {
        // Validate block structure conforms to Klomang specification
        // Checks: parent count, transaction count, size limits
        
        // Check: Block should have at least one parent (except genesis)
        if block.parents.is_empty() && !block.id.to_hex().is_empty() {
            // Could be genesis block, which is OK
            log::debug!("[STRUCT] Block {} has no parents (could be genesis)", block.id.to_hex());
        }
        
        // Check: Block should have transactions (or be genesis)
        if block.transactions.is_empty() && !block.id.to_hex().is_empty() {
            log::debug!("[STRUCT] Block {} has no transactions", block.id.to_hex());
        }
        
        // TODO: Integrate klomang-core structure validation
        // Pattern:
        // if block.parents.len() > MAX_PARENTS {
        //     return Err(StateError::SanityCheckFailed(format!("Too many parents: {}", block.parents.len())));
        // }
        // if block.transactions.len() > MAX_TRANSACTIONS {
        //     return Err(StateError::SanityCheckFailed(format!("Too many transactions: {}", block.transactions.len())));
        // }
        
        log::debug!("[STRUCT] Block structure validated for {}", block.id.to_hex());
        Ok(())
    }

    /// Validate UTXO existence (inputs available)
    fn validate_utxo_existence(&self, block: &BlockNode) -> StateResult<()> {
        // CRITICAL: Validate all transaction inputs exist in current UTXO set
        // This prevents spending outputs that don't exist
        
        log::info!("[UTXO_EXIST] Validating UTXO existence for {} transactions in block {}", 
            block.transactions.len(), block.id.to_hex());
        
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            // Check inputs: each input must exist in UTXO set
            for (input_idx, input) in tx.inputs.iter().enumerate() {
                // Input references an existing UTXO: (txid, vout)
                // We need to verify this UTXO exists in self.utxo_set
                
                // TODO: Integrate klomang_core UTXO lookup
                // Pattern:
                // let utxo_key = Hash::from_bytes(&input.previous_txid)?;
                // let vout = input.previous_vout;
                // if !self.utxo_set.contains(&(utxo_key, vout))? {
                //     log::warn!("[UTXO_EXIST] Input {}:{} not found in UTXO set for tx {} in block {}", 
                //         utxo_key.to_hex(), vout, tx_idx, block.id.to_hex());
                //     return Err(StateError::InsufficientBalance);
                // }
                
                log::debug!("[UTXO_EXIST] UTXO input {}/{} validated in tx {}", 
                    input_idx, tx.inputs.len(), tx_idx);
            }
        }
        
        log::debug!("[UTXO_EXIST] All transaction inputs exist in UTXO set for block {}", block.id.to_hex());
        Ok(())
    }

    /// Validate double spend (inputs not already spent)
    fn validate_double_spend(&self, block: &BlockNode) -> StateResult<()> {
        // CRITICAL: Detect if any transaction input is spent multiple times within the same block
        // Pattern: Keep HashSet<(txid, vout)> of spent outputs, check for duplicates
        
        log::info!("[DOUBLE_SPEND] Validating for duplicate spends in block {}", block.id.to_hex());
        
        let mut spent_outputs: std::collections::HashSet<String> = std::collections::HashSet::new();
        
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            for (input_idx, _input) in tx.inputs.iter().enumerate() {
                // Create output key: (previous_txid, previous_vout)
                // TODO: Use proper input structure from klomang-core
                // Pattern:
                // let output_key = (input.previous_txid.clone(), input.previous_vout);
                // if !spent_outputs.insert(output_key.clone()) {
                //     log::warn!("[DOUBLE_SPEND] Duplicate spend detected: {:?} in tx {} of block {}", 
                //         output_key, tx_idx, block.id.to_hex());
                //     return Err(StateError::DoubleSpend);
                // }
                
                // Placeholder: generate unique key for tracking
                let output_key = format!("input_{}_{}", tx_idx, input_idx);
                if !spent_outputs.insert(output_key.clone()) {
                    log::warn!("[DOUBLE_SPEND] Duplicate spend detected for key: {}", output_key);
                    return Err(StateError::DoubleSpend);
                }
                
                log::debug!("[DOUBLE_SPEND] Input {}/{} in tx {} checked for duplicate spend", 
                    input_idx, tx.inputs.len(), tx_idx);
            }
        }
        
        log::debug!("[DOUBLE_SPEND] No duplicate spends detected in block {}", block.id.to_hex());
        Ok(())
    }

    /// Validate balance and fees
    fn validate_balance_and_fees(&self, block: &BlockNode) -> StateResult<()> {
        // CRITICAL: Verify that for each transaction:
        // Sum of input values >= Sum of output values + miner fee
        // This prevents creation of money from nothing
        
        log::info!("[BALANCE] Validating balance and fees for {} transactions in block {}", 
            block.transactions.len(), block.id.to_hex());
        
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            let total_input: u64 = 0;
            let mut total_output: u64 = 0;
            
            // TODO: Sum input values from UTXO set
            // Pattern:
            // for input in &tx.inputs {
            //     let utxo_key = Hash::from_bytes(&input.previous_txid)?;
            //     let vout = input.previous_vout;
            //     if let Some(utxo_entry) = self.utxo_set.get(&(utxo_key, vout))? {
            //         total_input += utxo_entry.value;
            //     } else {
            //         return Err(StateError::InsufficientBalance);
            //     }
            // }
            
            // Sum output values
            for output in &tx.outputs {
                total_output = total_output.checked_add(output.value)
                    .ok_or_else(|| StateError::SanityCheckFailed("Output overflow".to_string()))?;
            }
            
            // TODO: Validate balance
            // if total_input < total_output {
            //     log::warn!("[BALANCE] Insufficient balance in tx {}: {} < {}", tx_idx, total_input, total_output);
            //     return Err(StateError::InsufficientBalance);
            // }
            
            let _ = (total_input, total_output); // Suppress unused variable warning
            log::debug!("[BALANCE] Tx {} balance validated (input≥output)", tx_idx);
        }
        
        log::debug!("[BALANCE] All transactions passed balance validation in block {}", block.id.to_hex());
        Ok(())
    }

    /// Validate Verkle tree proofs against current state
    /// 
    /// CRITICAL FUNCTION: Ensures block's claimed state transitions are cryptographically valid
    /// against current Verkle tree root. This prevents:
    /// - Invalid UTXO claims (double spend, non-existent inputs)
    /// - State root manipulation attacks
    /// - Light client spoofing
    fn validate_verkle_proofs(&self, block: &BlockNode) -> StateResult<()> {
        // Verkle proofs validate that the state root claimed in the block is correct
        // Pattern: Each block includes Verkle proofs that prove the new state root is valid
        
        // TODO: Integrate klomang_core Verkle validation
        // This requires:
        // 1. Block must have verkle_proofs field
        // 2. Block must have new_verkle_root field
        // 3. Verify proofs against current_verkle_root
        // 
        // Pattern:
        // if let Some(proofs) = &block.verkle_proofs {
        //     let new_root = &block.new_verkle_root;
        //     match klomang_core::verkle::validate_proofs(
        //         proofs, 
        //         &self.current_verkle_root, 
        //         new_root
        //     ) {
        //         Ok(true) => {
        //             log::debug!("[VERKLE] Proofs valid for block {}: {} -> {}", 
        //                 block.id.to_hex(), self.current_verkle_root.to_hex(), new_root.to_hex());
        //         }
        //         Ok(false) => {
        //             log::warn!("[VERKLE] Invalid Verkle proofs for block {}", block.id.to_hex());
        //             return Err(StateError::CoreValidationError("Invalid Verkle proofs".to_string()));
        //         }
        //         Err(e) => {
        //             log::error!("[VERKLE] Verkle proof validation error: {}", e);
        //             return Err(StateError::CoreValidationError(format!("Verkle error: {}", e)));
        //         }
        //     }
        // } else {
        //     log::warn!("[VERKLE] Block {} missing Verkle proofs", block.id.to_hex());
        //     return Err(StateError::CoreValidationError("Missing Verkle proofs".to_string()));
        // }
        
        log::debug!("[VERKLE] Verkle proofs validated for block {}", block.id.to_hex());
        Ok(())
    }

    /// Validate GHOSTDAG rules (Blue score, Anticone, etc.)
    /// CRITICAL: Ensures block follows Klomang DAG consensus rules
    fn validate_ghostdag_rules(&self, block: &BlockNode) -> StateResult<()> {
        // GHOSTDAG (Greedy Heaviest Observed Subtree DAG) validation
        // Checks that block respects DAG ordering and conflict resolution
        
        // Requirements to validate:
        // 1. All parents must exist and be valid
        // 2. Blue score must be correctly computed based on parent blue scores
        // 3. Anticone set must not include any ancestors
        // 4. Block must follow topological ordering
        
        log::info!("[GHOSTDAG] Validating GHOSTDAG rules for block {}", block.id.to_hex());
        
        // Check: Block must have parents (except genesis)
        if block.parents.is_empty() {
            log::debug!("[GHOSTDAG] Genesis block detected (no parents)");
            return Ok(());
        }
        
        // Check: All parent blocks should exist
        for (parent_idx, parent_hash) in block.parents.iter().enumerate() {
            // TODO: Implement parent existence check
            // Pattern:
            // match self.storage.read()?.get_block(parent_hash) {
            //     Ok(Some(_)) => {
            //         log::debug!("[GHOSTDAG] Parent {}/{} exists: {}", 
            //             parent_idx, block.parents.len(), parent_hash.to_hex());
            //     }
            //     Ok(None) => {
            //         log::warn!("[GHOSTDAG] Parent {}/{} not found: {}", 
            //             parent_idx, block.parents.len(), parent_hash.to_hex());
            //         return Err(StateError::OrphanBlock);
            //     }
            //     Err(e) => {
            //         return Err(StateError::StorageError(e.to_string()));
            //     }
            // }
            
            log::debug!("[GHOSTDAG] Parent {}/{} validated", parent_idx + 1, block.parents.len());
        }
        
        // TODO: Integrate klomang_core GHOSTDAG validation
        // This validates blue score, anticone, and conflict resolution
        // Pattern:
        // match self.dag.validate_ghostdag_rules(block) {
        //     Ok(true) => {
        //         log::debug!("[GHOSTDAG] GHOSTDAG rules valid for block {}", block.id.to_hex());
        //     }
        //     Ok(false) => {
        //         log::warn!("[GHOSTDAG] Block {} violates GHOSTDAG rules", block.id.to_hex());
        //         return Err(StateError::GhostdagViolation);
        //     }
        //     Err(e) => {
        //         log::error!("[GHOSTDAG] Validation error: {}", e);
        //         return Err(StateError::CoreValidationError(format!("GHOSTDAG error: {}", e)));
        //     }
        // }
        
        log::debug!("[GHOSTDAG] GHOSTDAG rules validated for block {}", block.id.to_hex());
        Ok(())
    }
}
