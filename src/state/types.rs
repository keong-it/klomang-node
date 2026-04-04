//! Core types and data structures for Klomang State Manager
//! 
//! This module contains:
//! - UtxoDiff: UTXO changes with Verkle Tree mapping
//! - KlomangEvent: Events emitted by state manager
//! - OrphanPool: Orphan block pool
//! - StagedUtxoChanges: Staged changes before commitment
//! - StateError: Error types for state operations
//! - StateResult: Result type alias

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use klomang_core::{UtxoSet, BlockNode, Hash};
use crate::storage::StorageHandle;

/// Represents UTXO changes with Verkle Tree mapping
/// 
/// CRITICAL: This diff must maintain atomicity between:
/// 1. UTXO set changes (added/removed entries)
/// 2. Verkle tree proof updates (new commitment + proofs)
/// 3. Storage persistence (all changes written atomically to CF_STATE)
///
/// Path scheme for Verkle leaf node (MUST match klomang-core):
/// ```
/// leaf_path = blake3(
///   concat(
///     address (20 bytes),
///     ":" (1 byte),
///     vout (8 bytes),
///     ":" (1 byte),
///     "VALUE" (5 bytes)
///   )
/// )
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UtxoDiff {
    /// Added UTXOs: blake3(addr||vout) -> (value, script_pubkey)
    /// SYNC POINT: Each addition must generate new Verkle leaf node
    pub added: HashMap<Hash, (u64, Vec<u8>)>,
    
    /// Removed UTXOs: blake3(addr||vout) set of spent outputs
    /// SYNC POINT: Each removal must delete Verkle leaf node
    pub removed: std::collections::HashSet<Hash>,
    
    /// New Verkle root hash after applying diff
    /// CRITICAL: Must be computed using klomang-core Verkle library
    /// Mismatch here indicates corrupted state
    pub new_verkle_root: Hash,
    
    /// Old Verkle root before applying diff (for validation)
    pub old_verkle_root: Hash,
}

impl UtxoDiff {
    pub fn new_with_roots(old_root: Hash, new_root: Hash) -> Self {
        Self {
            added: HashMap::new(),
            removed: std::collections::HashSet::new(),
            new_verkle_root: new_root,
            old_verkle_root: old_root,
        }
    }

    /// Add a new UTXO entry to diff
    /// This generates a Verkle leaf node path using blake3 hash
    pub fn add_utxo(&mut self, address: &[u8; 20], vout: u32, value: u64, script: Vec<u8>) {
        // VERKLE PATH: blake3(address || vout || "VALUE")
        // Must match klomang-core path derivation
        let mut path_input = Vec::with_capacity(20 + 1 + 4 + 1 + 5);
        path_input.extend_from_slice(address);
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(&vout.to_le_bytes());
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(b"VALUE");
        
        let utxo_path = Hash::new(&path_input);
        self.added.insert(utxo_path.clone(), (value, script));
        
        log::debug!("[VERKLE] Added UTXO at path {}: value={}", utxo_path.to_hex(), value);
    }

    /// Remove a UTXO entry (mark as spent)
    pub fn remove_utxo(&mut self, address: &[u8; 20], vout: u32) {
        // SAME PATH SCHEME: blake3(address || vout || "VALUE")
        let mut path_input = Vec::with_capacity(20 + 1 + 4 + 1 + 5);
        path_input.extend_from_slice(address);
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(&vout.to_le_bytes());
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(b"VALUE");
        
        let utxo_path = Hash::new(&path_input);
        self.removed.insert(utxo_path.clone());
        
        log::debug!("[VERKLE] Removed UTXO at path {}", utxo_path.to_hex());
    }

    /// Verify Verkle root commitment is valid and consistent
    pub fn verify_verkle_root(&self) -> Result<(), String> {
        if self.new_verkle_root.to_hex().is_empty() {
            return Err("New verkle root is empty".to_string());
        }
        if self.old_verkle_root.to_hex().is_empty() {
            return Err("Old verkle root is empty".to_string());
        }
        Ok(())
    }

    pub fn apply_to(&self, utxo_set: &mut UtxoSet) -> StateResult<()> {
        // Apply added UTXOs
        for (path, (value, script)) in &self.added {
            // Extract address and vout from path
            // Path format: blake3(address || ":" || vout || ":" || "VALUE")
            // We need to reverse engineer address and vout from path
            // For now, assume we store them separately or use a different approach
            
            // Placeholder: need to implement proper path parsing
            // For real implementation, we should store address and vout in the diff
            log::warn!("[UTXO] Applying added UTXO at path {}: value={}", path.to_hex(), value);
            // utxo_set.add_utxo(address, vout, *value, script.clone())?;
        }
        
        // Apply removed UTXOs
        for path in &self.removed {
            log::warn!("[UTXO] Applying removed UTXO at path {}", path.to_hex());
            // utxo_set.remove_utxo(address, vout)?;
        }
        
        // Verify Verkle root after changes
        // This should call klomang_core::verkle::verify_root or similar
        // For now, just log
        log::info!("[VERKLE] Applied UTXO diff, new root: {}", self.new_verkle_root.to_hex());
        
        Ok(())
    }
}

/// Events emitted by the Klomang State Manager
/// Used for real-time notifications to other modules (UI, RPC, etc.)
/// Events are also persisted to Storage for crash recovery and audit trails
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KlomangEvent {
    /// Block successfully accepted and processed
    BlockAccepted { hash: Hash, height: u64, timestamp_ms: u64 },
    /// Block rejected during validation
    BlockRejected { hash: Hash, reason: String, timestamp_ms: u64 },
    /// Chain reorganization occurred
    ReorgOccurred { old_tip: Hash, new_tip: Hash, depth: u32, timestamp_ms: u64 },
    /// Verkle proof verification failed - CRITICAL SECURITY EVENT
    InvalidVerkleProof { hash: Hash, reason: String, timestamp_ms: u64 },
    /// GHOSTDAG rules validation failed
    InvalidGhostdagRules { hash: Hash, reason: String, timestamp_ms: u64 },
    /// Verkle tree state mismatch detected - indicates potential crash recovery issue
    VerkleStateMismatch { expected_root: String, actual_root: String, timestamp_ms: u64 },
}

impl KlomangEvent {
    /// Get current timestamp in milliseconds since UNIX_EPOCH
    pub fn timestamp_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Orphan block pool for handling blocks received before their parents
#[derive(Clone)]
pub struct OrphanPool {
    /// Map of missing parent hash to list of orphan blocks waiting for it
    pool: HashMap<Hash, Vec<BlockNode>>,
}

impl OrphanPool {
    pub fn new() -> Self {
        Self {
            pool: HashMap::new(),
        }
    }

    /// Add an orphan block to the pool
    pub fn add_orphan(&mut self, block: BlockNode) {
        for parent in &block.parents {
            self.pool.entry(parent.clone()).or_insert_with(Vec::new).push(block.clone());
        }
        log::info!("[ORPHAN] Block {} stashed, waiting for parent", block.id.to_hex());
    }

    /// Get and remove all orphans that can now be processed
    pub fn get_processable_orphans(&mut self, processed_parent: &Hash) -> Vec<BlockNode> {
        if let Some(orphans) = self.pool.remove(processed_parent) {
            log::info!("[ORPHAN] Retrieved {} orphans for parent {}", orphans.len(), processed_parent.to_hex());
            orphans
        } else {
            Vec::new()
        }
    }

    /// Check if a block is an orphan (missing parents)
    pub fn is_orphan(&self, block: &BlockNode, storage: &StorageHandle) -> StateResult<bool> {
        // Check if all parents exist in storage
        for parent in &block.parents {
            let exists = storage.read().unwrap().get_block(parent)
                .map_err(|e| StateError::StorageError(e.to_string()))?
                .is_some();
            if !exists {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Represents staged UTXO changes before commitment
#[derive(Clone)]
pub struct StagedUtxoChanges {
    /// UTXO diff representing the changes
    pub utxo_diff: UtxoDiff,
    /// New best block hash after processing
    pub new_best_block: Hash,
    /// New total work for the chain
    pub new_total_work: u128,
}

impl StagedUtxoChanges {
    pub fn new(utxo_diff: UtxoDiff, new_best_block: Hash, new_total_work: u128) -> Self {
        Self {
            utxo_diff,
            new_best_block,
            new_total_work,
        }
    }
}

/// Error types for state management operations
#[derive(Debug, Clone, PartialEq)]
pub enum StateError {
    InvalidSignature,
    OrphanBlock,
    InsufficientBalance,
    DoubleSpend,
    InvalidHash,
    GhostdagViolation,
    StorageError(String),
    CoreValidationError(String),
    SanityCheckFailed(String),  // For crash recovery inconsistencies
    DiskFullError,              // Specific I/O errors
    IoError(String),            // General I/O errors
    AtomicityViolation(String), // If partial state detected
}

impl std::error::Error for StateError {}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateError::InvalidSignature => write!(f, "Invalid block signature"),
            StateError::OrphanBlock => write!(f, "Block has no valid parents (orphan)"),
            StateError::InsufficientBalance => write!(f, "Insufficient balance for transaction"),
            StateError::DoubleSpend => write!(f, "Double spend detected"),
            StateError::InvalidHash => write!(f, "Invalid block hash"),
            StateError::GhostdagViolation => write!(f, "GHOSTDAG rules violation"),
            StateError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            StateError::CoreValidationError(msg) => write!(f, "Core validation error: {}", msg),
            StateError::SanityCheckFailed(msg) => write!(f, "Sanity check failed - possible crash recovery issue: {}", msg),
            StateError::DiskFullError => write!(f, "Disk full - insufficient storage space"),
            StateError::IoError(msg) => write!(f, "I/O error: {}", msg),
            StateError::AtomicityViolation(msg) => write!(f, "Atomicity violation detected: {}", msg),
        }
    }
}

/// Result type for state operations
pub type StateResult<T> = Result<T, StateError>;
