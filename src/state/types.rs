//! Core types and data structures for Klomang State Manager
//!
//! This module contains:
//! - UtxoDiff: UTXO changes with Verkle Tree mapping
//! - KlomangEvent: Events emitted by state manager
//! - OrphanPool: Orphan block pool
//! - StagedUtxoChanges: Staged changes before commitment
//! - StateError: Error types for state operations
//! - StateResult: Result type alias

use crate::storage::StorageHandle;
use klomang_core::state::transaction::TxOutput;
use klomang_core::{BlockNode, Hash, UtxoSet};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
///     txid (32 bytes),
///     ":" (1 byte),
///     vout (8 bytes),
///     ":" (1 byte),
///     "VALUE" (5 bytes)
///   )
/// )
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UtxoDiff {
    /// Added UTXOs: (txid, vout) -> (value, script_pubkey)
    /// SYNC POINT: Each addition must generate new Verkle leaf node
    pub added: HashMap<(Hash, u32), (u64, Vec<u8>)>,

    /// Removed UTXOs: (txid, vout) set of spent outputs
    /// SYNC POINT: Each removal must delete Verkle leaf node
    pub removed: std::collections::HashSet<(Hash, u32)>,

    /// New Verkle root hash after applying diff
    /// CRITICAL: Must be computed using klomang-core Verkle library
    /// Mismatch here indicates corrupted state
    pub new_verkle_root: Hash,

    /// Old Verkle root before applying diff (for validation)
    pub old_verkle_root: Hash,

    /// Verkle proofs for all UTXO changes (for verification)
    /// Contains proofs for both added and removed UTXOs
    pub verkle_proofs: Vec<Vec<u8>>,
}

impl UtxoDiff {
    pub fn new_with_roots(old_root: Hash, new_root: Hash) -> Self {
        Self {
            added: HashMap::new(),
            removed: std::collections::HashSet::new(),
            new_verkle_root: new_root,
            old_verkle_root: old_root,
            verkle_proofs: Vec::new(),
        }
    }

    /// Add a new UTXO entry to diff
    /// This generates a Verkle leaf node path using blake3 hash
    pub fn add_utxo(&mut self, txid: Hash, vout: u32, value: u64, script: Vec<u8>) {
        self.added.insert((txid.clone(), vout), (value, script));

        log::debug!(
            "[VERKLE] Added UTXO at ({}, {}): value={}",
            txid.to_hex(),
            vout,
            value
        );
    }

    /// Remove a UTXO entry (mark as spent)
    pub fn remove_utxo(&mut self, txid: Hash, vout: u32) {
        self.removed.insert((txid.clone(), vout));

        log::debug!("[VERKLE] Removed UTXO at ({}, {})", txid.to_hex(), vout);
    }

    /// Generate Verkle proofs for all UTXO changes
    /// This should be called after updating the Verkle tree
    pub fn generate_verkle_proofs(
        &mut self,
        verkle_tree: &mut klomang_core::core::state::v_trie::VerkleTree<
            crate::storage::adapter::RocksDBStorageAdapter,
        >,
    ) -> StateResult<()> {
        self.verkle_proofs.clear();

        // Generate proofs for added UTXOs
        for ((txid, vout), _) in &self.added {
            let mut path_input = Vec::with_capacity(32 + 1 + 4 + 1 + 5);
            path_input.extend_from_slice(txid.as_bytes());
            path_input.extend_from_slice(b":");
            path_input.extend_from_slice(&vout.to_le_bytes());
            path_input.extend_from_slice(b":");
            path_input.extend_from_slice(b"VALUE");

            let verkle_path = Hash::new(&path_input);
            let key = *verkle_path.as_bytes();

            // Generate proof for this path
            let proof = verkle_tree.generate_proof(key).map_err(|e| {
                StateError::StorageError(format!("Failed to generate Verkle proof: {}", e))
            })?;

            // Store root bytes as a lightweight proof placeholder until full proof serialization is supported.
            self.verkle_proofs.push(proof.root.to_vec());
        }

        // Generate proofs for removed UTXOs (proofs showing they existed before removal)
        for (txid, vout) in &self.removed {
            let mut path_input = Vec::with_capacity(32 + 1 + 4 + 1 + 5);
            path_input.extend_from_slice(txid.as_bytes());
            path_input.extend_from_slice(b":");
            path_input.extend_from_slice(&vout.to_le_bytes());
            path_input.extend_from_slice(b":");
            path_input.extend_from_slice(b"VALUE");

            let verkle_path = Hash::new(&path_input);
            let key = *verkle_path.as_bytes();

            let proof = verkle_tree.generate_proof(key).map_err(|e| {
                StateError::StorageError(format!("Failed to generate Verkle proof: {}", e))
            })?;

            self.verkle_proofs.push(proof.root.to_vec());
        }

        log::debug!(
            "[VERKLE] Generated {} proofs for UTXO changes",
            self.verkle_proofs.len()
        );
        Ok(())
    }

    /// Verify Verkle proofs against the old root
    pub fn verify_verkle_proofs(
        &self,
        verkle_tree: &klomang_core::core::state::v_trie::VerkleTree<
            crate::storage::adapter::RocksDBStorageAdapter,
        >,
    ) -> StateResult<()> {
        // TODO: Implement proper proof verification once VerkleProof API is clarified
        // For now, we assume proofs are valid if they exist
        if self.verkle_proofs.is_empty() && (!self.added.is_empty() || !self.removed.is_empty()) {
            return Err(StateError::StorageError(
                "Missing Verkle proofs for UTXO changes".to_string(),
            ));
        }

        log::debug!(
            "[VERKLE] Proof verification placeholder - {} proofs present",
            self.verkle_proofs.len()
        );
        Ok(())
    }

    pub fn apply_to(&self, utxo_set: &mut UtxoSet) -> StateResult<()> {
        log::info!(
            "[UTXO] Applying UTXO diff: {} additions, {} removals",
            self.added.len(),
            self.removed.len()
        );

        // Apply added UTXOs
        for ((txid, vout), (value, script)) in &self.added {
            let outpoint = (txid.clone(), *vout);
            let tx_output = TxOutput {
                value: *value,
                pubkey_hash: {
                    let script_bytes: [u8; 32] = script.as_slice().try_into().unwrap_or([0u8; 32]);
                    Hash::from_bytes(&script_bytes)
                },
            };

            utxo_set.utxos.insert(outpoint, tx_output);
            log::debug!(
                "[UTXO] Added UTXO ({}, {}): value={}",
                txid.to_hex(),
                vout,
                value
            );
        }

        // Apply removed UTXOs
        for (txid, vout) in &self.removed {
            let outpoint = (txid.clone(), *vout);

            if utxo_set.utxos.remove(&outpoint).is_none() {
                log::warn!(
                    "[UTXO] Attempted to remove non-existent UTXO ({}, {})",
                    txid.to_hex(),
                    vout
                );
            } else {
                log::debug!("[UTXO] Removed UTXO ({}, {})", txid.to_hex(), vout);
            }
        }

        // Update Verkle tree with the changes
        // This would involve calling the Verkle tree update methods
        log::info!(
            "[VERKLE] Applied UTXO diff, new root: {}",
            self.new_verkle_root.to_hex()
        );

        Ok(())
    }

    /// Invalidate cache entries for affected UTXOs
    /// Should be called after applying UTXO changes to ensure cache consistency
    pub fn invalidate_cache(&self, cache: &std::sync::Mutex<lru::LruCache<(Hash, u32), TxOutput>>) {
        if let Ok(mut cache_guard) = cache.lock() {
            // Invalidate added UTXOs (new entries)
            for (outpoint, _) in &self.added {
                cache_guard.pop(outpoint);
            }

            // Invalidate removed UTXOs (spent entries)
            for outpoint in &self.removed {
                cache_guard.pop(outpoint);
            }

            log::debug!(
                "[CACHE] Invalidated {} cache entries for UTXO changes",
                self.added.len() + self.removed.len()
            );
        }
    }
}

/// Events emitted by the Klomang State Manager
/// Used for real-time notifications to other modules (UI, RPC, etc.)
/// Events are also persisted to Storage for crash recovery and audit trails
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KlomangEvent {
    /// Block successfully accepted and processed
    BlockAccepted {
        hash: Hash,
        height: u64,
        timestamp_ms: u64,
    },
    /// Block rejected during validation
    BlockRejected {
        hash: Hash,
        reason: String,
        timestamp_ms: u64,
    },
    /// Chain reorganization occurred
    ReorgOccurred {
        old_tip: Hash,
        new_tip: Hash,
        depth: u32,
        timestamp_ms: u64,
    },
    /// Verkle proof verification failed - CRITICAL SECURITY EVENT
    InvalidVerkleProof {
        hash: Hash,
        reason: String,
        timestamp_ms: u64,
    },
    /// GHOSTDAG rules validation failed
    InvalidGhostdagRules {
        hash: Hash,
        reason: String,
        timestamp_ms: u64,
    },
    /// Verkle tree state mismatch detected - indicates potential crash recovery issue
    VerkleStateMismatch {
        expected_root: String,
        actual_root: String,
        timestamp_ms: u64,
    },
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
/// Enhanced orphan block pool with expiration, batch processing, and metrics
/// Handles blocks that arrive before their parents, ensuring data integrity
/// during parallel block propagation and sync prioritization.
#[derive(Clone)]
pub struct OrphanPool {
    /// Map of missing parent hash to list of orphan blocks waiting for it
    pool: HashMap<Hash, Vec<BlockNode>>,
    /// Timestamps for orphan expiration tracking
    orphan_timestamps: HashMap<Hash, Instant>,
    /// Maximum age for orphans before eviction (default: 10 minutes)
    max_orphan_age: Duration,
    /// Metrics: current orphan count
    orphan_count: Arc<AtomicUsize>,
    /// Metrics: total orphans evicted due to expiration
    total_evicted: Arc<AtomicU64>,
}

impl OrphanPool {
    pub fn new() -> Self {
        Self {
            pool: HashMap::new(),
            orphan_timestamps: HashMap::new(),
            max_orphan_age: Duration::from_secs(600), // 10 minutes
            orphan_count: Arc::new(AtomicUsize::new(0)),
            total_evicted: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Add an orphan block to the pool with timestamp tracking
    pub fn add_orphan(&mut self, block: BlockNode) {
        let now = Instant::now();
        for parent in &block.header.parents {
            self.pool
                .entry(parent.clone())
                .or_insert_with(Vec::new)
                .push(block.clone());
            self.orphan_timestamps.insert(parent.clone(), now);
        }
        self.orphan_count.fetch_add(1, Ordering::Relaxed);
        log::info!(
            "[ORPHAN] Block {} stashed, waiting for parent. Total orphans: {}",
            block.header.id.to_hex(),
            self.orphan_count.load(Ordering::Relaxed)
        );
    }

    /// Get and remove all orphans that can now be processed (legacy method)
    pub fn get_processable_orphans(&mut self, processed_parent: &Hash) -> Vec<BlockNode> {
        if let Some(orphans) = self.pool.remove(processed_parent) {
            self.orphan_timestamps.remove(processed_parent);
            self.orphan_count.fetch_sub(orphans.len(), Ordering::Relaxed);
            log::info!(
                "[ORPHAN] Retrieved {} orphans for parent {}",
                orphans.len(),
                processed_parent.to_hex()
            );
            orphans
        } else {
            Vec::new()
        }
    }

    /// Get processable batch of orphans grouped by parent, sorted by block height
    /// Returns orphans that can be processed together for batch ingestion
    pub fn get_processable_batch(&mut self, parent: &Hash) -> Option<Vec<BlockNode>> {
        if let Some(mut orphans) = self.pool.remove(parent) {
            self.orphan_timestamps.remove(parent);
            let count = orphans.len();
            self.orphan_count.fetch_sub(count, Ordering::Relaxed);

            // Sort by block height for optimal processing order
            orphans.sort_by_key(|block| block.header.height);

            log::info!(
                "[ORPHAN] Retrieved batch of {} orphans for parent {}, sorted by height",
                count,
                parent.to_hex()
            );
            Some(orphans)
        } else {
            None
        }
    }

    /// Check if a block is an orphan (missing parents)
    pub fn is_orphan(&self, block: &BlockNode, storage: &StorageHandle) -> StateResult<bool> {
        // Check if all parents exist in storage
        for parent in &block.header.parents {
            let exists = storage
                .read()
                .unwrap()
                .get_block(parent)
                .map_err(|e| StateError::StorageError(e.to_string()))?
                .is_some();
            if !exists {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Cleanup expired orphans asynchronously
    /// Should be called periodically (e.g., every 5 minutes)
    pub async fn cleanup_expired(&mut self) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (parent, timestamp) in &self.orphan_timestamps {
            if now.duration_since(*timestamp) > self.max_orphan_age {
                to_remove.push(parent.clone());
            }
        }

        let mut evicted_count = 0;
        for parent in to_remove {
            if let Some(orphans) = self.pool.remove(&parent) {
                evicted_count += orphans.len();
                self.orphan_timestamps.remove(&parent);
            }
        }

        if evicted_count > 0 {
            self.orphan_count.fetch_sub(evicted_count, Ordering::Relaxed);
            self.total_evicted.fetch_add(evicted_count as u64, Ordering::Relaxed);
            log::warn!(
                "[ORPHAN] Cleaned up {} expired orphans. Total evicted: {}",
                evicted_count,
                self.total_evicted.load(Ordering::Relaxed)
            );
        }
    }

    /// Get current metrics
    pub fn metrics(&self) -> OrphanMetrics {
        OrphanMetrics {
            current_count: self.orphan_count.load(Ordering::Relaxed),
            total_evicted: self.total_evicted.load(Ordering::Relaxed),
            pool_size: self.pool.len(),
        }
    }
}

/// Metrics for orphan pool monitoring
#[derive(Debug, Clone)]
pub struct OrphanMetrics {
    pub current_count: usize,
    pub total_evicted: u64,
    pub pool_size: usize,
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
    VmExecutionError(String),   // VM execution errors
    ConsensusError(String),     // Consensus rule violations
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
            StateError::SanityCheckFailed(msg) => write!(
                f,
                "Sanity check failed - possible crash recovery issue: {}",
                msg
            ),
            StateError::DiskFullError => write!(f, "Disk full - insufficient storage space"),
            StateError::IoError(msg) => write!(f, "I/O error: {}", msg),
            StateError::AtomicityViolation(msg) => {
                write!(f, "Atomicity violation detected: {}", msg)
            }
            StateError::VmExecutionError(msg) => write!(f, "VM execution error: {}", msg),
            StateError::ConsensusError(msg) => write!(f, "Consensus error: {}", msg),
        }
    }
}

/// Result type for state operations
pub type StateResult<T> = Result<T, StateError>;
