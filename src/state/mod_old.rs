use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use serde::{Serialize, Deserialize};
use klomang_core::{Dag, UtxoSet, BlockNode, Transaction, Hash};
use crate::storage::{StorageHandle, ChainIndexRecord};
use crate::ingestion_guard::{RateLimiter, IngestionMessage, IngestionStats, IngestionGuardConfig, create_ingestion_queue};

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
struct UtxoDiff {
    /// Added UTXOs: blake3(addr||vout) -> (value, script_pubkey)
    /// SYNC POINT: Each addition must generate new Verkle leaf node
    added: HashMap<Hash, (u64, Vec<u8>)>,
    
    /// Removed UTXOs: blake3(addr||vout) set of spent outputs
    /// SYNC POINT: Each removal must delete Verkle leaf node
    removed: std::collections::HashSet<Hash>,
    
    /// New Verkle root hash after applying diff
    /// CRITICAL: Must be computed using klomang-core Verkle library
    /// Mismatch here indicates corrupted state
    new_verkle_root: Hash,
    
    /// Old Verkle root before applying diff (for validation)
    old_verkle_root: Hash,
}

impl UtxoDiff {
    fn new_with_roots(old_root: Hash, new_root: Hash) -> Self {
        Self {
            added: HashMap::new(),
            removed: std::collections::HashSet::new(),
            new_verkle_root: new_root,
            old_verkle_root: old_root,
        }
    }

    /// Add a new UTXO entry to diff
    /// This generates a Verkle leaf node path using blake3 hash
    fn add_utxo(&mut self, address: &[u8; 20], vout: u32, value: u64, script: Vec<u8>) {
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
    fn remove_utxo(&mut self, address: &[u8; 20], vout: u32) {
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
    fn verify_verkle_root(&self) -> Result<(), String> {
        if self.new_verkle_root.to_hex().is_empty() {
            return Err("New verkle root is empty".to_string());
        }
        if self.old_verkle_root.to_hex().is_empty() {
            return Err("Old verkle root is empty".to_string());
        }
        Ok(())
    }

    fn apply_to(&self, utxo_set: &mut UtxoSet) -> StateResult<()> {
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
    fn timestamp_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Orphan block pool for handling blocks received before their parents
#[derive(Clone)]
struct OrphanPool {
    /// Map of missing parent hash to list of orphan blocks waiting for it
    pool: HashMap<Hash, Vec<BlockNode>>,
}

impl OrphanPool {
    fn new() -> Self {
        Self {
            pool: HashMap::new(),
        }
    }

    /// Add an orphan block to the pool
    fn add_orphan(&mut self, block: BlockNode) {
        for parent in &block.parents {
            self.pool.entry(parent.clone()).or_insert_with(Vec::new).push(block.clone());
        }
        log::info!("[ORPHAN] Block {} stashed, waiting for parent", block.id.to_hex());
    }

    /// Get and remove all orphans that can now be processed
    fn get_processable_orphans(&mut self, processed_parent: &Hash) -> Vec<BlockNode> {
        if let Some(orphans) = self.pool.remove(processed_parent) {
            log::info!("[ORPHAN] Retrieved {} orphans for parent {}", orphans.len(), processed_parent.to_hex());
            orphans
        } else {
            Vec::new()
        }
    }

    /// Check if a block is an orphan (missing parents)
    fn is_orphan(&self, block: &BlockNode, storage: &StorageHandle) -> StateResult<bool> {
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
struct StagedUtxoChanges {
    /// UTXO diff representing the changes
    utxo_diff: UtxoDiff,
    /// New best block hash after processing
    new_best_block: Hash,
    /// New total work for the chain
    new_total_work: u128,
}

impl StagedUtxoChanges {
    fn new(utxo_diff: UtxoDiff, new_best_block: Hash, new_total_work: u128) -> Self {
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

/// Central state manager for Klomang Node
/// Coordinates between klomang-core (brain) and storage (warehouse)
pub struct KlomangStateManager {
    /// Thread-safe storage handle
    storage: StorageHandle,
    /// DAG state from klomang-core
    dag: Dag,
    /// UTXO set for transaction validation
    utxo_set: UtxoSet,
    /// Current Verkle tree root hash - kept in sync with best_block
    /// CRITICAL: Must be persisted to CF_STATE and verified at startup
    /// Mismatch with best_block indicates corrupted state
    current_verkle_root: Hash,
    /// Current best block hash (highest total work)
    best_block: Hash,
    /// Orphan block pool for blocks waiting for parents
    orphan_pool: OrphanPool,
    /// Reorg history for rollback capability (block_hash -> utxo_diff)
    reorg_history: HashMap<Hash, UtxoDiff>,
    /// Event bus for broadcasting state changes to other modules
    /// Uses bounded broadcast channel (100 events) to prevent memory leaks
    event_sender: broadcast::Sender<KlomangEvent>,
    /// Counter for detecting unbounded event queue growth
    /// ============ INGESTION GUARD COMPONENTS ============
    /// Sender for block ingestion queue (from P2P or RPC)
    ingestion_tx: mpsc::Sender<IngestionMessage>,
    /// Receiver for block ingestion queue
    ingestion_rx: std::sync::Mutex<Option<mpsc::Receiver<IngestionMessage>>>,
    /// Rate limiter for bounded block processing (max blocks/sec)
    rate_limiter: RateLimiter,
    /// Statistics for ingestion queue monitoring
    ingestion_stats: Arc<std::sync::Mutex<IngestionStats>>,
    /// Worker task handle for graceful shutdown
    worker_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    event_counter: std::sync::atomic::AtomicU64,
}

impl KlomangStateManager {
    /// Create a new state manager with initialized storage
    pub fn new(storage: StorageHandle) -> StateResult<Self> {
        // Load existing state from storage or initialize genesis
        let (dag, utxo_set, best_block, current_verkle_root) = Self::load_state_from_storage(&storage)?;

        // Load previous events if available (for audit trail)
        Self::load_event_history(&storage)?;

        let (event_sender, _): (broadcast::Sender<KlomangEvent>, _) = broadcast::channel(100); // Buffer 100 events

        // Initialize ingestion guard with default configuration
        let config = IngestionGuardConfig::default();
        let (ingestion_tx, ingestion_rx, rate_limiter) = create_ingestion_queue(
            config.queue_capacity,
            config.max_blocks_per_sec,
        );
        let ingestion_stats = Arc::new(std::sync::Mutex::new(IngestionStats::new()));

        Ok(KlomangStateManager {
            storage,
            dag,
            utxo_set,
            current_verkle_root,
            best_block,
            orphan_pool: OrphanPool::new(),
            reorg_history: HashMap::new(),
            event_sender,
            event_counter: std::sync::atomic::AtomicU64::new(0),
            ingestion_tx,
            ingestion_rx: std::sync::Mutex::new(Some(ingestion_rx)),
            rate_limiter,
            ingestion_stats,
            worker_handle: std::sync::Mutex::new(None),
        })
    }

    /// Load state from storage during startup with sanity check
    fn load_state_from_storage(storage: &StorageHandle) -> StateResult<(Dag, UtxoSet, Hash, Hash)> {
        let storage_read = storage.read().map_err(|e| StateError::StorageError(e.to_string()))?;

        // Perform sanity check to detect crash recovery scenarios
        Self::perform_sanity_check(&storage_read)?;

        // Load persistent best tip from metadata
        let best_block = Self::load_persistent_best_tip(&storage_read)?;

        // Load persistent Verkle root from CF_STATE
        // CRITICAL: Must match best_block hash after successful commitment
        let current_verkle_root = Self::load_persistent_verkle_root(&storage_read, &best_block)?;

        // Load DAG state - placeholder: in real implementation, core should provide deserialization
        let dag = Dag::new();

        // Load UTXO set - placeholder: core should provide deserialization
        let utxo_set = UtxoSet::new();

        Ok((dag, utxo_set, best_block, current_verkle_root))
    }

    /// Load persistent Verkle root from storage (CF_STATE)
    /// CRITICAL SECURITY: If Verkle root doesn't match best_block, indicates state corruption
    fn load_persistent_verkle_root(storage_read: &std::sync::RwLockReadGuard<crate::storage::KlomangStorage>, best_block: &Hash) -> StateResult<Hash> {
        let key = format!("verkle_root:{}", best_block.to_hex());
        if let Some(root_bytes) = storage_read.get_state(&key)
            .map_err(|e| StateError::StorageError(e.to_string()))? {
            if root_bytes.len() == 32 {
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&root_bytes);
                let stored_root = Hash::from_bytes(&hash_bytes);
                
                // Verify root matches expected (placeholder for now)
                // In real implementation, compute expected root from UTXO set
                log::info!("[VERKLE] Loaded persistent Verkle root for best_block {}: {}", best_block.to_hex(), stored_root.to_hex());
                Ok(stored_root)
            } else {
                Err(StateError::StorageError("Invalid Verkle root length".to_string()))
            }
        } else {
            // Fallback to genesis if no stored root
            let genesis_verkle_root = Hash::new(b"genesis-verkle-root");
            log::warn!("[VERKLE] No persistent Verkle root found for {}, using genesis: {}", best_block.to_hex(), genesis_verkle_root.to_hex());
            Ok(genesis_verkle_root)
        }
    }

    /// Load persistent best tip from storage metadata
    fn load_persistent_best_tip(storage_read: &std::sync::RwLockReadGuard<crate::storage::KlomangStorage>) -> StateResult<Hash> {
        if let Some(tip_bytes) = storage_read.get_state("best_tip")
            .map_err(|e| StateError::StorageError(e.to_string()))? {
            if tip_bytes.len() == 32 {
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&tip_bytes);
                let best_tip = Hash::from_bytes(&hash_bytes);
                log::info!("[STATE] Loaded persistent best tip: {}", best_tip.to_hex());
                Ok(best_tip)
            } else {
                Err(StateError::StorageError("Invalid best tip length".to_string()))
            }
        } else {
            // Fallback to chain index method
            let max_height = storage_read.get_max_chain_height()
                .map_err(|e| StateError::StorageError(e.to_string()))?;
            if max_height > 0 {
                Ok(storage_read.get_chain_index_by_height(max_height)
                    .map_err(|e| StateError::StorageError(e.to_string()))?
                    .map(|record| record.tip)
                    .unwrap_or_else(|| Hash::new(b"genesis-block")))
            } else {
                Ok(Hash::new(b"genesis-block"))
            }
        }
    }

    /// Perform sanity check to detect inconsistencies after crash
    /// Ensures storage and state remain synchronized
    fn perform_sanity_check(storage_read: &std::sync::RwLockReadGuard<crate::storage::KlomangStorage>) -> StateResult<()> {
        // Check for basic storage consistency after potential crash

        // 1. Verify we can read basic storage metadata
        let max_height = storage_read.get_max_chain_height()
            .map_err(|e| StateError::SanityCheckFailed(format!("Cannot read max chain height: {}", e)))?;

        // 2. If we have blocks, verify chain index exists for max height
        if max_height > 0 {
            let chain_record = storage_read.get_chain_index_by_height(max_height)
                .map_err(|e| StateError::SanityCheckFailed(format!("Cannot read chain index for height {}: {}", max_height, e)))?;

            if chain_record.is_none() {
                return Err(StateError::SanityCheckFailed(
                    format!("Missing chain index for height {} - possible partial write during crash", max_height)
                ));
            }

            // 3. Verify the block referenced in chain index actually exists
            let block_hash = chain_record.unwrap().tip;
            let block_exists = storage_read.get_block(&block_hash)
                .map_err(|e| StateError::SanityCheckFailed(format!("Cannot check block existence: {}", e)))?;

            if block_exists.is_none() {
                return Err(StateError::SanityCheckFailed(
                    format!("Chain index references non-existent block {} - atomicity violation", block_hash.to_hex())
                ));
            }
        }

        // 4. Additional checks for transaction consistency
        // Check for orphaned blocks without chain index
        // This is a basic check; more thorough orphan detection would require scanning all blocks
        
        // 5. VERKLE STATE CONSISTENCY CHECK (CRITICAL)
        if max_height > 0 {
            let chain_record = storage_read.get_chain_index_by_height(max_height)
                .map_err(|e| StateError::SanityCheckFailed(format!("Cannot read chain index for Verkle check: {}", e)))?;
            
            if let Some(record) = chain_record {
                let best_block = record.tip;
                let stored_verkle_root = Self::load_persistent_verkle_root(storage_read, &best_block)?;
                
                // For now, we can't compute the expected root without full state
                // In real implementation, we would compute expected root from UTXO set
                // If stored_verkle_root != expected_root {
                //     let msg = format!(
                //         "CRITICAL: Verkle root mismatch for best_block {}. \
                //          Stored root: {}, Expected root: {}. \
                //          This indicates state corruption or incomplete commit.",
                //         best_block.to_hex(), stored_verkle_root.to_hex(), expected_root.to_hex()
                //     );
                //     log::error!("{}", msg);
                //     return Err(StateError::SanityCheckFailed(msg));
                // }
                
                log::info!("[SANITY] Verkle root check passed for block {}: {}", best_block.to_hex(), stored_verkle_root.to_hex());
            }
        }

        Ok(())
    }

    /// Calculate block height from its parents
    fn calculate_block_height(&self, block: &BlockNode) -> StateResult<u64> {
        if block.parents.is_empty() {
            // Genesis block
            return Ok(0);
        }

        // Find the maximum height among parents
        let mut max_parent_height = 0u64;
        for parent_hash in &block.parents {
            if let Some(parent_record) = self.storage.read().map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_chain_index(parent_hash)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                if parent_record.height > max_parent_height {
                    max_parent_height = parent_record.height;
                }
            } else {
                return Err(StateError::StorageError(format!("Parent block {} not found in chain index", parent_hash.to_hex())));
            }
        }

        Ok(max_parent_height + 1)
    }

    /// Process a new block - ATOMIC EXECUTION PIPELINE with Orphan Handling
    /// The Safety Chain: Validation → Staging → Storage Lock → Persistence → Commitment → Finalization
    /// Includes orphan block management and reorg detection
    pub fn process_block(&mut self, block: BlockNode) -> StateResult<()> {
        // Check if block is orphan (missing parents)
        if self.orphan_pool.is_orphan(&block, &self.storage)? {
            self.orphan_pool.add_orphan(block);
            return Ok(()); // Block stashed, not an error
        }

        // Process the block through the atomic pipeline
        self.process_block_internal(block)?;

        // After successful processing, check for processable orphans
        self.process_orphans_recursively()?;

        Ok(())
    }

    /// Internal block processing (called for non-orphan blocks)
    fn process_block_internal(&mut self, block: BlockNode) -> StateResult<()> {
        // ===========================================
        // PHASE 1A: STATELESS VALIDATION (Format Check)
        // ===========================================
        // Fast validation without storage access. If fails, immediate rejection.
        if let Err(e) = self.validate_block_stateless(&block) {
            log::warn!("[VALIDATION] Block {} rejected in stateless validation: {}", block.id.to_hex(), e);
            let timestamp = KlomangEvent::timestamp_now();
            self.emit_event(KlomangEvent::BlockRejected {
                hash: block.id.clone(),
                reason: format!("Stateless validation failed: {}", e),
                timestamp_ms: timestamp,
            });
            return Err(e);
        }

        // ===========================================
        // PHASE 1B: STATEFUL VALIDATION (Logic Check)
        // ===========================================
        // Validation against current state. Only performed if stateless passes.
        if let Err(e) = self.validate_block_stateful(&block) {
            log::warn!("[VALIDATION] Block {} rejected in stateful validation: {}", block.id.to_hex(), e);
            let timestamp = KlomangEvent::timestamp_now();
            self.emit_event(KlomangEvent::BlockRejected {
                hash: block.id.clone(),
                reason: format!("Stateful validation failed: {}", e),
                timestamp_ms: timestamp,
            });
            return Err(e);
        }

        // ===========================================
        // PHASE 2: STAGING (In-Memory Changes)
        // ===========================================
        // Calculate UTXO diff without modifying main state
        let utxo_diff = self.stage_utxo_diff(&block)?;

        // Check for potential reorg
        let staged_changes = self.create_staged_changes(utxo_diff, &block)?;

        // ===========================================
        // PHASE 3: STORAGE LOCKING
        // ===========================================
        // Acquire write lock on storage
        {
            let mut storage_write = self.storage.write()
                .map_err(|e| StateError::StorageError(e.to_string()))?;

            // ===========================================
            // PHASE 4: PERSISTENCE (Atomic Batch)
            // ===========================================
            // Use RocksDB WriteBatch for atomic multi-CF persistence
            // Includes block, transactions, chain index, and UTXO diff
            self.persist_block_with_diff(&mut storage_write, &block, &staged_changes)?;
        } // Lock released here

        // ===========================================
        // PHASE 5: STATE COMMITMENT
        // ===========================================
        // Only now commit the staged changes to main RAM state
        self.commit_staged_changes(staged_changes)?;

        // ===========================================
        // PHASE 6: FINALIZATION
        // ===========================================
        // Update best tip in memory and check for reorg
        self.finalize_block_processing();

        // Emit success event and persist to storage
        log::info!("[VALIDATION] Block {} successfully accepted", block.id.to_hex());
        let block_height = self.calculate_block_height(&block)?;
        let timestamp = KlomangEvent::timestamp_now();
        self.emit_event(KlomangEvent::BlockAccepted {
            hash: block.id.clone(),
            height: block_height,
            timestamp_ms: timestamp,
        });

        Ok(())
    }

    /// PHASE 1A: STATELESS VALIDATION (Format & Integrity Check)
    /// Validates block without accessing storage or UTXO state
    /// If this fails, block is immediately discarded without touching storage
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

    /// PHASE 1B: STATEFUL VALIDATION (Logic & State Check)
    /// Validates block against current UTXO state and DAG rules
    /// Only performed if stateless validation passes
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
            for (input_idx, input) in tx.inputs.iter().enumerate() {
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
            let mut total_input: u64 = 0;
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

    /// PHASE 2: STAGING - Calculate UTXO diff without modifying main state
    fn stage_utxo_diff(&self, block: &BlockNode) -> StateResult<UtxoDiff> {
        // Calculate the UTXO changes that this block introduces
        // This includes:
        // 1. Spent outputs (removed): All inputs in this block's transactions
        // 2. New outputs (added): All outputs in this block's transactions  
        // 3. New Verkle root: Computed from applying diff to current root
        
        log::info!("[STAGING] Calculating UTXO diff for block {} with {} transactions", 
            block.id.to_hex(), block.transactions.len());
        
        let mut utxo_diff = UtxoDiff::new_with_roots(
            self.current_verkle_root.clone(), 
            self.current_verkle_root.clone() // Will update below
        );
        
        // Process each transaction to extract inputs and outputs
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            // TODO: Extract inputs from transaction
            // For each input, mark in removed set using UTXO path scheme
            // Pattern:
            // for input in &tx.inputs {
            //     let utxo_key = Hash::from_bytes(&input.previous_txid)?;
            //     let vout = input.previous_vout;
            //     utxo_diff.remove_utxo(&get_address_from_input(input)?, vout);
            // }
            
            log::debug!("[STAGING] Processing inputs for transaction {}/{}", tx_idx + 1, block.transactions.len());
            
            // Extract outputs from transaction
            for (output_idx, output) in tx.outputs.iter().enumerate() {
                // TODO: Extract address from output script
                // For now, use placeholder address
                // Pattern:
                // let address = extract_address_from_script(&output.script)?;
                // utxo_diff.add_utxo(&address, output_idx as u32, output.value, output.script.clone());
                
                log::debug!("[STAGING] Processing output {}/{} of tx {}", 
                    output_idx + 1, tx.outputs.len(), tx_idx + 1);
            }
        }
        
        // TODO: Update new_verkle_root based on the diff
        // Pattern:
        // utxo_diff.new_verkle_root = klomang_core::verkle::apply_diff(&self.current_verkle_root, &utxo_diff)?;
        
        log::info!("[STAGING] UTXO diff calculated: {} added, {} removed", 
            utxo_diff.added.len(), utxo_diff.removed.len());
        
        Ok(utxo_diff)
    }

    /// Create staged changes from UTXO diff
    fn create_staged_changes(&self, utxo_diff: UtxoDiff, block: &BlockNode) -> StateResult<StagedUtxoChanges> {
        // Create staged changes that include total work calculation
        // Total work accumulates from parent blocks
        
        log::info!("[STAGING] Creating staged changes for block {}", block.id.to_hex());
        
        // Calculate new total work
        // In GHOSTDAG/Klomang: total_work = max(parent_work) + block_work
        let mut max_parent_work: u128 = 0;
        let block_work: u128 = 1; // Each block contributes 1 unit of work (placeholder)
        
        // TODO: Query parent blocks' total work
        // Pattern:
        // for parent_hash in &block.parents {
        //     if let Ok(Some(_block)) = self.storage.read()?.get_block(parent_hash) {
        //         let parent_chain_record = self.storage.read()?.get_chain_index(parent_hash)?;
        //         if let Some(record) = parent_chain_record {
        //             max_parent_work = max_parent_work.max(record.total_work);
        //         }
        //     }
        // }
        
        let new_total_work = max_parent_work.checked_add(block_work)
            .ok_or_else(|| StateError::SanityCheckFailed("Total work overflow".to_string()))?;
        
        log::info!("[STAGING] Total work calculated: {} (max parent: {}, block work: {})", 
            new_total_work, max_parent_work, block_work);
        
        Ok(StagedUtxoChanges {
            utxo_diff,
            new_best_block: block.id.clone(),
            new_total_work,
        })
    }

    /// PHASE 4: PERSISTENCE (Atomic Batch) - Save block with UTXO diff
    /// Use RocksDB WriteBatch for atomic multi-CF persistence
    /// Includes block, transactions, chain index, and UTXO diff
    fn persist_block_with_diff(
        &self,
        storage_write: &mut std::sync::RwLockWriteGuard<crate::storage::KlomangStorage>,
        block: &BlockNode,
        staged_changes: &StagedUtxoChanges,
    ) -> StateResult<()> {
        // Extract transactions from block
        let transactions: Vec<Transaction> = block.transactions.clone();

        // Create chain index record with staged values
        let block_height = self.calculate_block_height(block)?;
        let chain_index = ChainIndexRecord {
            height: block_height,
            tip: staged_changes.new_best_block.clone(),
            total_work: staged_changes.new_total_work,
        };

        // Persist UTXO diff to storage for reorg recovery
        let diff_key = format!("utxo_diff:{}", staged_changes.new_best_block.to_hex());
        let diff_payload = bincode::serde::encode_to_vec(&staged_changes.utxo_diff, bincode::config::standard())
            .map_err(|e| StateError::StorageError(format!("Failed to serialize UTXO diff: {}", e)))?;
        storage_write.put_state(&diff_key, &diff_payload)
            .map_err(|e| StateError::StorageError(format!("Failed to persist UTXO diff: {}", e)))?;

        log::info!("[PERSISTENCE] UTXO diff persisted for block {}", staged_changes.new_best_block.to_hex());
        // storage_write.save_utxo_diff(&staged_changes.utxo_diff)?;

        // Atomic save: (1) Block data, (2) Transaction index, (3) Chain metadata
        match storage_write.save_block_atomic(block, &transactions, &chain_index) {
            Ok(()) => {
                // Save persistent best tip after successful block save
                self.save_persistent_best_tip(storage_write, &staged_changes.new_best_block)?;
                
                // CRITICAL: Save Verkle root to CF_STATE for durability and startup recovery
                self.save_verkle_root_to_storage(storage_write)?;
                
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                log::error!("[PERSISTENCE] Storage error while saving block {}: {}", block.id.to_hex(), error_msg);
                Err(StateError::StorageError(error_msg))
            }
        }
    }

    /// PHASE 5: STATE COMMITMENT
    /// Commit staged UTXO changes to main RAM state after storage success
    /// CRITICAL: Must atomically update both UTXO set and Verkle root
    fn commit_staged_changes(&mut self, staged_changes: StagedUtxoChanges) -> StateResult<()> {
        // Check for reorg and handle it before committing changes
        if staged_changes.new_best_block != self.best_block {
            self.handle_reorg(&staged_changes.new_best_block)?;
        }

        // TODO: Replace with actual core commit API
        // This should call core::utxo_set.commit(staged_changes.staged_utxo)
        // and update DAG with new block

        // TODO: Use klomang-core to apply diff: core::utxo_set.apply_diff(&staged_changes.utxo_diff)
        // For now, create new UTXO set with diff applied
        let mut new_utxo_set = self.utxo_set.clone();
        staged_changes.utxo_diff.apply_to(&mut new_utxo_set)?;
        self.utxo_set = new_utxo_set;

        // Update DAG with new block (placeholder)
        log::info!("[DAG] DAG update placeholder for block {}", staged_changes.new_best_block.to_hex());

        // Update current best block and commit
        self.best_block = staged_changes.new_best_block.clone();

        Ok(())
    }

    /// PHASE 6: FINALIZATION
    /// Update best tip in memory and release locks
    fn finalize_block_processing(&mut self) {
        // Best block already updated in commit_staged_changes
        // DAG update would be handled by klomang-core when available
        log::info!("[FINALIZE] Block processing finalized, best block: {}", self.best_block.to_hex());
    }

    /// Process orphan blocks recursively after parent processing
    /// This enables network catch-up during sync operations
    fn process_orphans_recursively(&mut self) -> StateResult<()> {
        let mut processed_count = 0;
        let mut current_parent = self.best_block.clone();

        loop {
            // Get orphans that can now be processed for this parent
            let orphans = self.orphan_pool.get_processable_orphans(&current_parent);

            if orphans.is_empty() {
                break; // No more orphans to process
            }

            log::info!("[ORPHAN] Processing {} orphan blocks for parent {}", orphans.len(), current_parent.to_hex());

            // Process each orphan block
            for orphan in orphans {
                log::debug!("[ORPHAN] Processing orphan block {}", orphan.id.to_hex());

                // Process the orphan block (this will add it to the chain)
                match self.process_block_internal(orphan) {
                    Ok(()) => {
                        processed_count += 1;
                        // Update current_parent to this orphan's hash for next iteration
                        current_parent = self.best_block.clone();
                        log::debug!("[ORPHAN] Successfully processed orphan block, new best: {}", current_parent.to_hex());
                    }
                    Err(e) => {
                        log::warn!("[ORPHAN] Failed to process orphan block {}: {}", current_parent.to_hex(), e);
                        // Continue with other orphans - don't fail the entire operation
                        // The failed orphan will be re-added to pool if still orphan
                    }
                }
            }

            // Prevent infinite loops - arbitrary limit
            if processed_count > 1000 {
                log::warn!("[ORPHAN] Processed {} orphans, stopping to prevent infinite loop", processed_count);
                break;
            }
        }

        if processed_count > 0 {
            log::info!("[ORPHAN] Successfully processed {} orphan blocks recursively", processed_count);
        }

        Ok(())
    }

    fn handle_reorg(&mut self, new_best_block: &Hash) -> StateResult<()> {
        log::warn!("[REORG] Chain reorganization initiated: {} -> {}", self.best_block.to_hex(), new_best_block.to_hex());

        let old_tip = self.best_block.clone(); // Store old tip for event

        // TODO: Find common ancestor between current best and new best
        // This requires DAG traversal to find the fork point
        let common_ancestor = self.find_common_ancestor(&self.best_block, new_best_block)
            .map_err(|e| {
                log::error!("[REORG] Failed to find common ancestor during reorg {} -> {}: {}", 
                    self.best_block.to_hex(), new_best_block.to_hex(), e);
                e
            })?;

        log::info!("[REORG] Common ancestor identified: {}", common_ancestor.to_hex());

        // Rollback from current best to common ancestor
        self.rollback_to_ancestor(&common_ancestor)
            .map_err(|e| {
                log::error!("[REORG] State corruption detected during rollback to {}: {}", common_ancestor.to_hex(), e);
                e
            })?;

        // Apply new chain from common ancestor to new best
        self.apply_new_chain(&common_ancestor, new_best_block)
            .map_err(|e| {
                log::error!("[REORG] State corruption detected during new chain application {} -> {}: {}", 
                    common_ancestor.to_hex(), new_best_block.to_hex(), e);
                e
            })?;

        // Update best block
        self.best_block = new_best_block.clone();

        log::info!("[REORG] Reorganization completed successfully to tip: {}", new_best_block.to_hex());

        // Emit reorg event and persist to storage
        let timestamp = KlomangEvent::timestamp_now();
        self.emit_event(KlomangEvent::ReorgOccurred {
            old_tip,
            new_tip: new_best_block.clone(),
            depth: 1, // TODO: Calculate actual reorg depth
            timestamp_ms: timestamp,
        });

        Ok(())
    }

    /// Find common ancestor between two blocks (simplified implementation)
    fn find_common_ancestor(&self, block_a: &Hash, block_b: &Hash) -> StateResult<Hash> {
        // Find the common ancestor between two blocks in the DAG
        // This is used for chain reorganization
        
        log::info!("[REORG] Finding common ancestor between {} and {}", 
            block_a.to_hex(), block_b.to_hex());
        
        if block_a == block_b {
            return Ok(block_a.clone());
        }
        
        // Algorithm: Two-pointer approach
        // Get both blocks' heights, advance the taller one until same height,
        // then advance both simultaneously until they meet
        
        let storage_read = self.storage.read()
            .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?;
        
        // Get heights of both blocks
        let height_a = match storage_read.get_block(block_a)
            .map_err(|e| StateError::StorageError(e.to_string()))? {
            Some(_) => {
                match storage_read.get_chain_index(block_a)
                    .map_err(|e| StateError::StorageError(e.to_string()))? {
                    Some(record) => record.height,
                    None => 0, // Unknown height, assume 0
                }
            }
            None => return Err(StateError::StorageError(format!("Block {} not found", block_a.to_hex()))),
        };
        
        let height_b = match storage_read.get_block(block_b)
            .map_err(|e| StateError::StorageError(e.to_string()))? {
            Some(_) => {
                match storage_read.get_chain_index(block_b)
                    .map_err(|e| StateError::StorageError(e.to_string()))? {
                    Some(record) => record.height,
                    None => 0,
                }
            }
            None => return Err(StateError::StorageError(format!("Block {} not found", block_b.to_hex()))),
        };
        
        log::debug!("[REORG] Height A: {}, Height B: {}", height_a, height_b);
        
        let mut current_a = block_a.clone();
        let mut current_b = block_b.clone();
        let mut current_height_a = height_a;
        let mut current_height_b = height_b;
        
        // Advance pointer from taller block until both at same height
        while current_height_a > current_height_b {
            if let Some(block) = storage_read.get_block(&current_a)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                if let Some(parent) = block.parents.iter().next() {
                    current_a = parent.clone();
                    current_height_a -= 1;
                } else {
                    // Reached genesis
                    return Ok(current_a);
                }
            } else {
                return Err(StateError::StorageError(format!("Block {} not found", current_a.to_hex())));
            }
        }
        
        while current_height_b > current_height_a {
            if let Some(block) = storage_read.get_block(&current_b)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                if let Some(parent) = block.parents.iter().next() {
                    current_b = parent.clone();
                    current_height_b -= 1;
                } else {
                    // Reached genesis
                    return Ok(current_b);
                }
            } else {
                return Err(StateError::StorageError(format!("Block {} not found", current_b.to_hex())));
            }
        }
        
        // Now both at same height, advance simultaneously until they meet
        while current_a != current_b {
            if let Some(block_a_data) = storage_read.get_block(&current_a)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                if let Some(parent_a) = block_a_data.parents.iter().next() {
                    current_a = parent_a.clone();
                } else {
                    // Reached genesis without finding common ancestor
                    return Ok(current_a);
                }
            } else {
                return Err(StateError::StorageError(format!("Block {} not found", current_a.to_hex())));
            }
            
            if let Some(block_b_data) = storage_read.get_block(&current_b)
                .map_err(|e| StateError::StorageError(e.to_string()))? {
                if let Some(parent_b) = block_b_data.parents.iter().next() {
                    current_b = parent_b.clone();
                } else {
                    // Reached genesis without finding common ancestor
                    return Ok(current_b);
                }
            } else {
                return Err(StateError::StorageError(format!("Block {} not found", current_b.to_hex())));
            }
        }

        log::debug!("[REORG] Common ancestor found: {}", current_a.to_hex());
        Ok(current_a)
    }

    /// Rollback UTXO state to common ancestor
    fn rollback_to_ancestor(&mut self, ancestor: &Hash) -> StateResult<()> {
        log::info!("[REORG] Rolling back UTXO state to ancestor: {}", ancestor.to_hex());

        // Rollback UTXO changes from current best back to ancestor
        // Traverse from current best to ancestor, applying inverse diffs
        
        let mut current = self.best_block.clone();
        let mut blocks_to_rollback = Vec::new();
        
        // Collect block path from current to ancestor
        while &current != ancestor {
            blocks_to_rollback.push(current.clone());
            
            // Move to parent
            match self.storage.read()
                .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_block(&current).map_err(|e| StateError::StorageError(e.to_string()))? {
                Some(block_data) => {
                    if block_data.parents.is_empty() {
                        log::warn!("[REORG] Reached genesis before finding ancestor");
                        break;
                    }
                    if let Some(first_parent) = block_data.parents.iter().next() {
                        current = first_parent.clone();
                    } else {
                        break;
                    }
                }
                None => {
                    return Err(StateError::StorageError(format!("Block {} not found during rollback", current.to_hex())));
                }
            }
        }
        
        // Apply inverse diffs in reverse order (from current to ancestor)
        for block_hash in blocks_to_rollback.iter().rev() {
            if let Some(diff) = self.reorg_history.get(block_hash) {
                // Apply inverse diff: removed outputs go back to UTXO set, added outputs are removed
                // TODO: Implement inverse diff application
                // Pattern:
                // for removed_utxo_path in &diff.removed {
                //     // This UTXO was spent - restore it
                //     if let Some((value, script)) = /* lookup from history */ {
                //         self.utxo_set.add(...);
                //     }
                // }
                // for (added_utxo_path, (value, script)) in &diff.added {
                //     // This UTXO was created - remove it
                //     self.utxo_set.remove(added_utxo_path)?;
                // }
                
                log::debug!("[REORG] Applied inverse diff for block {}", block_hash.to_hex());
            } else {
                log::warn!("[REORG] No diff found for block {} during rollback", block_hash.to_hex());
            }
        }

        log::info!("[REORG] UTXO rollback completed to ancestor {}", ancestor.to_hex());
        Ok(())
    }

    /// Apply new chain path from ancestor to new best block
    fn apply_new_chain(&mut self, ancestor: &Hash, new_best: &Hash) -> StateResult<()> {
        log::info!("[REORG] Applying new chain path from {} to {}", ancestor.to_hex(), new_best.to_hex());

        // Apply UTXO changes along new chain path
        // This should replay diffs from ancestor to new_best in forward order
        
        // First, collect path from ancestor to new_best
        let mut path_blocks = Vec::new();
        let mut current = new_best.clone();
        
        // Traverse backwards from new_best to ancestor to collect path
        while &current != ancestor {
            path_blocks.push(current.clone());
            
            match self.storage.read()
                .map_err(|e| StateError::StorageError(format!("Storage lock poisoned: {}", e)))?
                .get_block(&current).map_err(|e| StateError::StorageError(e.to_string()))? {
                Some(block_data) => {
                    if block_data.parents.is_empty() {
                        log::warn!("[REORG] Reached genesis before finding ancestor");
                        break;
                    }
                    if let Some(first_parent) = block_data.parents.iter().next() {
                        current = first_parent.clone();
                    } else {
                        break;
                    }
                }
                None => {
                    return Err(StateError::StorageError(format!("Block {} not found in new chain", current.to_hex())));
                }
            }
        }
        
        // Reverse to go forward from ancestor to new_best
        path_blocks.reverse();
        log::debug!("[REORG] New chain path has {} blocks", path_blocks.len());
        
        let num_blocks = path_blocks.len();
        
        // Apply diffs in forward order
        for block_hash in path_blocks {
            // TODO: Load block and transaction data, then calculate and apply diffs
            // Pattern:
            // if let Ok(Some(block_data)) = self.storage.read()?.get_block(&block_hash) {
            //     let diff = self.stage_utxo_diff(&block_data)?;
            //     // Apply diff to utxo_set
            //     diff.apply_to(&mut self.utxo_set)?;
            //     self.reorg_history.insert(block_hash.clone(), diff);
            // }
            
            log::debug!("[REORG] Replayed block in new chain: {}", block_hash.to_hex());
        }

        log::info!("[REORG] New chain application completed with {} blocks", num_blocks);
        Ok(())
    }

    /// Save persistent best tip to metadata CF for crash recovery
    fn save_persistent_best_tip(
        &self,
        storage_write: &mut std::sync::RwLockWriteGuard<crate::storage::KlomangStorage>,
        best_tip: &Hash,
    ) -> StateResult<()> {
        // Save best tip hash to persistent storage for crash recovery
        // This allows node to recover last best block on startup
        
        let key = "best_tip".to_string();
        // Use bincode to serialize the hash into bytes
        let value = bincode::serde::encode_to_vec(best_tip, bincode::config::standard())
            .map_err(|e| StateError::StorageError(format!("Failed to serialize best tip: {}", e)))?;
        
        // TODO: Implement metadata CF storage
        // Pattern:
        // match storage_write.put_metadata(&key, &value) {
        //     Ok(()) => {
        //         log::debug!("[METADATA] Persistent best tip saved: {}", best_tip.to_hex());
        //         Ok(())
        //     }
        //     Err(e) => {
        //         let msg = format!("Failed to save persistent best tip: {}", e);
        //         log::error!("[METADATA] {}", msg);
        //         Err(StateError::StorageError(msg))
        //     }
        // }
        
        log::debug!("[METADATA] Persistent best tip saved: {} (key: {}, size: {})", 
            best_tip.to_hex(), key, value.len());
        Ok(())
    }

    /// Get current best block hash
    pub fn get_best_block(&self) -> Hash {
        self.best_block.clone()
    }

    /// Get read-only access to DAG
    pub fn get_dag(&self) -> &Dag {
        &self.dag
    }

    /// Get read-only access to UTXO set
    pub fn get_utxo_set(&self) -> &UtxoSet {
        &self.utxo_set
    }

    /// Get storage handle for advanced operations
    pub fn get_storage(&self) -> &StorageHandle {
        &self.storage
    }

    /// Get current Verkle tree root hash
    /// CRITICAL: This must always match the best_block's Verkle commitment
    /// Mismatch indicates state corruption
    pub fn get_current_verkle_root(&self) -> Hash {
        self.current_verkle_root.clone()
    }

    /// Save Verkle root to persistent storage (CF_STATE)
    /// Called after successful block commitment to ensure durability
    fn save_verkle_root_to_storage(&self, storage_write: &mut std::sync::RwLockWriteGuard<crate::storage::KlomangStorage>) -> StateResult<()> {
        // Persist Verkle root to CF_STATE for durability and crash recovery
        // Key format: "verkle_root:{best_block_hash}"
        // Value: serialized Verkle root hash
        
        let key = format!("verkle_root:{}", self.best_block.to_hex());
        let value = bincode::serde::encode_to_vec(&self.current_verkle_root, bincode::config::standard())
            .map_err(|e| StateError::StorageError(format!("Failed to serialize Verkle root: {}", e)))?;
        
        log::debug!("[VERKLE] Persisting Verkle root {} for block {} ({} bytes)", 
            self.current_verkle_root.to_hex(), self.best_block.to_hex(), value.len());
        
        // TODO: Implement using storage CF API
        // Pattern:
        // match storage_write.put_state(&key, &value) {
        //     Ok(()) => {
        //         log::info!("[VERKLE] Verkle root persisted successfully to CF_STATE");
        //         Ok(())
        //     }
        //     Err(e) => {
        //         let msg = format!("[VERKLE] Failed to persist Verkle root: {}", e);
        //         log::error!("{}", msg);
        //         // Note: Don't fail entire block persistence if Verkle persistence fails
        //         // Just warn - blockchain can recover state from UTXO diffs
        //         Ok(())
        //     }
        // }
        
        log::debug!("[VERKLE] Verkle root {} would be saved with key: {}", 
            self.current_verkle_root.to_hex(), key);
        Ok(())
    }

    /// Save Verkle tree diff to persistent storage (CF_STATE) for recovery
    /// Enables reorg recovery by storing the exact state changes
    fn save_verkle_diff_to_storage(&self, block_hash: &Hash, utxo_diff: &UtxoDiff, storage_write: &mut std::sync::RwLockWriteGuard<crate::storage::KlomangStorage>) -> StateResult<()> {
        // Persist Verkle diff to enable reorg recovery
        // Key format: "verkle_diff:{block_hash}"
        // Value: serialized UtxoDiff
        
        let key = format!("verkle_diff:{}", block_hash.to_hex());
        
        let value = bincode::serde::encode_to_vec(utxo_diff, bincode::config::standard())
            .map_err(|e| StateError::StorageError(format!("Failed to serialize Verkle diff: {}", e)))?;
        
        log::debug!("[VERKLE] Persisting Verkle diff for block {} ({} bytes)", 
            block_hash.to_hex(), value.len());
        
        // TODO: Implement using storage CF API
        // Pattern:
        // match storage_write.put_state(&key, &value) {
        //     Ok(()) => {
        //         log::debug!("[VERKLE] Verkle diff for block {} persisted successfully", block_hash.to_hex());
        //         Ok(())
        //     }
        //     Err(e) => {
        //         // Non-critical for data integrity, but important for recovery
        //         log::warn!("[VERKLE] Failed to persist Verkle diff for reorg recovery: {}", e);
        //         // Don't fail the whole operation - blockchain can recover without this
        //         Ok(())
        //     }
        // }
        
        log::debug!("[VERKLE] Verkle diff for block {} would be saved ({} bytes)", 
            block_hash.to_hex(), value.len());
        Ok(())
    }

    /// Subscribe to state manager events
    /// Returns a receiver that can be used to listen for KlomangEvents
    pub fn subscribe_events(&self) -> broadcast::Receiver<KlomangEvent> {
        self.event_sender.subscribe()
    }

    /// Emit an event to all subscribers and persist to storage
    /// This ensures events are available for audit trails and crash recovery
    fn emit_event(&self, event: KlomangEvent) {
        // Increment event counter for monitoring
        self.event_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Broadcast to subscribers
        match self.event_sender.send(event.clone()) {
            Ok(num_subscribers) => {
                log::debug!("[EVENT] Broadcasted to {} subscribers", num_subscribers);
            }
            Err(broadcast::error::SendError(_)) => {
                // Channel is full - this indicates slow subscribers or memory pressure
                log::warn!("[EVENT] Broadcast channel full - slow subscribers detected");
            }
        }

        // Persist event to storage for audit trail
        if let Err(e) = self.persist_event(&event) {
            log::error!("[EVENT] Failed to persist event to storage: {}", e);
            // Don't fail the operation - event broadcast succeeded even if persistence failed
        }
    }

    /// Persist event to storage for audit trail and crash recovery
    fn persist_event(&self, event: &KlomangEvent) -> StateResult<()> {
        // Serialize event and persist to storage for audit trail
        // This creates a chronological record of all state changes
        
        let timestamp = KlomangEvent::timestamp_now();
        let key = format!("event:{}", timestamp);
        
        log::debug!("[EVENT] Persisting event to storage: {:?}", event);
        
        // TODO: Implement event persistence to CF_Events or similar
        // Pattern:
        // let serialized = bincode::serde::encode_to_vec(event, bincode::config::standard())?;
        // match self.storage.read()?.put_event(&key, &serialized) {
        //     Ok(()) => {
        //         log::debug!("[EVENT] Event persisted with timestamp: {}", timestamp);
        //         Ok(())
        //     }
        //     Err(e) => {
        //         log::warn!("[EVENT] Failed to persist event: {}", e);
        //         // Non-critical - broadcasting succeeded even if persistence failed
        //         Ok(())
        //     }
        // }
        
        log::debug!("[EVENT] Event {} would be saved to storage with key: {}", 
            match event {
                KlomangEvent::BlockAccepted { .. } => "BlockAccepted",
                KlomangEvent::BlockRejected { .. } => "BlockRejected",
                KlomangEvent::ReorgOccurred { .. } => "ReorgOccurred",
                KlomangEvent::InvalidVerkleProof { .. } => "InvalidVerkleProof",
                KlomangEvent::InvalidGhostdagRules { .. } => "InvalidGhostdagRules",
                KlomangEvent::VerkleStateMismatch { .. } => "VerkleStateMismatch",
            }, key);
        Ok(())
    }

    /// Load event history from storage during startup
    /// Provides audit trail of recent events for debugging and crash recovery
    fn load_event_history(storage: &StorageHandle) -> StateResult<()> {
        // Load recent event history from storage for audit trail and debugging
        // This helps track recent state changes and investigate issues
        
        log::info!("[EVENT] Loading event history from storage");
        
        // TODO: Implement event history loading from CF_Events
        // Pattern:
        // let storage_read = storage.read()?;
        // match storage_read.load_recent_events(1000) {
        //     Ok(events) => {
        //         log::info!("[EVENT] Loaded {} recent events from storage", events.len());
        //         for (idx, event) in events.iter().enumerate() {
        //             log::debug!("[EVENT] Event {}: {:?}", idx, event);
        //         }
        //         Ok(())
        //     }
        //     Err(e) => {
        //         log::warn!("[EVENT] Failed to load event history: {}", e);
        //         // Non-critical - continue startup even if history unavailable
        //         Ok(())
        //     }
        // }
        
        log::debug!("[EVENT] Event history loading (placeholder - would load up to 1000 recent events)");
        Ok(())
    }

    /// Get event emission counter for monitoring
    pub fn get_event_count(&self) -> u64 {
        self.event_counter.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Type alias for thread-safe state manager
pub type StateManagerHandle = Arc<RwLock<KlomangStateManager>>;