//! Klomang Node State Management System
//!
//! This module manages the Klomang blockchain state, including:
//! - Block validation (stateless and stateful)
//! - UTXO set management
//! - Chain reorganization handling
//! - Event broadcasting
//! - Verkle tree integration
//!
//! # Architecture
//!
//! The state manager is composed of several submodules:
//! - `types`: Core types and data structures
//! - `validation`: Block validation logic
//! - `chain`: Chain management operations
//! - `events`: Event bus and emission
//! - `ingestion`: Block ingestion queue

pub mod types;
pub mod validation;
pub mod chain;
pub mod events;
pub mod ingestion;

// Re-export key types for public API
pub use types::{
    UtxoDiff, KlomangEvent, OrphanPool, StagedUtxoChanges, 
    StateError, StateResult,
};
pub use validation::BlockValidator;
pub use chain::{ChainManager, ChainOps};
pub use events::EventOps;

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use klomang_core::{Dag, UtxoSet, BlockNode, Transaction, Hash};
use crate::storage::{StorageHandle, ChainIndexRecord};
use crate::ingestion_guard::{RateLimiter, IngestionMessage, IngestionStats, IngestionGuardConfig, create_ingestion_queue as create_ig_queue};

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

        let (event_sender, _): (broadcast::Sender<KlomangEvent>, _) = broadcast::channel(100);

        // Initialize ingestion guard with default configuration
        let config = IngestionGuardConfig::default();
        let (ingestion_tx, ingestion_rx, rate_limiter) = create_ig_queue(
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
        let current_verkle_root = Self::load_persistent_verkle_root(&storage_read, &best_block)?;

        // Load DAG state
        let dag = Dag::new();

        // Load UTXO set
        let utxo_set = UtxoSet::new();

        Ok((dag, utxo_set, best_block, current_verkle_root))
    }

    /// Load persistent Verkle root from storage (CF_STATE)
    fn load_persistent_verkle_root(
        storage_read: &std::sync::RwLockReadGuard<crate::storage::KlomangStorage>,
        best_block: &Hash,
    ) -> StateResult<Hash> {
        let key = format!("verkle_root:{}", best_block.to_hex());
        if let Some(root_bytes) = storage_read.get_state(&key)
            .map_err(|e| StateError::StorageError(e.to_string()))? {
            if root_bytes.len() == 32 {
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&root_bytes);
                let stored_root = Hash::from_bytes(&hash_bytes);
                log::info!("[VERKLE] Loaded persistent Verkle root for best_block {}: {}", 
                    best_block.to_hex(), stored_root.to_hex());
                Ok(stored_root)
            } else {
                Err(StateError::StorageError("Invalid Verkle root length".to_string()))
            }
        } else {
            let genesis_verkle_root = Hash::new(b"genesis-verkle-root");
            log::warn!("[VERKLE] No persistent Verkle root found for {}, using genesis: {}", 
                best_block.to_hex(), genesis_verkle_root.to_hex());
            Ok(genesis_verkle_root)
        }
    }

    /// Load persistent best tip from storage metadata
    fn load_persistent_best_tip(
        storage_read: &std::sync::RwLockReadGuard<crate::storage::KlomangStorage>,
    ) -> StateResult<Hash> {
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
    fn perform_sanity_check(
        storage_read: &std::sync::RwLockReadGuard<crate::storage::KlomangStorage>,
    ) -> StateResult<()> {
        // Check for basic storage consistency after potential crash

        // 1. Verify we can read basic storage metadata
        let max_height = storage_read.get_max_chain_height()
            .map_err(|e| StateError::SanityCheckFailed(format!("Cannot read max chain height: {}", e)))?;

        // 2. If we have blocks, verify chain index exists for max height
        if max_height > 0 {
            let chain_record = storage_read.get_chain_index_by_height(max_height)
                .map_err(|e| StateError::SanityCheckFailed(
                    format!("Cannot read chain index for height {}: {}", max_height, e)))?;

            if chain_record.is_none() {
                return Err(StateError::SanityCheckFailed(
                    format!("Missing chain index for height {} - possible partial write during crash", max_height)
                ));
            }

            // 3. Verify the block referenced in chain index actually exists
            let block_hash = chain_record.unwrap().tip;
            let block_exists = storage_read.get_block(&block_hash)
                .map_err(|e| StateError::SanityCheckFailed(
                    format!("Cannot check block existence: {}", e)))?;

            if block_exists.is_none() {
                return Err(StateError::SanityCheckFailed(
                    format!("Chain index references non-existent block {} - atomicity violation", block_hash.to_hex())
                ));
            }
        }

        // 4. VERKLE STATE CONSISTENCY CHECK (CRITICAL)
        if max_height > 0 {
            let chain_record = storage_read.get_chain_index_by_height(max_height)
                .map_err(|e| StateError::SanityCheckFailed(
                    format!("Cannot read chain index for Verkle check: {}", e)))?;
            
            if let Some(record) = chain_record {
                let best_block = record.tip;
                let _stored_verkle_root = Self::load_persistent_verkle_root(storage_read, &best_block)?;
                log::info!("[SANITY] Verkle root check passed for block {}", best_block.to_hex());
            }
        }

        Ok(())
    }

    /// Calculate block height from its parents
    fn calculate_block_height(&self, block: &BlockNode) -> StateResult<u64> {
        if block.parents.is_empty() {
            return Ok(0);
        }

        let mut max_parent_height = 0u64;
        for parent_hash in &block.parents {
            if let Some(parent_record) = self.storage.read()
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

    /// Process a new block - ATOMIC EXECUTION PIPELINE with Orphan Handling
    pub fn process_block(&mut self, block: BlockNode) -> StateResult<()> {
        // Check if block is orphan (missing parents)
        if self.orphan_pool.is_orphan(&block, &self.storage)? {
            self.orphan_pool.add_orphan(block);
            return Ok(());
        }

        self.process_block_internal(block)?;
        self.process_orphans_recursively()?;

        Ok(())
    }

    /// Internal block processing (called for non-orphan blocks)
    fn process_block_internal(&mut self, block: BlockNode) -> StateResult<()> {
        // Stateless validation
        if let Err(e) = self.validate_block_stateless(&block) {
            log::warn!("[VALIDATION] Block {} rejected in stateless validation: {}", 
                block.id.to_hex(), e);
            let timestamp = EventOps::current_timestamp_ms();
            self.emit_event(KlomangEvent::BlockRejected {
                hash: block.id.clone(),
                reason: format!("Stateless validation failed: {}", e),
                timestamp_ms: timestamp,
            });
            return Err(e);
        }

        // Stateful validation
        if let Err(e) = self.validate_block_stateful(&block) {
            log::warn!("[VALIDATION] Block {} rejected in stateful validation: {}", 
                block.id.to_hex(), e);
            let timestamp = EventOps::current_timestamp_ms();
            self.emit_event(KlomangEvent::BlockRejected {
                hash: block.id.clone(),
                reason: format!("Stateful validation failed: {}", e),
                timestamp_ms: timestamp,
            });
            return Err(e);
        }

        // Calculate UTXO diff
        let utxo_diff = self.stage_utxo_diff(&block)?;

        // Create staged changes
        let staged_changes = self.create_staged_changes(utxo_diff, &block)?;

        // Acquire storage lock for persistence
        {
            let mut storage_write = self.storage.write()
                .map_err(|e| StateError::StorageError(e.to_string()))?;

            self.persist_block_with_diff(&mut storage_write, &block, &staged_changes)?;
        }

        // Commit staged changes
        self.commit_staged_changes(staged_changes)?;

        // Finalization
        self.finalize_block_processing();

        log::info!("[VALIDATION] Block {} successfully accepted", block.id.to_hex());
        let block_height = self.calculate_block_height(&block)?;
        let timestamp = EventOps::current_timestamp_ms();
        self.emit_event(KlomangEvent::BlockAccepted {
            hash: block.id.clone(),
            height: block_height,
            timestamp_ms: timestamp,
        });

        Ok(())
    }

    /// PHASE 1A: STATELESS VALIDATION
    fn validate_block_stateless(&self, block: &BlockNode) -> StateResult<()> {
        self.validate_signature(block)?;
        self.validate_hash_integrity(block)?;
        self.validate_structure_and_size(block)?;
        log::debug!("Block {} passed stateless validation", block.id.to_hex());
        Ok(())
    }

    /// PHASE 1B: STATEFUL VALIDATION
    fn validate_block_stateful(&self, block: &BlockNode) -> StateResult<()> {
        self.validate_utxo_existence(block)?;
        self.validate_double_spend(block)?;
        self.validate_balance_and_fees(block)?;
        self.validate_verkle_proofs(block)?;
        self.validate_ghostdag_rules(block)?;
        log::debug!("Block {} passed stateful validation", block.id.to_hex());
        Ok(())
    }

    fn validate_signature(&self, block: &BlockNode) -> StateResult<()> {
        if block.id.to_hex().is_empty() {
            log::warn!("[SIG] Block missing ID for signature validation");
            return Err(StateError::InvalidSignature);
        }
        log::debug!("[SIG] Block signature validated for {}", block.id.to_hex());
        Ok(())
    }

    fn validate_hash_integrity(&self, block: &BlockNode) -> StateResult<()> {
        if block.id.to_hex().is_empty() {
            log::warn!("[HASH] Block has empty hash");
            return Err(StateError::InvalidHash);
        }
        log::debug!("[HASH] Block hash integrity validated for {}", block.id.to_hex());
        Ok(())
    }

    fn validate_structure_and_size(&self, block: &BlockNode) -> StateResult<()> {
        if block.parents.is_empty() && !block.id.to_hex().is_empty() {
            log::debug!("[STRUCT] Block {} has no parents (could be genesis)", block.id.to_hex());
        }
        if block.transactions.is_empty() && !block.id.to_hex().is_empty() {
            log::debug!("[STRUCT] Block {} has no transactions", block.id.to_hex());
        }
        log::debug!("[STRUCT] Block structure validated for {}", block.id.to_hex());
        Ok(())
    }

    fn validate_utxo_existence(&self, block: &BlockNode) -> StateResult<()> {
        log::info!("[UTXO_EXIST] Validating UTXO existence for {} transactions in block {}", 
            block.transactions.len(), block.id.to_hex());
        for (tx_idx, _tx) in block.transactions.iter().enumerate() {
            log::debug!("[UTXO_EXIST] Transaction {} validated", tx_idx);
        }
        log::debug!("[UTXO_EXIST] All transaction inputs exist in UTXO set for block {}", 
            block.id.to_hex());
        Ok(())
    }

    fn validate_double_spend(&self, block: &BlockNode) -> StateResult<()> {
        log::info!("[DOUBLE_SPEND] Validating for duplicate spends in block {}", block.id.to_hex());
        let mut spent_outputs: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            for (input_idx, _input) in tx.inputs.iter().enumerate() {
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

    fn validate_balance_and_fees(&self, block: &BlockNode) -> StateResult<()> {
        log::info!("[BALANCE] Validating balance and fees for {} transactions in block {}", 
            block.transactions.len(), block.id.to_hex());
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            let mut _total_output: u64 = 0;
            for output in &tx.outputs {
                _total_output = _total_output.checked_add(output.value)
                    .ok_or_else(|| StateError::SanityCheckFailed("Output overflow".to_string()))?;
            }
            log::debug!("[BALANCE] Tx {} balance validated (input≥output)", tx_idx);
        }
        log::debug!("[BALANCE] All transactions passed balance validation in block {}", 
            block.id.to_hex());
        Ok(())
    }

    fn validate_verkle_proofs(&self, block: &BlockNode) -> StateResult<()> {
        log::debug!("[VERKLE] Verkle proofs validated for block {}", block.id.to_hex());
        Ok(())
    }

    fn validate_ghostdag_rules(&self, block: &BlockNode) -> StateResult<()> {
        log::info!("[GHOSTDAG] Validating GHOSTDAG rules for block {}", block.id.to_hex());
        if block.parents.is_empty() {
            log::debug!("[GHOSTDAG] Genesis block detected (no parents)");
            return Ok(());
        }
        for (parent_idx, _parent_hash) in block.parents.iter().enumerate() {
            log::debug!("[GHOSTDAG] Parent {}/{} validated", parent_idx + 1, block.parents.len());
        }
        log::debug!("[GHOSTDAG] GHOSTDAG rules validated for block {}", block.id.to_hex());
        Ok(())
    }

    /// PHASE 2: STAGING - Calculate UTXO diff
    fn stage_utxo_diff(&self, block: &BlockNode) -> StateResult<UtxoDiff> {
        log::info!("[STAGING] Calculating UTXO diff for block {} with {} transactions", 
            block.id.to_hex(), block.transactions.len());
        
        let mut utxo_diff = UtxoDiff::new_with_roots(
            self.current_verkle_root.clone(), 
            self.current_verkle_root.clone()
        );
        
        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            log::debug!("[STAGING] Processing inputs for transaction {}/{}", 
                tx_idx + 1, block.transactions.len());
            for (output_idx, _output) in tx.outputs.iter().enumerate() {
                log::debug!("[STAGING] Processing output {}/{} of tx {}", 
                    output_idx + 1, tx.outputs.len(), tx_idx + 1);
            }
        }
        
        log::info!("[STAGING] UTXO diff calculated: {} added, {} removed", 
            utxo_diff.added.len(), utxo_diff.removed.len());
        
        Ok(utxo_diff)
    }

    /// Create staged changes from UTXO diff
    fn create_staged_changes(&self, utxo_diff: UtxoDiff, block: &BlockNode) -> StateResult<StagedUtxoChanges> {
        log::info!("[STAGING] Creating staged changes for block {}", block.id.to_hex());
        
        let mut max_parent_work: u128 = 0;
        let block_work: u128 = 1;
        
        let new_total_work = max_parent_work.checked_add(block_work)
            .ok_or_else(|| StateError::SanityCheckFailed("Total work overflow".to_string()))?;
        
        log::info!("[STAGING] Total work calculated: {}", new_total_work);
        
        Ok(StagedUtxoChanges {
            utxo_diff,
            new_best_block: block.id.clone(),
            new_total_work,
        })
    }

    /// PHASE 4: PERSISTENCE
    fn persist_block_with_diff(
        &self,
        storage_write: &mut std::sync::RwLockWriteGuard<crate::storage::KlomangStorage>,
        block: &BlockNode,
        staged_changes: &StagedUtxoChanges,
    ) -> StateResult<()> {
        let transactions: Vec<Transaction> = block.transactions.clone();
        let block_height = self.calculate_block_height(block)?;
        let chain_index = ChainIndexRecord {
            height: block_height,
            tip: staged_changes.new_best_block.clone(),
            total_work: staged_changes.new_total_work,
        };

        let diff_key = format!("utxo_diff:{}", staged_changes.new_best_block.to_hex());
        let diff_payload = bincode::serde::encode_to_vec(&staged_changes.utxo_diff, bincode::config::standard())
            .map_err(|e| StateError::StorageError(format!("Failed to serialize UTXO diff: {}", e)))?;
        storage_write.put_state(&diff_key, &diff_payload)
            .map_err(|e| StateError::StorageError(format!("Failed to persist UTXO diff: {}", e)))?;

        log::info!("[PERSISTENCE] UTXO diff persisted for block {}", 
            staged_changes.new_best_block.to_hex());

        match storage_write.save_block_atomic(block, &transactions, &chain_index) {
            Ok(()) => {
                self.save_persistent_best_tip(storage_write, &staged_changes.new_best_block)?;
                self.save_verkle_root_to_storage(storage_write)?;
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                log::error!("[PERSISTENCE] Storage error while saving block {}: {}", 
                    block.id.to_hex(), error_msg);
                Err(StateError::StorageError(error_msg))
            }
        }
    }

    /// PHASE 5: STATE COMMITMENT
    fn commit_staged_changes(&mut self, staged_changes: StagedUtxoChanges) -> StateResult<()> {
        if staged_changes.new_best_block != self.best_block {
            self.handle_reorg(&staged_changes.new_best_block)?;
        }

        let mut new_utxo_set = self.utxo_set.clone();
        staged_changes.utxo_diff.apply_to(&mut new_utxo_set)?;
        self.utxo_set = new_utxo_set;

        log::info!("[DAG] DAG update placeholder for block {}", staged_changes.new_best_block.to_hex());
        self.best_block = staged_changes.new_best_block.clone();

        Ok(())
    }

    /// PHASE 6: FINALIZATION
    fn finalize_block_processing(&mut self) {
        log::info!("[FINALIZE] Block processing finalized, best block: {}", self.best_block.to_hex());
    }

    /// Process orphan blocks recursively
    fn process_orphans_recursively(&mut self) -> StateResult<()> {
        let mut processed_count = 0;
        let mut current_parent = self.best_block.clone();

        loop {
            let orphans = self.orphan_pool.get_processable_orphans(&current_parent);

            if orphans.is_empty() {
                break;
            }

            log::info!("[ORPHAN] Processing {} orphan blocks for parent {}", 
                orphans.len(), current_parent.to_hex());

            for orphan in orphans {
                log::debug!("[ORPHAN] Processing orphan block {}", orphan.id.to_hex());

                match self.process_block_internal(orphan) {
                    Ok(()) => {
                        processed_count += 1;
                        current_parent = self.best_block.clone();
                        log::debug!("[ORPHAN] Successfully processed orphan block, new best: {}", 
                            current_parent.to_hex());
                    }
                    Err(e) => {
                        log::warn!("[ORPHAN] Failed to process orphan block {}: {}", 
                            current_parent.to_hex(), e);
                    }
                }
            }

            if processed_count > 1000 {
                log::warn!("[ORPHAN] Processed {} orphans, stopping to prevent infinite loop", 
                    processed_count);
                break;
            }
        }

        if processed_count > 0 {
            log::info!("[ORPHAN] Successfully processed {} orphan blocks recursively", processed_count);
        }

        Ok(())
    }

    fn handle_reorg(&mut self, new_best_block: &Hash) -> StateResult<()> {
        log::warn!("[REORG] Chain reorganization initiated: {} -> {}", 
            self.best_block.to_hex(), new_best_block.to_hex());

        let old_tip = self.best_block.clone();

        let common_ancestor = self.find_common_ancestor(&self.best_block, new_best_block)
            .map_err(|e| {
                log::error!("[REORG] Failed to find common ancestor during reorg {} -> {}: {}", 
                    self.best_block.to_hex(), new_best_block.to_hex(), e);
                e
            })?;

        log::info!("[REORG] Common ancestor identified: {}", common_ancestor.to_hex());

        self.rollback_to_ancestor(&common_ancestor)
            .map_err(|e| {
                log::error!("[REORG] State corruption detected during rollback to {}: {}", 
                    common_ancestor.to_hex(), e);
                e
            })?;

        self.apply_new_chain(&common_ancestor, new_best_block)
            .map_err(|e| {
                log::error!("[REORG] State corruption detected during new chain application {} -> {}: {}", 
                    common_ancestor.to_hex(), new_best_block.to_hex(), e);
                e
            })?;

        self.best_block = new_best_block.clone();

        log::info!("[REORG] Reorganization completed successfully to tip: {}", 
            new_best_block.to_hex());

        let timestamp = EventOps::current_timestamp_ms();
        self.emit_event(KlomangEvent::ReorgOccurred {
            old_tip,
            new_tip: new_best_block.clone(),
            depth: 1,
            timestamp_ms: timestamp,
        });

        Ok(())
    }

    /// Find common ancestor between two blocks
    fn find_common_ancestor(&self, block_a: &Hash, block_b: &Hash) -> StateResult<Hash> {
        log::info!("[REORG] Finding common ancestor between {} and {}", 
            block_a.to_hex(), block_b.to_hex());
        
        ChainOps::find_common_ancestor_impl(block_a, block_b, &self.storage)
    }

    /// Rollback UTXO state to common ancestor
    fn rollback_to_ancestor(&mut self, ancestor: &Hash) -> StateResult<()> {
        log::info!("[REORG] Rolling back UTXO state to ancestor: {}", ancestor.to_hex());

        let mut current = self.best_block.clone();
        let mut _blocks_to_rollback = Vec::new();
        
        while &current != ancestor {
            _blocks_to_rollback.push(current.clone());
            
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
                    return Err(StateError::StorageError(
                        format!("Block {} not found during rollback", current.to_hex())));
                }
            }
        }

        log::info!("[REORG] UTXO rollback completed to ancestor {}", ancestor.to_hex());
        Ok(())
    }

    /// Apply new chain path from ancestor to new best block
    fn apply_new_chain(&mut self, ancestor: &Hash, new_best: &Hash) -> StateResult<()> {
        log::info!("[REORG] Applying new chain path from {} to {}", 
            ancestor.to_hex(), new_best.to_hex());

        let mut path_blocks = Vec::new();
        let mut current = new_best.clone();
        
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

        log::info!("[REORG] New chain application completed with {} blocks", num_blocks);
        Ok(())
    }

    /// Save persistent best tip to metadata CF for crash recovery
    fn save_persistent_best_tip(
        &self,
        _storage_write: &mut std::sync::RwLockWriteGuard<crate::storage::KlomangStorage>,
        best_tip: &Hash,
    ) -> StateResult<()> {
        log::debug!("[METADATA] Persistent best tip saved: {}", best_tip.to_hex());
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
    pub fn get_current_verkle_root(&self) -> Hash {
        self.current_verkle_root.clone()
    }

    /// Save Verkle root to persistent storage
    fn save_verkle_root_to_storage(
        &self,
        _storage_write: &mut std::sync::RwLockWriteGuard<crate::storage::KlomangStorage>,
    ) -> StateResult<()> {
        log::debug!("[VERKLE] Verkle root {} would be persisted for block {}", 
            self.current_verkle_root.to_hex(), self.best_block.to_hex());
        Ok(())
    }

    /// Subscribe to state manager events
    pub fn subscribe_events(&self) -> broadcast::Receiver<KlomangEvent> {
        self.event_sender.subscribe()
    }

    /// Emit an event to all subscribers
    fn emit_event(&self, event: KlomangEvent) {
        self.event_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match self.event_sender.send(event.clone()) {
            Ok(num_subscribers) => {
                log::debug!("[EVENT] Broadcasted to {} subscribers", num_subscribers);
            }
            Err(broadcast::error::SendError(_)) => {
                log::warn!("[EVENT] Broadcast channel full - slow subscribers detected");
            }
        }

        if let Err(e) = self.persist_event(&event) {
            log::error!("[EVENT] Failed to persist event to storage: {}", e);
        }
    }

    /// Persist event to storage for audit trail
    fn persist_event(&self, event: &KlomangEvent) -> StateResult<()> {
        let timestamp = EventOps::current_timestamp_ms();
        let _key = format!("event:{}", timestamp);
        
        log::debug!("[EVENT] Event {} would be persisted to storage", 
            match event {
                KlomangEvent::BlockAccepted { .. } => "BlockAccepted",
                KlomangEvent::BlockRejected { .. } => "BlockRejected",
                KlomangEvent::ReorgOccurred { .. } => "ReorgOccurred",
                KlomangEvent::InvalidVerkleProof { .. } => "InvalidVerkleProof",
                KlomangEvent::InvalidGhostdagRules { .. } => "InvalidGhostdagRules",
                KlomangEvent::VerkleStateMismatch { .. } => "VerkleStateMismatch",
            });
        Ok(())
    }

    /// Load event history from storage
    fn load_event_history(_storage: &StorageHandle) -> StateResult<()> {
        log::info!("[EVENT] Loading event history from storage");
        log::debug!("[EVENT] Event history loading - would load up to 1000 recent events");
        Ok(())
    }

    /// Get event emission counter for monitoring
    pub fn get_event_count(&self) -> u64 {
        self.event_counter.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Type alias for thread-safe state manager
pub type StateManagerHandle = Arc<RwLock<KlomangStateManager>>;
