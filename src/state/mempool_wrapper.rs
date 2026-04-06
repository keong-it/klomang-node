//! Mempool wrapper and transaction management
//!
//! This module handles:
//! - Transaction addition and validation
//! - Mempool integration and updates
//! - Transaction confirmation tracking
//! - Mempool task management

use super::types::{KlomangEvent, StateError, StateResult};
use crate::mempool::AdvancedMempool;
use crate::vm::VirtualMachine;
use klomang_core::{BlockNode, Hash, SignedTransaction};
use log;
use std::sync::Arc;

/// Wrapper around mempool for state management
pub struct MempoolWrapper {
    mempool: Option<Arc<AdvancedMempool>>,
}

impl MempoolWrapper {
    /// Create a new mempool wrapper
    pub fn new(mempool: Option<AdvancedMempool>) -> Self {
        Self {
            mempool: mempool.map(Arc::new),
        }
    }

    /// Add a transaction to the mempool
    pub fn add_transaction(
        &mut self,
        tx: SignedTransaction,
        vm: Option<&VirtualMachine>,
        verkle_tree: Option<
            &klomang_core::core::state::v_trie::VerkleTree<crate::storage::RocksDBStorageAdapter>,
        >,
    ) -> StateResult<Hash> {
        if let Some(mempool) = &self.mempool {
            // Validate contract if VM available
            if let (Some(vm), Some(verkle_tree)) = (vm, verkle_tree) {
                self.validate_contract_transaction(&tx, vm, verkle_tree)?;
            }

            let tx_hash = mempool.add_transaction(tx.clone()).map_err(|e| {
                StateError::StorageError(format!("Failed to add transaction: {:?}", e))
            })?;
            log::debug!("[MEMPOOL] Added transaction: {}", tx.id);
            Ok(tx_hash)
        } else {
            Err(StateError::StorageError(
                "Mempool not initialized".to_string(),
            ))
        }
    }

    /// Validate contract transaction with dry-run
    pub fn validate_contract_transaction(
        &self,
        tx: &SignedTransaction,
        vm: &VirtualMachine,
        verkle_tree: &klomang_core::core::state::v_trie::VerkleTree<
            crate::storage::RocksDBStorageAdapter,
        >,
    ) -> StateResult<()> {
        if !tx.execution_payload.is_empty() {
            log::info!("[MEMPOOL] Dry-running contract transaction {}", tx.id);

            // Dry-run execution
            let result = vm
                .dry_run_contract(
                    &tx.contract_address.unwrap_or([0u8; 32]),
                    &tx.execution_payload,
                    verkle_tree,
                    tx.max_fee_per_gas as u64,
                    tx.gas_limit,
                )
                .map_err(|e| StateError::VmExecutionError(format!("Dry-run failed: {}", e)))?;

            // Check if execution would succeed
            if result.reverted {
                return Err(StateError::VmExecutionError(
                    "Contract execution would revert".to_string(),
                ));
            }

            // Check gas usage vs max_fee
            let gas_cost = result.gas_used * (tx.max_fee_per_gas as u64);
            // NOTE: Need to check user balance, but for now assume ok

            log::debug!(
                "[MEMPOOL] Contract dry-run passed, gas used: {}",
                result.gas_used
            );
        }
        Ok(())
    }

    /// Remove confirmed transactions from mempool
    pub fn remove_confirmed_transactions(&self, block: &BlockNode) -> StateResult<()> {
        if let Some(mempool) = &self.mempool {
            mempool.remove_confirmed_transactions(block).map_err(|e| {
                StateError::StorageError(format!("Failed to remove confirmed txs: {:?}", e))
            })?;
            log::debug!(
                "[MEMPOOL] Removed {} confirmed transactions from mempool",
                block.transactions.len()
            );
            Ok(())
        } else {
            log::warn!("[MEMPOOL] Mempool not initialized, skipping confirmation cleanup");
            Ok(())
        }
    }

    /// Start background mempool maintenance tasks
    pub fn start_mempool_tasks(
        &self,
        _event_tx: tokio::sync::broadcast::Sender<KlomangEvent>,
    ) -> StateResult<tokio::task::JoinHandle<()>> {
        if let Some(mempool) = &self.mempool {
            log::info!("[MEMPOOL] Starting mempool background tasks");

            // Start the background tasks for the mempool
            mempool.start_background_tasks().map_err(|e| {
                StateError::StorageError(format!("Failed to start mempool tasks: {:?}", e))
            })?;

            let mempool_clone = mempool.clone();

            let handle = tokio::spawn(async move {
                let mut cleanup_interval =
                    tokio::time::interval(std::time::Duration::from_secs(60));

                loop {
                    cleanup_interval.tick().await;
                    log::debug!("[MEMPOOL] Running periodic monitoring");

                    // Get current block template size as proxy for mempool size
                    let template = mempool_clone.get_block_template(10_000_000); // 10M weight limit
                    log::debug!(
                        "[MEMPOOL] Current template size: {} transactions",
                        template.len()
                    );

                    if template.len() > 10_000 {
                        log::warn!(
                            "[MEMPOOL] Mempool size very high: {} transactions",
                            template.len()
                        );
                    }
                }
            });

            Ok(handle)
        } else {
            Err(StateError::StorageError(
                "Mempool not initialized".to_string(),
            ))
        }
    }

    /// Get reference to mempool if available
    pub fn get_mempool(&self) -> Option<&AdvancedMempool> {
        self.mempool.as_ref().map(|arc| arc.as_ref())
    }

    /// Get mutable reference to mempool if available
    pub fn get_mempool_mut(&mut self) -> Option<&mut AdvancedMempool> {
        None
    }

    /// Get block template for mining
    pub fn get_block_template(&self, max_weight: u64) -> Vec<SignedTransaction> {
        if let Some(mempool) = &self.mempool {
            mempool.get_block_template(max_weight)
        } else {
            Vec::new()
        }
    }

    /// Save mempool snapshot
    pub fn save_snapshot(&self) -> StateResult<()> {
        if let Some(mempool) = &self.mempool {
            mempool.save_snapshot().map_err(|e| {
                StateError::StorageError(format!("Failed to save mempool snapshot: {:?}", e))
            })
        } else {
            log::debug!("[MEMPOOL] No mempool to save");
            Ok(())
        }
    }

    /// Load mempool snapshot
    pub fn load_snapshot(&self) -> StateResult<()> {
        if let Some(mempool) = &self.mempool {
            mempool.load_snapshot().map_err(|e| {
                StateError::StorageError(format!("Failed to load mempool snapshot: {:?}", e))
            })
        } else {
            log::debug!("[MEMPOOL] No mempool to load");
            Ok(())
        }
    }
}

/// Mempool operations trait for cleanup and maintenance
pub trait MempoolOps {
    fn cleanup_expired_transactions(&mut self) -> StateResult<usize>;
}

impl MempoolOps for MempoolWrapper {
    fn cleanup_expired_transactions(&mut self) -> StateResult<usize> {
        log::debug!("[MEMPOOL] Running synchronous cleanup");
        // This is a wrapper for async operations that can be called in sync context
        // Actual expiration logic runs in background tasks managed by start_mempool_tasks()
        Ok(0)
    }
}
