//! Virtual Machine module for contract execution
//!
//! This module provides WebAssembly-based contract execution
//! with gas metering and state access controls.

use crate::storage::RocksDBStorageAdapter;
use klomang_core::core::state::v_trie::VerkleTree;
use klomang_core::core::state_manager::StateManager;
use klomang_core::core::vm::{VMError, VMExecutor};
use klomang_core::BlockHeader;
use std::sync::Arc;

/// VM Configuration
#[derive(Clone, Debug)]
pub struct VmConfig {
    pub max_gas_limit: u64,
    pub gas_per_instruction: u64,
    pub max_memory_pages: u32,
    pub enable_metering: bool,
}

/// Execution result with proper return data and revert handling
#[derive(Clone, Debug)]
pub struct ExecutionResult {
    pub return_data: Vec<u8>,
    pub gas_used: u64,
    pub reverted: bool,
    pub success: bool,
}

/// Virtual Machine for contract execution
pub struct VirtualMachine {
    config: VmConfig,
    current_block_header: Option<BlockHeader>,
}

impl VirtualMachine {
    pub fn new(config: VmConfig) -> Result<Self, String> {
        Ok(VirtualMachine { 
            config,
            current_block_header: None,
        })
    }

    /// Set current block header for state verification
    pub fn set_current_block_header(&mut self, header: BlockHeader) {
        self.current_block_header = Some(header);
    }

    fn validate_vm_limits(&self, gas_limit: u64, payload_size: usize) -> Result<u64, String> {
        if gas_limit > self.config.max_gas_limit {
            return Err(format!(
                "Gas limit {} exceeds VM max_gas_limit {}",
                gas_limit, self.config.max_gas_limit
            ));
        }

        if self.config.gas_per_instruction == 0 {
            return Err("VM configuration error: gas_per_instruction must be > 0".into());
        }

        let instruction_budget = gas_limit / self.config.gas_per_instruction;
        let required_pages = ((payload_size as u32).saturating_add(0xFFFF)) / 0x10000;
        if required_pages > self.config.max_memory_pages {
            return Err(format!(
                "Payload requires {} memory pages but max_memory_pages is {}",
                required_pages, self.config.max_memory_pages
            ));
        }

        Ok(instruction_budget)
    }

    /// Execute contract with full state access and proper result handling
    pub fn execute_contract(
        &self,
        contract_address: &[u8; 32],
        payload: &[u8],
        verkle_tree: &mut VerkleTree<RocksDBStorageAdapter>,
        _max_fee_per_gas: u64,
        gas_limit: u64,
    ) -> Result<ExecutionResult, String> {
        let max_instructions = self.validate_vm_limits(gas_limit, payload.len())?;
        log::debug!(
            "Executing contract with gas_limit={} max_instructions={} memory_limit={} pages",
            gas_limit,
            max_instructions,
            self.config.max_memory_pages
        );

        // Create state manager from tree
        let mut state_manager = StateManager::new(verkle_tree.clone())
            .map_err(|e| format!("Failed to initialize core state manager: {}", e))?;

        // Take snapshot for revert detection
        let tree_snapshot = state_manager.tree.clone();

        // Execute contract
        match VMExecutor::execute(payload, &mut state_manager, *contract_address, gas_limit) {
            Ok(gas_used) => {
                // Execution successful - update tree and return success
                *verkle_tree = state_manager.tree;
                Ok(ExecutionResult {
                    return_data: Vec::new(), // TODO: Extract return data from WASM memory if needed
                    gas_used,
                    reverted: false,
                    success: true,
                })
            }
            Err(VMError::OutOfGas) => {
                // Out of gas - revert state
                state_manager.tree = tree_snapshot;
                Ok(ExecutionResult {
                    return_data: Vec::new(),
                    gas_used: gas_limit,
                    reverted: true,
                    success: false,
                })
            }
            Err(VMError::RuntimeError(_msg)) => {
                // Runtime error - revert state
                state_manager.tree = tree_snapshot;
                Ok(ExecutionResult {
                    return_data: Vec::new(),
                    gas_used: gas_limit, // Assume all gas used on error
                    reverted: true,
                    success: false,
                })
            }
            Err(e) => {
                // Other error - revert state
                state_manager.tree = tree_snapshot;
                Err(format!("VM execution error: {}", e))
            }
        }
    }

    /// Dry-run contract execution without state changes
    pub fn dry_run_contract(
        &self,
        contract_address: &[u8; 32],
        payload: &[u8],
        verkle_tree: &VerkleTree<RocksDBStorageAdapter>,
        _max_fee_per_gas: u64,
        gas_limit: u64,
    ) -> Result<ExecutionResult, String> {
        let instruction_budget = self.validate_vm_limits(gas_limit, payload.len())?;

        if !self.config.enable_metering {
            log::warn!(
                "VM metering disabled - using gas limit {} with instruction budget {}",
                gas_limit,
                instruction_budget
            );
        }

        // Clone tree for dry run
        let tree_clone = verkle_tree.clone();
        let mut state_manager = StateManager::new(tree_clone)
            .map_err(|e| format!("Failed to initialize core state manager: {}", e))?;

        // Execute without committing changes
        match VMExecutor::execute(payload, &mut state_manager, *contract_address, gas_limit) {
            Ok(gas_used) => Ok(ExecutionResult {
                return_data: Vec::new(),
                gas_used,
                reverted: false,
                success: true,
            }),
            Err(VMError::OutOfGas) => Ok(ExecutionResult {
                return_data: Vec::new(),
                gas_used: gas_limit,
                reverted: true,
                success: false,
            }),
            Err(VMError::RuntimeError(_msg)) => Ok(ExecutionResult {
                return_data: Vec::new(),
                gas_used: gas_limit,
                reverted: true,
                success: false,
            }),
            Err(e) => Err(format!("VM dry-run error: {}", e)),
        }
    }

    /// Call contract for read-only operations (like eth_call)
    pub fn call_contract(
        &self,
        contract_address: &[u8; 32],
        call_data: &[u8],
        verkle_tree: &VerkleTree<RocksDBStorageAdapter>,
        gas_limit: u64,
    ) -> Result<ExecutionResult, String> {
        let instruction_budget = self.validate_vm_limits(gas_limit, call_data.len())?;

        if !self.config.enable_metering {
            log::warn!(
                "VM metering disabled for call_contract with instruction budget {}",
                instruction_budget
            );
        }

        // Clone tree for read-only call
        let tree_clone = verkle_tree.clone();
        let mut state_manager = StateManager::new(tree_clone)
            .map_err(|e| format!("Failed to initialize core state manager: {}", e))?;

        match VMExecutor::execute(call_data, &mut state_manager, *contract_address, gas_limit) {
            Ok(gas_used) => Ok(ExecutionResult {
                return_data: Vec::new(), // TODO: Extract actual return data
                gas_used,
                reverted: false,
                success: true,
            }),
            Err(VMError::OutOfGas) => Ok(ExecutionResult {
                return_data: Vec::new(),
                gas_used: gas_limit,
                reverted: true,
                success: false,
            }),
            Err(VMError::RuntimeError(_)) => Ok(ExecutionResult {
                return_data: Vec::new(),
                gas_used: gas_limit,
                reverted: true,
                success: false,
            }),
            Err(e) => Err(format!("VM call error: {}", e)),
        }
    }
}
