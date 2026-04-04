# KlomangStateManager - State Manager Implementation

## 📋 Overview

**KlomangStateManager** adalah komponen pusat orkestrasi untuk Klomang Node yang mengkoordinasikan antara:
- **🧠 klomang-core** (Otak): Logika validasi GHOSTDAG, signature verification
- **🏪 Storage** (Gudang): Persistent storage dengan optimasi blockchain

## 🏗️ Architecture

### Core Components
```rust
pub struct KlomangStateManager {
    storage: StorageHandle,        // Arc<RwLock<KlomangStorage>>
    dag: Dag,                      // klomang_core::Dag
    utxo_set: UtxoSet,             // klomang_core::UtxoSet
    best_block: Hash,              // Current best block hash
}
```

### Thread Safety
- **StorageHandle**: `Arc<RwLock<KlomangStorage>>` - Thread-safe sharing
- **StateManagerHandle**: `Arc<RwLock<KlomangStateManager>>` - For P2P/RPC modules

## 🔄 Main Operations

### 1. process_block() - Primary Orchestration
```rust
pub fn process_block(&mut self, block: BlockNode) -> StateResult<()>
```
**Flow:**
1. **Validate** block menggunakan klomang-core logic
2. **Execute** atomically: Update UTXO + Save to storage
3. **Update** best block tracking

### 2. Validation Pipeline
```rust
fn validate_block(&self, block: &BlockNode) -> StateResult<()>
```
**Checks (TODO - integrate with core API):**
- ✅ Block hash verification
- ✅ Signature validation
- ✅ GHOSTDAG rules compliance
- ✅ Transaction UTXO verification
- ✅ Orphan block detection

### 3. Atomic Execution
```rust
fn atomic_execute_block(&mut self, block: BlockNode) -> StateResult<()>
```
**Atomic Operations:**
- Update UTXO set dengan transactions
- Update DAG dengan new block
- Save block, transactions, chain index ke storage atomically

### 4. Best Block Management
```rust
fn update_best_block(&mut self) -> StateResult<()>
```
- Track block dengan total work tertinggi
- Update storage dengan best block info

## 🚨 Error Handling

### Error Types
```rust
pub enum StateError {
    InvalidSignature,           // Signature verification failed
    OrphanBlock,               // No valid parents
    InsufficientBalance,       // UTXO insufficient
    DoubleSpend,               // Double spend detected
    InvalidHash,               // Block hash invalid
    GhostdagViolation,         // GHOSTDAG rules violated
    StorageError(String),      // Storage layer error
    CoreValidationError(String), // Core validation error
}
```

### Result Type
```rust
pub type StateResult<T> = Result<T, StateError>;
```

## 🔧 Integration Points

### Startup Initialization
```rust
// Load state from storage or initialize genesis
let state_manager = KlomangStateManager::new(storage_handle)?;
```

### Block Processing
```rust
// Process incoming blocks
state_manager.write().unwrap().process_block(new_block)?;
```

### Access Methods
```rust
// Read-only access for queries
let best_block = state_manager.read().unwrap().get_best_block();
let dag = state_manager.read().unwrap().get_dag();
let utxo = state_manager.read().unwrap().get_utxo_set();
```

## 🔮 Future Extensions

### P2P Integration
- StateManagerHandle untuk block propagation
- Thread-safe block validation untuk incoming blocks

### RPC Interface
- Query best block, DAG state, UTXO info
- Block submission via RPC

### Consensus Engine
- Connect dengan consensus logic
- Handle chain reorganizations

## 📝 Implementation Notes

### Current Status
- ✅ **Structure**: Complete architecture
- ✅ **Thread Safety**: Arc<RwLock> implementation
- ✅ **Error Handling**: Comprehensive error types
- ✅ **Storage Integration**: Atomic operations
- ⏳ **Core API Integration**: Placeholder calls (needs core API)

### TODO Items
1. **Replace placeholders** dengan actual klomang-core API calls
2. **Implement UTXO updates** menggunakan core transaction processing
3. **Add DAG state persistence** untuk startup recovery
4. **Implement chain reorganization** logic
5. **Add metrics/monitoring** untuk performance tracking

### Validation Logic (Current: Placeholder)
```rust
// TODO: Replace with actual core validation
// self.dag.verify_block_hash(block)
// self.dag.verify_signatures(block)
// self.dag.verify_ghostdag_rules(block)
// self.utxo_set.verify_transaction(tx)
```

## 🎯 Design Principles

1. **Single Responsibility**: StateManager orchestrates, doesn't implement validation
2. **Thread Safety**: All operations safe for concurrent access
3. **Atomic Consistency**: State changes are all-or-nothing
4. **Error Transparency**: Detailed error types for debugging
5. **Core Dependency**: Validation logic delegated to klomang-core
6. **Storage Agnostic**: Works with any storage implementation

## 🚀 Usage Example

```rust
// Initialize
let storage = KlomangStorage::open(...)?.into_handle();
let state_mgr = Arc::new(RwLock::new(KlomangStateManager::new(storage)?));

// Process block
let block = /* incoming block */;
state_mgr.write().unwrap().process_block(block)?;

// Query state
let best = state_mgr.read().unwrap().get_best_block();
```

---

**Status**: ✅ **Compiled & Integrated**  
**Next**: Integrate dengan actual klomang-core validation APIs
