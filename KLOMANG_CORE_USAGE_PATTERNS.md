# Klomang-Core Usage Patterns

Comprehensive guide showing how to use klomang-core APIs for block validation, transaction processing, UTXO management, and DAG operations across the klomang-node codebase.

---

## 1. BLOCK SIGNATURE VERIFICATION

### Pattern 1A: Verify Block Signature (Not Yet Integrated)

**Location**: [src/state/validation.rs](src/state/validation.rs#L63-L74)

```rust
fn validate_signature(&self, block: &BlockNode) -> StateResult<()> {
    if block.header.id.to_hex().is_empty() {
        log::warn!("[SIG] Block missing ID for signature validation");
        return Err(StateError::InvalidSignature);
    }

    // TODO: Integrate actual signature verification
    // if !verify_block_signature(block) {
    //     log::warn!("[SIG] Block signature invalid for {}", block.header.id.to_hex());
    //     return Err(StateError::InvalidSignature);
    // }

    log::debug!("[SIG] Block signature validated for {}", block.header.id.to_hex());
    Ok(())
}
```

**Expected Implementation Pattern from klomang-core**:
```rust
// Import from klomang_core:
// use klomang_core::crypto::verify_block_signature;
// OR
// use klomang_core::BlockNode;  // BlockNode should have verify_signature() method

fn validate_signature(&self, block: &BlockNode) -> StateResult<()> {
    // Option 1: Static function call
    if !klomang_core::crypto::verify_block_signature(block) {
        log::warn!("[SIG] Block signature invalid for {}", block.header.id.to_hex());
        return Err(StateError::InvalidSignature);
    }

    // Option 2: Method call on BlockNode
    if !block.verify_signature() {
        log::warn!("[SIG] Block signature invalid for {}", block.header.id.to_hex());
        return Err(StateError::InvalidSignature);
    }

    log::debug!("[SIG] Block signature validated for {}", block.header.id.to_hex());
    Ok(())
}
```

### Pattern 1B: Access Signature Fields from BlockNode

**Assumption**: BlockNode has fields like `miner_key`, `signature`, or similar

```rust
// Access signature components from BlockNode
let miner_key = &block.miner_key;  // Ed25519/Schnorr public key
let signature = &block.signature;  // Signature bytes
let message = block.serialize_without_signature()?;  // Message that was signed

// Verify: signature == sign(message, miner_key)
if !verify_ed25519(&message, signature, miner_key) {
    return Err(StateError::InvalidSignature);
}
```

---

## 2. GHOSTDAG RULES VALIDATION

### Pattern 2A: Validate GHOSTDAG Rules

**Location**: [src/state/validation.rs](src/state/validation.rs#L298-L330)

```rust
fn validate_ghostdag_rules(&self, block: &BlockNode) -> StateResult<()> {
    // GHOSTDAG (Greedy Heaviest Observed Subtree DAG) validation
    // Checks that block respects DAG ordering and conflict resolution
    
    // Requirements to validate:
    // 1. All parents must exist and be valid
    // 2. Blue score must be correctly computed based on parent blue scores
    // 3. Anticone set must not include any ancestors
    // 4. Block must follow topological ordering
    
    log::info!("[GHOSTDAG] Validating GHOSTDAG rules for block {}", block.header.id.to_hex());
    
    // Check: Block must have parents (except genesis)
    if block.header.parents.is_empty() {
        log::debug!("[GHOSTDAG] Genesis block detected (no parents)");
        return Ok(());
    }
    
    // Check: All parent blocks should exist
    for (parent_idx, parent_hash) in block.header.parents.iter().enumerate() {
        // TODO: Implement parent existence check using storage
        // Pattern:
        // match self.storage.read()?.get_block(parent_hash) {
        //     Ok(Some(_)) => { ... }
        //     Ok(None) => return Err(StateError::OrphanBlock),
        //     Err(e) => return Err(StateError::StorageError(e.to_string())),
        // }
        
        log::debug!("[GHOSTDAG] Parent {}/{} validated", parent_idx + 1, block.header.parents.len());
    }
    
    // TODO: Integrate klomang_core GHOSTDAG validation
    // This validates blue score, anticone, and conflict resolution
    // Expected Pattern:
    // match self.dag.validate_ghostdag_rules(block) {
    //     Ok(true) => {
    //         log::debug!("[GHOSTDAG] GHOSTDAG rules valid");
    //     }
    //     Ok(false) => {
    //         log::warn!("[GHOSTDAG] Block {} violates GHOSTDAG rules", block.header.id.to_hex());
    //         return Err(StateError::GhostdagViolation);
    //     }
    //     Err(e) => {
    //         log::error!("[GHOSTDAG] Validation error: {}", e);
    //         return Err(StateError::CoreValidationError(format!("GHOSTDAG error: {}", e)));
    //     }
    // }
    
    log::debug!("[GHOSTDAG] GHOSTDAG rules validated for block {}", block.header.id.to_hex());
    Ok(())
}
```

### Pattern 2B: Access GHOSTDAG Fields from BlockNode

**Fields to expect on BlockNode header**:
```rust
// BlockNode structure with GHOSTDAG-related fields
pub struct BlockHeader {
    pub id: Hash,                           // Block hash
    pub parents: Vec<Hash>,                 // Parent block hashes (Merkle DAG parents)
    pub blue_score: u64,                    // GHOSTDAG blue score
    pub anticone: Vec<Hash>,                // Blocks not reachable through parents
    pub merkle_root: Hash,                  // Merkle root of transactions
    pub timestamp: u64,                     // Block creation time
    pub nonce: u64,                         // Proof-of-work nonce
    // ... other fields
}

pub struct BlockNode {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    // ... other fields
}

// Access patterns in validation code:
if block.header.parents.is_empty() { /* genesis */ }
for parent_hash in &block.header.parents { /* check each parent */ }
let blue_score = block.header.blue_score;
let anticone = &block.header.anticone;  // Blocks conflicting with this block
```

### Pattern 2C: DAG Method Calls for GHOSTDAG

**Location**: [src/state/mod.rs](src/state/mod.rs#L38)

```rust
use klomang_core::{Dag, UtxoSet, BlockNode, Transaction, Hash};

pub struct KlomangStateManager {
    dag: Dag,  // klomang_core::Dag instance
    utxo_set: UtxoSet,
    // ...
}

// Expected Dag methods for GHOSTDAG validation:
impl KlomangStateManager {
    // Create/initialize DAG
    let dag = Dag::new();
    
    // Add block to DAG and compute GHOSTDAG scores
    dag.add_block(block)?;  // Should compute blue_score, anticone automatically
    
    // Validate GHOSTDAG rules
    if dag.validate_ghostdag_rules(block)? {
        // Block follows GHOSTDAG consensus rules
    }
    
    // Query DAG state
    let blue_score = dag.get_blue_score(&block.header.id)?;
    let anticone = dag.get_anticone(&block.header.id)?;
    let selected_parent = dag.get_selected_parent(&block.header.id)?;
}
```

---

## 3. TRANSACTION INPUT/OUTPUT ACCESS PATTERNS

### Pattern 3A: Access Transaction Inputs and Outputs

**Location**: [src/state/validation.rs](src/state/validation.rs#L133-L161)

```rust
// Iterate through transaction inputs
for (tx_idx, tx) in block.transactions.iter().enumerate() {
    // Access inputs
    for (input_idx, input) in tx.inputs.iter().enumerate() {
        // input has fields like:
        // - input.prev_out: OutPoint { txid: Hash, vout: u32 }
        // - input.script_sig: Script signature
        // - input.sequence: Sequence number
        
        log::debug!("[UTXO_EXIST] Processing input {}/{} of tx {}", 
            input_idx, tx.inputs.len(), tx_idx);
    }
    
    // Access outputs
    for output in &tx.outputs {
        // output has fields like:
        // - output.value: u64 (satoshis)
        // - output.script_pubkey: Script
        
        log::debug!("[BALANCE] Processing output of tx {}", tx_idx);
    }
}
```

### Pattern 3B: Transaction Structure

**Expected Transaction Structure**:
```rust
pub struct Transaction {
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
    pub version: u32,
    pub locktime: u32,
}

pub struct Input {
    pub prev_out: OutPoint,         // Previous UTXO being spent
    pub script_sig: Vec<u8>,        // Unlocking script
    pub sequence: u32,              // Sequence number
}

pub struct OutPoint {
    pub txid: Hash,                 // Previous tx hash
    pub vout: u32,                  // Output index
}

pub struct Output {
    pub value: u64,                 // Satoshis
    pub script_pubkey: Vec<u8>,     // Locking script
}
```

### Pattern 3C: Validate Double Spend Within Block

**Location**: [src/state/mod.rs](src/state/mod.rs#L421-L438)

```rust
fn validate_double_spend(&self, block: &BlockNode) -> StateResult<()> {
    log::info!("[DOUBLE_SPEND] Validating for duplicate spends in block {}", 
        block.header.id.to_hex());
    
    let mut spent_outputs: std::collections::HashSet<String> = std::collections::HashSet::new();
    
    for (tx_idx, tx) in block.transactions.iter().enumerate() {
        for (input_idx, _input) in tx.inputs.iter().enumerate() {
            // Create output key: (previous_txid, previous_vout)
            // Expected Pattern:
            // let output_key = format!("{}:{}", input.prev_out.txid.to_hex(), input.prev_out.vout);
            
            let output_key = format!("input_{}_{}", tx_idx, input_idx);
            if !spent_outputs.insert(output_key.clone()) {
                log::warn!("[DOUBLE_SPEND] Duplicate spend detected for key: {}", output_key);
                return Err(StateError::DoubleSpend);
            }
            
            log::debug!("[DOUBLE_SPEND] Input {}/{} in tx {} checked", 
                input_idx, tx.inputs.len(), tx_idx);
        }
    }
    
    log::debug!("[DOUBLE_SPEND] No duplicate spends detected in block {}", 
        block.header.id.to_hex());
    Ok(())
}
```

### Pattern 3D: Check Transactions Have Inputs and Outputs

**Location**: [src/mempool/network.rs](src/mempool/network.rs#L239)

```rust
// Basic transaction validity check
fn is_valid_transaction(tx: &Transaction) -> bool {
    !tx.inputs.is_empty() && !tx.outputs.is_empty()
}
```

### Pattern 3E: Sum Transaction Values for Balance Validation

**Location**: [src/state/validation.rs](src/state/validation.rs#L219-L235)

```rust
fn validate_balance_and_fees(&self, block: &BlockNode) -> StateResult<()> {
    log::info!("[BALANCE] Validating balance and fees for {} transactions", 
        block.transactions.len());
    
    for (tx_idx, tx) in block.transactions.iter().enumerate() {
        let mut total_input: u64 = 0;
        let mut total_output: u64 = 0;
        
        // Sum input values from UTXO set
        // Expected Pattern:
        // for input in &tx.inputs {
        //     let utxo_key = (input.prev_out.txid.clone(), input.prev_out.vout);
        //     if let Some(utxo_entry) = self.utxo_set.get(&utxo_key)? {
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
        
        // Validate: total_input >= total_output + fees
        // if total_input < total_output {
        //     log::warn!("[BALANCE] Insufficient balance in tx {}: {} < {}", 
        //         tx_idx, total_input, total_output);
        //     return Err(StateError::InsufficientBalance);
        // }
        
        log::debug!("[BALANCE] Tx {} balance validated (input≥output)", tx_idx);
    }
    
    Ok(())
}
```

### Pattern 3F: Transaction Dependency Tracking

**Location**: [src/mempool/graph.rs](src/mempool/graph.rs#L45-L65)

```rust
pub fn add_transaction(&mut self, tx: &SignedTransaction) {
    let hash = tx.hash();
    self.transactions.insert(hash, tx.clone());
    
    // Build dependency relationships by examining inputs
    let mut tx_parents = HashSet::new();
    for input in &tx.inputs {
        // Each input.prev_out references a previous transaction output
        if let Some(parent_hash) = self.find_spending_transaction(&input.prev_out) {
            // This transaction depends on parent_hash
            tx_parents.insert(parent_hash);
            self.children.entry(parent_hash)
                .or_insert(HashSet::new())
                .insert(hash);
        }
    }
    
    self.parents.insert(hash, tx_parents);
}

fn find_spending_transaction(&self, prev_out: &OutPoint) -> Option<Hash> {
    // Find if any transaction in mempool spends this output
    // Returns the hash of that transaction
}
```

---

## 4. UTXO SET METHODS AND ACCESS PATTERNS

### Pattern 4A: Initialize UtxoSet

**Location**: [src/state/mod.rs](src/state/mod.rs#L137), [src/main.rs](src/main.rs#L35)

```rust
use klomang_core::UtxoSet;

// Initialize new empty UTXO set
let utxo_set = UtxoSet::new();

// Or load from storage (expected pattern)
let utxo_set = UtxoSet::load_from_storage(&storage_handle)?;
```

### Pattern 4B: Check if UTXO Exists

**Location**: [src/state/validation.rs](src/state/validation.rs#L133-L161)

```rust
fn validate_utxo_existence(&self, block: &BlockNode) -> StateResult<()> {
    log::info!("[UTXO_EXIST] Validating UTXO existence for {} transactions", 
        block.transactions.len());
    
    for (tx_idx, tx) in block.transactions.iter().enumerate() {
        for (input_idx, input) in tx.inputs.iter().enumerate() {
            // Check if UTXO exists: (txid, vout)
            // Expected Pattern:
            // let utxo_key = (input.prev_out.txid.clone(), input.prev_out.vout);
            // if !self.utxo_set.contains(&utxo_key)? {
            //     log::warn!("[UTXO_EXIST] UTXO {:?} not found", utxo_key);
            //     return Err(StateError::InsufficientBalance);
            // }
            
            log::debug!("[UTXO_EXIST] UTXO input {}/{} validated in tx {}", 
                input_idx, tx.inputs.len(), tx_idx);
        }
    }
    
    log::debug!("[UTXO_EXIST] All inputs exist in UTXO set for block {}", 
        block.header.id.to_hex());
    Ok(())
}
```

### Pattern 4C: Get UTXO Entry (Value and Script)

**Expected UtxoSet API**:
```rust
impl UtxoSet {
    // Check existence
    pub fn contains(&self, key: &(Hash, u32)) -> Result<bool, UtxoSetError> { }
    
    // Get UTXO value
    pub fn get(&self, key: &(Hash, u32)) -> Result<Option<UtxoEntry>, UtxoSetError> { }
    
    // Add new UTXO (after block is validated)
    pub fn add(&mut self, key: (Hash, u32), entry: UtxoEntry) -> Result<(), UtxoSetError> { }
    
    // Remove UTXO (mark as spent)
    pub fn remove(&mut self, key: &(Hash, u32)) -> Result<(), UtxoSetError> { }
    
    // Batch operations
    pub fn add_batch(&mut self, utxos: Vec<(Hash, u32, UtxoEntry)>) -> Result<(), UtxoSetError> { }
    pub fn remove_batch(&mut self, keys: Vec<(Hash, u32)>) -> Result<(), UtxoSetError> { }
}

pub struct UtxoEntry {
    pub value: u64,
    pub script_pubkey: Vec<u8>,
}

// Usage in balance validation:
if let Some(utxo_entry) = self.utxo_set.get(&(input.prev_out.txid.clone(), input.prev_out.vout))? {
    total_input += utxo_entry.value;
    // Can also access: utxo_entry.script_pubkey
} else {
    return Err(StateError::InsufficientBalance);
}
```

### Pattern 4D: UTXO Diff for State Updates

**Location**: [src/state/types.rs](src/state/types.rs)

```rust
pub struct UtxoDiff {
    pub added: HashMap<Hash, (u64, Vec<u8>)>,  // New UTXOs
    pub removed: std::collections::HashSet<Hash>,  // Spent UTXOs
    pub new_verkle_root: Hash,
    pub old_verkle_root: Hash,
}

impl UtxoDiff {
    /// Add UTXO to diff with Verkle path derivation
    pub fn add_utxo(&mut self, address: &[u8; 20], vout: u32, value: u64, script: Vec<u8>) {
        // Verkle path: blake3(address || vout || "VALUE")
        let mut path_input = Vec::with_capacity(20 + 1 + 4 + 1 + 5);
        path_input.extend_from_slice(address);
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(&vout.to_le_bytes());
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(b"VALUE");
        
        let utxo_path = Hash::new(&path_input);
        self.added.insert(utxo_path.clone(), (value, script));
    }

    /// Remove UTXO from diff
    pub fn remove_utxo(&mut self, address: &[u8; 20], vout: u32) {
        let mut path_input = Vec::with_capacity(20 + 1 + 4 + 1 + 5);
        path_input.extend_from_slice(address);
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(&vout.to_le_bytes());
        path_input.extend_from_slice(b":");
        path_input.extend_from_slice(b"VALUE");
        
        let utxo_path = Hash::new(&path_input);
        self.removed.insert(utxo_path);
    }
}
```

### Pattern 4E: Clone and Update UtxoSet

**Location**: [src/state/mod.rs](src/state/mod.rs#L577)

```rust
fn persist_block_with_diff(&self, /* ... */) -> StateResult<()> {
    // Clone current UTXO set for update
    let mut new_utxo_set = self.utxo_set.clone();
    
    // Apply changes from UTXO diff
    // Expected:
    // for (utxo_key, (value, script)) in &staged_changes.utxo_diff.added {
    //     new_utxo_set.add(*utxo_key, UtxoEntry { value, script })?;
    // }
    // for utxo_key in &staged_changes.utxo_diff.removed {
    //     new_utxo_set.remove(utxo_key)?;
    // }
    
    // Update state manager's reference
    self.utxo_set = new_utxo_set;
    
    Ok(())
}
```

---

## 5. DAG METHOD CALLS

### Pattern 5A: Initialize DAG

**Location**: [src/state/mod.rs](src/state/mod.rs#L134), [src/main.rs](src/main.rs#L34)

```rust
use klomang_core::Dag;

// Initialize new DAG
let dag = Dag::new();

// Or load from storage
let dag = Dag::load_from_storage(&storage_handle)?;

// Check block count
let block_count = dag.get_block_count();
println!("Dag has {} blocks", block_count);
```

### Pattern 5B: Expected Dag API Methods

**Expected methods on Dag struct**:
```rust
impl Dag {
    // Initialization
    pub fn new() -> Self { }
    pub fn load_from_storage(storage: &StorageHandle) -> Result<Self, DagError> { }
    
    // Block management
    pub fn add_block(&mut self, block: &BlockNode) -> Result<(), DagError> { }
    pub fn get_block(&self, hash: &Hash) -> Result<Option<BlockNode>, DagError> { }
    pub fn has_block(&self, hash: &Hash) -> Result<bool, DagError> { }
    
    // Block count/height
    pub fn get_block_count(&self) -> u64 { }
    pub fn get_tip(&self) -> Hash { }
    
    // GHOSTDAG validation and queries
    pub fn validate_ghostdag_rules(&self, block: &BlockNode) -> Result<bool, DagError> { }
    pub fn get_blue_score(&self, block_hash: &Hash) -> Result<u64, DagError> { }
    pub fn get_selected_parent(&self, block_hash: &Hash) -> Result<Hash, DagError> { }
    pub fn get_anticone(&self, block_hash: &Hash) -> Result<Vec<Hash>, DagError> { }
    
    // DAG properties
    pub fn get_parent_hashes(&self, block_hash: &Hash) -> Result<Vec<Hash>, DagError> { }
    pub fn get_children(&self, block_hash: &Hash) -> Result<Vec<Hash>, DagError> { }
    
    // Storage sync
    pub fn save_to_storage(&self, storage: &StorageHandle) -> Result<(), DagError> { }
}
```

### Pattern 5C: Access Dag Field from State Manager

**Location**: [src/state/mod.rs](src/state/mod.rs#L48-L52)

```rust
pub struct KlomangStateManager {
    /// DAG state from klomang-core
    dag: Dag,
    
    /// UTXO set for transaction validation
    utxo_set: UtxoSet,
    
    // ...
}

// Getter method
pub fn get_dag(&self) -> &Dag {
    &self.dag
}

// Usage in validation
pub fn validate_ghostdag_rules(&self, block: &BlockNode) -> StateResult<()> {
    // Can access through self.dag
    if self.dag.validate_ghostdag_rules(block)? {
        log::debug!("GHOSTDAG validation passed");
    }
    Ok(())
}
```

---

## 6. BLOCKNODE METHODS AND FIELDS

### Pattern 6A: Access BlockNode Fields

**Location**: [src/network/mod.rs](src/network/mod.rs#L31), [src/network/mod.rs](src/network/mod.rs#L67)

```rust
use klomang_core::BlockNode;

// BlockNode is used throughout the codebase
pub enum NetworkMessage {
    Block(BlockNode),  // Received block
    Transaction(Transaction),
}

// Access block fields
fn process_block(&self, block: &BlockNode) {
    // Header information
    let block_hash = &block.header.id;
    let block_timestamp = block.header.timestamp;
    let parent_hashes = &block.header.parents;
    
    // Convert hash to hex for logging
    log::info!("Processing block {}", block.header.id.to_hex());
    
    // Transaction access
    let tx_count = block.transactions.len();
    for tx in &block.transactions {
        // Process each transaction
    }
}
```

### Pattern 6B: Expected BlockNode Structure

```rust
pub struct BlockNode {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
}

pub struct BlockHeader {
    pub id: Hash,                       // Block hash
    pub parents: Vec<Hash>,             // Parent hashes (DAG parents)
    pub merkle_root: Hash,             // Merkle root of transactions
    pub timestamp: u64,                 // Unix timestamp
    pub nonce: u64,                     // Proof-of-work nonce
    pub blue_score: u64,                // GHOSTDAG blue score
    pub version: u32,                   // Block version
    pub bits: u32,                      // Difficulty target
    pub verkle_root: Hash,             // Verkle tree root
    pub verkle_proofs: Option<Vec<u8>>, // Verkle proofs
    // Optional signature fields:
    // pub miner_key: PublicKey,
    // pub signature: Signature,
}

pub struct BlockNode {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    
    // Methods (expected):
    pub fn hash(&self) -> Hash { }
    pub fn serialize(&self) -> Result<Vec<u8>, SerializeError> { }
    pub fn serialize_without_signature(&self) -> Result<Vec<u8>, SerializeError> { }
    pub fn verify_signature(&self) -> bool { }  // If signature fields present
}
```

### Pattern 6C: BlockNode in Mempool

**Location**: [src/mempool/mod.rs](src/mempool/mod.rs#L147-L151)

```rust
pub fn remove_confirmed_transactions(&self, block: &klomang_core::BlockNode) -> Result<(), MempoolError> {
    // Iterate through block's transactions
    for tx in &block.transactions {
        // Remove from mempool since they're now confirmed
        let tx_hash = tx.hash();
        // graph.remove_transaction(&tx_hash);
        // miner.remove_transaction(&tx_hash);
    }
    Ok(())
}
```

### Pattern 6D: BlockNode Conversions

```rust
// Expected conversion methods on BlockNode:
impl BlockNode {
    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> { }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> { }
    pub fn hash(&self) -> Hash { }
    pub fn header_hash(&self) -> Hash { } // Hash of header only, often same as hash()
}

// Usage pattern:
let block_bytes = block.to_bytes()?;
let block = BlockNode::from_bytes(&block_bytes)?;
let hash = block.hash();
```

---

## 7. COMPLETE VALIDATION PIPELINE

### Pattern 7A: Full Block Validation Flow

**Location**: [src/state/mod.rs](src/state/mod.rs#L273-L340)

```rust
pub fn process_block(&mut self, block: BlockNode) -> StateResult<()> {
    log::info!("[PROCESS] Processing block {}", block.header.id.to_hex());
    
    // PHASE 1: VALIDATION
    self.process_block_internal(block.clone())?;
    
    // PHASE 2-4: Staging, Execution, Persistence
    // ... (handled internally)
    
    log::info!("[VALIDATION] Block {} successfully accepted", block.header.id.to_hex());
    Ok(())
}

fn process_block_internal(&mut self, block: BlockNode) -> StateResult<()> {
    // PHASE 1A: Stateless validation (fast)
    self.validate_block_stateless(&block)?;
    
    // PHASE 1B: Stateful validation (requires UTXO + DAG state)
    self.validate_block_stateful(&block)?;
    
    // PHASE 2: Stage UTXO changes
    let utxo_diff = self.stage_utxo_diff(&block)?;
    let staged_changes = self.create_staged_changes(utxo_diff, &block)?;
    
    // PHASE 3: Execute atomic state update
    // ... update best_block, verkle_root, etc.
    
    // PHASE 4: Persist to storage
    // ... save block, UTXO diff, etc.
    
    Ok(())
}
```

### Pattern 7B: Validation Methods Called in Sequence

```rust
fn validate_block_stateless(&self, block: &BlockNode) -> StateResult<()> {
    self.validate_signature(block)?;           // 1. Check signature
    self.validate_hash_integrity(block)?;      // 2. Hash integrity
    self.validate_structure_and_size(block)?;  // 3. Structure/size
    Ok(())
}

fn validate_block_stateful(&self, block: &BlockNode) -> StateResult<()> {
    self.validate_utxo_existence(block)?;          // 1. UTXO exists
    self.validate_double_spend(block)?;            // 2. No double spend
    self.validate_balance_and_fees(block)?;        // 3. Balance valid
    self.validate_verkle_proofs(block)?;           // 4. Verkle proofs
    self.validate_ghostdag_rules(block)?;          // 5. GHOSTDAG rules
    Ok(())
}
```

---

## 8. SUMMARY TABLE: Key API Methods

| Component | Method | Location | Purpose |
|-----------|--------|----------|---------|
| **BlockNode** | `header.id.to_hex()` | [src/state/mod.rs:L359] | Get block hash as hex string |
| **BlockNode** | `header.parents` | [src/state/validation.rs:L108] | Access parent block hashes |
| **BlockNode** | `transactions` | [src/state/validation.rs:L141] | Get list of transactions |
| **Transaction** | `inputs` | [src/mempool/graph.rs:L53] | Get transaction inputs |
| **Transaction** | `outputs` | [src/state/validation.rs:L227] | Get transaction outputs |
| **Input** | `prev_out` | [src/mempool/graph.rs:L54] | Reference to previous UTXO |
| **Output** | `value` | [src/state/validation.rs:L227] | Satoshi amount |
| **UtxoSet** | `new()` | [src/state/mod.rs:L137] | Create new UTXO set |
| **UtxoSet** | `contains(key)` | [src/state/validation.rs:L149] | Check UTXO existence |
| **UtxoSet** | `get(key)` | [src/state/validation.rs:L219] | Retrieve UTXO entry |
| **UtxoSet** | `add(key, entry)` | [src/state/mod.rs:L515] | Add new UTXO |
| **UtxoSet** | `remove(key)` | [src/state/mod.rs:L520] | Mark UTXO as spent |
| **Dag** | `new()` | [src/state/mod.rs:L134] | Create new DAG |
| **Dag** | `validate_ghostdag_rules()` | [src/state/validation.rs:L318] | Validate GHOSTDAG |
| **Dag** | `get_block_count()` | [src/main.rs:L37] | Get block count |

---

## 9. IMPLEMENTATION Checklist

- [ ] Implement `BlockNode::verify_signature()` or `klomang_core::crypto::verify_block_signature()`
- [ ] Integrate `Dag::validate_ghostdag_rules()` in [src/state/validation.rs](src/state/validation.rs#L318)
- [ ] Implement UTXO lookup: `self.utxo_set.get(&(txid, vout))`
- [ ] Implement UTXO additions after block confirmation
- [ ] Add Verkle proof validation matching klomang-core format
- [ ] Integrate serialization: `block.serialize_without_signature()`
- [ ] Add parent existence checks from storage
- [ ] Implement balance validation with proper value summation
- [ ] Add transaction dependency resolution in mempool
