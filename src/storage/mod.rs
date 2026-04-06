/// Klomang Storage Layer - Production-Grade Blockchain Storage Engine
///
/// Modul struktur (Single Responsibility Principle):
/// - keys.rs: Prefix & key design dengan helper functions
/// - config.rs: Cache, bloom filter, BlockBasedOptions tuning, dan PruningStrategy
/// - db.rs: KlomangStorage core struct, open(), open_with_recovery(), dan helper functions
/// - ops_block.rs: Block operations (save, read, batch save)
/// - ops_tx.rs: Transaction operations
/// - ops_chain.rs: Chain index & metadata operations
/// - pruning.rs: Garbage collection & pruning logic
/// - stats.rs: Metrics & monitoring operations
///
/// Guarantees:
/// - Atomic writes via WriteBatch
/// - Consistent reads via Snapshot
/// - Durability through fsync + WAL control
/// - Performance through LRU cache + bloom filter
/// - Thread safety via Arc<RwLock<>> wrapper
/// - Disk efficiency via Hot/Cold data separation
pub mod adapter;
pub mod config;
pub mod db;
pub mod keys;
pub mod ops_block;
pub mod ops_chain;
pub mod ops_tx;
pub mod pruning;
pub mod stats;

// Re-export main types untuk convenience
#[allow(unused_imports)]
pub use adapter::{RocksDBStorageAdapter, UtxoStorage};
pub use config::PruningStrategy;
pub use db::{ChainIndexRecord, KlomangStorage, StorageHandle};
#[allow(unused_imports)]
pub use keys::KeyBuilder;
