/// KlomangStorage - Blockchain Storage Engine Core
/// Menyediakan durability, consistency guarantee, dan thread-safe access
///
/// Modul operasi dispisahkan untuk Single Responsibility Principle:
/// - ops_block.rs: Block save/read operations
/// - ops_tx.rs: Transaction operations
/// - ops_chain.rs: Chain index & metadata operations
/// - pruning.rs: Pruning & garbage collection
/// - stats.rs: Monitoring & statistics
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};

use log::{error, info, warn};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, DBCompactionStyle, Options, SliceTransform, Snapshot,
};

use super::config;
use super::keys::CURRENT_DB_VERSION;
use super::keys::KeyBuilder;

/// StorageHandle adalah Arc<RwLock<KlomangStorage>> untuk thread-safe access
/// Memungkinkan multiple threads untuk read concurrent dan exclusive write
pub type StorageHandle = Arc<RwLock<KlomangStorage>>;

pub const CF_BLOCKS: &str = "Blocks";
pub const CF_TRANSACTIONS: &str = "Transactions";
pub const CF_CHAIN_INDEX: &str = "ChainIndex";
pub const CF_STATE: &str = "State"; // Verkle Tree and UTXO commitment storage

/// StorageStats menyimpan performance metrics dari RocksDB untuk monitoring dan tuning
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StorageStats {
    /// Jumlah operasi baca per detik
    pub reads_per_sec: u64,
    /// Jumlah operasi tulis per detik
    pub writes_per_sec: u64,
    /// Hit rate cache dalam persen (0-100)
    pub cache_hit_rate: f64,
    /// Waktu total compaction dalam milliseconds
    pub compaction_time_ms: u64,
    /// Jumlah SST files yang aktif
    pub num_sst_files: u32,
    /// Ukuran database aktual dalam bytes
    pub db_size_bytes: u64,
}

/// ChainIndexRecord menyimpan metadata chain height dengan tip dan total work
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ChainIndexRecord {
    pub height: u64,
    pub tip: klomang_core::core::crypto::Hash,
    pub total_work: u128,
}

/// KlomangStorage adalah blockchain storage engine dengan durability & consistency guarantee
/// Menggunakan RocksDB dengan optimisasi untuk blockchain use case
///
/// Untuk operations, lihat module terpisah:
/// - ops_block, ops_tx, ops_chain, pruning, stats
pub struct KlomangStorage {
    pub(crate) db: DB,
    pub(crate) pruning_strategy: config::PruningStrategy,
}

impl KlomangStorage {
    /// Open atau create KlomangStorage di path yang diberikan
    /// Mengatur semua options untuk production-grade durability & performance
    /// Menerima pruning_strategy untuk mengontrol disk usage
    ///
    /// Configurasi:
    /// - COLD CF (Blocks): ZSTD compression, disk-optimized
    /// - HOT CF (Transactions & ChainIndex): LZ4 compression, fast access
    /// - Write protection: 128MB memtables x4, Level0 management
    pub fn open(
        path: impl AsRef<Path>,
        pruning_strategy: config::PruningStrategy,
    ) -> Result<Self, Box<dyn Error>> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let mut options = Options::default();

        // Durability & WAL Control
        options.set_use_fsync(true);
        options.set_bytes_per_sync(1048576);
        options.set_wal_ttl_seconds(0);

        // Level-Style Compaction (Blockchain Optimized)
        options.set_compaction_style(DBCompactionStyle::Level);
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_max_bytes_for_level_base(268435456); // 256MB
        options.set_target_file_size_base(67108864); // 64MB

        // Resource Management
        options.set_max_open_files(1000);

        // Write Stall Protection & Memtable Tuning untuk EXTREME blockchain loads
        config::configure_write_stall_protection(&mut options);

        // Background Jobs & Threading Tuning
        config::configure_background_jobs(&mut options);

        // Metrics & Monitoring Configuration
        config::configure_metrics(&mut options);

        // WAL Limits Configuration
        config::configure_wal_limits(&mut options);

        // Set prefix extractor untuk block prefix optimization
        options.set_prefix_extractor(SliceTransform::create_fixed_prefix(1));

        // BlockBasedOptions dengan cache & bloom filter tuning
        let block_opts = config::build_block_based_options();
        options.set_block_based_table_factory(&block_opts);

        // Basic setup
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Column Family Configurations dengan Hot/Cold Data Separation

        // Default CF
        let mut cf_default_opts = Options::default();
        cf_default_opts.create_if_missing(true);

        // COLD CF (Blocks): ZSTD compression, larger files, standard bloom filter
        let mut cf_blocks_opts = Options::default();
        cf_blocks_opts.create_if_missing(true);
        config::configure_write_stall_protection(&mut cf_blocks_opts);
        config::configure_cold_cf_options(&mut cf_blocks_opts);
        let cold_block_opts = config::build_cold_data_options();
        cf_blocks_opts.set_block_based_table_factory(&cold_block_opts);
        info!("Configured CF_BLOCKS (COLD data): ZSTD compression, optimized for disk efficiency");

        // HOT CF (Transactions): LZ4 compression, smaller files, aggressive bloom filter
        let mut cf_txs_opts = Options::default();
        cf_txs_opts.create_if_missing(true);
        config::configure_write_stall_protection(&mut cf_txs_opts);
        config::configure_hot_cf_options(&mut cf_txs_opts);
        let hot_block_opts = config::build_hot_data_options();
        cf_txs_opts.set_block_based_table_factory(&hot_block_opts);
        info!("Configured CF_TRANSACTIONS (HOT data): LZ4 compression, optimized for fast access");

        // HOT CF (ChainIndex): LZ4 compression, smaller files, aggressive bloom filter
        let mut cf_index_opts = Options::default();
        cf_index_opts.create_if_missing(true);
        config::configure_write_stall_protection(&mut cf_index_opts);
        config::configure_hot_cf_options(&mut cf_index_opts);
        let hot_index_block_opts = config::build_hot_data_options();
        cf_index_opts.set_block_based_table_factory(&hot_index_block_opts);
        info!(
            "Configured CF_CHAIN_INDEX (HOT data): LZ4 compression, optimized for fast chain state access"
        );

        // WARM CF (State): Contains Verkle tree and UTXO commitment data
        // Uses ZSTD for good compression (not as frequent as HOT), larger cache than COLD
        let mut cf_state_opts = Options::default();
        cf_state_opts.create_if_missing(true);
        config::configure_write_stall_protection(&mut cf_state_opts);
        config::configure_cold_cf_options(&mut cf_state_opts); // Use ZSTD compression
        let state_block_opts = config::build_cold_data_options(); // 128MB cache for state access
        // Override cache for state to be larger than regular cold data
        cf_state_opts.set_block_based_table_factory(&state_block_opts);
        info!(
            "Configured CF_STATE (WARM data): ZSTD compression, optimized for Verkle tree and state commitment persistence"
        );

        let cfs = vec![
            ColumnFamilyDescriptor::new("default", cf_default_opts),
            ColumnFamilyDescriptor::new(CF_BLOCKS, cf_blocks_opts),
            ColumnFamilyDescriptor::new(CF_TRANSACTIONS, cf_txs_opts),
            ColumnFamilyDescriptor::new(CF_CHAIN_INDEX, cf_index_opts),
            ColumnFamilyDescriptor::new(CF_STATE, cf_state_opts),
        ];

        let db = DB::open_cf_descriptors(&options, path, cfs).map_err(|e| {
            error!("Failed to open database at {}: {}", path.display(), e);
            e
        })?;

        info!("KlomangStorage opened successfully at: {}", path.display());
        info!("Blockchain Storage Architecture:");
        info!("  - COLD Data (Blocks CF): ZSTD compression, 64MB cache, 32KB block size");
        info!(
            "  - HOT Data (Transactions CF): LZ4 compression, 256MB cache, aggressive Bloom filter"
        );
        info!(
            "  - HOT Data (ChainIndex CF): LZ4 compression, 256MB cache, aggressive Bloom filter"
        );
        info!(
            "  - WARM Data (State CF): ZSTD compression, 128MB cache for Verkle tree & UTXO commitment"
        );
        info!("  - Write Protection: 128MB memtables x4, Level0 slowdown@20, stop@36");

        // Create storage dan perform schema validation
        let storage = Self {
            db,
            pruning_strategy,
        };
        storage.validate_schema()?;

        Ok(storage)
    }

    /// Validate database schema version untuk ensure compatibility
    fn validate_schema(&self) -> Result<(), Box<dyn Error>> {
        let cf_default = self.cf("default")?;
        let version_key = KeyBuilder::db_version_key();

        match self.db.get_cf(cf_default, &version_key)? {
            Some(version_bytes) => {
                let stored_version = u32::from_le_bytes(
                    version_bytes[..4]
                        .try_into()
                        .map_err(|_| "Invalid version bytes in metadata")?,
                );

                info!(
                    "Found database version: {}, current app version: {}",
                    stored_version, CURRENT_DB_VERSION
                );

                if stored_version > CURRENT_DB_VERSION {
                    let msg = format!(
                        "Database version mismatch: stored version {} is newer than app version {}. Please upgrade klomang-node.",
                        stored_version, CURRENT_DB_VERSION
                    );
                    error!("{}", msg);
                    return Err(msg.into());
                }

                if stored_version < CURRENT_DB_VERSION {
                    let msg = format!(
                        "Database schema migration required: stored version {} -> app version {}. Please run migration tool.",
                        stored_version, CURRENT_DB_VERSION
                    );
                    warn!("{}", msg);
                    return Err(msg.into());
                }

                info!("Schema validation passed");
                Ok(())
            }
            None => {
                // New database, initialize with current version
                info!(
                    "New database detected, initializing with schema version {}",
                    CURRENT_DB_VERSION
                );

                let version_bytes = CURRENT_DB_VERSION.to_le_bytes();
                self.db.put_cf(cf_default, &version_key, &version_bytes)?;

                info!("Database schema version initialized successfully");
                Ok(())
            }
        }
    }

    /// Get ColumnFamily handle dengan error handling yang proper
    pub(crate) fn cf(&self, name: &str) -> Result<&ColumnFamily, Box<dyn Error>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| format!("Column family '{}' not found", name).into())
    }

    /// Get snapshot untuk consistent point-in-time reads
    /// Mencegah dirty reads dan race condition saat data sedang di-update
    pub fn get_snapshot_read(&self) -> Snapshot<'_> {
        self.db.snapshot()
    }

    /// Open dengan automatic recovery jika database corrupt
    /// Ini adalah entry point yang recommended untuk production nodes
    pub fn open_with_recovery(
        path: impl AsRef<Path>,
        pruning_strategy: config::PruningStrategy,
    ) -> Result<Self, Box<dyn Error>> {
        let path = path.as_ref();
        info!("Attempting to open KlomangStorage at: {}", path.display());

        // Attempt normal open first
        match Self::open(&path, pruning_strategy) {
            Ok(storage) => {
                info!("Successfully opened KlomangStorage");
                return Ok(storage);
            }
            Err(e) => {
                error!("Failed to open KlomangStorage: {}", e);
                warn!("Attempting automatic database repair...");

                // Attempt repair
                match DB::repair(&Options::default(), &path) {
                    Ok(()) => {
                        info!("Database repair completed successfully");

                        // Retry open after repair
                        match Self::open(&path, pruning_strategy) {
                            Ok(storage) => {
                                info!("Successfully opened KlomangStorage after repair");
                                return Ok(storage);
                            }
                            Err(retry_err) => {
                                error!("Failed to open KlomangStorage after repair: {}", retry_err);
                                return Err(format!(
                                    "Database recovery failed. Original error: {}, Retry error: {}",
                                    e, retry_err
                                )
                                .into());
                            }
                        }
                    }
                    Err(repair_err) => {
                        error!("Database repair failed: {}", repair_err);
                        return Err(format!(
                            "Cannot open database and repair failed. Original error: {}, Repair error: {}",
                            e, repair_err
                        )
                        .into());
                    }
                }
            }
        }
    }

    /// Graceful shutdown untuk minimize corruption risk saat node restart
    pub fn shutdown(&self) -> Result<(), Box<dyn Error>> {
        info!("Initiating graceful shutdown of KlomangStorage");
        self.db.flush()?;
        info!("Database flush completed");
        Ok(())
    }

    /// Graceful close (deprecated, untuk backward compatibility)
    #[allow(dead_code)]
    pub fn close(self) -> Result<(), Box<dyn Error>> {
        self.shutdown()?;
        drop(self.db);
        info!("Database closed successfully");
        Ok(())
    }

    /// Create StorageHandle (Arc<RwLock<Self>>) untuk multi-threaded access
    pub fn into_handle(self) -> StorageHandle {
        Arc::new(RwLock::new(self))
    }
}
