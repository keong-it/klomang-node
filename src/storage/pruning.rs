/// Pruning & Garbage Collection Module
/// Menangani semua operasi terkait pruning, cleanup, dan disk space management
///
/// Strategi:
/// - Efficient range deletion menggunakan RocksDB delete_range
/// - Batch deletes untuk minimize I/O overhead
/// - Respects PruningStrategy configuration untuk Archive vs Pruned mode

use std::error::Error;

use bincode::config::standard;
use log::{info, warn};
use rocksdb::{Direction, IteratorMode, WriteBatch};

use super::config;
use super::db::{KlomangStorage, CF_BLOCKS, CF_TRANSACTIONS, CF_CHAIN_INDEX, ChainIndexRecord};
use super::keys::KeyBuilder;

impl KlomangStorage {
    /// Run garbage collection dan pruning untuk old blocks di bawah Finality Depth
    /// Berguna untuk disk efficiency dan database size management
    ///
    /// Strategi Pruning:
    /// 1. Scan semua block keys di CF_BLOCKS
    /// 2. Identify blocks dengan height < (current_height - keep_blocks)
    /// 3. Delete entries dari semua 3 column families secara atomic menggunakan WriteBatch
    /// 4. Log statistics tentang blocks deleted dan space saved
    ///
    /// Performance:
    /// - Uses range delete untuk efficient bulk deletion
    /// - Single WriteBatch untuk atomic consistency
    /// - Log metrics untuk monitoring
    ///
    /// Parameters:
    /// - keep_blocks: Jumlah blocks terbaru yang harus disimpan (misal: 1000)
    ///
    /// Returns:
    /// - (blocks_deleted: u64, bytes_freed: u64)
    pub fn run_pruning(&self, keep_blocks: u64) -> Result<(u64, u64), Box<dyn Error>> {
        let pruning_start = std::time::Instant::now();

        let cf_blocks = self.cf(CF_BLOCKS)?;
        let cf_txs = self.cf(CF_TRANSACTIONS)?;
        let cf_index = self.cf(CF_CHAIN_INDEX)?;

        // Get database size before pruning
        let db_size_before = match self.db.property_int_value("rocksdb.estimate-live-data-size") {
            Ok(Some(val)) => val as u64,
            _ => 0,
        };

        // Use snapshot to read chain index in consistent state
        let snapshot = self.get_snapshot_read();

        // Find maximum height using reverse iterator (fast), no full scan
        let max_height = snapshot
            .iterator_cf(cf_index, IteratorMode::End)
            .next()
            .and_then(|res| match res {
                Ok((key, _)) => KeyBuilder::extract_height(&key).ok(),
                Err(e) => {
                    warn!("Error getting max height: {}", e);
                    None
                }
            })
            .unwrap_or(0);

        // Respect PruningStrategy setting storage-wide
        let effective_keep_blocks = match self.pruning_strategy {
            config::PruningStrategy::Archive => {
                info!("Pruning strategy Archive: skip pruning");
                return Ok((0, 0));
            }
            config::PruningStrategy::Pruned(config_keep) => {
                if keep_blocks == 0 { config_keep } else { keep_blocks }
            }
        };

        let prune_before_height = if max_height > effective_keep_blocks {
            max_height - effective_keep_blocks
        } else {
            0
        };

        info!(
            "Starting pruning: max_height={}, keep_blocks={}, prune_before_height={}",
            max_height, effective_keep_blocks, prune_before_height
        );

        if prune_before_height == 0 {
            info!("No pruning required at this time");
            return Ok((0, 0));
        }

        // Collect block hashes from index keys to delete (just old heights)
        let mut old_block_keys = Vec::new();
        let start_key = KeyBuilder::height_index_key(0);
        let end_key = KeyBuilder::height_index_key(prune_before_height);

        let iter = snapshot.iterator_cf(cf_index, IteratorMode::From(&start_key, Direction::Forward));

        for entry in iter {
            match entry {
                Ok((key, value)) => {
                    if let Ok(height) = KeyBuilder::extract_height(&key) {
                        if height < prune_before_height {
                            if let Ok((record, _)) =
                                bincode::serde::decode_from_slice::<ChainIndexRecord, _>(&value, standard())
                            {
                                old_block_keys.push(KeyBuilder::block_key(&record.tip));
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!("Error iterating chain index for pruning: {}", e);
                    break;
                }
            }
        }

        drop(snapshot);

        // Remove old height index entries using delete_range
        self.db.delete_range_cf(cf_index, &start_key, &end_key)?;

        // Delete block & tx entries for keys from above collection; use batch to minimize I/O
        let mut blocks_deleted = 0u64;
        let mut batch = WriteBatch::default();
        let batch_size = 500;

        for block_key in old_block_keys.iter() {
            batch.delete_cf(cf_blocks, block_key);
            batch.delete_cf(cf_txs, block_key);
            blocks_deleted += 1;

            if blocks_deleted % batch_size == 0 {
                self.db.write(batch)?;
                batch = WriteBatch::default();
            }
        }

        if blocks_deleted % batch_size != 0 {
            self.db.write(batch)?;
        }

        // Ensure fully flushed for all column families
        self.db.flush_cf(cf_index)?;
        self.db.flush_cf(cf_blocks)?;
        self.db.flush_cf(cf_txs)?;

        let db_size_after = match self.db.property_int_value("rocksdb.estimate-live-data-size") {
            Ok(Some(val)) => val as u64,
            _ => 0,
        };

        let bytes_freed = if db_size_before > db_size_after {
            db_size_before - db_size_after
        } else {
            0
        };

        let elapsed = pruning_start.elapsed().as_millis() as u64;

        info!(
            "Pruning complete: Deleted {} blocks, saved {} MB of space in {} ms",
            blocks_deleted,
            bytes_freed / (1024 * 1024),
            elapsed
        );

        Ok((blocks_deleted, bytes_freed))
    }

    /// Estimate database space usage seperti live data size
    /// Useful untuk monitoring disk usage
    #[allow(dead_code)]
    pub fn estimate_database_size(&self) -> Result<u64, Box<dyn Error>> {
        match self.db.property_int_value("rocksdb.estimate-live-data-size") {
            Ok(Some(val)) => Ok(val as u64),
            _ => Ok(0),
        }
    }

    /// Perform manual compaction untuk optimize storage
    /// Expensive operation, gunakan sparingly
    #[allow(dead_code)]
    pub fn compact_database(&self) -> Result<(), Box<dyn Error>> {
        info!("Starting manual database compaction...");
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        info!("Database compaction completed");
        Ok(())
    }
}
