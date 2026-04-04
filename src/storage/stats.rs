/// Storage Statistics & Monitoring Module
/// Menangani semua operasi terkait metrics collection, monitoring, dan health checks
///
/// Optimasi:
/// - Efficient property queries dari RocksDB
/// - Non-blocking stats collection
/// - Production-grade monitoring support

use std::error::Error;

use log::info;

use super::db::{KlomangStorage, StorageStats};

impl KlomangStorage {
    /// Get storage statistics untuk monitoring dan performance tuning
    /// Mengekspor metrics dari RocksDB internal statistics
    ///
    /// Metrics yang dikembalikan:
    /// - reads_per_sec: Throughput operasi baca
    /// - writes_per_sec: Throughput operasi tulis
    /// - cache_hit_rate: Efektivitas LRU cache (percentage 0-100)
    /// - compaction_time_ms: Total waktu compaction
    /// - num_sst_files: Jumlah SST files aktif
    /// - db_size_bytes: Ukuran database
    ///
    /// Note: reads_per_sec dan writes_per_sec memerlukan continuous monitoring
    /// di production untuk akurat; saat ini placeholder
    pub fn get_storage_stats(&self) -> Result<StorageStats, Box<dyn Error>> {
        // Estimate dari RocksDB properties jika stats tidak tersedia
        // In real production, bisa menggunakan continuous monitoring untuk detailed metrics
        
        // Try to get SST file count
        let num_sst_files = match self.db.property_int_value("rocksdb.num-files-at-level0") {
            Ok(Some(val)) => val as u32,
            _ => 0,
        };

        // Try to get approximate size
        let db_size_bytes = match self.db.property_int_value("rocksdb.estimate-live-data-size") {
            Ok(Some(val)) => val as u64,
            _ => 0,
        };

        // Cache hit rate: estimate dari rocksdb stats
        // In production dengan enable_statistics, bisa get dari rocksdb::Statistics
        let cache_hit_rate = 0.0; // Placeholder for actual measurement

        let stats = StorageStats {
            reads_per_sec: 0,     // Would need continuous monitoring
            writes_per_sec: 0,    // Would need continuous monitoring
            cache_hit_rate,
            compaction_time_ms: 0, // Would need statistics tracking
            num_sst_files,
            db_size_bytes,
        };

        info!(
            "Storage stats - Files: {}, Size: {} bytes",
            stats.num_sst_files, stats.db_size_bytes
        );

        Ok(stats)
    }

    /// Get detailed RocksDB property untuk specific metric
    /// Property names: https://github.com/facebook/rocksdb/wiki/Properties
    pub fn get_db_property(&self, property_name: &str) -> Result<Option<String>, Box<dyn Error>> {
        Ok(self.db.property_value(property_name)?)
    }

    /// Get integer RocksDB property untuk numeric metrics
    pub fn get_db_property_int(&self, property_name: &str) -> Result<Option<u64>, Box<dyn Error>> {
        Ok(self.db.property_int_value(property_name)?.map(|v| v as u64))
    }

    /// Get memory usage estimate dari database
    pub fn estimate_memory_usage(&self) -> Result<u64, Box<dyn Error>> {
        // Estimate memory usage dari cache misalnya
        match self.db.property_int_value("rocksdb.block-cache-usage") {
            Ok(Some(val)) => Ok(val as u64),
            _ => Ok(0),
        }
    }

    /// Perform quick health check pada database
    pub fn health_check(&self) -> Result<bool, Box<dyn Error>> {
        // Try quick read-write operations
        let cf = self.cf("default")?;

        // Test read
        let test_key = b"_health_check_test_key_";
        let test_value = b"ok";

        // Quick write test
        self.db.put_cf(cf, test_key, test_value)?;

        // Quick read test
        match self.db.get_cf(cf, test_key)? {
            Some(value) => {
                if value == test_value {
                    // Read-write works, cleanup
                    self.db.delete_cf(cf, test_key)?;
                    info!("Database health check passed");
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => Ok(false),
        }
    }

    /// Get number of files at specific compaction level
    #[allow(dead_code)]
    pub fn get_files_at_level(&self, level: u32) -> Result<u32, Box<dyn Error>> {
        let property_name = format!("rocksdb.num-files-at-level{}", level);
        match self.db.property_int_value(&property_name)? {
            Some(val) => Ok(val as u32),
            None => Ok(0),
        }
    }

    /// Get total compaction stats
    #[allow(dead_code)]
    pub fn get_compaction_stats(&self) -> Result<String, Box<dyn Error>> {
        match self.db.property_value("rocksdb.stats")? {
            Some(stats) => Ok(stats),
            None => Ok(String::from("No stats available")),
        }
    }
}
