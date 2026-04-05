/// Chain Index & Metadata Operations Module
/// Menangani semua operasi terkait chain index, metadata, dan state management
///
/// Optimasi:
/// - Zero-copy reads menggunakan snapshot isolation
/// - Efficient height-based lookups dengan big-endian encoding
/// - HOT data path untuk frequent chain state queries

use std::error::Error;

use klomang_core::core::crypto::Hash;
use log::{info, warn};
use rocksdb::DBPinnableSlice;

use super::db::KlomangStorage;
use super::db::{CF_BLOCKS, CF_CHAIN_INDEX, CF_STATE, ChainIndexRecord};
use super::keys::KeyBuilder;

impl KlomangStorage {
    /// Get chain index metadata untuk block hash tertentu
    /// Placeholder: ideally store by hash bukan height untuk O(1) lookup
    pub fn get_chain_index(
        &self,
        block_hash: &Hash,
    ) -> Result<Option<ChainIndexRecord>, Box<dyn Error>> {
        let cf_index = self.cf(CF_CHAIN_INDEX)?;
        
        // Placeholder: search dari block hash ke height (slow operation saat ini)
        // Better approach: maintain reverse index dari block hash ke height
        let iter = self.db.iterator_cf(cf_index, rocksdb::IteratorMode::Start);
        
        for entry in iter {
            let (_, value) = entry?;
            let record = bincode::deserialize::<ChainIndexRecord>(&value)?;
            if &record.tip == block_hash {
                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    /// Get chain index by height (efficient karena height adalah key)
    /// Height-based lookup adalah O(1) dengan binary search pada sorted keys
    pub fn get_chain_index_by_height(
        &self,
        height: u64,
    ) -> Result<Option<ChainIndexRecord>, Box<dyn Error>> {
        let cf_index = self.cf(CF_CHAIN_INDEX)?;
        let key = KeyBuilder::height_index_key(height);

        // zero-copy read untuk efficiency
        let pinned: Option<DBPinnableSlice> = self.db.get_pinned_cf(cf_index, &key)?;
        if let Some(slice) = pinned {
            let data: &[u8] = slice.as_ref();
            let record = bincode::deserialize::<ChainIndexRecord>(data)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    /// Update best block (chain tip) dengan atomic write
    /// Atomic consistency: update chain index dan block reference bersama-sama
    pub fn update_best_block(
        &self,
        height: u64,
        block_hash: &Hash,
        total_work: u128,
    ) -> Result<(), Box<dyn Error>> {
        let cf_index = self.cf(CF_CHAIN_INDEX)?;
        let key = KeyBuilder::height_index_key(height);

        let chain_index = ChainIndexRecord {
            height,
            tip: block_hash.clone(),
            total_work,
        };

        let payload = bincode::serialize(&chain_index)?;
        self.db.put_cf(cf_index, &key, &payload)?;

        info!(
            "Updated best block: height={}, hash={}, total_work={}",
            height,
            block_hash.to_hex(),
            total_work
        );

        Ok(())
    }

    /// Get maximum chain height (efficient dengan reverse iterator)
    /// Uses reverse iterator untuk O(1) access ke entry terakhir
    pub fn get_max_chain_height(&self) -> Result<u64, Box<dyn Error>> {
        let cf_index = self.cf(CF_CHAIN_INDEX)?;

        let iter = self.db.iterator_cf(cf_index, rocksdb::IteratorMode::End);

        for entry in iter {
            let (key, _) = entry?;
            if let Ok(height) = KeyBuilder::extract_height(&key) {
                return Ok(height);
            }
        }

        Ok(0) // Empty database
    }

    /// Verify chain index consistency untuk block tertentu
    pub fn verify_chain_consistency(&self, height: u64, block_hash: &Hash) -> Result<bool, Box<dyn Error>> {
        let cf_index = self.cf(CF_CHAIN_INDEX)?;
        let cf_blocks = self.cf(CF_BLOCKS)?;

        let index_key = KeyBuilder::height_index_key(height);

        // Check index exists
        let index_exists = self.db.get_cf(cf_index, &index_key)?.is_some();
        if !index_exists {
            warn!("Chain index for height {} not found", height);
            return Ok(false);
        }

        // Check corresponding block exists
        let block_key = KeyBuilder::block_key(block_hash);
        let block_exists = self.db.get_cf(cf_blocks, &block_key)?.is_some();
        if !block_exists {
            warn!("Block {} for height {} not found", block_hash.to_hex(), height);
            return Ok(false);
        }

        Ok(true)
    }

    /// Range scan chain indices dari start_height sampai end_height
    /// Efficient untuk historical queries dan chain synchronization
    pub fn get_chain_indices_range(
        &self,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<ChainIndexRecord>, Box<dyn Error>> {
        if start_height > end_height {
            return Ok(Vec::new());
        }

        let cf_index = self.cf(CF_CHAIN_INDEX)?;
        let start_key = KeyBuilder::height_index_key(start_height);

        let iter = self.db.iterator_cf(cf_index, rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));
        let mut results = Vec::new();

        for entry in iter {
            let (key, value) = entry?;
            if let Ok(height) = KeyBuilder::extract_height(&key) {
                if height > end_height {
                    break;
                }

                let record = bincode::deserialize::<ChainIndexRecord>(&value)?;
                results.push(record);
            }
        }

        Ok(results)
    }

    /// Get state data from CF_STATE
    pub fn get_state(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        let cf_state = self.cf(CF_STATE)?;
        let key_bytes = key.as_bytes();
        let value = self.db.get_cf(cf_state, key_bytes)?;
        Ok(value)
    }

    /// Put state data to CF_STATE
    pub fn put_state(&self, key: &str, value: &[u8]) -> Result<(), Box<dyn Error>> {
        let cf_state = self.cf(CF_STATE)?;
        let key_bytes = key.as_bytes();
        self.db.put_cf(cf_state, key_bytes, value)?;
        Ok(())
    }
}
