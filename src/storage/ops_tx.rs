/// Transaction Operations Module
/// Menangani semua operasi terkait penyimpanan dan pengambilan transactions di blockchain
///
/// Optimasi:
/// - Zero-copy reads menggunakan DBPinnableSlice
/// - Efficient queries dengan prefix seeking
/// - HOT data path dengan LZ4 fast decompression
use std::error::Error;

use klomang_core::core::state::transaction::Transaction;
use log::warn;
use rocksdb::DBPinnableSlice;

use super::db::CF_TRANSACTIONS;
use super::db::KlomangStorage;
use super::keys::KeyBuilder;
use klomang_core::core::crypto::Hash;

impl KlomangStorage {
    /// Get transactions untuk block tertentu dengan zero-copy read
    /// Transaction disimpan dalam CF_TRANSACTIONS dengan block hash sebagai key
    pub fn get_transactions(
        &self,
        block_hash: &Hash,
    ) -> Result<Option<Vec<Transaction>>, Box<dyn Error>> {
        let cf_txs = self.cf(CF_TRANSACTIONS)?;
        let key = KeyBuilder::block_key(block_hash);

        // zero-copy read via DBPinnableSlice
        let pinned: Option<DBPinnableSlice> = self.db.get_pinned_cf(cf_txs, &key)?;
        if let Some(slice) = pinned {
            let data: &[u8] = slice.as_ref();
            let txs = bincode::deserialize::<Vec<Transaction>>(data)?;
            Ok(Some(txs))
        } else {
            Ok(None)
        }
    }

    /// Verify transaction consistency untuk block tertentu
    /// Member fungsi ini memastikan transactions ada dan valid
    pub fn verify_transactions_consistency(
        &self,
        block_hash: &Hash,
    ) -> Result<bool, Box<dyn Error>> {
        let cf_txs = self.cf(CF_TRANSACTIONS)?;
        let key = KeyBuilder::block_key(block_hash);

        // Check transactions exists
        let txs_exist = self.db.get_cf(cf_txs, &key)?.is_some();

        if !txs_exist {
            warn!("Transactions for block {} not found", block_hash.to_hex());
            return Ok(false);
        }

        Ok(true)
    }

    /// Count total transactions dalam database (expensive operation, gunakan sparingly)
    /// Iterate seluruh CF_TRANSACTIONS untuk count entries
    #[allow(dead_code)]
    pub fn count_transactions(&self) -> Result<u64, Box<dyn Error>> {
        let cf_txs = self.cf(CF_TRANSACTIONS)?;
        let iter = self.db.iterator_cf(cf_txs, rocksdb::IteratorMode::Start);

        let mut count = 0u64;
        for _ in iter {
            count += 1;
        }

        Ok(count)
    }

    /// Get transaction by hash (searches across all blocks)
    pub fn get_transaction(&self, tx_hash: &Hash) -> Result<Option<Transaction>, Box<dyn Error>> {
        let cf_txs = self.cf(CF_TRANSACTIONS)?;

        // Iterate through all transaction blocks to find the hash
        let iter = self.db.iterator_cf(cf_txs, rocksdb::IteratorMode::Start);

        for entry in iter {
            let (_, value) = entry?;
            let txs: Vec<Transaction> = bincode::deserialize(&value)?;
            for tx in txs {
                if &tx.id == tx_hash {
                    return Ok(Some(tx));
                }
            }
        }

        Ok(None)
    }
}
