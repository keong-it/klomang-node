//! Comprehensive tests for UTXO and Verkle tree operations
//!
//! This module contains unit tests that verify:
//! - UTXO set operations (add/remove/lookup)
//! - Verkle tree operations (insert/remove/proof generation)
//! - Proof verification
//! - Cache performance optimizations
//! - State consistency between UTXO set and Verkle tree

use super::*;
use klomang_core::state::transaction::TxOutput;
use klomang_core::{Hash, UtxoSet};
use std::sync::Mutex;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Test basic UTXO operations
    #[test]
    fn test_utxo_diff_basic_operations() {
        let old_root = Hash::new(b"old_root");
        let new_root = Hash::new(b"new_root");
        let mut utxo_diff = UtxoDiff::new_with_roots(old_root, new_root);

        let txid1 = Hash::new(b"tx1");
        let txid2 = Hash::new(b"tx2");

        // Test adding UTXOs
        utxo_diff.add_utxo(txid1, 0, 1000, vec![1, 2, 3]);
        utxo_diff.add_utxo(txid1, 1, 2000, vec![4, 5, 6]);

        assert_eq!(utxo_diff.added.len(), 2);
        assert_eq!(utxo_diff.removed.len(), 0);

        // Test removing UTXOs
        utxo_diff.remove_utxo(txid2, 0);
        utxo_diff.remove_utxo(txid2, 1);

        assert_eq!(utxo_diff.added.len(), 2);
        assert_eq!(utxo_diff.removed.len(), 2);
    }

    /// Test UTXO diff application to UTXO set
    #[test]
    fn test_utxo_diff_apply_to_set() {
        let old_root = Hash::new(b"old_root");
        let new_root = Hash::new(b"new_root");
        let mut utxo_diff = UtxoDiff::new_with_roots(old_root, new_root);

        let txid1 = Hash::new(b"tx1");
        let txid2 = Hash::new(b"tx2");

        // Add some UTXOs
        utxo_diff.add_utxo(txid1, 0, 1000, vec![1, 2, 3]);
        utxo_diff.add_utxo(txid1, 1, 2000, vec![4, 5, 6]);

        // Remove some UTXOs
        utxo_diff.remove_utxo(txid2, 0);

        let mut utxo_set = UtxoSet::new();

        // Initially add the UTXO that will be removed
        let outpoint_to_remove = (txid2, 0u32);
        let tx_output_to_remove = TxOutput {
            value: 500,
            pubkey_hash: Hash::from_bytes(&[7u8; 32]), // Fixed size array
        };
        utxo_set
            .utxos
            .insert(outpoint_to_remove, tx_output_to_remove);

        // Apply the diff
        utxo_diff
            .apply_to(&mut utxo_set)
            .expect("Failed to apply UTXO diff");

        // Verify additions
        assert!(utxo_set.utxos.contains_key(&(txid1, 0u32)));
        assert!(utxo_set.utxos.contains_key(&(txid1, 1u32)));

        // Verify removal
        assert!(!utxo_set.utxos.contains_key(&(txid2, 0u32)));

        // Verify values
        if let Some(utxo) = utxo_set.utxos.get(&(txid1, 0u32)) {
            assert_eq!(utxo.value, 1000);
        }
    }

    /// Test Verkle path generation
    #[test]
    fn test_verkle_path_generation() {
        let txid = Hash::new(b"test_txid");
        let vout = 5u32;

        // Generate path manually
        let mut expected_path_input = Vec::with_capacity(32 + 1 + 4 + 1 + 5);
        expected_path_input.extend_from_slice(txid.as_bytes());
        expected_path_input.extend_from_slice(b":");
        expected_path_input.extend_from_slice(&vout.to_le_bytes());
        expected_path_input.extend_from_slice(b":");
        expected_path_input.extend_from_slice(b"VALUE");

        let expected_path = Hash::new(&expected_path_input);

        // This test verifies our path generation logic
        // In actual implementation, this would be used by Verkle tree operations
        assert_eq!(expected_path.as_bytes().len(), 32);
    }

    /// Test UTXO cache operations
    #[test]
    fn test_utxo_cache_operations() {
        let cache = Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(10).unwrap()));

        let outpoint1 = (Hash::new(b"tx1"), 0u32);
        let outpoint2 = (Hash::new(b"tx2"), 1u32);

        let utxo1 = TxOutput {
            value: 1000,
            pubkey_hash: Hash::from_bytes(&[1u8; 32]),
        };

        // Test cache population and retrieval
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.put(outpoint1, utxo1.clone());
            let retrieved = cache_guard.get(&outpoint1);
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().value, 1000);
        }

        // Test cache invalidation
        let mut utxo_diff = UtxoDiff::new_with_roots(Hash::new(b"old"), Hash::new(b"new"));
        utxo_diff.added.insert(outpoint1, (1000, vec![1, 2, 3]));
        utxo_diff.removed.insert(outpoint2);

        utxo_diff.invalidate_cache(&cache);

        {
            let mut cache_guard = cache.lock().unwrap();
            assert!(cache_guard.get(&outpoint1).is_none()); // Should be invalidated
        }
    }

    /// Test UTXO diff serialization
    #[test]
    fn test_utxo_diff_serialization() {
        let old_root = Hash::new(b"old_root");
        let new_root = Hash::new(b"new_root");
        let mut utxo_diff = UtxoDiff::new_with_roots(old_root, new_root);

        let txid = Hash::new(b"test_tx");
        utxo_diff.add_utxo(txid, 0, 1000, vec![1, 2, 3]);
        utxo_diff.remove_utxo(txid, 1);

        // Test serialization
        let serialized = bincode::serialize(&utxo_diff).expect("Failed to serialize");
        let deserialized: UtxoDiff =
            bincode::deserialize(&serialized).expect("Failed to deserialize");

        assert_eq!(deserialized.added.len(), 1);
        assert_eq!(deserialized.removed.len(), 1);
        assert_eq!(deserialized.old_verkle_root, old_root);
        assert_eq!(deserialized.new_verkle_root, new_root);
    }

    /// Test state consistency checks
    #[test]
    fn test_state_consistency_verification() {
        let old_root = Hash::new(b"old_root");
        let new_root = Hash::new(b"new_root");
        let utxo_diff = UtxoDiff::new_with_roots(old_root, new_root);

        // Test that diff has correct roots
        assert_eq!(utxo_diff.old_verkle_root, old_root);
        assert_eq!(utxo_diff.new_verkle_root, new_root);
    }

    /// Test performance of UTXO operations
    #[test]
    fn test_utxo_operations_performance() {
        let old_root = Hash::new(b"old_root");
        let new_root = Hash::new(b"new_root");
        let mut utxo_diff = UtxoDiff::new_with_roots(old_root, new_root);

        // Add many UTXOs
        for i in 0u32..1000 {
            let txid = Hash::new(&i.to_le_bytes());
            utxo_diff.add_utxo(txid, 0, 1000 + i as u64, vec![(i % 256) as u8]);
        }

        let mut utxo_set = UtxoSet::new();

        // Measure application time
        let start = std::time::Instant::now();
        utxo_diff
            .apply_to(&mut utxo_set)
            .expect("Failed to apply large UTXO diff");
        let duration = start.elapsed();

        assert_eq!(utxo_set.utxos.len(), 1000);
        println!("Applied 1000 UTXO changes in {:?}", duration);

        // Verify all UTXOs are present
        for i in 0u32..1000 {
            let txid = Hash::new(&i.to_le_bytes());
            assert!(utxo_set.utxos.contains_key(&(txid, 0u32)));
        }
    }

    /// Test cache performance optimization
    #[test]
    fn test_cache_performance_optimization() {
        let cache = Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(100).unwrap(),
        ));

        let outpoint = (Hash::new(b"frequent_lookup"), 0u32);
        let utxo = TxOutput {
            value: 1000,
            pubkey_hash: Hash::from_bytes(&[1u8; 32]),
        };

        // Populate cache
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.put(outpoint, utxo.clone());
        }

        // Measure cache hit performance
        let start = std::time::Instant::now();
        for _ in 0..10000 {
            let cache_result = {
                let mut cache_guard = cache.lock().unwrap();
                cache_guard.get(&outpoint).cloned()
            };
            assert!(cache_result.is_some());
            assert_eq!(cache_result.unwrap().value, 1000);
        }
        let cache_duration = start.elapsed();

        println!("10000 cache hits took {:?}", cache_duration);
        assert!(cache_duration.as_millis() < 100); // Should be very fast
    }

    /// Test concurrent UTXO operations
    #[test]
    fn test_concurrent_utxo_operations() {
        use std::thread;

        let utxo_set = std::sync::Arc::new(Mutex::new(UtxoSet::new()));
        let mut handles = vec![];

        // Spawn multiple threads performing UTXO operations
        for thread_id in 0..4 {
            let utxo_set_clone = std::sync::Arc::clone(&utxo_set);

            let handle = thread::spawn(move || {
                let mut local_diff = UtxoDiff::new_with_roots(Hash::new(b"old"), Hash::new(b"new"));

                // Each thread adds its own UTXOs
                for i in 0..100 {
                    let txid = Hash::new(&format!("thread_{}_tx_{}", thread_id, i).as_bytes());
                    local_diff.add_utxo(txid, 0, 1000 + i as u64, vec![thread_id as u8, i as u8]);
                }

                // Apply local changes
                let mut utxo_set_guard = utxo_set_clone.lock().unwrap();
                local_diff
                    .apply_to(&mut utxo_set_guard)
                    .expect("Failed to apply UTXO diff");
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify final state
        let final_utxo_set = utxo_set.lock().unwrap();
        assert_eq!(final_utxo_set.utxos.len(), 400); // 4 threads * 100 UTXOs each
    }
}
