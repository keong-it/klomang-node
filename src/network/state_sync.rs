use crate::network::protocol::{SyncRequest, SyncResponse};
use crate::network::manager::NetworkManager;
use crate::state::KlomangStateManager;
use crate::storage::db::StorageHandle;
use klomang_core::{Hash, BlockHeader};
use libp2p::PeerId;
use std::collections::{HashMap, BTreeMap};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// State Fetch Manager for partial state synchronization
pub struct StateFetchManager {
    network_manager: Arc<RwLock<NetworkManager>>,
    state_manager: Arc<RwLock<KlomangStateManager>>,
    storage: StorageHandle,
    /// Tracks locally available key ranges: (start_key, end_key) -> true
    local_ranges: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    /// In-memory cache for on-demand retrieved state: key -> (value, proof)
    state_cache: Mutex<HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>>,
    /// Pending fetches: key -> requesting_peer
    pending_fetches: Mutex<HashMap<Vec<u8>, PeerId>>,
    /// Cache TTL in seconds
    cache_ttl: u64,
}

impl StateFetchManager {
    pub fn new(
        network_manager: Arc<RwLock<NetworkManager>>,
        state_manager: Arc<RwLock<KlomangStateManager>>,
        storage: StorageHandle,
    ) -> Self {
        StateFetchManager {
            network_manager,
            state_manager,
            storage,
            local_ranges: Mutex::new(BTreeMap::new()),
            state_cache: Mutex::new(HashMap::new()),
            pending_fetches: Mutex::new(HashMap::new()),
            cache_ttl: 300, // 5 minutes
        }
    }

    /// Check if a key range is locally available
    pub async fn is_range_available(&self, start_key: &[u8], end_key: &[u8]) -> bool {
        let local_ranges = self.local_ranges.lock().await;
        // Check if there's any local range that covers the requested range
        for (local_start, local_end) in local_ranges.iter() {
            if local_start.as_slice() <= start_key && local_end.as_slice() >= end_key {
                return true;
            }
        }
        false
    }

    /// Mark a key range as locally available
    pub async fn mark_range_available(&self, start_key: Vec<u8>, end_key: Vec<u8>) {
        let mut local_ranges = self.local_ranges.lock().await;
        local_ranges.insert(start_key, end_key);
    }

    /// Fetch state proof for a specific key on-demand
    pub async fn fetch_state_proof(&self, key: &[u8], current_block_header: &BlockHeader) -> Result<(Vec<u8>, Vec<u8>), String> {
        // Check cache first
        {
            let cache = self.state_cache.lock().await;
            if let Some((value, proof)) = cache.get(key) {
                return Ok((value.clone(), proof.clone()));
            }
        }

        // Check if already pending
        {
            let pending = self.pending_fetches.lock().await;
            if pending.contains_key(key) {
                return Err("Fetch already in progress".to_string());
            }
        }

        // Select best peer for state proof request
        let peers = {
            let nm = self.network_manager.read().unwrap();
            nm.get_top_sync_peers()
        };

        if peers.is_empty() {
            return Err("No peers available for state proof".to_string());
        }

        // Mark as pending
        {
            let mut pending = self.pending_fetches.lock().await;
            pending.insert(key.to_vec(), peers[0].clone());
        }

        // Send request to first peer
        let request = SyncRequest::GetStateProof { key: key.to_vec() };
        let response = {
            let mut nm = self.network_manager.write().unwrap();
            nm.send_sync_request(&peers[0], request).await
        };

        // Remove from pending
        {
            let mut pending = self.pending_fetches.lock().await;
            pending.remove(key);
        }

        match response {
            Ok(SyncResponse::StateProof(proof_data)) => {
                // Verify proof against current block's state root
                let state_root = current_block_header.verkle_root;
                let (value, proof) = self.parse_and_verify_proof(&proof_data, key, &state_root)?;

                // Cache the result
                {
                    let mut cache = self.state_cache.lock().await;
                    cache.insert(key.to_vec(), (value.clone(), proof.clone()));
                }

                Ok((value, proof))
            }
            Ok(SyncResponse::NotFound) => Err("State proof not found".to_string()),
            Ok(SyncResponse::RateLimited) => Err("Peer rate limited".to_string()),
            Ok(SyncResponse::Busy) => Err("Peer busy".to_string()),
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => {
                log::warn!("[STATE_SYNC] State proof request failed: {}", e);
                Err(format!("Request failed: {}", e))
            }
        }
    }

    /// Fetch state proof range for partial state sync
    pub async fn fetch_state_proof_range(&self, start_key: &[u8], end_key: &[u8], current_block_header: &BlockHeader) -> Result<Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>, String> {
        let peers = {
            let nm = self.network_manager.read().unwrap();
            nm.get_top_sync_peers()
        };

        if peers.is_empty() {
            return Err("No peers available for state proof range".to_string());
        }

        let request = SyncRequest::GetStateProofRange {
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
        };

        let response = {
            let mut nm = self.network_manager.write().unwrap();
            nm.send_sync_request(&peers[0], request).await
        };

        match response {
            Ok(SyncResponse::StateProofRange(proofs)) => {
                let mut verified_proofs = Vec::new();
                let state_root = current_block_header.verkle_root;

                for (key, proof_data) in proofs {
                    match self.parse_and_verify_proof(&proof_data, &key, &state_root) {
                        Ok((value, proof)) => {
                            verified_proofs.push((key, value, proof));
                        }
                        Err(e) => {
                            log::warn!("[STATE_SYNC] Proof verification failed for key: {}", e);
                            return Err(format!("Invalid proof in range: {}", e));
                        }
                    }
                }

                // Mark range as available
                self.mark_range_available(start_key.to_vec(), end_key.to_vec()).await;

                Ok(verified_proofs)
            }
            Ok(SyncResponse::NotFound) => Err("State proof range not found".to_string()),
            Ok(SyncResponse::RateLimited) => Err("Peer rate limited".to_string()),
            Ok(SyncResponse::Busy) => Err("Peer busy".to_string()),
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => {
                log::warn!("[STATE_SYNC] State proof range request failed: {}", e);
                Err(format!("Request failed: {}", e))
            }
        }
    }

    /// Parse and verify proof data
    fn parse_and_verify_proof(&self, proof_data: &[u8], key: &[u8], expected_root: &Hash) -> Result<(Vec<u8>, Vec<u8>), String> {
        // Parse proof data (simplified - in practice, deserialize Verkle proof)
        // For now, assume proof_data contains (value, proof_bytes)
        if proof_data.len() < 32 {
            return Err("Invalid proof data length".to_string());
        }

        let value_len = proof_data[0] as usize;
        if proof_data.len() < 1 + value_len + 32 {
            return Err("Invalid proof data structure".to_string());
        }

        let value = proof_data[1..1+value_len].to_vec();
        let proof = proof_data[1+value_len..].to_vec();

        // Verify proof using klomang-core
        // This would use the actual verify_proof function from Verkle tree
        let sm = self.state_manager.read().unwrap();
        match sm.verify_state_proof(key, &value, &proof, expected_root) {
            Ok(true) => Ok((value, proof)),
            Ok(false) => Err("Proof verification failed".to_string()),
            Err(e) => Err(format!("Proof verification error: {}", e)),
        }
    }

    /// Get cached state value if available
    pub async fn get_cached_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cache = self.state_cache.lock().await;
        cache.get(key).map(|(value, _)| value.clone())
    }

    /// Clear expired cache entries
    pub async fn cleanup_cache(&self) {
        // In a real implementation, track timestamps and remove expired entries
        // For now, this is a placeholder
    }
}