use crate::network::protocol::{StateChunk, SyncRequest, SyncResponse};
use crate::network::manager::NetworkManager;
use crate::state::KlomangStateManager;
use crate::storage::db::StorageHandle;
use klomang_core::{BlockHeader, Hash};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Chunk manager for parallel state download
pub struct ChunkManager {
    checkpoint_height: u64,
    total_chunks: u32,
    received_chunks: HashMap<u32, StateChunk>,
    pending_chunks: HashSet<u32>,
    failed_chunks: HashMap<u32, u32>, // chunk_index -> retry_count
    max_retries: u32,
    chunk_timeout: Duration,
    last_request_time: HashMap<u32, Instant>,
}

impl ChunkManager {
    pub fn new(checkpoint_height: u64, total_chunks: u32) -> Self {
        let pending_chunks = (0..total_chunks).collect();
        ChunkManager {
            checkpoint_height,
            total_chunks,
            received_chunks: HashMap::new(),
            pending_chunks,
            failed_chunks: HashMap::new(),
            max_retries: 3,
            chunk_timeout: Duration::from_secs(30),
            last_request_time: HashMap::new(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.received_chunks.len() == self.total_chunks as usize
    }

    pub fn get_pending_chunks(&self, limit: usize) -> Vec<u32> {
        self.pending_chunks
            .iter()
            .filter(|&&chunk_index| {
                !self.last_request_time.contains_key(&chunk_index) ||
                self.last_request_time[&chunk_index].elapsed() > self.chunk_timeout
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn mark_requested(&mut self, chunk_indices: &[u32]) {
        for &index in chunk_indices {
            self.last_request_time.insert(index, Instant::now());
        }
    }

    pub fn add_chunk(&mut self, chunk: StateChunk) -> Result<(), String> {
        if chunk.index >= self.total_chunks {
            return Err(format!("Invalid chunk index: {}", chunk.index));
        }

        // Verify checksum
        let computed_checksum = Hash::new(&chunk.data);
        if computed_checksum != chunk.checksum {
            self.failed_chunks.entry(chunk.index).and_modify(|e| *e += 1).or_insert(1);
            if *self.failed_chunks.get(&chunk.index).unwrap() >= self.max_retries {
                self.pending_chunks.remove(&chunk.index);
            }
            return Err(format!("Checksum mismatch for chunk {}", chunk.index));
        }

        self.received_chunks.insert(chunk.index, chunk);
        self.pending_chunks.remove(&chunk.index);
        self.failed_chunks.remove(&chunk.index);
        Ok(())
    }

    pub fn get_failed_chunks(&self) -> Vec<u32> {
        self.failed_chunks
            .iter()
            .filter(|(_, &retries)| retries < self.max_retries)
            .map(|(&index, _)| index)
            .collect()
    }

    pub fn assemble_state(&self) -> Result<Vec<u8>, String> {
        if !self.is_complete() {
            return Err("Not all chunks received".to_string());
        }

        let mut state_data = Vec::new();
        for i in 0..self.total_chunks {
            if let Some(chunk) = self.received_chunks.get(&i) {
                state_data.extend(&chunk.data);
            } else {
                return Err(format!("Missing chunk {}", i));
            }
        }
        Ok(state_data)
    }
}

/// Fast sync orchestrator
pub struct FastSync {
    network_manager: Arc<RwLock<NetworkManager>>,
    state_manager: Arc<RwLock<KlomangStateManager>>,
    storage: StorageHandle,
    chunk_manager: Option<ChunkManager>,
    snapshot_root_hash: Option<Hash>,
    headers: Vec<BlockHeader>,
    sync_sender: mpsc::UnboundedSender<FastSyncMessage>,
    sync_receiver: Mutex<Option<mpsc::UnboundedReceiver<FastSyncMessage>>>,
}

#[derive(Debug)]
pub enum FastSyncMessage {
    HeadersReceived(Vec<BlockHeader>),
    SnapshotReceived { root_hash: Hash, total_chunks: u32 },
    ChunksReceived(Vec<StateChunk>),
    VerificationComplete(bool),
    TransitionToNormalSync,
}

impl FastSync {
    pub fn new(
        network_manager: Arc<RwLock<NetworkManager>>,
        state_manager: Arc<RwLock<KlomangStateManager>>,
        storage: StorageHandle,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        FastSync {
            network_manager,
            state_manager,
            storage,
            chunk_manager: None,
            snapshot_root_hash: None,
            headers: Vec::new(),
            sync_sender: tx,
            sync_receiver: Mutex::new(Some(rx)),
        }
    }

    pub async fn start_fast_sync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("[FAST_SYNC] Starting fast sync process");

        // Start message receiver
        let receiver_handle = self.start_message_receiver();

        // Phase 1: Header-first sync
        self.sync_headers().await?;

        // Phase 2: Find latest checkpoint
        let checkpoint_height = self.find_checkpoint_height();
        log::info!("[FAST_SYNC] Using checkpoint at height {}", checkpoint_height);

        // Phase 3: Download state snapshot
        self.download_state_snapshot(checkpoint_height).await?;

        // Phase 4: Download state chunks in parallel
        self.download_state_chunks().await?;

        // Phase 5: Verify and apply state
        self.verify_and_apply_state().await?;

        // Phase 6: Transition to normal sync
        self.transition_to_normal_sync().await?;

        // Stop receiver
        receiver_handle.abort();

        log::info!("[FAST_SYNC] Fast sync completed successfully");
        Ok(())
    }

    fn start_message_receiver(&mut self) -> tokio::task::JoinHandle<()> {
        let mut receiver = self.sync_receiver.lock().unwrap().take().unwrap();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    FastSyncMessage::HeadersReceived(headers) => {
                        log::info!("[FAST_SYNC] Received {} headers", headers.len());
                        // In practice, merge and validate headers
                    }
                    FastSyncMessage::SnapshotReceived { root_hash, total_chunks } => {
                        log::info!("[FAST_SYNC] Received snapshot: root={}, chunks={}", root_hash, total_chunks);
                    }
                    FastSyncMessage::ChunksReceived(chunks) => {
                        log::info!("[FAST_SYNC] Received {} chunks", chunks.len());
                    }
                    FastSyncMessage::VerificationComplete(success) => {
                        log::info!("[FAST_SYNC] Verification {}", if success { "successful" } else { "failed" });
                    }
                    FastSyncMessage::TransitionToNormalSync => {
                        log::info!("[FAST_SYNC] Transitioning to normal sync");
                    }
                }
            }
        })
    }

    async fn sync_headers(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("[FAST_SYNC] Starting header-first sync");

        // Get best peers for header sync
        let peers = {
            let nm = self.network_manager.read().unwrap();
            nm.get_top_sync_peers()
        };

        // Send header requests to multiple peers in parallel
        let mut header_tasks = Vec::new();
        for peer in peers.into_iter().take(8) {
            let network_manager = Arc::clone(&self.network_manager);
            let task = tokio::spawn(async move {
                let request = SyncRequest::GetHeaders {
                    start_height: 0,
                    count: 1000,
                };
                network_manager.write().unwrap().send_sync_request(&peer, request).await
            });
            header_tasks.push(task);
        }

        // Collect responses
        let mut all_headers = Vec::new();
        for task in header_tasks {
            match task.await {
                Ok(Ok(SyncResponse::Headers(headers))) => {
                    all_headers.extend(headers);
                }
                Ok(Ok(response)) => {
                    log::warn!("[FAST_SYNC] Unexpected response: {:?}", response);
                }
                Ok(Err(e)) => {
                    log::warn!("[FAST_SYNC] Header request failed: {}", e);
                }
                Err(e) => {
                    log::warn!("[FAST_SYNC] Task join error: {}", e);
                }
            }
        }

        // Sort and deduplicate headers (by timestamp as proxy for sequence)
        all_headers.sort_by_key(|h| h.timestamp);
        all_headers.dedup_by_key(|h| h.id.clone());

        // Validate header chain
        {
            let sm = self.state_manager.read().unwrap();
            sm.validate_header_chain_public(&all_headers)?;
        }

        // Store headers
        {
            let mut sm = self.state_manager.write().unwrap();
            sm.store_headers(all_headers.clone())?;
        }

        // Send message to receiver
        let _ = self.sync_sender.send(FastSyncMessage::HeadersReceived(all_headers));

        log::info!("[FAST_SYNC] Header sync completed");
        Ok(())
    }

    fn find_checkpoint_height(&self) -> u64 {
        // Find the latest checkpoint (simplified - every 1000 blocks)
        let latest_height = self.headers.len() as u64 - 1;
        (latest_height / 1000) * 1000
    }

    async fn download_state_snapshot(&mut self, checkpoint_height: u64) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("[FAST_SYNC] Downloading state snapshot for height {}", checkpoint_height);

        let top_peers = {
            let nm = self.network_manager.read().unwrap();
            nm.get_top_sync_peers()
        };

        for peer in top_peers {
            let request = SyncRequest::GetStateSnapshot { checkpoint_height };
            match self.network_manager.write().unwrap().send_sync_request(&peer, request).await {
                Ok(SyncResponse::StateSnapshot { root_hash, total_chunks }) => {
                    self.snapshot_root_hash = Some(root_hash.clone());
                    self.chunk_manager = Some(ChunkManager::new(checkpoint_height, total_chunks));
                    log::info!("[FAST_SYNC] Snapshot info: root={}, chunks={}", root_hash.to_hex(), total_chunks);
                    return Ok(());
                }
                _ => continue,
            }
        }

        Err("Failed to download state snapshot".into())
    }

    async fn download_state_chunks(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("[FAST_SYNC] Starting parallel chunk download");

        let chunk_manager = self.chunk_manager.as_mut().unwrap();
        let checkpoint_height = chunk_manager.checkpoint_height;

        while !chunk_manager.is_complete() {
            let pending_chunks = chunk_manager.get_pending_chunks(10); // Request 10 chunks at a time
            if pending_chunks.is_empty() {
                break;
            }

            let top_peers = {
                let nm = self.network_manager.read().unwrap();
                nm.get_top_sync_peers()
            };

            // Distribute chunk requests across peers
            let mut chunk_tasks = Vec::new();
            for (i, peer) in top_peers.into_iter().cycle().take(pending_chunks.len()).enumerate() {
                let chunk_index = pending_chunks[i];
                let nm = Arc::clone(&self.network_manager);
                let tx = self.sync_sender.clone();
                let request = SyncRequest::GetStateChunks {
                    checkpoint_height,
                    chunk_indices: vec![chunk_index],
                };

                chunk_manager.mark_requested(&[chunk_index]);

                let task = tokio::spawn(async move {
                    match nm.write().unwrap().send_sync_request(&peer, request).await {
                        Ok(SyncResponse::StateChunks(chunks)) => {
                            let _ = tx.send(FastSyncMessage::ChunksReceived(chunks));
                        }
                        _ => {} // Handle failures
                    }
                });
                chunk_tasks.push(task);
            }

            // Wait for chunk responses
            for task in chunk_tasks {
                let _ = task.await;
            }

            // Process received chunks
            // In practice, this would be handled by the message receiver
            tokio::time::sleep(Duration::from_millis(100)).await; // Small delay
        }

        if !chunk_manager.is_complete() {
            return Err("Failed to download all chunks".into());
        }

        log::info!("[FAST_SYNC] All chunks downloaded");
        Ok(())
    }

    async fn verify_and_apply_state(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("[FAST_SYNC] Verifying and applying state");

        let chunk_manager = self.chunk_manager.as_ref().unwrap();
        let state_data = chunk_manager.assemble_state()?;

        // Compute local Verkle root hash
        let computed_root = self.compute_verkle_root(&state_data)?;

        // Verify against snapshot root hash
        if let Some(expected_root) = &self.snapshot_root_hash {
            if &computed_root != expected_root {
                // Ban the peer that provided the snapshot
                // For simplicity, ban all peers that provided chunks (in practice, track per chunk)
                log::error!("[FAST_SYNC] Root hash verification failed: expected {}, got {}", 
                           expected_root.to_hex(), computed_root.to_hex());
                return Err("State verification failed".into());
            }
        } else {
            return Err("No snapshot root hash available for verification".into());
        }

        // Apply state to Verkle tree
        self.apply_state_to_verkle_tree(state_data).await?;

        log::info!("[FAST_SYNC] State verification successful");
        Ok(())
    }

    fn compute_verkle_root(&self, state_data: &[u8]) -> Result<Hash, Box<dyn std::error::Error>> {
        // In practice, this would use klomang-core Verkle tree computation
        // For now, return a dummy hash
        Ok(Hash::new(state_data))
    }

    async fn apply_state_to_verkle_tree(&self, state_data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        // Apply the state data to the Verkle tree
        // This would integrate with klomang-core
        let mut sm = self.state_manager.write().unwrap();
        sm.apply_fast_sync_state(state_data)?;
        Ok(())
    }

    async fn transition_to_normal_sync(&self) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("[FAST_SYNC] Transitioning to normal sync");

        // Sync UTXO set with the new Verkle root
        let mut sm = self.state_manager.write().unwrap();
        sm.sync_utxo_with_verkle()?;

        // Start normal sync from checkpoint height + 1
        // This would trigger the normal sync process
        Ok(())
    }
}