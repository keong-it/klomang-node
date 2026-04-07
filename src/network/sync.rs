use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Sync prioritization score for a peer
#[derive(Clone, Debug)]
pub struct SyncPeerScore {
    pub peer_id: PeerId,
    pub blue_score: u64,        // GHOSTDAG blue score
    pub latency_ms: u64,        // Average ping latency
    pub blocks_per_sec: f64,    // Block serving rate
    pub last_seen: Instant,
    pub is_sync_peer: bool,     // Preferred for IBD
}

impl SyncPeerScore {
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            blue_score: 0,
            latency_ms: 1000, // Default 1 second
            blocks_per_sec: 0.0,
            last_seen: Instant::now(),
            is_sync_peer: false,
        }
    }

    /// Calculate sync priority score (higher = better for sync)
    pub fn sync_priority(&self) -> f64 {
        let recency_factor = 1.0 / (self.last_seen.elapsed().as_secs_f64() + 1.0);
        let latency_factor = 1.0 / (self.latency_ms as f64 + 1.0);
        let score_factor = self.blue_score as f64;

        // Weighted formula: prioritize blue score, then low latency, then recency
        (score_factor * 0.6) + (latency_factor * 1000.0 * 0.3) + (recency_factor * 0.1)
    }
}

/// Sync prioritization manager
#[derive(Clone)]
pub struct SyncPrioritizer {
    peer_scores: Arc<RwLock<HashMap<PeerId, SyncPeerScore>>>,
    max_sync_peers: usize,
}

impl SyncPrioritizer {
    pub fn new(max_sync_peers: usize) -> Self {
        Self {
            peer_scores: Arc::new(RwLock::new(HashMap::new())),
            max_sync_peers,
        }
    }

    /// Update peer score with latest metrics
    pub fn update_peer_score(&self, peer_id: PeerId, blue_score: u64, latency_ms: u64, blocks_per_sec: f64) {
        let mut scores = self.peer_scores.write().unwrap();
        let score = scores.entry(peer_id).or_insert_with(|| SyncPeerScore::new(peer_id));
        score.blue_score = blue_score;
        score.latency_ms = latency_ms;
        score.blocks_per_sec = blocks_per_sec;
        score.last_seen = Instant::now();
    }

    /// Get top sync peers sorted by priority
    pub fn get_top_sync_peers(&self) -> Vec<PeerId> {
        let scores = self.peer_scores.read().unwrap();
        let mut peers: Vec<_> = scores.values().collect();
        peers.sort_by(|a, b| b.sync_priority().partial_cmp(&a.sync_priority()).unwrap());

        peers.into_iter()
            .take(self.max_sync_peers)
            .map(|score| score.peer_id)
            .collect()
    }

    /// Mark peer as preferred sync source
    pub fn mark_sync_peer(&self, peer_id: &PeerId, is_sync: bool) {
        if let Some(score) = self.peer_scores.write().unwrap().get_mut(peer_id) {
            score.is_sync_peer = is_sync;
        }
    }
}
