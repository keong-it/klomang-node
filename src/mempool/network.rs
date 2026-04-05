//! Network Peer Intelligence for Mempool
//!
//! Handles peer scoring, rate limiting, gossip filtering, and rebroadcast logic.

use std::collections::HashMap;

use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use libp2p::PeerId;
use klomang_core::SignedTransaction;

/// Peer reputation score
#[derive(Clone, Debug)]
pub struct PeerScore {
    /// Current score (higher is better)
    score: i32,
    /// Last activity timestamp
    last_seen: Instant,
    /// Transactions sent per second
    tx_rate: f64,
    /// Bad transaction count
    bad_tx_count: u32,
    /// Is peer banned
    banned: bool,
}

impl PeerScore {
    fn new() -> Self {
        PeerScore {
            score: 100, // Start with neutral score
            last_seen: Instant::now(),
            tx_rate: 0.0,
            bad_tx_count: 0,
            banned: false,
        }
    }

    /// Update score based on behavior
    fn update_score(&mut self, delta: i32) {
        self.score = (self.score + delta).max(0).min(1000);
        self.last_seen = Instant::now();

        // Ban if score too low
        if self.score < 10 {
            self.banned = true;
        }
    }

    /// Record bad transaction
    fn record_bad_tx(&mut self) {
        self.bad_tx_count += 1;
        self.update_score(-10);
    }

    /// Record good transaction
    fn record_good_tx(&mut self) {
        self.update_score(1);
    }
}

/// Rate limiter for transactions per peer
pub struct RateLimiter {
    /// Transactions per peer in last window
    peer_counts: HashMap<PeerId, Vec<Instant>>,
    /// Maximum transactions per second per peer
    max_tx_per_sec: f64,
    /// Window size for rate calculation
    window: Duration,
}

impl RateLimiter {
    pub fn new(max_tx_per_sec: f64) -> Self {
        RateLimiter {
            peer_counts: HashMap::new(),
            max_tx_per_sec,
            window: Duration::from_secs(60), // 1 minute window
        }
    }

    /// Check if peer is within rate limit
    pub fn check_rate_limit(&mut self, peer: &PeerId) -> bool {
        let now = Instant::now();

        // Clean old entries
        if let Some(times) = self.peer_counts.get_mut(peer) {
            times.retain(|&time| now.duration_since(time) < self.window);
        }

        // Check current rate
        let count = self.peer_counts.get(peer).map(|v| v.len()).unwrap_or(0) as f64;
        let rate = count / self.window.as_secs_f64();

        rate < self.max_tx_per_sec
    }

    /// Record transaction for peer
    pub fn record_tx(&mut self, peer: PeerId) {
        self.peer_counts.entry(peer).or_insert(Vec::new()).push(Instant::now());
    }
}

/// Gossip filter for transaction deduplication
pub struct GossipFilter {
    /// Recently seen transaction hashes
    seen_txs: lru::LruCache<klomang_core::Hash, ()>,
    /// Recently gossiped transactions
    gossiped_txs: lru::LruCache<klomang_core::Hash, Instant>,
}

impl GossipFilter {
    pub fn new() -> Self {
        GossipFilter {
            seen_txs: lru::LruCache::new(std::num::NonZeroUsize::new(50000).unwrap()),
            gossiped_txs: lru::LruCache::new(std::num::NonZeroUsize::new(10000).unwrap()),
        }
    }

    /// Check if transaction was recently seen
    pub fn is_recently_seen(&mut self, hash: &klomang_core::Hash) -> bool {
        self.seen_txs.contains(hash)
    }

    /// Mark transaction as seen
    pub fn mark_seen(&mut self, hash: klomang_core::Hash) {
        self.seen_txs.put(hash, ());
    }

    /// Check if transaction was recently gossiped
    pub fn should_gossip(&mut self, hash: &klomang_core::Hash) -> bool {
        if let Some(last_gossip) = self.gossiped_txs.get(hash) {
            // Don't gossip again within 5 minutes
            Instant::now().duration_since(*last_gossip) > Duration::from_secs(300)
        } else {
            true
        }
    }

    /// Mark transaction as gossiped
    pub fn mark_gossiped(&mut self, hash: klomang_core::Hash) {
        self.gossiped_txs.put(hash, Instant::now());
    }
}

/// Peer manager for network intelligence
pub struct PeerManager {
    /// Peer scores
    peer_scores: HashMap<PeerId, PeerScore>,
    /// Rate limiter
    rate_limiter: RateLimiter,
    /// Gossip filter
    gossip_filter: GossipFilter,
    /// Rebroadcast queue
    rebroadcast_queue: mpsc::UnboundedSender<SignedTransaction>,
    /// Rebroadcast receiver
    rebroadcast_receiver: std::sync::Mutex<Option<mpsc::UnboundedReceiver<SignedTransaction>>>,
}

impl PeerManager {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        PeerManager {
            peer_scores: HashMap::new(),
            rate_limiter: RateLimiter::new(10.0), // 10 tx/sec per peer
            gossip_filter: GossipFilter::new(),
            rebroadcast_queue: tx,
            rebroadcast_receiver: std::sync::Mutex::new(Some(rx)),
        }
    }

    /// Handle incoming transaction from peer
    pub fn handle_incoming_transaction(
        &mut self,
        peer: PeerId,
        tx: SignedTransaction,
    ) -> Result<(), String> {
        // Check rate limit
        if !self.rate_limiter.check_rate_limit(&peer) {
            self.penalize_peer(&peer, "rate limit exceeded");
            return Err("Rate limit exceeded".to_string());
        }

        // Check gossip filter
        let tx_hash = tx.id;
        if self.gossip_filter.is_recently_seen(&tx_hash) {
            return Err("Transaction already seen".to_string());
        }

        // Record transaction
        self.rate_limiter.record_tx(peer.clone());
        self.gossip_filter.mark_seen(tx_hash);

        // Get or create peer score
        let peer_score = self.peer_scores.entry(peer).or_insert(PeerScore::new());

        // Basic validation (would be more thorough in real implementation)
        if self.is_valid_transaction(&tx) {
            peer_score.record_good_tx();
            Ok(())
        } else {
            peer_score.record_bad_tx();
            Err("Invalid transaction".to_string())
        }
    }

    /// Rebroadcast transaction to network
    pub fn rebroadcast_transaction(&mut self, tx: SignedTransaction) {
        let _ = self.rebroadcast_queue.send(tx);
    }

    /// Get peer score
    pub fn get_peer_score(&self, peer: &PeerId) -> Option<&PeerScore> {
        self.peer_scores.get(peer)
    }

    /// Check if peer is banned
    pub fn is_peer_banned(&self, peer: &PeerId) -> bool {
        self.peer_scores.get(peer).map(|s| s.banned).unwrap_or(false)
    }

    /// Penalize peer for bad behavior
    pub fn penalize_peer(&mut self, peer: &PeerId, reason: &str) {
        if let Some(score) = self.peer_scores.get_mut(peer) {
            score.update_score(-20);
            log::warn!("Penalized peer {}: {}", peer, reason);
        }
    }

    /// Get rebroadcast receiver for processing
    pub fn take_rebroadcast_receiver(&self) -> Option<mpsc::UnboundedReceiver<SignedTransaction>> {
        self.rebroadcast_receiver.lock().unwrap().take()
    }

    /// Basic transaction validation (placeholder)
    fn is_valid_transaction(&self, tx: &SignedTransaction) -> bool {
        // In real implementation, this would do signature verification,
        // input validation, etc.
        !tx.inputs.is_empty() && !tx.outputs.is_empty()
    }

    /// Clean up old peer data
    pub fn cleanup(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(3600); // 1 hour

        self.peer_scores.retain(|_, score| {
            score.last_seen > cutoff && !score.banned
        });
    }
}