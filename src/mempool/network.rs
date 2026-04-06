//! Network Peer Intelligence for Mempool
//!
//! Handles peer scoring, rate limiting, gossip filtering, and rebroadcast logic.

use std::collections::HashMap;

use klomang_core::SignedTransaction;
use libp2p::PeerId;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use sysinfo::{System, SystemExt, CpuExt};

/// Token bucket for rate limiting
#[derive(Clone, Debug)]
pub struct TokenBucket {
    /// Current tokens
    tokens: f64,
    /// Maximum capacity
    capacity: f64,
    /// Refill rate per second
    refill_rate: f64,
    /// Last refill time
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        TokenBucket {
            tokens: capacity,
            capacity,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on time elapsed
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;
    }

    /// Try to consume tokens
    pub fn try_consume(&mut self, amount: f64) -> bool {
        self.refill();
        if self.tokens >= amount {
            self.tokens -= amount;
            true
        } else {
            false
        }
    }

    /// Set new refill rate for adaptive throttling
    pub fn set_refill_rate(&mut self, new_rate: f64) {
        self.refill_rate = new_rate;
    }
}

/// Granular rate limiter per peer
#[derive(Clone, Debug)]
pub struct PeerRateLimiter {
    /// TPS buckets (transactions per second)
    tps_buckets: HashMap<PeerId, TokenBucket>,
    /// Block request buckets (requests per minute)
    block_request_buckets: HashMap<PeerId, TokenBucket>,
    /// Bandwidth buckets (bytes per second)
    bandwidth_buckets: HashMap<PeerId, TokenBucket>,
    /// System monitor for adaptive throttling
    system: System,
    /// Last system check
    last_system_check: Instant,
}

impl PeerRateLimiter {
    pub fn new() -> Self {
        PeerRateLimiter {
            tps_buckets: HashMap::new(),
            block_request_buckets: HashMap::new(),
            bandwidth_buckets: HashMap::new(),
            system: System::new_all(),
            last_system_check: Instant::now(),
        }
    }

    /// Check if system load is high (>80% CPU or RAM)
    fn is_high_load(&mut self) -> bool {
        if self.last_system_check.elapsed() > Duration::from_secs(5) {
            self.system.refresh_cpu();
            self.system.refresh_memory();
            self.last_system_check = Instant::now();
        }

        let cpu_usage = self.system.global_cpu_info().cpu_usage() as f64;
        let memory_usage = (self.system.used_memory() as f64 / self.system.total_memory() as f64) * 100.0;

        cpu_usage > 80.0 || memory_usage > 80.0
    }

    /// Get adaptive multiplier (reduce to 50% if high load)
    fn adaptive_multiplier(&mut self) -> f64 {
        if self.is_high_load() { 0.5 } else { 1.0 }
    }

    /// Get or create TPS bucket for peer
    fn get_tps_bucket(&mut self, peer: &PeerId) -> &mut TokenBucket {
        self.tps_buckets.entry(*peer).or_insert_with(|| {
            let base_rate = 50.0; // 50 TPS
            let multiplier = self.adaptive_multiplier();
            TokenBucket::new(base_rate, base_rate * multiplier)
        })
    }

    /// Get or create block request bucket for peer
    fn get_block_request_bucket(&mut self, peer: &PeerId) -> &mut TokenBucket {
        self.block_request_buckets.entry(*peer).or_insert_with(|| {
            let base_rate = 10.0 / 60.0; // 10 per minute = ~0.166 per second
            let multiplier = self.adaptive_multiplier();
            TokenBucket::new(10.0, base_rate * multiplier)
        })
    }

    /// Get or create bandwidth bucket for peer
    fn get_bandwidth_bucket(&mut self, peer: &PeerId) -> &mut TokenBucket {
        self.bandwidth_buckets.entry(*peer).or_insert_with(|| {
            let base_rate = 1_000_000.0; // 1 MB/s
            let multiplier = self.adaptive_multiplier();
            TokenBucket::new(base_rate, base_rate * multiplier)
        })
    }

    /// Check TPS limit
    pub fn check_tps_limit(&mut self, peer: &PeerId) -> bool {
        self.get_tps_bucket(peer).try_consume(1.0)
    }

    /// Check block request limit
    pub fn check_block_request_limit(&mut self, peer: &PeerId) -> bool {
        self.get_block_request_bucket(peer).try_consume(1.0)
    }

    /// Check bandwidth limit
    pub fn check_bandwidth_limit(&mut self, peer: &PeerId, bytes: usize) -> bool {
        self.get_bandwidth_bucket(peer).try_consume(bytes as f64)
    }

    /// Update adaptive rates if load changed
    pub fn update_adaptive_rates(&mut self) {
        let multiplier = self.adaptive_multiplier();
        for bucket in self.tps_buckets.values_mut() {
            bucket.set_refill_rate(50.0 * multiplier);
        }
        for bucket in self.block_request_buckets.values_mut() {
            bucket.set_refill_rate((10.0 / 60.0) * multiplier);
        }
        for bucket in self.bandwidth_buckets.values_mut() {
            bucket.set_refill_rate(1_000_000.0 * multiplier);
        }
    }
}

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
        self.peer_counts
            .entry(peer)
            .or_insert(Vec::new())
            .push(Instant::now());
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
    /// Granular rate limiter
    granular_limiter: PeerRateLimiter,
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
            granular_limiter: PeerRateLimiter::new(),
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
        // Check granular TPS limit
        if !self.granular_limiter.check_tps_limit(&peer) {
            self.penalize_peer(&peer, "TPS rate limit exceeded");
            return Err("TPS rate limit exceeded".to_string());
        }

        // Check old rate limit for compatibility
        if !self.rate_limiter.check_rate_limit(&peer) {
            self.penalize_peer(&peer, "rate limit exceeded");
            return Err("Rate limit exceeded".to_string());
        }

        // Check gossip filter
        let tx_hash = tx.id.clone();
        if self.gossip_filter.is_recently_seen(&tx_hash) {
            return Err("Transaction already seen".to_string());
        }

        let is_valid = self.is_valid_transaction(&tx);

        // Record transaction
        self.rate_limiter.record_tx(peer.clone());
        self.gossip_filter.mark_seen(tx_hash);

        // Get or create peer score
        let peer_score = self.peer_scores.entry(peer).or_insert(PeerScore::new());

        // Basic validation (would be more thorough in real implementation)
        if is_valid {
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
        self.peer_scores
            .get(peer)
            .map(|s| s.banned)
            .unwrap_or(false)
    }

    /// Check block request rate limit
    pub fn check_block_request_limit(&mut self, peer: &PeerId) -> bool {
        self.granular_limiter.check_block_request_limit(peer)
    }

    /// Check bandwidth limit
    pub fn check_bandwidth_limit(&mut self, peer: &PeerId, bytes: usize) -> bool {
        self.granular_limiter.check_bandwidth_limit(peer, bytes)
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

        self.peer_scores
            .retain(|_, score| score.last_seen > cutoff && !score.banned);
    }
}
