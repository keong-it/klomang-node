use klomang_core::Hash;
use libp2p::PeerId;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

/// Gossip filter for transaction deduplication
#[derive(Debug)]
pub struct GossipFilter {
    pub seen_txs: LruCache<Hash, ()>,
    pub gossiped_txs: LruCache<Hash, Instant>,
}

impl GossipFilter {
    pub fn new() -> Self {
        GossipFilter {
            seen_txs: LruCache::new(NonZeroUsize::new(50_000).unwrap()),
            gossiped_txs: LruCache::new(NonZeroUsize::new(10_000).unwrap()),
        }
    }

    pub fn is_recently_seen(&mut self, hash: &Hash) -> bool {
        self.seen_txs.contains(hash)
    }

    pub fn mark_seen(&mut self, hash: Hash) {
        self.seen_txs.put(hash, ());
    }

    pub fn should_gossip(&mut self, hash: &Hash) -> bool {
        if let Some(last_gossip) = self.gossiped_txs.get(hash) {
            Instant::now().duration_since(*last_gossip) > Duration::from_secs(300)
        } else {
            true
        }
    }

    pub fn mark_gossiped(&mut self, hash: Hash) {
        self.gossiped_txs.put(hash, Instant::now());
    }
}

/// Simple transaction rate limiter for flood protection
#[derive(Debug)]
pub struct RateLimiter {
    peer_counts: HashMap<PeerId, Vec<Instant>>,
    max_tx_per_sec: f64,
    window: Duration,
}

impl RateLimiter {
    pub fn new(max_tx_per_sec: f64) -> Self {
        RateLimiter {
            peer_counts: HashMap::new(),
            max_tx_per_sec,
            window: Duration::from_secs(60),
        }
    }

    pub fn check_rate_limit(&mut self, peer: &PeerId) -> bool {
        let now = Instant::now();

        if let Some(times) = self.peer_counts.get_mut(peer) {
            times.retain(|&time| now.duration_since(time) < self.window);
        }

        let count = self.peer_counts.get(peer).map(|v| v.len()).unwrap_or(0) as f64;
        let rate = count / self.window.as_secs_f64();

        rate < self.max_tx_per_sec
    }

    pub fn record_tx(&mut self, peer: PeerId) {
        self.peer_counts.entry(peer).or_insert_with(Vec::new).push(Instant::now());
    }
}
