use libp2p::PeerId;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use sysinfo::{CpuExt, System, SystemExt};

/// Token bucket for rate limiting
#[derive(Clone, Debug)]
pub struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_rate: f64,
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

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;
    }

    pub fn try_consume(&mut self, amount: f64) -> bool {
        self.refill();
        if self.tokens >= amount {
            self.tokens -= amount;
            true
        } else {
            false
        }
    }

    pub fn set_refill_rate(&mut self, new_rate: f64) {
        self.refill_rate = new_rate;
    }
}

/// Granular rate limiter per peer
#[derive(Debug)]
pub struct PeerRateLimiter {
    pub tps_buckets: HashMap<PeerId, TokenBucket>,
    pub block_request_buckets: HashMap<PeerId, TokenBucket>,
    pub bandwidth_buckets: HashMap<PeerId, TokenBucket>,
    system: System,
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

    fn adaptive_multiplier(&mut self) -> f64 {
        if self.is_high_load() {
            0.7
        } else {
            1.0
        }
    }

    fn refresh_bucket_rates(&mut self, peer: &PeerId) {
        let multiplier = self.adaptive_multiplier();
        if let Some(bucket) = self.tps_buckets.get_mut(peer) {
            bucket.set_refill_rate(50.0 * multiplier);
        }
        if let Some(bucket) = self.block_request_buckets.get_mut(peer) {
            bucket.set_refill_rate((20.0 / 60.0) * multiplier);
        }
        if let Some(bucket) = self.bandwidth_buckets.get_mut(peer) {
            bucket.set_refill_rate(1_000_000.0 * multiplier);
        }
    }

    fn get_tps_bucket(&mut self, peer: &PeerId) -> &mut TokenBucket {
        let multiplier = self.adaptive_multiplier();
        self.tps_buckets.entry(*peer).or_insert_with(|| TokenBucket::new(50.0, 50.0 * multiplier))
    }

    fn get_block_request_bucket(&mut self, peer: &PeerId) -> &mut TokenBucket {
        let multiplier = self.adaptive_multiplier();
        self.block_request_buckets.entry(*peer).or_insert_with(|| {
            TokenBucket::new(20.0, (20.0 / 60.0) * multiplier)
        })
    }

    fn get_bandwidth_bucket(&mut self, peer: &PeerId) -> &mut TokenBucket {
        let multiplier = self.adaptive_multiplier();
        self.bandwidth_buckets.entry(*peer).or_insert_with(|| {
            TokenBucket::new(1_000_000.0, 1_000_000.0 * multiplier)
        })
    }

    pub fn check_tps_limit(&mut self, peer: &PeerId) -> bool {
        self.get_tps_bucket(peer).try_consume(1.0)
    }

    pub fn check_block_request_limit(&mut self, peer: &PeerId) -> bool {
        self.get_block_request_bucket(peer).try_consume(1.0)
    }

    pub fn check_bandwidth_limit(&mut self, peer: &PeerId, bytes: usize) -> bool {
        self.get_bandwidth_bucket(peer).try_consume(bytes as f64)
    }

    pub fn update_adaptive_rates(&mut self) {
        let multiplier = self.adaptive_multiplier();
        for bucket in self.tps_buckets.values_mut() {
            bucket.set_refill_rate(50.0 * multiplier);
        }
        for bucket in self.block_request_buckets.values_mut() {
            bucket.set_refill_rate((20.0 / 60.0) * multiplier);
        }
        for bucket in self.bandwidth_buckets.values_mut() {
            bucket.set_refill_rate(1_000_000.0 * multiplier);
        }
    }
}
