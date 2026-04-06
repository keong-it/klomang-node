use klomang_core::BlockNode;
/// Ingestion Guard Components for KlomangStateManager
/// Prevents node freeze under block flood attacks with bounded processing queue and rate limiting

/// Messages sent to ingestion queue
#[derive(Clone, Debug)]
pub enum IngestionMessage {
    Block(BlockNode),
}
use libp2p::PeerId;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::mpsc;

/// Rate limiter using Token Bucket algorithm for bounded block processing
#[derive(Clone)]
pub struct RateLimiter {
    /// Maximum blocks per second
    pub max_blocks_per_sec: u64,
    /// Current available tokens
    tokens: Arc<std::sync::Mutex<f64>>,
    /// Last refill time
    last_refill: Arc<std::sync::Mutex<Instant>>,
}

impl RateLimiter {
    /// Create a new rate limiter with specified max blocks per second
    pub fn new(max_blocks_per_sec: u64) -> Self {
        Self {
            max_blocks_per_sec,
            tokens: Arc::new(std::sync::Mutex::new(max_blocks_per_sec as f64)),
            last_refill: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }

    /// Try to acquire one token. Returns true if successful (not rate limited).
    pub fn try_acquire(&self) -> Result<bool, String> {
        let mut tokens = self
            .tokens
            .lock()
            .map_err(|e| format!("Tokens lock poisoned: {}", e))?;
        let mut last_refill = self
            .last_refill
            .lock()
            .map_err(|e| format!("Last refill lock poisoned: {}", e))?;

        // Refill tokens based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);
        let refill_rate = self.max_blocks_per_sec as f64 / 1000.0; // Per millisecond
        let refill_amount = elapsed.as_millis() as f64 * refill_rate;

        *tokens = (*tokens + refill_amount).min(self.max_blocks_per_sec as f64);
        *last_refill = now;

        if *tokens >= 1.0 {
            *tokens -= 1.0;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get current token count (for monitoring)
    pub fn current_tokens(&self) -> Result<f64, String> {
        Ok(*self
            .tokens
            .lock()
            .map_err(|e| format!("Tokens lock poisoned: {}", e))?)
    }
}

/// Configuration for ingestion guard
#[derive(Clone, Debug)]
pub struct IngestionGuardConfig {
    /// Capacity of ingestion queue (bounded buffer)
    pub queue_capacity: usize,
    /// Maximum blocks per second (rate limiting)
    pub max_blocks_per_sec: u64,
    /// Threshold for warning: queue depth >= capacity * threshold_percentage
    pub warning_threshold_pct: f64,
}

/// Statistics for ingestion queue monitoring
#[derive(Clone, Debug)]
pub struct IngestionStats {
    /// Blocks dropped due to rate limiting
    pub dropped_blocks_count: u64,
    /// Blocks successfully processed
    pub processed_blocks_count: u64,
    /// Maximum queue depth observed
    pub max_queue_depth_observed: u64,
    /// Current queue depth
    pub current_queue_depth: u64,
}

impl IngestionStats {
    /// Create new stats tracker
    pub fn new() -> Self {
        Self {
            dropped_blocks_count: 0,
            processed_blocks_count: 0,
            max_queue_depth_observed: 0,
            current_queue_depth: 0,
        }
    }
}

/// Adaptive Ingestion Guard with dynamic rate adjustment and per-peer limiting
/// Supports sync prioritization by adjusting rates based on network conditions
#[derive(Clone)]
pub struct AdaptiveIngestionGuard {
    /// Current rate limiter for normal mode
    normal_limiter: RateLimiter,
    /// Sync mode flag (higher rate for IBD/sync)
    sync_mode: Arc<AtomicBool>,
    /// Current adaptive rate (can be 100-5000 blocks/sec)
    current_rate: Arc<AtomicU64>,
    /// Per-peer rate limiting to prevent single peer flooding
    peer_limits: Arc<RwLock<HashMap<libp2p::PeerId, RateLimiter>>>,
    /// Per-peer block timestamps for rate calculation
    peer_block_counts: Arc<RwLock<HashMap<libp2p::PeerId, VecDeque<Instant>>>>,
    /// Configuration
    config: IngestionGuardConfig,
}

impl Default for IngestionGuardConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 500,         // 500 blocks max in buffer
            max_blocks_per_sec: 100,     // 100 blocks/sec normal mode
            warning_threshold_pct: 0.80, // Warn at 80% capacity
        }
    }
}

impl AdaptiveIngestionGuard {
    /// Create new adaptive ingestion guard
    pub fn new(config: IngestionGuardConfig) -> Self {
        Self {
            normal_limiter: RateLimiter::new(config.max_blocks_per_sec),
            sync_mode: Arc::new(AtomicBool::new(false)),
            current_rate: Arc::new(AtomicU64::new(config.max_blocks_per_sec)),
            peer_limits: Arc::new(RwLock::new(HashMap::new())),
            peer_block_counts: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Try to acquire permission for a block from specific peer
    /// Returns true if block should be processed
    pub fn try_acquire(&self, peer_id: &libp2p::PeerId) -> Result<bool, String> {
        // Check per-peer rate limiting first
        if !self.check_peer_rate_limit(peer_id)? {
            return Ok(false);
        }

        // Check global rate limiting
        let current_rate = self.current_rate.load(Ordering::Relaxed);
        let limiter = if self.sync_mode.load(Ordering::Relaxed) {
            // In sync mode, use higher rate limiter
            RateLimiter::new(current_rate)
        } else {
            self.normal_limiter.clone()
        };

        limiter.try_acquire()
    }

    /// Check per-peer rate limiting (max 10 blocks/sec per peer)
    fn check_peer_rate_limit(&self, peer_id: &libp2p::PeerId) -> Result<bool, String> {
        const MAX_PEER_RATE: u64 = 10; // 10 blocks/sec per peer
        const WINDOW_SECS: u64 = 60; // 1 minute window

        let mut peer_counts = self.peer_block_counts.write()
            .map_err(|e| format!("Peer counts lock poisoned: {}", e))?;

        let now = Instant::now();
        let counts = peer_counts.entry(*peer_id).or_insert_with(VecDeque::new);

        // Clean old entries outside window
        while let Some(timestamp) = counts.front() {
            if now.duration_since(*timestamp).as_secs() > WINDOW_SECS {
                counts.pop_front();
            } else {
                break;
            }
        }

        // Check rate
        if counts.len() >= MAX_PEER_RATE as usize {
            return Ok(false);
        }

        // Add current timestamp
        counts.push_back(now);
        Ok(true)
    }

    /// Determine if rate should be increased based on queue depth and orphan count
    /// Returns true if rate should increase (queue low, few orphans)
    pub fn should_increase_rate(&self, queue_depth: usize, orphan_count: usize) -> bool {
        let capacity_threshold = (self.config.queue_capacity as f64 * 0.3) as usize; // 30%
        let orphan_threshold = 10;

        queue_depth < capacity_threshold && orphan_count < orphan_threshold
    }

    /// Determine if rate should be decreased (queue high, many orphans)
    pub fn should_decrease_rate(&self, queue_depth: usize, orphan_count: usize) -> bool {
        let capacity_threshold = (self.config.queue_capacity as f64 * 0.8) as usize; // 80%
        let orphan_threshold = 50;

        queue_depth > capacity_threshold || orphan_count > orphan_threshold
    }

    /// Adjust rate dynamically (called periodically)
    pub fn adjust_rate(&self, queue_depth: usize, orphan_count: usize) {
        let current = self.current_rate.load(Ordering::Relaxed);
        let new_rate = if self.should_increase_rate(queue_depth, orphan_count) {
            // Increase rate up to 5000 blocks/sec in sync mode
            let max_rate = if self.sync_mode.load(Ordering::Relaxed) { 5000 } else { 1000 };
            (current * 2).min(max_rate)
        } else if self.should_decrease_rate(queue_depth, orphan_count) {
            // Decrease rate down to 50 blocks/sec
            (current / 2).max(50)
        } else {
            current
        };

        if new_rate != current {
            self.current_rate.store(new_rate, Ordering::Relaxed);
            log::info!(
                "[INGESTION] Adjusted rate: {} -> {} blocks/sec (queue: {}, orphans: {})",
                current, new_rate, queue_depth, orphan_count
            );
        }
    }

    /// Enable sync mode (higher rates for initial block download)
    pub fn enable_sync_mode(&self) {
        self.sync_mode.store(true, Ordering::Relaxed);
        self.current_rate.store(2000, Ordering::Relaxed); // Start at 2000 blocks/sec
        log::info!("[INGESTION] Enabled sync mode with high throughput");
    }

    /// Disable sync mode (back to normal rates)
    pub fn disable_sync_mode(&self) {
        self.sync_mode.store(false, Ordering::Relaxed);
        self.current_rate.store(self.config.max_blocks_per_sec, Ordering::Relaxed);
        log::info!("[INGESTION] Disabled sync mode, back to normal rates");
    }

    /// Get current rate for monitoring
    pub fn current_rate(&self) -> u64 {
        self.current_rate.load(Ordering::Relaxed)
    }

    /// Get sync mode status
    pub fn is_sync_mode(&self) -> bool {
        self.sync_mode.load(Ordering::Relaxed)
    }
}

/// Create bounded ingestion queue with adaptive rate limiter
pub fn create_adaptive_ingestion_queue(
    config: IngestionGuardConfig,
) -> (
    mpsc::Sender<IngestionMessage>,
    mpsc::Receiver<IngestionMessage>,
    AdaptiveIngestionGuard,
) {
    let (tx, rx) = mpsc::channel(config.queue_capacity);
    let guard = AdaptiveIngestionGuard::new(config);
    (tx, rx, guard)
}

/// Legacy function for backward compatibility
pub fn create_ingestion_queue(
    queue_capacity: usize,
    max_blocks_per_sec: u64,
) -> (
    mpsc::Sender<IngestionMessage>,
    mpsc::Receiver<IngestionMessage>,
    RateLimiter,
) {
    let (tx, rx) = mpsc::channel(queue_capacity);
    let limiter = RateLimiter::new(max_blocks_per_sec);
    (tx, rx, limiter)
}
