/// Ingestion Guard Components for KlomangStateManager
/// Prevents node freeze under block flood attacks with bounded processing queue and rate limiting

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use klomang_core::BlockNode;

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
        let mut tokens = self.tokens.lock().map_err(|e| format!("Tokens lock poisoned: {}", e))?;
        let mut last_refill = self.last_refill.lock().map_err(|e| format!("Last refill lock poisoned: {}", e))?;

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
        Ok(*self.tokens.lock().map_err(|e| format!("Tokens lock poisoned: {}", e))?)
    }
}

/// Message type for block ingestion queue
#[derive(Clone, Debug)]
pub enum IngestionMessage {
    /// Block from P2P or RPC layer
    Block(BlockNode),
    /// Shutdown signal for worker
    Shutdown,
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

impl Default for IngestionGuardConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 500,           // 500 blocks max in buffer
            max_blocks_per_sec: 100,       // 100 blocks/sec normal mode
            warning_threshold_pct: 0.80,   // Warn at 80% capacity
        }
    }
}

/// Create bounded ingestion queue with rate limiter
pub fn create_ingestion_queue(
    queue_capacity: usize,
    max_blocks_per_sec: u64,
) -> (mpsc::Sender<IngestionMessage>, mpsc::Receiver<IngestionMessage>, RateLimiter) {
    let (tx, rx) = mpsc::channel(queue_capacity);
    let limiter = RateLimiter::new(max_blocks_per_sec);
    (tx, rx, limiter)
}
