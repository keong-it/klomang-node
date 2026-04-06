//! Block ingestion queue and worker for Klomang Node
//!
//! This module handles:
//! - Ingestion queue management
//! - Worker loop for processing blocks
//! - Rate limiting and backpressure
//! - Orphan block processing retry loop

use crate::ingestion_guard::IngestionMessage;
use tokio::sync::mpsc;

/// Configuration for ingestion queue
pub struct IngestionConfig {
    pub queue_capacity: usize,
    pub worker_count: usize,
    pub rate_limit_per_second: u32,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        IngestionConfig {
            queue_capacity: 10_000,
            worker_count: 4,
            rate_limit_per_second: 1000,
        }
    }
}

/// Ingestion queue sender (producer side)
pub type IngestionSender = mpsc::Sender<IngestionMessage>;

/// Ingestion queue receiver (consumer side)
pub type IngestionReceiver = mpsc::Receiver<IngestionMessage>;

/// Create an ingestion queue
pub fn create_ingestion_queue(capacity: usize) -> (IngestionSender, IngestionReceiver) {
    mpsc::channel(capacity)
}

/// Ingestion operations
pub struct IngestionOps;

impl IngestionOps {
    /// Apply rate limiting to ingestion
    pub async fn apply_rate_limit(
        rate_limit_per_second: u32,
        previous_time: &mut std::time::Instant,
    ) {
        const SECOND_NANOS: u128 = 1_000_000_000;
        let nanos_per_block = SECOND_NANOS / (rate_limit_per_second as u128);

        let elapsed_nanos = previous_time.elapsed().as_nanos();
        if elapsed_nanos < nanos_per_block {
            let sleep_nanos = nanos_per_block - elapsed_nanos;
            let sleep_duration = std::time::Duration::from_nanos(sleep_nanos as u64);
            tokio::time::sleep(sleep_duration).await;
        }

        *previous_time = std::time::Instant::now();
    }

    /// Check queue overflow condition
    pub fn check_queue_overflow(queue_capacity: usize, pending_count: usize) -> bool {
        // Trigger backpressure at 90% capacity
        pending_count > (queue_capacity * 90 / 100)
    }

    /// Log ingestion statistics
    pub fn log_stats(processed: u64, rejected: u64, queue_size: usize, orphans: usize) {
        log::info!(
            "[INGESTION] Processed: {}, Rejected: {}, Queue: {}, Orphans: {}",
            processed,
            rejected,
            queue_size,
            orphans
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingestion_config_default() {
        let config = IngestionConfig::default();
        assert_eq!(config.queue_capacity, 10_000);
        assert_eq!(config.worker_count, 4);
        assert_eq!(config.rate_limit_per_second, 1000);
    }

    #[tokio::test]
    async fn test_create_ingestion_queue() {
        let (tx, rx) = create_ingestion_queue(100);
        assert_eq!(tx.capacity(), 100);
        drop(rx);
    }

    #[test]
    fn test_queue_overflow_check() {
        assert!(!IngestionOps::check_queue_overflow(1000, 850));
        assert!(IngestionOps::check_queue_overflow(1000, 900));
        assert!(IngestionOps::check_queue_overflow(1000, 1000));
    }
}
