use std::time::Instant;

/// Reputation score for a peer
#[derive(Clone, Debug)]
pub struct PeerScore {
    pub score: i32,
    pub last_seen: Instant,
    pub tx_rate: f64,
    pub bad_tx_count: u32,
    pub penalty_count: u8,
    pub banned: bool,
}

impl PeerScore {
    pub fn new() -> Self {
        PeerScore {
            score: 100,
            last_seen: Instant::now(),
            tx_rate: 0.0,
            bad_tx_count: 0,
            penalty_count: 0,
            banned: false,
        }
    }

    pub fn update_score(&mut self, delta: i32) {
        self.score = (self.score + delta).max(0).min(1000);
        self.last_seen = Instant::now();

        if self.score < 10 || self.penalty_count >= 5 {
            self.banned = true;
        }
    }

    pub fn record_bad_tx(&mut self) {
        self.bad_tx_count = self.bad_tx_count.saturating_add(1);
        self.penalty_count = self.penalty_count.saturating_add(1);
        self.update_score(-10);
    }

    pub fn record_good_tx(&mut self) {
        self.update_score(1);
    }
}
