//! Block Template Builder for Mining Optimization
//!
//! Selects optimal transaction set for block templates based on fee,
//! weight, and dependency ordering.

#![allow(dead_code)]

use klomang_core::{Hash, SignedTransaction};
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Transaction with mining priority
#[derive(Clone, Debug)]
struct PrioritizedTransaction {
    hash: Hash,
    fee: u128,
    weight: u64,
    fee_per_weight: f64,
    dependencies: HashSet<Hash>,
}

impl PrioritizedTransaction {
    fn new(tx: &SignedTransaction) -> Self {
        let hash = tx.id.clone();
        let fee = tx.max_fee_per_gas.saturating_mul(tx.gas_limit as u128);
        // Calculate weight as transaction byte size
        let weight = bincode::serialize(tx).map(|bytes| bytes.len() as u64).unwrap_or(1000);
        let fee_per_weight = if weight > 0 {
            fee as f64 / weight as f64
        } else {
            0.0
        };

        PrioritizedTransaction {
            hash,
            fee,
            weight,
            fee_per_weight,
            dependencies: HashSet::new(),
        }
    }
}

impl PartialEq for PrioritizedTransaction {
    fn eq(&self, other: &Self) -> bool {
        self.fee_per_weight == other.fee_per_weight
    }
}

impl Eq for PrioritizedTransaction {}

impl PartialOrd for PrioritizedTransaction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedTransaction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.fee_per_weight
            .partial_cmp(&other.fee_per_weight)
            .unwrap_or(std::cmp::Ordering::Equal)
            .reverse()
    }
}

/// Block template builder
pub struct BlockTemplateBuilder {
    /// Full transaction store for mining selection
    transactions: HashMap<Hash, SignedTransaction>,
    /// Prioritized transaction entries
    priority_queue: BinaryHeap<PrioritizedTransaction>,
    /// Selected transactions for current template
    selected: HashSet<Hash>,
    /// Total weight of selected transactions
    total_weight: u64,
    /// Total gas of selected transactions
    total_gas: u64,
    /// Maximum block weight
    max_weight: u64,
    /// Maximum block gas
    max_gas: u64,
}

impl BlockTemplateBuilder {
    pub fn new() -> Self {
        BlockTemplateBuilder {
            transactions: HashMap::new(),
            priority_queue: BinaryHeap::new(),
            selected: HashSet::new(),
            total_weight: 0,
            total_gas: 0,
            max_weight: 4_000_000,
            max_gas: 60_000_000,
        }
    }

    /// Add transaction to template builder
    pub fn add_transaction(&mut self, tx: &SignedTransaction) {
        let hash = tx.id.clone();
        if self.transactions.contains_key(&hash) {
            return;
        }

        let prioritized = PrioritizedTransaction::new(tx);
        self.transactions.insert(hash.clone(), tx.clone());
        self.priority_queue.push(prioritized);
    }

    /// Remove transaction from template builder
    pub fn remove_transaction(&mut self, hash: &Hash) {
        self.transactions.remove(hash);
        self.selected.remove(hash);

        self.rebuild_queue();
    }

    /// Build optimal block template
    pub fn build_template(&mut self, max_weight: u64) -> Vec<SignedTransaction> {
        self.max_weight = max_weight;
        self.total_weight = 0;
        self.total_gas = 0;
        self.selected.clear();

        let mut template = Vec::new();
        let mut temp_queue = self.priority_queue.clone();

        while let Some(tx) = temp_queue.pop() {
            // Check weight (byte size) limit
            if self.total_weight + tx.weight > self.max_weight {
                continue;
            }

            // Check gas limit
            let tx_gas = match self.transactions.get(&tx.hash) {
                Some(t) => t.gas_limit,
                None => continue,
            };
            if self.total_gas + tx_gas > self.max_gas {
                continue;
            }

            if !self.dependencies_satisfied(&tx) {
                continue;
            }

            let tx_hash = tx.hash.clone();
            self.selected.insert(tx_hash.clone());
            self.total_weight += tx.weight;
            self.total_gas += tx_gas;

            if let Some(tx_data) = self.transactions.get(&tx_hash) {
                template.push(tx_data.clone());
            }
        }

        template
    }

    /// Get current template statistics
    pub fn get_stats(&self) -> TemplateStats {
        let total_fee: u128 = self
            .selected
            .iter()
            .filter_map(|hash| self.transactions.get(hash))
            .map(|tx| tx.max_fee_per_gas.saturating_mul(tx.gas_limit as u128))
            .sum();

        TemplateStats {
            transaction_count: self.selected.len(),
            total_weight: self.total_weight,
            total_fee,
            average_fee_per_weight: if self.total_weight > 0 {
                total_fee as f64 / self.total_weight as f64
            } else {
                0.0
            },
        }
    }

    /// Check if transaction dependencies are satisfied
    fn dependencies_satisfied(&self, tx: &PrioritizedTransaction) -> bool {
        tx.dependencies
            .iter()
            .all(|dep| self.selected.contains(dep))
    }

    /// Rebuild priority queue after removals
    fn rebuild_queue(&mut self) {
        self.priority_queue.clear();
        for tx in self.transactions.values() {
            if !self.selected.contains(&tx.id) {
                self.priority_queue.push(PrioritizedTransaction::new(tx));
            }
        }
    }

    /// Update transaction dependencies
    pub fn update_dependencies(&mut self, hash: Hash, dependencies: HashSet<Hash>) {
        if let Some(tx) = self.transactions.get_mut(&hash) {
            let mut prioritized = PrioritizedTransaction::new(tx);
            prioritized.dependencies = dependencies;
            self.priority_queue.push(prioritized);
        }
    }
}

/// Block template statistics
#[derive(Clone, Debug)]
pub struct TemplateStats {
    pub transaction_count: usize,
    pub total_weight: u64,
    pub total_fee: u128,
    pub average_fee_per_weight: f64,
}
