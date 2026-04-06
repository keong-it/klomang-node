//! Transaction Dependency Graph
//!
//! Manages transaction dependencies and topological ordering for proper
//! validation and inclusion sequencing.

use klomang_core::{Hash, SignedTransaction};
use lru::LruCache;
use std::collections::{HashMap, HashSet, VecDeque};

/// Transaction dependency graph
pub struct TransactionGraph {
    /// Transaction hash -> transaction data
    transactions: HashMap<Hash, SignedTransaction>,
    /// Transaction hash -> set of child transactions
    children: HashMap<Hash, HashSet<Hash>>,
    /// Transaction hash -> set of parent transactions
    parents: HashMap<Hash, HashSet<Hash>>,
    /// Deduplication cache for rejected transactions
    rejected_cache: LruCache<Hash, ()>,
    /// Deduplication cache for confirmed transactions
    confirmed_cache: LruCache<Hash, ()>,
}

impl TransactionGraph {
    pub fn new() -> Self {
        TransactionGraph {
            transactions: HashMap::new(),
            children: HashMap::new(),
            parents: HashMap::new(),
            rejected_cache: LruCache::new(std::num::NonZeroUsize::new(10000).unwrap()),
            confirmed_cache: LruCache::new(std::num::NonZeroUsize::new(50000).unwrap()),
        }
    }

    /// Add transaction to graph
    pub fn add_transaction(&mut self, tx: &SignedTransaction) {
        let hash = tx.id.clone();

        // Skip if already confirmed
        if self.confirmed_cache.contains(&hash) {
            return;
        }

        // Skip if rejected recently
        if self.rejected_cache.contains(&hash) {
            return;
        }

        self.transactions.insert(hash.clone(), tx.clone());

        // Build dependency relationships
        let mut tx_parents = HashSet::new();
        for input in &tx.inputs {
            // Use prev_tx and index to identify parent transaction
            if let Some(parent_hash) =
                self.find_spending_transaction(&(input.prev_tx.clone(), input.index))
            {
                tx_parents.insert(parent_hash.clone());
                self.children
                    .entry(parent_hash.clone())
                    .or_insert(HashSet::new())
                    .insert(hash.clone());
            }
        }

        self.parents.insert(hash, tx_parents);
    }

    /// Remove transaction from graph
    pub fn remove_transaction(&mut self, hash: &Hash) {
        self.transactions.remove(hash);
        self.parents.remove(hash);

        if let Some(children) = self.children.remove(hash) {
            for child in children {
                if let Some(parents) = self.parents.get_mut(&child) {
                    parents.remove(hash);
                }
            }
        }

        // Add to confirmed cache
        self.confirmed_cache.put(hash.clone(), ());
    }

    /// Check transaction dependencies
    pub fn check_dependencies(&self, tx: &SignedTransaction) -> Result<(), String> {
        let hash = tx.id.clone();

        // Check if all parents exist
        for input in &tx.inputs {
            if let Some(parent_hash) =
                self.find_spending_transaction(&(input.prev_tx.clone(), input.index))
            {
                if !self.transactions.contains_key(&parent_hash) {
                    return Err(format!("Missing parent transaction: {}", parent_hash));
                }
            }
        }

        // Check for cycles (simplified)
        if self.would_create_cycle(hash.clone(), &self.get_parents(hash.clone())) {
            return Err("Transaction would create cycle".to_string());
        }

        Ok(())
    }

    /// Get topological order for processing
    pub fn get_topological_order(&self) -> Vec<Hash> {
        let mut order = Vec::new();
        let mut visited = HashSet::new();
        let mut temp_visited = HashSet::new();

        for hash in self.transactions.keys() {
            if !visited.contains(hash) {
                self.dfs(hash.clone(), &mut visited, &mut temp_visited, &mut order);
            }
        }

        order.reverse(); // Reverse for dependency order
        order
    }

    /// Get ancestors of transaction
    pub fn get_ancestors(&self, hash: Hash) -> HashSet<Hash> {
        let mut ancestors = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(hash.clone());

        while let Some(current) = queue.pop_front() {
            if let Some(parents) = self.parents.get(&current) {
                for parent in parents {
                    if ancestors.insert(parent.clone()) {
                        queue.push_back(parent.clone());
                    }
                }
            }
        }

        ancestors
    }

    /// Get descendants of transaction
    pub fn get_descendants(&self, hash: Hash) -> HashSet<Hash> {
        let mut descendants = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(hash.clone());

        while let Some(current) = queue.pop_front() {
            if let Some(children) = self.children.get(&current) {
                for child in children {
                    if descendants.insert(child.clone()) {
                        queue.push_back(child.clone());
                    }
                }
            }
        }

        descendants
    }

    /// Mark transaction as rejected
    pub fn mark_rejected(&mut self, hash: Hash) {
        self.rejected_cache.put(hash.clone(), ());
        self.remove_transaction(&hash);
    }

    /// Find transaction that spends given output
    fn find_spending_transaction(&self, _prev_out: &(Hash, u32)) -> Option<Hash> {
        // Implementation would search mempool for spending tx
        None // Placeholder
    }

    /// Get parents of transaction
    fn get_parents(&self, hash: Hash) -> HashSet<Hash> {
        self.parents.get(&hash).cloned().unwrap_or_default()
    }

    /// Check if adding transaction would create cycle
    fn would_create_cycle(&self, hash: Hash, parents: &HashSet<Hash>) -> bool {
        for parent in parents {
            if self.has_path(parent.clone(), hash.clone()) {
                return true;
            }
        }
        false
    }

    /// Check if there's a path from start to end
    fn has_path(&self, start: Hash, end: Hash) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![start.clone()];

        while let Some(current) = stack.pop() {
            if current == end {
                return true;
            }

            if visited.insert(current.clone()) {
                if let Some(children) = self.children.get(&current) {
                    stack.extend(children.iter().map(|c| c.clone()));
                }
            }
        }

        false
    }

    /// DFS for topological sort
    fn dfs(
        &self,
        hash: Hash,
        visited: &mut HashSet<Hash>,
        temp_visited: &mut HashSet<Hash>,
        order: &mut Vec<Hash>,
    ) {
        if visited.contains(&hash) {
            return;
        }

        if !temp_visited.insert(hash.clone()) {
            // Cycle detected, but we'll continue
            return;
        }

        // Visit parents first
        if let Some(parents) = self.parents.get(&hash) {
            for parent in parents {
                self.dfs(parent.clone(), visited, temp_visited, order);
            }
        }

        temp_visited.remove(&hash);
        visited.insert(hash.clone());
        order.push(hash);
    }
}

/// Dependency cache for fast lookups
pub struct DependencyCache {
    /// Cache of transaction ancestors
    ancestor_cache: LruCache<Hash, HashSet<Hash>>,
    /// Cache of transaction descendants
    descendant_cache: LruCache<Hash, HashSet<Hash>>,
}

impl DependencyCache {
    pub fn new() -> Self {
        DependencyCache {
            ancestor_cache: LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()),
            descendant_cache: LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()),
        }
    }

    /// Get cached ancestors
    pub fn get_ancestors(&mut self, hash: Hash, graph: &TransactionGraph) -> HashSet<Hash> {
        if let Some(ancestors) = self.ancestor_cache.get(&hash) {
            ancestors.clone()
        } else {
            let ancestors = graph.get_ancestors(hash.clone());
            self.ancestor_cache.put(hash, ancestors.clone());
            ancestors
        }
    }

    /// Get cached descendants
    pub fn get_descendants(&mut self, hash: Hash, graph: &TransactionGraph) -> HashSet<Hash> {
        if let Some(descendants) = self.descendant_cache.get(&hash) {
            descendants.clone()
        } else {
            let descendants = graph.get_descendants(hash.clone());
            self.descendant_cache.put(hash, descendants.clone());
            descendants
        }
    }

    /// Invalidate cache for transaction
    pub fn invalidate(&mut self, hash: Hash) {
        self.ancestor_cache.pop(&hash);
        self.descendant_cache.pop(&hash);
    }
}
