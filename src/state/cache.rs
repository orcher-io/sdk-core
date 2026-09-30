//! LRU cache of workflow state.
//!
//! Keeping recently active workflows in memory avoids reloading their execution journals
//! (the event-sourced history of each run) from the server.

use crate::proto::orcher::v1::JournalEntry;
use crate::types::WorkflowExecution;
use lru::LruCache;
use parking_lot::RwLock;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Workflow state held in the cache.
#[derive(Debug, Clone)]
pub struct CachedWorkflowState {
    /// Identifiers of the workflow execution.
    pub execution: WorkflowExecution,

    /// Execution journal entries loaded so far.
    pub execution_journal: Vec<JournalEntry>,

    /// ID of the last journal entry in `execution_journal`.
    pub last_event_id: i64,

    /// When the entry was cached; [`WorkflowCache::evict_old`] ages entries by this.
    pub cached_at: std::time::Instant,
}

/// Thread-safe LRU cache of workflow state, keyed by workflow ID.
///
/// Avoids reloading state from the server for workflows that run often. Clones share the
/// same underlying cache.
///
/// # Examples
///
/// ```
/// use orcher_sdk_core::state::WorkflowCache;
///
/// let cache = WorkflowCache::new(1000); // Cache up to 1000 workflows
/// ```
pub struct WorkflowCache {
    cache: Arc<RwLock<LruCache<String, CachedWorkflowState>>>,
}

impl WorkflowCache {
    /// Creates a cache that holds at most `capacity` workflows.
    ///
    /// A `capacity` of zero falls back to 100.
    ///
    /// # Examples
    ///
    /// ```
    /// use orcher_sdk_core::state::WorkflowCache;
    ///
    /// let cache = WorkflowCache::new(1000);
    /// ```
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap());
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
        }
    }

    /// Returns a copy of the cached state for `workflow_id`, if present, and marks it as
    /// most recently used.
    pub fn get(&self, workflow_id: &str) -> Option<CachedWorkflowState> {
        // A write lock, because an LRU lookup updates recency.
        let mut cache = self.cache.write();
        cache.get(workflow_id).cloned()
    }

    /// Stores `state` for `workflow_id`, replacing any existing entry.
    ///
    /// If the cache is full, the least recently used entry is evicted.
    pub fn put(&self, workflow_id: String, state: CachedWorkflowState) {
        let mut cache = self.cache.write();
        cache.put(workflow_id, state);
    }

    /// Removes `workflow_id` from the cache and returns its state, if it was cached.
    pub fn remove(&self, workflow_id: &str) -> Option<CachedWorkflowState> {
        let mut cache = self.cache.write();
        cache.pop(workflow_id)
    }

    /// Removes every entry.
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }

    /// Returns the number of cached workflows.
    pub fn len(&self) -> usize {
        let cache = self.cache.read();
        cache.len()
    }

    /// Returns `true` if no workflows are cached.
    pub fn is_empty(&self) -> bool {
        let cache = self.cache.read();
        cache.is_empty()
    }

    /// Returns the maximum number of workflows the cache holds.
    pub fn capacity(&self) -> usize {
        let cache = self.cache.read();
        cache.cap().get()
    }

    /// Evicts every entry cached longer than `max_age` ago and returns how many were
    /// evicted.
    pub fn evict_old(&self, max_age: std::time::Duration) -> usize {
        let mut cache = self.cache.write();
        let now = std::time::Instant::now();
        let mut evicted = 0;

        // Collect the keys first: the cache cannot be modified while it is being iterated.
        let keys_to_evict: Vec<String> = cache
            .iter()
            .filter(|(_, state)| now.duration_since(state.cached_at) > max_age)
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_evict {
            cache.pop(&key);
            evicted += 1;
        }

        evicted
    }
}

impl Clone for WorkflowCache {
    fn clone(&self) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
        }
    }
}

impl Default for WorkflowCache {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WorkflowExecution;

    fn create_test_state(workflow_id: &str) -> CachedWorkflowState {
        CachedWorkflowState {
            execution: WorkflowExecution::new(workflow_id, "test-run"),
            execution_journal: vec![],
            last_event_id: 10,
            cached_at: std::time::Instant::now(),
        }
    }

    #[test]
    fn test_cache_basic_operations() {
        let cache = WorkflowCache::new(10);

        let state = create_test_state("workflow-1");
        cache.put("workflow-1".to_string(), state.clone());

        let retrieved = cache.get("workflow-1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().execution.workflow_id, "workflow-1");

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
    }

    #[test]
    fn test_cache_capacity() {
        let cache = WorkflowCache::new(2);
        assert_eq!(cache.capacity(), 2);

        cache.put("workflow-1".to_string(), create_test_state("workflow-1"));
        cache.put("workflow-2".to_string(), create_test_state("workflow-2"));
        cache.put("workflow-3".to_string(), create_test_state("workflow-3"));

        // workflow-1 is the least recently used, so it is evicted.
        assert_eq!(cache.len(), 2);
        assert!(cache.get("workflow-1").is_none());
        assert!(cache.get("workflow-2").is_some());
        assert!(cache.get("workflow-3").is_some());
    }

    #[test]
    fn test_cache_remove() {
        let cache = WorkflowCache::new(10);
        cache.put("workflow-1".to_string(), create_test_state("workflow-1"));

        let removed = cache.remove("workflow-1");
        assert!(removed.is_some());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_clear() {
        let cache = WorkflowCache::new(10);
        cache.put("workflow-1".to_string(), create_test_state("workflow-1"));
        cache.put("workflow-2".to_string(), create_test_state("workflow-2"));

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_evict_old() {
        let cache = WorkflowCache::new(10);

        let mut old_state = create_test_state("workflow-old");
        old_state.cached_at = std::time::Instant::now() - std::time::Duration::from_secs(120);

        cache.put("workflow-old".to_string(), old_state);
        cache.put(
            "workflow-new".to_string(),
            create_test_state("workflow-new"),
        );

        let evicted = cache.evict_old(std::time::Duration::from_secs(60));

        assert_eq!(evicted, 1);
        assert_eq!(cache.len(), 1);
        assert!(cache.get("workflow-old").is_none());
        assert!(cache.get("workflow-new").is_some());
    }
}
