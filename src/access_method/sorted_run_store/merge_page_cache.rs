// src/access_method/sorted_run_store/merge_page_cache.rs

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use crate::bp::{MemPool, PageFrameKey};
use crate::prelude::ContainerKey;

/// A specialized page cache to optimize memory usage during merge operations.
/// This cache helps reduce buffer pool contention by intelligently managing 
/// which pages stay in memory.
pub struct MergePageCache<M: MemPool> {
    mem_pool: Arc<M>,
    // Maps container key to a set of page frame keys
    cached_pages: Mutex<HashMap<ContainerKey, HashSet<PageFrameKey>>>,
    // Keep track of access counts to implement LFU eviction
    access_counts: Mutex<HashMap<PageFrameKey, usize>>,
    // Maximum number of pages to keep in the cache
    max_pages: usize,
}

impl<M: MemPool> MergePageCache<M> {
    /// Creates a new `MergePageCache` with the specified capacity.
    /// 
    /// # Arguments
    /// * `mem_pool` - The memory pool to use for page operations
    /// * `max_pages` - Maximum number of pages to cache (recommended: 5000-10000)
    pub fn new(mem_pool: Arc<M>, max_pages: usize) -> Self {
        Self {
            mem_pool,
            cached_pages: Mutex::new(HashMap::new()),
            access_counts: Mutex::new(HashMap::new()),
            max_pages,
        }
    }
    
    /// Adds a page to the cache and marks it as frequently accessed.
    /// If the cache is full, the least frequently used page will be evicted.
    pub fn add_page(&self, key: PageFrameKey) {
        let mut cached_pages = self.cached_pages.lock().unwrap();
        let mut access_counts = self.access_counts.lock().unwrap();
        
        // Add the page to the cache
        let container_entry = cached_pages
            .entry(key.p_key().c_key)
            .or_insert_with(HashSet::new);
        
        container_entry.insert(key);
        
        // Initialize or increment the access count
        let count = access_counts.entry(key).or_insert(0);
        *count += 1;
        
        // If we're over capacity, evict the least frequently used page
        if self.total_pages(&cached_pages) > self.max_pages {
            self.evict_lfu(&mut cached_pages, &mut access_counts);
        }
    }
    
    /// Prefetches a set of pages into the cache.
    /// Only prefetches pages that aren't already in memory.
    pub fn prefetch_pages(&self, keys: &[PageFrameKey]) {
        for &key in keys {
            // Only prefetch if not already in memory
            if !self.mem_pool.is_in_mem(key) {
                let _ = self.mem_pool.prefetch_page(key);
                self.add_page(key);
            }
        }
    }
    
    /// Records an access to a page, increasing its priority in the cache.
    pub fn record_access(&self, key: PageFrameKey) {
        let mut access_counts = self.access_counts.lock().unwrap();
        let count = access_counts.entry(key).or_insert(0);
        *count += 1;
    }
    
    /// Clears all cached pages for a specific container.
    /// This is useful when a container is no longer needed.
    pub fn clear_container(&self, c_key: ContainerKey) {
        let mut cached_pages = self.cached_pages.lock().unwrap();
        let mut access_counts = self.access_counts.lock().unwrap();
        
        if let Some(pages) = cached_pages.remove(&c_key) {
            for page in pages {
                access_counts.remove(&page);
                // Fast evict to release resources
                let _ = self.mem_pool.fast_evict(page.frame_id());
            }
        }
    }
    
    /// Counts the total number of pages in the cache
    fn total_pages(&self, cached_pages: &HashMap<ContainerKey, HashSet<PageFrameKey>>) -> usize {
        cached_pages.values().map(|set| set.len()).sum()
    }
    
    /// Evicts the least frequently used page from the cache
    fn evict_lfu(
        &self, 
        cached_pages: &mut HashMap<ContainerKey, HashSet<PageFrameKey>>,
        access_counts: &mut HashMap<PageFrameKey, usize>
    ) {
        // Find the least frequently used page
        if let Some((&lfu_key, _)) = access_counts.iter().min_by_key(|(_, &count)| count) {
            // Remove it from the access counts
            access_counts.remove(&lfu_key);
            
            // Remove it from the cached pages
            if let Some(pages) = cached_pages.get_mut(&lfu_key.p_key().c_key) {
                pages.remove(&lfu_key);
                
                // If the container is now empty, remove it
                if pages.is_empty() {
                    cached_pages.remove(&lfu_key.p_key().c_key);
                }
            }
            
            // Fast evict to release resources
            let _ = self.mem_pool.fast_evict(lfu_key.frame_id());
        }
    }
}