//! Module for managing sorted runs in external sorting.
//!
//! This module provides the `SortedRunStore` and associated iterator for managing
//! sorted runs stored in pages. Each page is a `SortedPage`, which stores key-value
//! pairs in sorted order. The `SortedRunStore` allows efficient scanning over the
//! sorted runs, including range scans with specified lower and upper bounds.
//!
//! # How `SortedPage` Works
//!
//! A `SortedPage` is a data structure designed to store key-value pairs in sorted
//! order within a fixed-size page (typically matching the size of a disk page or memory block).
//! It provides efficient insertion and lookup operations.
//!
//! - **Insertion**: Keys and values are appended to the page in sorted order.
//!   If the page does not have enough space to accommodate a new key-value pair,
//!   a new page is created.
//! - **Search**: Since the keys are stored in sorted order, binary search can be
//!   used to efficiently locate keys within the page.
//! - **Iteration**: The page provides methods to iterate over the stored key-value
//!   pairs in order.
//!
//! The `SortedPage` structure ensures that the data within a page is always sorted,
//! facilitating efficient external sorting algorithms that rely on merging sorted runs.

pub mod sorted_page;
use sorted_page::SortedPage;

use super::AccessMethodError;
use crate::bp::{FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey};
use crate::page::Page;
use crate::prelude::FrameReadGuard;
use crate::prelude::{ContainerKey, PageId};
use std::sync::Mutex;
use std::sync::{atomic::AtomicU32, Arc};
use std::time::Duration;

#[derive(Debug)]
/// `SortedRunStore` manages a collection of sorted pages (runs) for external sorting.
///
/// It allows for storing sorted key-value pairs across multiple pages and provides
/// methods to scan over the sorted data, including range scans.
pub struct SortedRunStore<T: MemPool> {
    pub c_key: ContainerKey, // Container key identifying the storage container
    pub mem_pool: Arc<T>,    // Shared memory pool for page management
    // page_ids: Vec<PageId>,    // List of page IDs that make up the sorted run xtx replace with num tuples in each page
    pub total_len: usize, // List of page IDs that make up the sorted run xtx replace with num tuples in each page
    pub min_keys: Vec<Vec<u8>>, // Stores the minimum key of each page for efficient lookup
    pub page_ids: Vec<PageFrameKey>,
}

impl<T: MemPool> SortedRunStore<T> {

    pub fn create(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        // Create and initialize the first data page
        let mut data_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        data_page.init();
        let data_key = {
            let page_id = data_page.get_id();
            let frame_id = data_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        SortedRunStore {
            c_key,
            mem_pool: mem_pool.clone(),
            total_len: 0,
            min_keys: Vec::new(),
            page_ids: vec![data_key],
        }
    }

    pub fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        // Initialize structures to track pages and keys
        let mut page_ids = Vec::new();
        let mut min_keys = Vec::new();
        let mut total_len = 0;
        
        // Constants for batch allocation
        const BATCH_SIZE: usize = 16; // Adjust based on your workload characteristics
        let mut batch_buffer: Vec<FrameWriteGuard> = Vec::with_capacity(BATCH_SIZE);
        let mut current_page_idx = 0;
        
        // Pre-allocate batch of pages - will be used as needed
        if let Ok(pages) = mem_pool.create_new_pages_for_write(c_key, BATCH_SIZE) {
            for mut page in pages {
                page.init();
                let page_frame_key = {
                    let page_id = page.get_id();
                    let frame_id = page.frame_id();
                    PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
                };
                page_ids.push(page_frame_key);
                batch_buffer.push(page);
            }
        } else {
            // Fallback to single page allocation if batch allocation fails
            let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
            page.init();
            let page_frame_key = {
                let page_id = page.get_id();
                let frame_id = page.frame_id();
                PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
            };
            page_ids.push(page_frame_key);
            batch_buffer.push(page);
        }

        // Process iterator of key-value pairs
        for (k, v) in iter {
            total_len += 1;
            let key_ref = k.as_ref();
            let value_ref = v.as_ref();

            // Try to append to the current page
            if current_page_idx < batch_buffer.len() && batch_buffer[current_page_idx].append(key_ref, value_ref) {
                // If this is the first key in the page, store it as the min key
                if batch_buffer[current_page_idx].slot_count() == 1 {
                    min_keys.push(key_ref.to_vec());
                }
            } else {
                // Current page is full or we've used all pre-allocated pages, get a new page
                current_page_idx += 1;
                
                // If we've used all pages in our batch, allocate more
                if current_page_idx >= batch_buffer.len() {
                    // Drop current batch to release resources
                    batch_buffer.clear();
                    
                    // Allocate a new batch
                    if let Ok(pages) = mem_pool.create_new_pages_for_write(c_key, BATCH_SIZE) {
                        for mut page in pages {
                            page.init();
                            let page_frame_key = {
                                let page_id = page.get_id();
                                let frame_id = page.frame_id();
                                PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
                            };
                            page_ids.push(page_frame_key);
                            batch_buffer.push(page);
                        }
                    } else {
                        // Fallback to single page allocation
                        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
                        page.init();
                        let page_frame_key = {
                            let page_id = page.get_id();
                            let frame_id = page.frame_id();
                            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
                        };
                        page_ids.push(page_frame_key);
                        batch_buffer.push(page);
                    }
                    current_page_idx = 0;
                }

                // Append to the new page (should succeed)
                assert!(batch_buffer[current_page_idx].append(key_ref, value_ref));
                min_keys.push(key_ref.to_vec());
            }
        }

        // Cleanup - release all pages in the batch buffer
        drop(batch_buffer);

        Self {
            c_key,
            mem_pool,
            total_len,
            page_ids,
            min_keys,
        }
    }

    /// Batch appends multiple key-value pairs to the store.
    /// This can be significantly more efficient for bulk operations.
    pub fn batch_append<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        kvs: &[(K, V)]
    ) -> Result<(), AccessMethodError> {
        if kvs.is_empty() {
            return Ok(());
        }
        
        // Get the last page key first to avoid borrow issues
        let last_page_key = *self.page_ids.last().unwrap();
        
        // Track state outside the page access
        let mut current_slot_count = 0;
        let mut current_key = last_page_key;
        let mut min_keys_to_add = Vec::new();
        let mut new_page_keys = Vec::new();
        
        // Increment total length
        self.total_len += kvs.len();
        
        // Process all KV pairs
        for (i, (key, value)) in kvs.iter().enumerate() {
            let key_ref = key.as_ref();
            let value_ref = value.as_ref();
            
            // Get the current page
            let mut current_page = self.write_page(&current_key);
            
            if i == 0 {
                // Initialize slot count for the first iteration
                current_slot_count = current_page.slot_count();
            }
            
            // Try to append to the current page
            if current_page.append(key_ref, value_ref) {
                // If this is the first key in the page, remember it for later
                if current_slot_count == 0 {
                    min_keys_to_add.push(key_ref.to_vec());
                }
                
                // Update slot count
                current_slot_count += 1;
                
                // Release the page
                drop(current_page);
            } else {
                // Current page is full, release it
                drop(current_page);
                
                // Create a new page
                let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
                new_page.init();
                
                let page_id = new_page.get_id();
                let frame_id = new_page.frame_id();
                
                // Create a new page key
                let new_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
                
                // Add the new page key to our list to be added later
                new_page_keys.push(new_key);
                
                // This is the first key in the page
                min_keys_to_add.push(key_ref.to_vec());
                
                // Append the key-value pair to the new page
                assert!(new_page.append(key_ref, value_ref));
                
                // Update tracking variables
                current_key = new_key;
                current_slot_count = 1;
                
                // Release the page
                drop(new_page);
            }
        }
        
        // Now update our data structures with the collected information
        self.page_ids.extend(new_page_keys);
        self.min_keys.extend(min_keys_to_add);
        
        Ok(())
    }

    fn write_page(&self, page_key: &PageFrameKey) -> FrameWriteGuard {
        //let base = Duration::from_micros(10);
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_write(*page_key) {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    attempts += 1;
                    // std::thread::sleep(base * attempts);
                }
                Err(e) => panic!("Error: {}", e),
            }
        }
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard<'static> {
        // let base: u64 = 2;
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_read(page_key) {
                Ok(page) => {
                    return unsafe {
                        std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(page)
                    }
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    // std::thread::sleep(Duration::from_nanos(base.pow(attempts)));
                    attempts += 1;
                }
                Err(e) => panic!("Error getting page key {}: {}", page_key, e),
            }
        }
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.total_len += 1;
        let last_key = self.page_ids.last().cloned().unwrap();
        let mut last_page = self.write_page(&last_key);

        // Try to append to the last page
        if last_page.append(key, value) {
            if last_page.slot_count() == 1 {
                drop(last_page);
                self.min_keys.push(key.to_vec());
            }
            Ok(())
        } else {
            drop(last_page);
            // Page overflow: create a new page
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            new_page.init();

            let page_id = new_page.get_id();
            let frame_id = new_page.frame_id();

            // self.stats.inc_num_pages();

            // Update the in-memory last key to the new page
            let new_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
            self.page_ids.push(new_key);

            // Append the key-value pair to the new page
            assert!(new_page.append(key, value));

            self.min_keys.push(key.to_vec());
            Ok(())
        }
    }

    /// Creates an iterator over all key-value pairs in the sorted run.
    ///
    /// This is equivalent to calling `scan_range` with empty bounds.
    pub fn scan<'a>(&'a self) -> SortedRunStoreRangeScanner<T> {
        self.scan_range(&[], &[])
    }

    /// Creates an iterator over a range of key-value pairs in the sorted run.
    ///
    /// The range is specified by `lower_bound` and `upper_bound`. If `lower_bound`
    /// is empty, iteration starts from the beginning. If `upper_bound` is empty,
    /// iteration continues to the end.
    ///
    /// # Arguments
    ///
    /// - `lower_bound`: The inclusive lower bound of keys to start iteration.
    /// - `upper_bound`: The exclusive upper bound of keys to stop iteration.
    pub fn scan_range<'a>(
        &'a self,
        lower_bound: &[u8],
        upper_bound: &[u8],
    ) -> SortedRunStoreRangeScanner<T> {
        // Determine the starting page index based on the lower bound
        let start_page_index = if lower_bound.is_empty() {
            0
        } else {
            // Binary search over min_keys to find the starting page index
            // We need to find the last page whose min_key is < lower_bound
            let mut left = 0;
            let mut right = self.min_keys.len();

            while left < right {
                let mid = left + (right - left) / 2;

                match self.min_keys[mid].as_slice().cmp(lower_bound) {
                    std::cmp::Ordering::Less => {
                        // The minimum key is less than our lower bound
                        // This page might contain our values, try to find a later page
                        left = mid + 1;
                    }
                    std::cmp::Ordering::Equal => {
                        // If we find an exact match, we want this page
                        // but also need to check previous pages for potential matches
                        right = mid;
                    }
                    std::cmp::Ordering::Greater => {
                        // The minimum key is greater than our lower bound
                        // Need to look in earlier pages
                        right = mid;
                    }
                }
            }

            // If left == min_keys.len(), we want the last page
            // Otherwise, we want the page before left (unless left is 0)
            if left == self.min_keys.len() {
                self.min_keys.len() - 1
            } else if left > 0 {
                // If the page at left-1 has min_key equal to lower_bound,
                // we might need to go back one more page
                if left > 1 && self.min_keys[left - 1].as_slice() == lower_bound {
                    left - 2
                } else {
                    left - 1
                }
            } else {
                0
            }
        };

        SortedRunStoreRangeScanner {
            storage: Arc::new(self.clone()),
            lower_bound: lower_bound.to_vec(),
            upper_bound: upper_bound.to_vec(),
            current_page_index: start_page_index,
            current_page: None,
            current_slot_id: 0,
            is_done: false,
            is_init: false,
        }
    }



    pub fn scan_range_arc(
        this: &Arc<Self>,
        lower_bound: &[u8],
        upper_bound: &[u8],
    ) -> SortedRunStoreRangeScanner<T> {
        // --- find the first page that could contain `lower_bound` -------------
        let start_page_index = if lower_bound.is_empty() {
            0
        } else {
            // binary‑search in `min_keys`
            let mut left = 0usize;
            let mut right = this.min_keys.len();
            while left < right {
                let mid = left + (right - left) / 2;
                match this.min_keys[mid].as_slice().cmp(lower_bound) {
                    std::cmp::Ordering::Less    => left  = mid + 1, // look to the right
                    _ /* Equal | Greater */     => right = mid,     // mid could still work
                }
            }
            if left == 0 { 0 } else { left - 1 }
        };

        // --- build the scanner -------------------------------------------------
        SortedRunStoreRangeScanner {
            storage:        Arc::clone(this),          // <‑‑ tiny & thread‑safe
            lower_bound:    lower_bound.to_vec(),
            upper_bound:    upper_bound.to_vec(),
            current_page_index: start_page_index,
            current_page:   None,
            current_slot_id: 0,
            is_init:        false,
            is_done:        false,
        }
    }


    pub fn scan_range_with_index<'a>(
        &'a self,
        lower_bound: &[u8],
        upper_bound: &[u8],
        start_page_index: usize,
    ) -> SortedRunStoreRangeScanner<T> {
        SortedRunStoreRangeScanner {
            storage: Arc::new(self.clone()),
            lower_bound: lower_bound.to_vec(),
            upper_bound: upper_bound.to_vec(),
            current_page_index: start_page_index,
            current_page: None,
            current_slot_id: 0,
            is_done: false,
            is_init: false,
        }
    }

    

    /// Returns the total number of tuples stored in the `SortedRunStore`.
    ///
    /// This method iterates over all the pages in the run and sums up the number of
    /// tuples in each page. It provides an aggregate count of all key-value pairs stored.
    pub fn len(&self) -> usize {
        self.total_len
    }

    /// Returns the total number of pages used in the `SortedRunStore`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let sorted_run_store = SortedRunStore::new(/* parameters */);
    /// let total_pages = sorted_run_store.num_pages();
    /// println!("Total pages used: {}", total_pages);
    /// ```
    pub fn num_pages(&self) -> usize {
        self.page_ids.len()
    }
}

impl<T: MemPool> Clone for SortedRunStore<T> {
    fn clone(&self) -> Self {
        SortedRunStore {
            c_key: self.c_key,
            mem_pool: self.mem_pool.clone(),
            page_ids: self.page_ids.clone(),
            total_len: self.total_len,
            min_keys: self.min_keys.clone(),
        }
    }
}

/// Iterator for scanning over a range of key-value pairs in a `SortedRunStore`.
pub struct SortedRunStoreRangeScanner<T: MemPool> {
    storage: Arc<SortedRunStore<T>>, // Reference to the storage
    lower_bound: Vec<u8>,            // Inclusive lower bound for keys
    upper_bound: Vec<u8>,            // Exclusive upper bound for keys
    pub current_page_index: usize,   // Index of the current page in `page_ids`
    current_page: Option<FrameReadGuard<'static>>, // Current page being scanned
    current_slot_id: u32,            // Current position within the page
    is_init: bool,
    is_done: bool,
}

impl<'a, T: MemPool> Iterator for SortedRunStoreRangeScanner<T> {
    type Item = (Vec<u8>, Vec<u8>); // Returns key-value pairs as (Vec<u8>, Vec<u8>)

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done {
            return None;
        }
        if !self.is_init {
            self.initialize();
        }
        loop {
            let page = self.current_page.as_ref().unwrap();
            while self.current_slot_id < page.slot_count() {
                let (key, value) = page.get(self.current_slot_id);
                self.current_slot_id += 1;

                // Lower bound: key must be >= lower_bound
                if key < self.lower_bound.as_slice() {
                    // keep searching
                    continue;
                }

                // **Upper bound: key must be < upper_bound** (always half-open)
                if !self.upper_bound.is_empty() {
                    if key >= self.upper_bound.as_slice() {
                        // This key is outside our range, so we can stop this iteration
                        return None;
                    }
                }

                // Key is within [lower_bound, upper_bound), so yield it
                return Some((key.to_vec(), value.to_vec()));
            }

            // If we exhaust this page, move on to the next
            self.current_page_index += 1;
            if self.current_page_index >= self.storage.page_ids.len() {
                // Done
                self.is_done = true;
                return None;
            } else {
                let next_page_id = self.storage.page_ids[self.current_page_index];
                let next_page = self.storage.read_page(next_page_id);
                self.current_page = Some(next_page);
                self.current_slot_id = 0;
            }
        }
    }
}

impl<T: MemPool> SortedRunStoreRangeScanner<T> {
    pub fn initialize(&mut self) {
        // If current_page is empty, move to the next page
        if self.current_page.is_none() {
            if self.current_page_index >= self.storage.page_ids.len() {
                panic!("exceeded capacity");
            }
            let page_key = self.storage.page_ids[self.current_page_index];
            let page = self
                .storage.read_page(page_key);
            self.current_page = Some(page);
            self.current_slot_id = 0;
            self.is_init = true;
        }
    }
}

/// A collection of multiple `SortedRunStore`s that can be iterated over as a single sorted sequence.
///
/// `BigSortedRunStore` maintains multiple `SortedRunStore`s and provides methods to iterate over them
/// as if they were a single sorted collection. While individual `SortedRunStore`s are internally sorted,
/// there is no guaranteed ordering between different stores. The iterator handles this by always
/// returning the smallest available key across all stores.
pub struct BigSortedRunStore<T: MemPool> {
    pub sorted_run_stores: Vec<Arc<SortedRunStore<T>>>,
    first_keys: Vec<Vec<u8>>,
}

impl<T: MemPool> BigSortedRunStore<T> {
    /// Creates a new empty `BigSortedRunStore`.
    ///
    /// Returns a `BigSortedRunStore` with no underlying stores. Stores can be added later using
    /// the `add_store` method.
    ///
    /// # Examples
    /// ```
    /// let big_store: BigSortedRunStore<MyMemPool> = BigSortedRunStore::new();
    /// ```
    pub fn new() -> Self {
        Self {
            sorted_run_stores: Vec::new(),
            first_keys: Vec::new(),
        }
    }

    /// Adds a `SortedRunStore` to the collection.
    ///
    /// The added store becomes part of the collection and will be included in all subsequent
    /// scans and iterations. Note that there is no guaranteed ordering between different stores -
    /// the iterator will handle sorting across stores during iteration.
    ///
    /// # Arguments
    /// * `store` - The `SortedRunStore` to add to the collection
    ///
    /// # Examples
    /// ```
    /// let mut big_store = BigSortedRunStore::new();
    /// let store = SortedRunStore::new(/* params */);
    /// big_store.add_store(store);
    /// ```
    pub fn add_store(&mut self, store: Arc<SortedRunStore<T>>) {
        // If the sorted run is empty, then its min_keys will be empty.
        // In that case, simply do not add it.
        if store.min_keys.is_empty() {
            return;
        }
        self.first_keys.push(store.min_keys[0].clone());
        self.sorted_run_stores.push(store);
    }

    /// Returns the total number of key-value pairs across all stores.
    ///
    /// This method sums up the lengths of all contained `SortedRunStore`s to provide
    /// the total count of key-value pairs in the collection.
    ///
    /// # Returns
    /// * The total number of key-value pairs in all stores combined
    ///
    /// # Examples
    /// ```
    /// let big_store = BigSortedRunStore::new();
    /// let total_pairs = big_store.len();
    /// println!("Total key-value pairs: {}", total_pairs);
    /// ```
    pub fn len(&self) -> usize {
        self.sorted_run_stores.iter().map(|store| store.len()).sum()
    }

    /// Returns the total number of pages used across all stores.
    ///
    /// This method sums up the number of pages in all contained `SortedRunStore`s to
    /// provide the total page count for the collection.
    ///
    /// # Returns
    /// * The total number of pages used by all stores combined
    ///
    /// # Examples
    /// ```
    /// let big_store = BigSortedRunStore::new();
    /// let total_pages = big_store.num_pages();
    /// println!("Total pages used: {}", total_pages);
    /// ```
    pub fn num_pages(&self) -> usize {
        self.sorted_run_stores
            .iter()
            .map(|store| store.num_pages())
            .sum()
    }

    /// Creates an iterator over all key-value pairs in all stores.
    ///
    /// This method provides an iterator that will return all key-value pairs across
    /// all stores in sorted order by key. This is equivalent to calling `scan_range`
    /// with empty bounds.
    ///
    /// # Returns
    /// * A `BigSortedRunStoreScanner` that iterates over all key-value pairs
    ///
    /// # Examples
    /// ```
    /// let big_store = BigSortedRunStore::new();
    /// for (key, value) in big_store.scan() {
    ///     println!("Key: {:?}, Value: {:?}", key, value);
    /// }
    /// ```
    pub fn scan(&self) -> BigSortedRunStoreScanner<T> {
        self.scan_range(&[], &[])
    }

    /// Creates an iterator over a range of key-value pairs across all stores.
    ///
    /// This method provides an iterator that will return key-value pairs within the
    /// specified range across all stores in sorted order by key. The range is inclusive
    /// of the lower bound and exclusive of the upper bound.
    ///
    /// # Arguments
    /// * `lower_bound` - The inclusive lower bound of keys to return (empty slice for no lower bound)
    /// * `upper_bound` - The exclusive upper bound of keys to return (empty slice for no upper bound)
    ///
    /// # Returns
    /// * A `BigSortedRunStoreScanner` that iterates over key-value pairs within the specified range
    ///
    /// # Examples
    /// ```
    /// let big_store = BigSortedRunStore::new();
    /// let lower = b"start";
    /// let upper = b"end";
    /// for (key, value) in big_store.scan_range(lower, upper) {
    ///     println!("Key: {:?}, Value: {:?}", key, value);
    /// }
    /// ```
    pub fn scan_range(
        &self,
        lower_bound: &[u8],
        upper_bound: &[u8],
    ) -> BigSortedRunStoreScanner<T> {
        let mut start_idx = if lower_bound.is_empty() {
            0
        } else {
            // Binary search for lower bound
            let mut left = 0;
            let mut right = self.first_keys.len();

            while left < right {
                let mid = left + (right - left) / 2;
                match self.first_keys[mid].as_slice().cmp(lower_bound) {
                    std::cmp::Ordering::Less => left = mid + 1,
                    std::cmp::Ordering::Equal => right = mid,
                    std::cmp::Ordering::Greater => right = mid,
                }
            }
            if left > 0 {
                left - 1
            } else {
                0
            }
        };

        let end_idx = if upper_bound.is_empty() {
            self.first_keys.len()
        } else {
            // Binary search for upper bound
            let mut left = start_idx;
            let mut right = self.first_keys.len();

            while left < right {
                let mid = left + (right - left) / 2;
                match self.first_keys[mid].as_slice().cmp(upper_bound) {
                    std::cmp::Ordering::Less => left = mid + 1,
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => right = mid,
                }
            }
            left
        };

        let scanners = self.sorted_run_stores[start_idx..end_idx]
            .iter()
            .map(|store| store.scan_range(lower_bound, upper_bound))
            .collect();

        BigSortedRunStoreScanner {
            scanners,
            current_values: vec![None; end_idx - start_idx],
        }
    }
    

    pub fn scan_range_arc(
        this: &Arc<Self>,
        lower_bound: &[u8],
        upper_bound: &[u8],
    ) -> BigSortedRunStoreScanner<T> {
        // 1) find the slice of inner runs we must touch
        let start_idx = if lower_bound.is_empty() {
            0
        } else {
            let mut left = 0;
            let mut right = this.first_keys.len();
            while left < right {
                let mid = left + (right - left) / 2;
                match this.first_keys[mid].as_slice().cmp(lower_bound) {
                    std::cmp::Ordering::Less => left = mid + 1,
                    _                         => right = mid,
                }
            }
            if left == 0 { 0 } else { left - 1 }
        };

        let end_idx = if upper_bound.is_empty() {
            this.first_keys.len()
        } else {
            let mut left = start_idx;
            let mut right = this.first_keys.len();
            while left < right {
                let mid = left + (right - left) / 2;
                match this.first_keys[mid].as_slice().cmp(upper_bound) {
                    std::cmp::Ordering::Less => left = mid + 1,
                    _                         => right = mid,
                }
            }
            left
        };

        // 2) build the per-run scanners
        let scanners: Vec<_> = (start_idx..end_idx)
            .map(|idx| {
                let inner = Arc::clone(&this.sorted_run_stores[idx]);
                SortedRunStore::scan_range_arc(&inner, lower_bound, upper_bound)
            })
            .collect();

        // 3) allocate the current_values buffer _before_ moving `scanners`
        let current_values = vec![None; scanners.len()];

        // 4) now move both into the scanner
        BigSortedRunStoreScanner {
            scanners,
            current_values,
        }
    }
}

/// Iterator for scanning over key-value pairs across multiple `SortedRunStore`s.
///
/// This scanner maintains a buffer of the current value from each underlying store
/// and always returns the key-value pair with the smallest key. This ensures that
/// even though the underlying stores might not be sorted relative to each other,
/// the iterator provides values in sorted order.
pub struct BigSortedRunStoreScanner<T: MemPool> {
    scanners: Vec<SortedRunStoreRangeScanner<T>>,
    current_values: Vec<Option<(Vec<u8>, Vec<u8>)>>,
}

impl<'a, T: MemPool> Iterator for BigSortedRunStoreScanner<T> {
    type Item = (Vec<u8>, Vec<u8>);

    /// Returns the next key-value pair in sorted order across all stores.
    ///
    /// This method maintains a buffer of the current value from each store and
    /// always returns the pair with the smallest key. When a value is returned,
    /// that store's buffer is refilled with its next value.
    ///
    /// # Returns
    /// * `Some((key, value))` if there are more pairs to return
    /// * `None` if iteration is complete
    fn next(&mut self) -> Option<Self::Item> {
        // Fill any empty slots in current_values with the next value from corresponding scanner
        for (i, scanner) in self.scanners.iter_mut().enumerate() {
            if self.current_values[i].is_none() {
                self.current_values[i] = scanner.next();
            }
        }

        // Find the minimum key among current values
        let mut min_idx = None;
        let mut min_key = None;

        for (i, value) in self.current_values.iter().enumerate() {
            if let Some((key, _)) = value {
                match min_key {
                    None => {
                        min_idx = Some(i);
                        min_key = Some(key);
                    }
                    Some(current_min) if key < current_min => {
                        min_idx = Some(i);
                        min_key = Some(key);
                    }
                    _ => {}
                }
            }
        }

        // Return and clear the minimum value if found
        min_idx.map(|i| self.current_values[i].take().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::bp::{get_test_bp, BufferPool};
    use crate::random::{gen_random_byte_vec, RandomKVs};

    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    fn get_c_key() -> ContainerKey {
        // Implementation of the container key creation
        ContainerKey::new(0, 0)
    }

    #[test]
    fn test_small_append() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let mut store = SortedRunStore::create(container_key, mem_pool);

        let key = b"small key";
        let value = b"small value";
        assert_eq!(store.append(key, value), Ok(()));
    }

    // #[test]
    // fn test_large_append() {
    //     let mem_pool = get_test_bp(10);
    //     let container_key = get_c_key();
    //     let mut store = SortedRunStore::create(container_key, mem_pool);

    //     let key = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
    //     let value = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
    //     assert_eq!(
    //         store.append(&key, &value),
    //         Err(AccessMethodError::RecordTooLarge)
    //     );

    //     // Scan should return nothing
    //     let mut scanner = store.scan();
    //     assert!(scanner.next().is_none());
    // }

    #[test]
    fn test_page_overflow() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let mut store = SortedRunStore::create(container_key, mem_pool);

        let key = gen_random_byte_vec(1000, 1000);
        let value = gen_random_byte_vec(1000, 1000);
        let num_appends = 100;

        for _ in 0..num_appends {
            assert_eq!(store.append(&key, &value), Ok(()));
        }
    }

    #[test]
    fn test_basic_scan() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let mut store = SortedRunStore::create(container_key, mem_pool.clone());

        let num_kvs = 10000;

        let key = b"scanned key";
        let value = b"scanned value";
        for i in 0..num_kvs {
            store.append(key, value).unwrap();
        }

        assert_eq!(store.len(), num_kvs);

        let mut scanner = store.scan();

        for i in 0..num_kvs {
            assert_eq!(scanner.next().unwrap(), (key.to_vec(), value.to_vec()));
        }
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_stress() {
        let num_keys = 10000;
        let key_size = 50;
        let val_min_size = 50;
        let val_max_size = 100;
        let vals = RandomKVs::new(
            false,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        )
        .pop()
        .unwrap();

        let mut store = SortedRunStore::create(get_c_key(), get_test_bp(10));

        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Appending record {} **********************",
                i
            );
            store.append(val.0, val.1).unwrap();
        }

        assert_eq!(store.len(), num_keys);

        let mut scanner = store.scan();
        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Scanning record {} **********************",
                i
            );
            assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
        }
    }

    /*
        #[test]
        fn test_concurrent_append() {
            let num_keys = 10000;
            let key_size = 50;
            let val_min_size = 50;
            let val_max_size = 100;
            let num_threads = 3;
            let vals = RandomKVs::new(
                false,
                false,
                num_threads,
                num_keys,
                key_size,
                val_min_size,
                val_max_size,
            );

            let store = Arc::new(AppendOnlyStore::new(get_c_key(), get_test_bp(10)));

            let mut verify_vals = HashSet::new();
            for val_i in vals.iter() {
                for val in val_i.iter() {
                    verify_vals.insert((val.0.to_vec(), val.1.to_vec()));
                }
            }

            thread::scope(|s| {
                for val_i in vals.iter() {
                    let store_clone = store.clone();
                    s.spawn(move || {
                        for val in val_i.iter() {
                            store_clone.append(val.0, val.1).unwrap();
                        }
                    });
                }
            });

            assert_eq!(store.num_kvs(), num_keys);

            // Check if all values are appended.
            let scanner = store.scan();
            for val in scanner {
                assert!(verify_vals.remove(&val));
            }
            assert!(verify_vals.is_empty());
        }
    */
    #[test]
    fn test_scan_finish_condition() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = SortedRunStore::create(container_key, mem_pool.clone());

        let mut scanner = store.scan();
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_bulk_insert_create() {
        let num_keys = 10000;
        let key_size = 50;
        let val_min_size = 50;
        let val_max_size = 100;
        let vals = RandomKVs::new(
            false,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        )
        .pop()
        .unwrap();

        let store = SortedRunStore::new(get_c_key(), get_test_bp(10), vals.iter());

        assert_eq!(store.len(), num_keys);

        let mut scanner = store.scan();
        for val in vals.iter() {
            assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
        }
    }

    /*
    #[test]
    fn test_durability() {
        let temp_dir = tempfile::tempdir().unwrap();

        let num_keys = 10000;
        let key_size = 50;
        let val_min_size = 50;
        let val_max_size = 100;
        let vals = RandomKVs::new(
            false,
            false,
            1,
            num_keys,
            key_size,
            val_min_size,
            val_max_size,
        )
        .pop()
        .unwrap();

        // Create a store and insert some values.
        // Drop the store and buffer pool
        {
            let bp = Arc::new(BufferPool::new(&temp_dir, 10, false).unwrap());

            let store = SortedRunStore::new(
                get_c_key(),
                bp.clone(),
                vals.iter(),
            );

            drop(store);
            drop(bp);
        }

        {
            let bp = Arc::new(BufferPool::new(&temp_dir, 10, false).unwrap());
            let store = SortedRunStore::create(get_c_key(), bp.clone(), 0);

            let mut scanner = store.scan();
            for val in vals.iter() {
                assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
            }
        }
    }
    */
}
