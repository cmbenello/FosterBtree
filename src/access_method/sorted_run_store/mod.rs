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

use std::sync::Arc;
use crate::bp::{MemPool, PageFrameKey};
use crate::prelude::{PageId, ContainerKey};
use crate::page::Page;
use crate::prelude::FrameReadGuard;

#[derive(Debug)]
/// `SortedRunStore` manages a collection of sorted pages (runs) for external sorting.
///
/// It allows for storing sorted key-value pairs across multiple pages and provides
/// methods to scan over the sorted data, including range scans.
pub struct SortedRunStore<T: MemPool> {
    c_key: ContainerKey,      // Container key identifying the storage container
    mem_pool: Arc<T>,         // Shared memory pool for page management
    page_ids: Vec<PageId>,    // List of page IDs that make up the sorted run xtx replace with num tuples in each page
    total_len: usize,    // List of page IDs that make up the sorted run xtx replace with num tuples in each page
    min_keys: Vec<Vec<u8>>,   // Stores the minimum key of each page for efficient lookup
}

impl<T: MemPool> SortedRunStore<T> {
    /// Creates a new `SortedRunStore` from an iterator of key-value pairs.
    ///
    /// The key-value pairs should be provided in sorted order. The method partitions
    /// the data into pages, ensuring that each page fits within the size constraints
    /// and maintains the sorted order.
    ///
    /// # Arguments
    ///
    /// - `c_key`: The container key for identifying the storage container.
    /// - `mem_pool`: Shared memory pool for managing pages.
    /// - `iter`: An iterator over key-value pairs to store.
    pub fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let mut min_keys = Vec::new();    // Minimum keys for each page
        let mut page_ids = Vec::new();    // IDs of the pages created
        // Create a new page for writing
        let mut current_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        current_page.init();              // Initialize the page
        let mut first_key_in_page = None; // Keep track of the first key in the current page

        let mut len = 0;
        // Iterate over key-value pairs
        for (key, value) in iter {
            // Attempt to append the key-value pair to the current page
            if current_page.append(key.as_ref(), value.as_ref()) {
                // If successful and this is the first key in the page, record it
                if first_key_in_page.is_none() {
                    first_key_in_page = Some(key.as_ref().to_vec());
                }
            } else {
                // If the page is full, finish the current page
                min_keys.push(first_key_in_page.take().unwrap());
                page_ids.push(current_page.get_id());

                // Create a new page
                drop(current_page); // Release the current page
                current_page = mem_pool.create_new_page_for_write(c_key).unwrap();
                current_page.init();
                // Append the key-value pair to the new page (should succeed)
                assert!(current_page.append(key.as_ref(), value.as_ref()));
                first_key_in_page = Some(key.as_ref().to_vec());
            }
            len += 1;
        }
        // Finish the last page if there are any remaining keys
        if let Some(first_key) = first_key_in_page {
            min_keys.push(first_key);
            page_ids.push(current_page.get_id());
        }
        drop(current_page); // Release the last page

        // Return the new SortedRunStore
        Self {
            c_key,
            mem_pool,
            page_ids,
            total_len : len,
            min_keys,
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
                    },
                    std::cmp::Ordering::Equal => {
                        // If we find an exact match, we want this page
                        // but also need to check previous pages for potential matches
                        right = mid;
                    },
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
                if left > 1 && self.min_keys[left-1].as_slice() == lower_bound {
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
    storage: Arc<SortedRunStore<T>>,            // Reference to the storage
    lower_bound: Vec<u8>,                      // Inclusive lower bound for keys
    upper_bound: Vec<u8>,                      // Exclusive upper bound for keys
    pub current_page_index: usize,                 // Index of the current page in `page_ids`
    current_page: Option<FrameReadGuard<'static>>, // Current page being scanned
    current_slot_id: u32,                      // Current position within the page
}

impl<'a, T: MemPool> Iterator for SortedRunStoreRangeScanner<T> {
    type Item = (Vec<u8>, Vec<u8>); // Returns key-value pairs as (Vec<u8>, Vec<u8>)

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If there is no current page loaded
            if self.current_page.is_none() {
                // Check if we have reached the end of the pages
                if self.current_page_index >= self.storage.page_ids.len() {
                    return None;
                }
                // Load the page
                let page_id = self.storage.page_ids[self.current_page_index];
                let page_key = PageFrameKey::new(self.storage.c_key, page_id);
                let page = self.storage.mem_pool.get_page_for_read(page_key).ok()?;
                // Extend the lifetime to 'static (unsafe operation)
                self.current_page = Some(unsafe {
                    std::mem::transmute::<FrameReadGuard<'_>, FrameReadGuard<'static>>(page)
                });
                self.current_slot_id = 0; // Start from the first slot in the page
            }
            // Get a reference to the current page
            let page = self.current_page.as_ref().unwrap();
            // Iterate over the slots in the current page
            while self.current_slot_id < page.slot_count() {
                let (key, value) = page.get(self.current_slot_id);
                self.current_slot_id += 1;
                // Check if the key is within the specified bounds
                if key >= self.lower_bound.as_slice()
                    && (self.upper_bound.is_empty() || key < self.upper_bound.as_slice())
                {
                    // Key is within bounds; return the key-value pair
                    return Some((key.to_vec(), value.to_vec()));
                } else if !self.upper_bound.is_empty() && key >= self.upper_bound.as_slice() {
                    // Key has exceeded the upper bound; end iteration
                    return None;
                }
            }
            // Move to the next page
            self.current_page = None;
            self.current_page_index += 1;
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
    sorted_run_stores: Vec<Arc<SortedRunStore<T>>>,
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
        self.sorted_run_stores.iter().map(|store| store.num_pages()).sum()
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
    pub fn scan_range(&self, lower_bound: &[u8], upper_bound: &[u8]) -> BigSortedRunStoreScanner<T> {
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
            if left > 0 { left - 1 } else { 0 }
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
    }}

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
        min_idx.map(|i| {
            self.current_values[i].take().unwrap()
        })
    }
}