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


pub struct BigSortedRunStore<T: MemPool>{
    sorted_run_stores: Vec<SortedRunStore<T>>,
}

impl<T: MemPool> BigSortedRunStore<T> {
}
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
    pub fn scan<'a>(&'a self) -> SortedRunStoreRangeScanner<'a, T> {
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
    ) -> SortedRunStoreRangeScanner<'a, T> {
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
            storage: self,
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

/// Iterator for scanning over a range of key-value pairs in a `SortedRunStore`.
pub struct SortedRunStoreRangeScanner<'a, T: MemPool> {
    storage: &'a SortedRunStore<T>,            // Reference to the storage
    lower_bound: Vec<u8>,                      // Inclusive lower bound for keys
    upper_bound: Vec<u8>,                      // Exclusive upper bound for keys
    pub current_page_index: usize,                 // Index of the current page in `page_ids`
    current_page: Option<FrameReadGuard<'static>>, // Current page being scanned
    current_slot_id: u32,                      // Current position within the page
}

impl<'a, T: MemPool> Iterator for SortedRunStoreRangeScanner<'a, T> {
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