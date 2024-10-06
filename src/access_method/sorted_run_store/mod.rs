// mod.rs

pub mod sorted_page;
use sorted_page::SortedPage;

use std::sync::Arc;
use crate::bp::{MemPool, PageFrameKey};
use crate::prelude::{PageId, ContainerKey};
use crate::page::Page;
use crate::prelude::FrameReadGuard;

pub struct SortedRunStore<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,
    page_ids: Vec<PageId>,
    min_keys: Vec<Vec<u8>>, // Stores the minimum key of each page
}

impl<T: MemPool> SortedRunStore<T> {
    pub fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let mut min_keys = Vec::new();
        let mut page_ids = Vec::new();
        let mut current_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        current_page.init();
        let mut first_key_in_page = None;

        for (key, value) in iter {
            if current_page.append(key.as_ref(), value.as_ref()) {
                if first_key_in_page.is_none() {
                    first_key_in_page = Some(key.as_ref().to_vec());
                }
            } else {
                // Finish the current page
                min_keys.push(first_key_in_page.take().unwrap());
                page_ids.push(current_page.get_id());

                // Create a new page
                drop(current_page); // Drop to release the borrow on mem_pool
                current_page = mem_pool.create_new_page_for_write(c_key).unwrap();
                current_page.init();
                assert!(current_page.append(key.as_ref(), value.as_ref()));
                first_key_in_page = Some(key.as_ref().to_vec());
            }
        }
        // Finish the last page
        if let Some(first_key) = first_key_in_page {
            min_keys.push(first_key);
            page_ids.push(current_page.get_id());
        }
        drop(current_page); // Drop to release the borrow on mem_pool
        Self {
            c_key,
            mem_pool,
            page_ids,
            min_keys,
        }
    }

    pub fn scan_range<'a>(
        &'a self,
        lower_bound: &[u8],
        upper_bound: &[u8],
    ) -> SortedRunStoreRangeScanner<'a, T> {
        // Binary search over min_keys to find the starting page index
        let mut left = 0;
        let mut right = self.min_keys.len();
        while left < right {
            let mid = (left + right) / 2;
            if self.min_keys[mid].as_slice() < lower_bound {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        let start_page_index = left;
        SortedRunStoreRangeScanner {
            storage: self,
            lower_bound: lower_bound.to_vec(),
            upper_bound: upper_bound.to_vec(),
            current_page_index: start_page_index,
            current_page: None,
            current_slot_id: 0,
        }
    }
}

pub struct SortedRunStoreRangeScanner<'a, T: MemPool> {
    storage: &'a SortedRunStore<T>,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    current_page_index: usize,
    current_page: Option<FrameReadGuard<'static>>,
    current_slot_id: u32,
}

impl<'a, T: MemPool> Iterator for SortedRunStoreRangeScanner<'a, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_page.is_none() {
                if self.current_page_index >= self.storage.page_ids.len() {
                    return None;
                }
                // Load the page
                let page_id = self.storage.page_ids[self.current_page_index];
                let page_key = PageFrameKey::new(self.storage.c_key, page_id);
                let page = self.storage.mem_pool.get_page_for_read(page_key).ok()?;
                // Extend the lifetime to 'static
                self.current_page = Some(unsafe {
                    std::mem::transmute::<FrameReadGuard<'_>, FrameReadGuard<'static>>(page)
                });
                self.current_slot_id = 0;
            }
            let page = self.current_page.as_ref().unwrap();
            while self.current_slot_id < page.slot_count() {
                let (key, value) = page.get(self.current_slot_id);
                self.current_slot_id += 1;
                if key >= self.lower_bound.as_slice() && key < self.upper_bound.as_slice() {
                    return Some((key.to_vec(), value.to_vec()));
                } else if key >= self.upper_bound.as_slice() {
                    return None;
                }
            }
            // Move to next page
            self.current_page = None;
            self.current_page_index += 1;
        }
    }
}