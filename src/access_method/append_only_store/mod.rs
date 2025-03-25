mod append_only_page;
use std::sync::atomic::Ordering;

use append_only_page::AppendOnlyPage;
use std::{
    sync::{atomic::AtomicUsize, Arc, Mutex, RwLock},
    time::Duration,
};

use crate::{
    bp::{FrameReadGuard, FrameWriteGuard, MemPoolStatus},
    page::{Page, PageId},
    prelude::{ContainerKey, MemPool, PageFrameKey},
};

use super::AccessMethodError;

pub mod prelude {
    pub use super::{AppendOnlyStore, AppendOnlyStoreScanner};
}

struct RuntimeStats {
    num_recs: AtomicUsize,
    num_pages: AtomicUsize,
}

impl RuntimeStats {
    fn new() -> Self {
        RuntimeStats {
            num_recs: AtomicUsize::new(0),
            num_pages: AtomicUsize::new(0),
        }
    }

    fn inc_num_recs(&self) {
        self.num_recs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn get_num_recs(&self) -> usize {
        self.num_recs.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn inc_num_pages(&self) {
        self.num_pages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn get_num_pages(&self) -> usize {
        self.num_pages.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Clone for RuntimeStats {
    fn clone(&self) -> Self {
        RuntimeStats {
            num_recs: AtomicUsize::new(self.num_recs.load(Ordering::Relaxed)),
            num_pages: AtomicUsize::new(self.num_pages.load(Ordering::Relaxed)),
        }
    }
}

/// In the append-only store, the pages forms a one-way linked list which we call a chain.
/// The first page is called the root page.
/// The append operation always appends data to the last page of the chain.
/// This is not optimized for multi-thread appends.
///
/// [Root Page] -> [Page 1] -> [Page 2] -> [Page 3] -> ... -> [Last Page]
///      |                                                        ^
///      |                                                        |
///      ----------------------------------------------------------
///

pub struct AppendOnlyStore<T: MemPool> {
    pub c_key: ContainerKey,
    pub root_key: PageFrameKey,        // Fixed.
    pub last_key: Mutex<PageFrameKey>, // Variable
    pub mem_pool: Arc<T>,
    stats: RuntimeStats, // Stats are not durable

    // Tracking page indexes useful for reading ahead xtx do i need to keep track of frames?
    pages: RwLock<Vec<(PageId, u32, u32)>>, // (page_id, frame_id, slot_count)

}

impl<T: MemPool> AppendOnlyStore<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        // Create and initialize the root page
        let mut root_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        root_page.init();
        let root_key = {
            let page_id = root_page.get_id();
            let frame_id = root_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        // Create and initialize the first data page
        let mut data_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        data_page.init();
        let data_key = {
            let page_id = data_page.get_id();
            let frame_id = data_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        // Link root page to the first data page
        root_page.set_next_page(data_page.get_id(), data_page.frame_id());

        // Serialize the first data page's info into the root page
        let data_key_bytes = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&data_page.get_id().to_be_bytes());
            bytes.extend_from_slice(&data_page.frame_id().to_be_bytes());
            bytes
        };
        assert!(root_page.append(&[], &data_key_bytes));

        // Initialize the pages index with the first data page
        let pages = RwLock::new(vec![(data_page.get_id(), data_page.frame_id(), data_page.slot_count())]);

        AppendOnlyStore {
            c_key,
            root_key,
            last_key: Mutex::new(data_key),
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
            pages,
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, root_id: PageId) -> Self {
        // Assumes that root page's page_id is 0.
        let root_key = PageFrameKey::new(c_key, root_id);
        let last_key = {
            let root_page = mem_pool.get_page_for_read(root_key).unwrap();
            let (_, val) = root_page.get(0);
            let page_id = u32::from_be_bytes(val[0..4].try_into().unwrap());
            let frame_id = u32::from_be_bytes(val[4..8].try_into().unwrap());
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };
        // let pages = RwLock::new(vec![(data_page.get_id(), data_page.frame_id(), data_page.slot_count())]); //xtx figure out
        let pages = RwLock::new(Vec::new());

        AppendOnlyStore {
            c_key,
            root_key,
            last_key: Mutex::new(last_key),
            mem_pool: mem_pool.clone(),
            stats: RuntimeStats::new(),
            pages,
        }
    }

    pub fn bulk_insert_create<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        let storage = Self::new(c_key, mem_pool);
        for (k, v) in iter {
            storage.append(k.as_ref(), v.as_ref()).unwrap();
        }
        storage
    }

    fn write_page(&self, page_key: &PageFrameKey) -> FrameWriteGuard {
        let base = Duration::from_micros(10);
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_write(*page_key) {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    attempts += 1;
                    std::thread::sleep(base * attempts);
                }
                Err(e) => panic!("Error: {}", e),
            }
        }
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard<'static> {
        let base: u64 = 2;
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_read(page_key) {
                Ok(page) => {
                    return unsafe {
                        std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(page)
                    }
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    std::thread::sleep(Duration::from_nanos(base.pow(attempts)));
                    attempts += 1;
                }
                Err(e) => panic!("Error: {}", e),
            }
        }
    }

    pub fn num_kvs(&self) -> usize {
        self.stats.get_num_recs()
    }

    pub fn num_pages(&self) -> usize {
        self.stats.get_num_pages()
    }

    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        let data_len = key.len() + value.len();
        if data_len > <Page as AppendOnlyPage>::max_record_size() {
            return Err(AccessMethodError::RecordTooLarge);
        }
        self.stats.inc_num_recs();

        let mut last_key = self.last_key.lock().unwrap();
        let mut last_page = self.write_page(&last_key);

        // Try to append to the last page
        if last_page.append(key, value) {
            // Update the slot count in the pages index
            let slot_count = last_page.slot_count();
            let mut pages = self.pages.write().unwrap();
            if let Some(last_entry) = pages.last_mut() {
                last_entry.2 = slot_count;
            }
            Ok(())
        } else {
            // Page overflow: create a new page
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            new_page.init();

            let page_id = new_page.get_id();
            let frame_id = new_page.frame_id();

            // Link the last page to the new page
            last_page.set_next_page(page_id, frame_id);
            drop(last_page); // Release the lock on the last page

            // Update the root page's next page info
            let new_key_bytes = {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&page_id.to_be_bytes());
                bytes.extend_from_slice(&frame_id.to_be_bytes());
                bytes
            };
            let mut root_page = self.write_page(&self.root_key);
            root_page.get_mut_val(0).copy_from_slice(&new_key_bytes);
            drop(root_page); // Release the lock on the root page

            self.stats.inc_num_pages();

            // Update the in-memory last key to the new page
            let new_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
            *last_key = new_key;

            // Append the key-value pair to the new page
            assert!(new_page.append(key, value));

            // Add the new page to the pages index
            let mut pages = self.pages.write().unwrap();
            pages.push((page_id, frame_id, new_page.slot_count()));
            // println!("added a new page");

            Ok(())
        }
    }

    pub fn scan(self: &Arc<Self>) -> AppendOnlyStoreScanner<T> {
        AppendOnlyStoreScanner {
            finished: false,
            initialized: false,
            storage: self.clone(),
            current_page: None,
            current_slot_id: 0,
            current_page_idx: 0,
            total_index: 0,
            end_index: 0, //xtx
        }
    }

    /// Finds the page index and slot within the page for the given record index using binary search.
    fn find_page_and_slot(&self, index: usize) -> Option<(usize, usize)> {
        let pages = self.pages.read().unwrap();
        let mut low = 0;
        let mut high = pages.len();
        let mut cumulative = 0;

        while low < high {
            let mid = (low + high) / 2;
            let page_slot_count = pages[mid].2 as usize;

            if index < cumulative + page_slot_count {
                high = mid;
            } else {
                cumulative += page_slot_count;
                low = mid + 1;
            }
        }

        if low < pages.len() {
            let slot_id = index - cumulative;
            Some((low, slot_id))
        } else {
            None
        }
    }

    /// Retrieves the slot at a specific index using binary search.
    pub fn get_slot_at(&self, index: usize) -> Option<(Vec<u8>, Vec<u8>)> {
        match self.find_page_and_slot(index) {
            Some((page_idx, slot_id)) => {
                let pages = self.pages.read().unwrap();
                let (page_id, frame_id, _) = pages[page_idx];
                let page_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
                let page = self.read_page(page_key);
                let (key, value) = page.get(slot_id as u32);
                Some((key.to_vec(), value.to_vec()))
            }
            None => None, // Index out of bounds
        }
    }

    /// Creates a RangeScan iterator for the specified range
    pub fn range_scan(&self, start: usize, end: usize) -> AppendOnlyStoreRangeIter<T> {
        AppendOnlyStoreRangeIter::new(Arc::new(self.clone()), start, end)
    }

        /// Initiates a scan starting from `start_index` up to `end_index`.
        pub fn scan_range_from(
            &self,
            start_index: usize,
            end_index: usize,
        ) -> Result<AppendOnlyStoreScanner<T>, AccessMethodError> {
            AppendOnlyStoreScanner::new(Arc::new(self.clone()), start_index, end_index)
        }

    // XTX num_tuples

}

impl<T: MemPool> Clone for AppendOnlyStore<T> {
    fn clone(&self) -> Self {
        AppendOnlyStore {
            c_key: self.c_key.clone(),
            root_key: self.root_key.clone(),
            last_key: Mutex::new(self.last_key.lock().unwrap().clone()),
            mem_pool: self.mem_pool.clone(),
            stats: self.stats.clone(),
            pages: RwLock::new(self.pages.read().unwrap().clone()),
        }
    }
}

pub struct AppendOnlyStoreScanner<T: MemPool> {
    finished: bool,
    initialized: bool,
    storage: Arc<AppendOnlyStore<T>>,
    current_page_idx: usize,
    current_slot_id: u32,
    current_page: Option<FrameReadGuard<'static>>,
    total_index: usize,
    end_index: usize,
}

impl<T: MemPool> AppendOnlyStoreScanner<T> {
    pub fn new(
        storage: Arc<AppendOnlyStore<T>>,
        start_index: usize,
        end_index: usize,
    ) -> Result<Self, AccessMethodError> {
        if start_index > end_index || end_index > storage.num_kvs() {
            return Err(AccessMethodError::Other("index out of bounds".to_string()));
        }

        // Find the starting page and slot using binary search
        let (page_idx, slot_id) = storage
            .find_page_and_slot(start_index)
            .ok_or(AccessMethodError::Other("no slot".to_string()))?;

        Ok(AppendOnlyStoreScanner {
            finished: false,
            initialized: false,
            storage,
            current_page_idx: page_idx,
            current_slot_id: slot_id as u32,
            current_page: None,
            total_index: start_index,
            end_index,
        })
    }

    fn initialize(&mut self) {
        let root_key = self.storage.root_key;
        let root_page = self.storage.read_page(root_key);
        // Read the first data page
        let (data_page_id, data_frame_id) = root_page.next_page().unwrap();
        let data_key =
            PageFrameKey::new_with_frame_id(self.storage.c_key, data_page_id, data_frame_id);
        let data_page = self.storage.read_page(data_key);
        self.current_page = Some(data_page);
        self.current_slot_id = 0;
    }

    fn prefetch_next_page(&self) {
        if let Some(current_page) = &self.current_page {
            if let Some((page_id, frame_id)) = current_page.next_page() {
                let next_key =
                    PageFrameKey::new_with_frame_id(self.storage.c_key, page_id, frame_id);
                self.storage.mem_pool.prefetch_page(next_key).unwrap();
            }
        }
    }

    /// Seeks the scanner to the specified index.
    pub fn seek_to_index(&mut self, index: usize) -> Result<(), AccessMethodError> {
        if index > self.end_index {
            return Err(AccessMethodError::Other("index out of bounds".to_string()));
        }

        let (page_idx, slot_id) = self
            .storage
            .find_page_and_slot(index)
            .ok_or(AccessMethodError::Other("index out of bounds".to_string()))?;

        self.current_page_idx = page_idx;
        self.current_slot_id = slot_id as u32;
        self.current_page = None; // Reset the current page; it will be loaded in next()
        self.total_index = index;

        Ok(())
    }

    /// Retrieves the next key-value pair efficiently without re-searching.
    pub fn next_record(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.total_index >= self.end_index {
            return None;
        }

        // Load the current page if not already loaded
        if self.current_page.is_none() {
            let pages = self.storage.pages.read().unwrap();

            if self.current_page_idx >= pages.len() {
                return None; // Reached the end of pages
            }

            let (page_id, frame_id, _) = pages[self.current_page_idx];
            let page_key = PageFrameKey::new_with_frame_id(self.storage.c_key, page_id, frame_id);
            self.current_page = Some(self.storage.read_page(page_key));
        }

        let current_page = self.current_page.as_ref().unwrap();

        // Fetch the slot count for boundary checks
        let slot_count = current_page.slot_count();

        if self.current_slot_id < slot_count {
            // Retrieve the key-value pair
            let (key, value) = current_page.get(self.current_slot_id);
            self.current_slot_id += 1;
            self.total_index += 1;
            Some((key.to_vec(), value.to_vec()))
        } else {
            // Move to the next page
            self.current_page_idx += 1;
            self.current_slot_id = 0;
            self.current_page = None; // Will load the next page in the next call
            self.next_record()
        }
    }
        
}

impl<T: MemPool> Iterator for AppendOnlyStoreScanner<T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if !self.initialized {
            self.initialize();
            self.prefetch_next_page();
            self.initialized = true;
        }

        assert!(self.current_page.is_some());

        // Try to read from the current page.
        // If there are no more records in the current page, move to the next page
        // and try to read from it.
        if self.current_slot_id < self.current_page.as_ref().unwrap().slot_count() {
            let record = self
                .current_page
                .as_ref()
                .unwrap()
                .get(self.current_slot_id);
            self.current_slot_id += 1;
            Some((record.0.to_vec(), record.1.to_vec()))
        } else {
            let current_page = self.current_page.take().unwrap();
            let next_page = current_page.next_page();
            match next_page {
                Some((page_id, frame_id)) => {
                    let next_key =
                        PageFrameKey::new_with_frame_id(self.storage.c_key, page_id, frame_id);
                    let next_page = self.storage.read_page(next_key);
                    // Fast path eviction
                    // self.storage
                    //     .mem_pool
                    //     .fast_evict(current_page.frame_id())
                    //     .unwrap();
                    drop(current_page);

                    self.current_page = Some(next_page);
                    self.current_slot_id = 0;
                    self.prefetch_next_page();
                    self.next()
                }
                None => {
                    // Fast path eviction
                    // self.storage
                    //     .mem_pool
                    //     .fast_evict(current_page.frame_id())
                    //     .unwrap();
                    drop(current_page);

                    self.finished = true;
                    None
                }
            }
        }
    }
}


pub struct AppendOnlyStoreRangeIter<M: MemPool> {
    store: Arc<AppendOnlyStore<M>>,
    start_index: usize,
    end_index: usize,
    current_index: usize,
}
// XTX should i use usize or u32


impl<M: MemPool> AppendOnlyStoreRangeIter<M> {
    pub fn new(store: Arc<AppendOnlyStore<M>>, start: usize, end: usize) -> Self {
        Self {
            store,
            start_index: start,
            end_index: end,
            current_index: start,
        }
    }
}

impl<T: MemPool> Iterator for AppendOnlyStoreRangeIter<T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.end_index {
            return None;
        }

        // Fetch the key-value pair at the current index
        let result = self.store.get_slot_at(self.current_index);

        self.current_index += 1;

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::bp::{get_test_bp, BufferPool};
    use crate::container::ContainerManager;
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
        let store = AppendOnlyStore::new(container_key, mem_pool);

        let key = b"small key";
        let value = b"small value";
        assert_eq!(store.append(key, value), Ok(()));
    }

    #[test]
    fn test_large_append() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool));

        let key = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
        let value = gen_random_byte_vec(Page::max_record_size() + 1, Page::max_record_size() + 1);
        assert_eq!(
            store.append(&key, &value),
            Err(AccessMethodError::RecordTooLarge)
        );

        // Scan should return nothing
        let mut scanner = store.scan();
        assert!(scanner.next().is_none());
    }

    #[test]
    fn test_page_overflow() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = AppendOnlyStore::new(container_key, mem_pool);

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
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool.clone()));

        let key = b"scanned key";
        let value = b"scanned value";
        for _ in 0..3 {
            store.append(key, value).unwrap();
        }

        assert_eq!(store.num_kvs(), 3);

        let mut scanner = store.scan();

        for _ in 0..3 {
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

        let store = Arc::new(AppendOnlyStore::new(get_c_key(), get_test_bp(10)));

        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Appending record {} **********************",
                i
            );
            store.append(val.0, val.1).unwrap();
        }

        assert_eq!(store.num_kvs(), num_keys);

        let mut scanner = store.scan();
        for (i, val) in vals.iter().enumerate() {
            println!(
                "********************** Scanning record {} **********************",
                i
            );
            assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
        }
    }

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

    #[test]
    fn test_scan_finish_condition() {
        let mem_pool = get_test_bp(10);
        let container_key = get_c_key();
        let store = Arc::new(AppendOnlyStore::new(container_key, mem_pool.clone()));

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

        let store = Arc::new(AppendOnlyStore::bulk_insert_create(
            get_c_key(),
            get_test_bp(10),
            vals.iter(),
        ));

        assert_eq!(store.num_kvs(), num_keys);

        let mut scanner = store.scan();
        for val in vals.iter() {
            assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
        }
    }

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
            let cm = Arc::new(ContainerManager::new(temp_dir.path(), false, false).unwrap());
            let bp = Arc::new(BufferPool::new(10, cm).unwrap());

            let store = Arc::new(AppendOnlyStore::bulk_insert_create(
                get_c_key(),
                bp.clone(),
                vals.iter(),
            ));

            drop(store);
            drop(bp);
        }

        {
            let cm = Arc::new(ContainerManager::new(temp_dir.path(), false, false).unwrap());
            let bp = Arc::new(BufferPool::new(10, cm).unwrap());
            let store = Arc::new(AppendOnlyStore::load(get_c_key(), bp.clone(), 0));

            let mut scanner = store.scan();
            for val in vals.iter() {
                assert_eq!(scanner.next().unwrap(), (val.0.to_vec(), val.1.to_vec()));
            }
        }
    }
}
