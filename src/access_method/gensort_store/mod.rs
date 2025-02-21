mod gensort_page;
use gensort_page::{GensortPage, RECORD_SIZE};

use std::sync::{Arc, Mutex};
use crate::prelude::{ContainerKey, MemPool, PageFrameKey, Page, PageId};

pub struct GensortStore<T: MemPool> {
    pub c_key: ContainerKey,
    pub root_key: PageFrameKey,
    pub last_key: Mutex<PageFrameKey>,
    pub mem_pool: Arc<T>,
    num_records: Mutex<usize>,
}

impl<T: MemPool> GensortStore<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        // Create and initialize the root page
        let binding = mem_pool.clone();
        let mut root_page = binding.create_new_page_for_write(c_key).unwrap();
        root_page.init_gensort();
        let root_key = {
            let page_id = root_page.get_id();
            let frame_id = root_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        // Create first data page
        let binding = mem_pool.clone();
        let mut data_page = binding.create_new_page_for_write(c_key).unwrap();
        data_page.init_gensort();
        let data_key = {
            let page_id = data_page.get_id();
            let frame_id = data_page.frame_id();
            PageFrameKey::new_with_frame_id(c_key, page_id, frame_id)
        };

        // Link root page to first data page
        root_page.set_next_page(data_page.get_id());

        GensortStore {
            c_key,
            root_key,
            last_key: Mutex::new(data_key),
            mem_pool,
            num_records: Mutex::new(0),
        }
    }

    pub fn append(&self, record: &[u8]) -> Result<(), &'static str> {
        if record.len() != RECORD_SIZE {
            return Err("Invalid record size");
        }

        let mut last_key = self.last_key.lock().unwrap();
        let mut last_page = self.mem_pool.get_page_for_write(*last_key).unwrap();

        if last_page.can_fit_record() {
            last_page.append_record(record);
            *self.num_records.lock().unwrap() += 1;
            Ok(())
        } else {
            // Create new page when current is full
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            new_page.init_gensort();
            
            let page_id = new_page.get_id();
            let frame_id = new_page.frame_id();

            // Link pages
            last_page.set_next_page(page_id);
            
            // Update last key
            *last_key = PageFrameKey::new_with_frame_id(self.c_key, page_id, frame_id);
            
            // Append to new page
            assert!(new_page.append_record(record));
            *self.num_records.lock().unwrap() += 1;
            Ok(())
        }
    }

    pub fn scan(&self) -> GensortStoreScan<T> {
        GensortStoreScan::new(self.clone())
    }

    pub fn get_num_records(&self) -> usize {
        *self.num_records.lock().unwrap()
    }

    pub fn get_record(&self, index: usize) -> Option<Vec<u8>> {
        // Calculate which page the record is on
        let records_per_page = Page::max_records_per_page();
        let page_number = index / records_per_page;
        let record_in_page = index % records_per_page;

        // Start from root page to find the target page
        let mut current_page_id = self.mem_pool.get_page_for_read(self.root_key).unwrap().next_page()?;
        
        for _ in 0..page_number {
            let page = self.mem_pool.get_page_for_read(PageFrameKey::new(self.c_key, current_page_id)).unwrap();
            current_page_id = page.next_page()?;
        }

        // Read the target page and get the record
        let page = self.mem_pool.get_page_for_read(PageFrameKey::new(self.c_key, current_page_id)).unwrap();
        if let Some(record) = page.get_record(record_in_page as u32) {
            Some(record.to_vec())
        } else {
            None
        }
    }
}

impl<T: MemPool> Clone for GensortStore<T> {
    fn clone(&self) -> Self {
        GensortStore {
            c_key: self.c_key,
            root_key: self.root_key,
            last_key: Mutex::new(*self.last_key.lock().unwrap()),
            mem_pool: self.mem_pool.clone(),
            num_records: Mutex::new(*self.num_records.lock().unwrap()),
        }
    }
}

pub struct GensortStoreScan<T: MemPool> {
    store: GensortStore<T>,
    current_page: Option<PageId>,
    current_idx: u32,
}

impl<T: MemPool> GensortStoreScan<T> {
    fn new(store: GensortStore<T>) -> Self {
        let first_page = store.mem_pool
            .get_page_for_read(store.root_key)
            .unwrap()
            .next_page();
            
        GensortStoreScan {
            store,
            current_page: first_page,
            current_idx: 0,
        }
    }
}

impl<T: MemPool> Iterator for GensortStoreScan<T> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(page_id) = self.current_page {
                let page_key = PageFrameKey::new(self.store.c_key, page_id);
                let page = self.store.mem_pool.get_page_for_read(page_key).unwrap();
                
                if let Some(record) = page.get_record(self.current_idx) {
                    // Get the record before updating state
                    let result = record.to_vec();
                    self.current_idx += 1;
                    return Some(result);
                } else {
                    // Store next page info and drop current page
                    let next_page = page.next_page();
                    drop(page);
                    
                    // Update state and continue loop
                    self.current_page = next_page;
                    self.current_idx = 0;
                    continue;
                }
            }
            return None;
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::{get_test_bp, BufferPool};
    use gensort_page::{RECORD_KEY_SIZE, RECORD_VALUE_SIZE};

    fn get_c_key() -> ContainerKey {
        ContainerKey::new(0, 0)
    }

    #[test]
    fn test_basic_append_and_scan() {
        let mem_pool = get_test_bp(10);
        let store = GensortStore::new(get_c_key(), mem_pool);

        let record = vec![42; RECORD_SIZE];
        store.append(&record).unwrap();

        let mut scanner = store.scan();
        assert_eq!(scanner.next(), Some(record));
        assert_eq!(scanner.next(), None);
    }

    #[test]
    fn test_multi_page_append() {
        let mem_pool = get_test_bp(10);
        let store = GensortStore::new(get_c_key(), mem_pool);

        let record = vec![42; RECORD_SIZE];
        let num_records = Page::max_records_per_page() * 2 + 1;

        for _ in 0..num_records {
            store.append(&record).unwrap();
        }

        let mut count = 0;
        for scanned_record in store.scan() {
            assert_eq!(scanned_record, record);
            count += 1;
        }
        assert_eq!(count, num_records);
    }

    #[test]
    fn test_invalid_record_size() {
        let mem_pool = get_test_bp(10);
        let store = GensortStore::new(get_c_key(), mem_pool);

        let invalid_record = vec![42; RECORD_SIZE + 1];
        assert!(store.append(&invalid_record).is_err());
    }

    #[test]
    fn test_large_dataset_sampling() {
        let mem_pool = get_test_bp(100); // Increased buffer pool size for larger dataset
        let store = Arc::new(GensortStore::new(get_c_key(), mem_pool));
    
        // Create 10,000 records where each key is exactly the record number as a string
        let num_records = 10_000;
        for i in 0..num_records {
            let mut record = vec![b' '; RECORD_SIZE];  // Initialize with spaces
            
            // Format number as string with leading zeros
            let key = format!("{:010}", i);  // 10-digit zero-padded number
            record[..RECORD_KEY_SIZE].copy_from_slice(key.as_bytes());
            
            // Fill value portion with some identifiable pattern
            let value_str = format!("value-{:090}", i);  // 90-char value to fill remaining space
            record[RECORD_KEY_SIZE..].copy_from_slice(&value_str.as_bytes()[..RECORD_VALUE_SIZE]);
            
            store.append(&record).unwrap();
        }
    
        println!("Finished inserting {} records", num_records);
    
        // Verify specific indices (every 100th record)
        for i in (0..num_records).step_by(100) {
            let record = store.get_record(i).unwrap();
            
            // Convert the key portion back to a number
            let key_str = std::str::from_utf8(&record[..RECORD_KEY_SIZE]).unwrap();
            let record_num = key_str.parse::<usize>().unwrap();
            
            // Check if we got exactly the record we expected
            assert_eq!(record_num, i, "Record at position {} has key {}", i, record_num);
            
            // Also verify the value portion
            let expected_value = format!("value-{:090}", i);
            let actual_value = std::str::from_utf8(&record[RECORD_KEY_SIZE..]).unwrap();
            assert_eq!(actual_value, &expected_value[..RECORD_VALUE_SIZE], 
                      "Value mismatch at position {}", i);
            
            println!("Successfully verified record {}", i);
        }
        
        // Also do a full scan to make sure all records are there
        let mut count = 0;
        let mut last_key = None;
        
        for record in store.scan() {
            let key_str = std::str::from_utf8(&record[..RECORD_KEY_SIZE]).unwrap();
            let record_num = key_str.parse::<usize>().unwrap();
            
            // Verify record number matches its position
            assert_eq!(record_num, count, "Sequential scan: record at position {} has key {}", count, record_num);
            
            // Verify monotonic increase
            if let Some(last) = last_key {
                assert!(record_num > last, "Keys not strictly increasing");
            }
            
            last_key = Some(record_num);
            count += 1;
        }
        
        assert_eq!(count, num_records, "Total record count mismatch");
    }
}