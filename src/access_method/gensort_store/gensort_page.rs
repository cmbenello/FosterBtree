use crate::access_method::gensort_store::{RECORD_KEY_SIZE, RECORD_SIZE, RECORD_VALUE_SIZE};
use crate::prelude::{Page, PageId, AVAILABLE_PAGE_SIZE};

// Constants for the gensort format

// Page header only needs minimal information
pub const PAGE_HEADER_SIZE: usize = 8; // next_page (4 bytes) + record_count (4 bytes)
                                       // xtx I think can update this so that it alligns with the page

pub trait GensortPage {
    fn init_gensort(&mut self);
    fn max_records_per_page() -> usize {
        (AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE) / RECORD_SIZE
    }

    // Header operations
    fn next_page(&self) -> Option<PageId>;
    fn set_next_page(&mut self, next_page_id: PageId);
    fn record_count(&self) -> u32;
    fn set_record_count(&mut self, count: u32);
    fn increment_record_count(&mut self) {
        let count = self.record_count();
        self.set_record_count(count + 1);
    }

    // Record operations
    fn can_fit_record(&self) -> bool {
        let used_space = PAGE_HEADER_SIZE + (self.record_count() as usize * RECORD_SIZE);
        used_space + RECORD_SIZE <= AVAILABLE_PAGE_SIZE
    }

    fn get_record_offset(&self, idx: u32) -> usize {
        PAGE_HEADER_SIZE + (idx as usize * RECORD_SIZE)
    }

    fn append_record(&mut self, record: &[u8]) -> bool;
    fn get_record(&self, idx: u32) -> Option<&[u8]>;
    fn get_key(&self, idx: u32) -> Option<&[u8]>;
    fn get_value(&self, idx: u32) -> Option<&[u8]>;
}

impl GensortPage for Page {
    fn init_gensort(&mut self) {
        // Initialize empty page with no next page and zero records
        self[0..4].copy_from_slice(&PageId::MAX.to_be_bytes());
        self[4..8].copy_from_slice(&0u32.to_be_bytes());
    }

    fn next_page(&self) -> Option<PageId> {
        let next_page_id = u32::from_be_bytes(self[0..4].try_into().unwrap());
        if next_page_id == PageId::MAX {
            None
        } else {
            Some(next_page_id)
        }
    }

    fn set_next_page(&mut self, next_page_id: PageId) {
        self[0..4].copy_from_slice(&next_page_id.to_be_bytes());
    }

    fn record_count(&self) -> u32 {
        u32::from_be_bytes(self[4..8].try_into().unwrap())
    }

    fn set_record_count(&mut self, count: u32) {
        self[4..8].copy_from_slice(&count.to_be_bytes());
    }

    fn append_record(&mut self, record: &[u8]) -> bool {
        if !self.can_fit_record() || record.len() != RECORD_SIZE {
            return false;
        }

        let offset = self.get_record_offset(self.record_count());
        self[offset..offset + RECORD_SIZE].copy_from_slice(record);
        self.increment_record_count();
        true
    }

    fn get_record(&self, idx: u32) -> Option<&[u8]> {
        if idx >= self.record_count() {
            return None;
        }
        let offset = self.get_record_offset(idx);
        Some(&self[offset..offset + RECORD_SIZE])
    }

    fn get_key(&self, idx: u32) -> Option<&[u8]> {
        if idx >= self.record_count() {
            return None;
        }
        let offset = self.get_record_offset(idx);
        Some(&self[offset..offset + RECORD_KEY_SIZE])
    }

    fn get_value(&self, idx: u32) -> Option<&[u8]> {
        if idx >= self.record_count() {
            return None;
        }
        let offset = self.get_record_offset(idx);
        Some(&self[offset + RECORD_KEY_SIZE..offset + RECORD_SIZE])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_initialization() {
        let mut page = Page::new_empty();
        page.init_gensort();

        assert_eq!(page.record_count(), 0);
        assert_eq!(page.next_page(), None);
    }

    #[test]
    fn test_record_append_and_retrieve() {
        let mut page = Page::new_empty();
        page.init_gensort();

        let record = vec![42; RECORD_SIZE];
        assert!(page.append_record(&record));

        assert_eq!(page.record_count(), 1);
        assert_eq!(page.get_record(0), Some(&record[..]));
    }

    #[test]
    fn test_record_capacity() {
        let mut page = Page::new_empty();
        page.init_gensort();

        let max_records = <Page as GensortPage>::max_records_per_page();
        let record = vec![42; RECORD_SIZE];

        for _ in 0..max_records {
            assert!(page.append_record(&record));
        }

        // Should not be able to add one more record
        assert!(!page.append_record(&record));
    }
}
