// sorted_page.rs

// Page layout:
// 4 bytes: next page id
// 4 bytes: next frame id
// 4 bytes: total bytes used (PAGE_HEADER_SIZE + slots + records)
// 4 bytes: slot count
// 4 bytes: free space offset
const PAGE_HEADER_SIZE: usize = 4 * 5;

use crate::prelude::{Page, PageId, AVAILABLE_PAGE_SIZE};

mod slot {
    pub const SLOT_SIZE: usize = std::mem::size_of::<u32>() * 3;

    pub struct Slot {
        offset: u32,
        key_size: u32,
        val_size: u32,
    }

    impl Slot {
        pub fn from_bytes(bytes: &[u8; SLOT_SIZE]) -> Self {
            let mut current_pos = 0;
            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().unwrap(),
            );
            current_pos += 4;
            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().unwrap(),
            );
            current_pos += 4;
            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().unwrap(),
            );

            Slot {
                offset,
                key_size,
                val_size,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0; SLOT_SIZE];
            let mut current_pos = 0;
            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.offset.to_be_bytes());
            current_pos += 4;
            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.key_size.to_be_bytes());
            current_pos += 4;
            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.val_size.to_be_bytes());
            bytes
        }

        pub fn new(offset: u32, key_size: u32, val_size: u32) -> Self {
            Slot {
                offset,
                key_size,
                val_size,
            }
        }

        pub fn offset(&self) -> u32 {
            self.offset
        }

        pub fn key_size(&self) -> u32 {
            self.key_size
        }

        pub fn val_size(&self) -> u32 {
            self.val_size
        }
    }
}
use slot::*;

pub trait SortedPage {
    fn init(&mut self);
    fn max_record_size() -> usize {
        AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE
    }

    // Header operations
    fn next_page(&self) -> Option<(PageId, u32)>; // (next_page_id, next_frame_id)
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);
    fn total_bytes_used(&self) -> u32;
    fn total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.total_bytes_used()
    }
    fn set_total_bytes_used(&mut self, total_bytes_used: u32);
    fn slot_count(&self) -> u32;
    fn set_slot_count(&mut self, slot_count: u32);
    fn increment_slot_count(&mut self) {
        let slot_count = self.slot_count();
        self.set_slot_count(slot_count + 1);
    }

    fn rec_start_offset(&self) -> u32;
    fn set_rec_start_offset(&mut self, rec_start_offset: u32);

    // Helpers
    fn slot_offset(&self, slot_id: u32) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }
    fn slot(&self, slot_id: u32) -> Option<Slot>;

    // Insert a slot at the specified position, shifting subsequent slots
    fn insert_slot(&mut self, slot_id: u32, slot: &Slot);

    /// Try to append a key-value pair to the page in sorted order.
    /// Returns `true` if successful, `false` if there's not enough space.
    fn append(&mut self, key: &[u8], value: &[u8]) -> bool;

    /// Get the record at the given slot index.
    fn get(&self, slot_id: u32) -> (&[u8], &[u8]);

    /// Search for a key using binary search within the page.
    /// Returns `Ok(slot_id)` if found, `Err(insert_pos)` if not.
    fn search_key(&self, key: &[u8]) -> Result<u32, u32>;
}

impl SortedPage for Page {
    fn init(&mut self) {
        let next_page_id = PageId::MAX;
        let next_frame_id = u32::MAX;
        let total_bytes_used = PAGE_HEADER_SIZE as u32;
        let slot_count = 0;
        let rec_start_offset = AVAILABLE_PAGE_SIZE as u32;

        self.set_next_page(next_page_id, next_frame_id);
        self.set_total_bytes_used(total_bytes_used);
        self.set_slot_count(slot_count);
        self.set_rec_start_offset(rec_start_offset);
    }

    fn next_page(&self) -> Option<(PageId, u32)> {
        let next_page_id = u32::from_be_bytes(self[0..4].try_into().unwrap());
        let next_frame_id = u32::from_be_bytes(self[4..8].try_into().unwrap());
        if next_page_id == PageId::MAX {
            None
        } else {
            Some((next_page_id, next_frame_id))
        }
    }

    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        self[0..4].copy_from_slice(&next_page_id.to_be_bytes());
        self[4..8].copy_from_slice(&frame_id.to_be_bytes());
    }

    fn total_bytes_used(&self) -> u32 {
        u32::from_be_bytes(self[8..12].try_into().unwrap())
    }

    fn set_total_bytes_used(&mut self, total_bytes_used: u32) {
        self[8..12].copy_from_slice(&total_bytes_used.to_be_bytes());
    }

    fn slot_count(&self) -> u32 {
        u32::from_be_bytes(self[12..16].try_into().unwrap())
    }

    fn set_slot_count(&mut self, slot_count: u32) {
        self[12..16].copy_from_slice(&slot_count.to_be_bytes());
    }

    fn rec_start_offset(&self) -> u32 {
        u32::from_be_bytes(self[16..20].try_into().unwrap())
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
        self[16..20].copy_from_slice(&rec_start_offset.to_be_bytes());
    }

    fn slot(&self, slot_id: u32) -> Option<Slot> {
        if slot_id < self.slot_count() {
            let offset = self.slot_offset(slot_id);
            let slot_bytes: [u8; SLOT_SIZE] = self[offset..offset + SLOT_SIZE].try_into().unwrap();
            Some(Slot::from_bytes(&slot_bytes))
        } else {
            None
        }
    }

    fn insert_slot(&mut self, slot_id: u32, slot: &Slot) {
        let slot_count = self.slot_count();
        assert!(slot_id <= slot_count);

        if slot_id < slot_count {
            let src_offset = self.slot_offset(slot_id);
            let dst_offset = self.slot_offset(slot_id + 1);
            let num_bytes = (slot_count - slot_id) as usize * SLOT_SIZE;
            self.copy_within(src_offset..src_offset + num_bytes, dst_offset);
        }

        // Insert the new slot
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());

        // Update slot count and total bytes used
        self.set_slot_count(slot_count + 1);
        let new_total_bytes_used = self.total_bytes_used() + SLOT_SIZE as u32;
        self.set_total_bytes_used(new_total_bytes_used);

        // Update rec_start_offset if necessary
        let offset = self.rec_start_offset().min(slot.offset());
        self.set_rec_start_offset(offset);
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> bool {
        let total_len = key.len() + value.len();
        if self.total_free_space() < SLOT_SIZE as u32 + total_len as u32 {
            false
        } else {
            let insert_pos = match self.search_key(key) {
                Ok(pos) => pos,    // Key already exists
                Err(pos) => pos,    // Position to insert
            };

            let rec_start_offset = self.rec_start_offset() - total_len as u32;
            self[rec_start_offset as usize..rec_start_offset as usize + key.len()]
                .copy_from_slice(key);
            self[rec_start_offset as usize + key.len()..rec_start_offset as usize + total_len]
                .copy_from_slice(value);

            let slot = Slot::new(rec_start_offset, key.len() as u32, value.len() as u32);
            self.insert_slot(insert_pos, &slot);

            // Update the total bytes used
            let new_total_bytes_used = self.total_bytes_used() + total_len as u32;
            self.set_total_bytes_used(new_total_bytes_used);

            true
        }
    }

    fn get(&self, slot_id: u32) -> (&[u8], &[u8]) {
        let slot = self.slot(slot_id).unwrap();
        let offset = slot.offset() as usize;
        let key = &self[offset..offset + slot.key_size() as usize];
        let value = &self[offset + slot.key_size() as usize
            ..offset + slot.key_size() as usize + slot.val_size() as usize];
        (key, value)
    }

    fn search_key(&self, key: &[u8]) -> Result<u32, u32> {
        let mut left = 0;
        let mut right = self.slot_count();
        while left < right {
            let mid = (left + right) / 2;
            let (mid_key, _) = self.get(mid);
            match mid_key.cmp(key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(left)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_initialization() {
        let mut page = Page::new_empty();
        page.init();

        assert_eq!(page.total_bytes_used(), PAGE_HEADER_SIZE as u32);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(
            page.total_free_space(),
            (AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE) as u32
        );
        assert_eq!(page.next_page(), None);
    }

    #[test]
    fn test_set_next_page() {
        let mut page = Page::new_empty();
        page.set_next_page(123, 456);

        assert_eq!(page.next_page(), Some((123, 456)));
    }

    #[test]
    fn test_slot_handling() {
        let mut page = Page::new_empty();
        page.init();

        let slot = Slot::new(100, 50, 200);
        page.insert_slot(0, &slot);

        assert_eq!(page.slot_count(), 1);
        assert_eq!(page.slot(0).unwrap().offset(), 100);
        assert_eq!(page.slot(0).unwrap().key_size(), 50);
        assert_eq!(page.slot(0).unwrap().val_size(), 200);
    }

    #[test]
    fn test_record_append() {
        let mut page = Page::new_empty();
        page.init();

        let key1 = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];

        let key2 = vec![2, 3, 4];
        let value2 = vec![5, 6, 7];

        let key3 = vec![3, 4, 5];
        let value3 = vec![6, 7, 8];

        assert!(page.append(&key2, &value2));
        assert!(page.append(&key1, &value1));
        assert!(page.append(&key3, &value3));

        assert_eq!(page.slot_count(), 3);

        let (k0, v0) = page.get(0);
        assert_eq!(k0, &key1[..]);
        assert_eq!(v0, &value1[..]);

        let (k1, v1) = page.get(1);
        assert_eq!(k1, &key2[..]);
        assert_eq!(v1, &value2[..]);

        let (k2, v2) = page.get(2);
        assert_eq!(k2, &key3[..]);
        assert_eq!(v2, &value3[..]);
    }

    #[test]
    fn test_search_key() {
        let mut page = Page::new_empty();
        page.init();

        let keys = vec![
            vec![1],
            vec![2],
            vec![3],
            vec![4],
            vec![5],
            vec![6],
            vec![7],
            vec![8],
            vec![9],
        ];
        for key in &keys {
            page.append(key, &[]);
        }

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(page.search_key(key).unwrap(), i as u32);
        }

        // Test search for non-existent keys
        assert_eq!(page.search_key(&[0]), Err(0));
        assert_eq!(page.search_key(&[5, 0]), Err(5));
        assert_eq!(page.search_key(&[10]), Err(9));
    }
}