#[allow(unused_imports)]
use crate::log;

use crate::{log_debug, random::gen_random_int, rwlatch::RwLatch};

use super::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    eviction_policy::EvictionPolicy,
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, PageFrameKey, PageKey},
};

use crate::file_manager::FileManager;

use std::{
    cell::UnsafeCell,
    collections::HashMap,
    fs::create_dir_all,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

const EVICTION_SCAN_TRIALS: usize = 5;
const EVICTION_SCAN_DEPTH: usize = 10;

#[cfg(feature = "stat")]
mod stat {
    use lazy_static::lazy_static;

    use std::{cell::UnsafeCell, sync::Mutex};
    /// Statistics #pages evicted from the buffer pool.
    pub struct BPStats {
        hit_rate: UnsafeCell<[usize; 5]>, // [fast_path_hit, slow_path_hit, slow_path_miss, new_page, latch_failures]
        victim_status: UnsafeCell<[usize; 4]>,
    }

    impl BPStats {
        pub fn new() -> Self {
            BPStats {
                hit_rate: UnsafeCell::new([0, 0, 0, 0, 0]),
                victim_status: UnsafeCell::new([0, 0, 0, 0]),
            }
        }

        pub fn to_string(&self) -> String {
            let hit_rate = unsafe { &*self.hit_rate.get() };
            let victim_status = unsafe { &*self.victim_status.get() };
            let mut result = String::new();
            result.push_str("Buffer Pool Statistics\n");
            result.push_str("Hit Rate\n");
            let total_access = hit_rate.iter().sum::<usize>();
            let labels = [
                "Fast Path Hit",
                "Slow Path Hit",
                "Slow Path Miss",
                "New Page",
                "Latch Failures (Retry)",
            ];
            for i in 0..5 {
                result.push_str(&format!(
                    "{:25}: {:8} ({:6.2}%)\n",
                    labels[i],
                    hit_rate[i],
                    (hit_rate[i] as f64 / total_access as f64) * 100.0
                ));
            }
            result.push_str(&format!(
                "{:25}: {:8} ({:6.2}%)\n",
                "Total Access", total_access, 100.0
            ));
            result.push_str("\n");
            result.push_str("Eviction Statistics\n");
            let op_types = ["Free", "Clean", "Dirty", "All Latched", "Total"];
            let total = victim_status.iter().sum::<usize>();
            for i in 0..4 {
                result.push_str(&format!(
                    "{:12}: {:8} ({:6.2}%)\n",
                    op_types[i],
                    victim_status[i],
                    (victim_status[i] as f64 / total as f64) * 100.0
                ));
            }
            result.push_str(&format!(
                "{:12}: {:8} ({:.2}%)\n",
                op_types[4], total, 100.0
            ));
            result
        }

        pub fn merge(&self, other: &BPStats) {
            let hit_rate = unsafe { &mut *self.hit_rate.get() };
            let other_hit_rate = unsafe { &*other.hit_rate.get() };
            for i in 0..5 {
                hit_rate[i] += other_hit_rate[i];
            }
            let victim_status = unsafe { &mut *self.victim_status.get() };
            let other_victim_status = unsafe { &*other.victim_status.get() };
            for i in 0..4 {
                victim_status[i] += other_victim_status[i];
            }
        }

        pub fn clear(&self) {
            let hit_rate = unsafe { &mut *self.hit_rate.get() };
            for i in 0..5 {
                hit_rate[i] = 0;
            }
            let victim_status = unsafe { &mut *self.victim_status.get() };
            for i in 0..4 {
                victim_status[i] = 0;
            }
        }
    }

    pub struct LocalBPStat {
        pub stat: BPStats,
    }

    impl Drop for LocalBPStat {
        fn drop(&mut self) {
            GLOBAL_BP_STAT.lock().unwrap().merge(&self.stat);
        }
    }

    lazy_static! {
        pub static ref GLOBAL_BP_STAT: Mutex<BPStats> = Mutex::new(BPStats::new());
    }

    thread_local! {
        pub static LOCAL_BP_STAT: LocalBPStat = LocalBPStat {
            stat: BPStats::new(),
        };
    }

    pub fn inc_local_bp_fast_path_hit() {
        LOCAL_BP_STAT.with(|stat| {
            let hit_rate = unsafe { &mut *stat.stat.hit_rate.get() };
            hit_rate[0] += 1;
        });
    }

    pub fn inc_local_bp_slow_path_hit() {
        LOCAL_BP_STAT.with(|stat| {
            let hit_rate = unsafe { &mut *stat.stat.hit_rate.get() };
            hit_rate[1] += 1;
        });
    }

    pub fn inc_local_bp_slow_path_miss() {
        LOCAL_BP_STAT.with(|stat| {
            let hit_rate = unsafe { &mut *stat.stat.hit_rate.get() };
            hit_rate[2] += 1;
        });
    }

    pub fn inc_local_bp_new_page() {
        LOCAL_BP_STAT.with(|stat| {
            let hit_rate = unsafe { &mut *stat.stat.hit_rate.get() };
            hit_rate[3] += 1;
        });
    }

    pub fn inc_local_bp_latch_failures() {
        LOCAL_BP_STAT.with(|stat| {
            let hit_rate = unsafe { &mut *stat.stat.hit_rate.get() };
            hit_rate[4] += 1;
        });
    }

    pub fn inc_local_bp_free_victim() {
        LOCAL_BP_STAT.with(|stat| {
            let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
            victim_status[0] += 1;
        });
    }

    pub fn inc_local_bp_clean_victim() {
        LOCAL_BP_STAT.with(|stat| {
            let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
            victim_status[1] += 1;
        });
    }

    pub fn inc_local_bp_dirty_victim() {
        LOCAL_BP_STAT.with(|stat| {
            let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
            victim_status[2] += 1;
        });
    }

    pub fn inc_local_bp_all_latched_victim() {
        LOCAL_BP_STAT.with(|stat| {
            let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
            victim_status[3] += 1;
        });
    }
}

use concurrent_queue::ConcurrentQueue;
#[cfg(feature = "stat")]
use stat::*;

/// Statistics kept by the buffer pool.
/// These statistics are used for decision making.
struct RuntimeStats {
    new_page: AtomicUsize,
    read_count: AtomicUsize,
    write_count: AtomicUsize,
}

impl RuntimeStats {
    pub fn new() -> Self {
        RuntimeStats {
            new_page: AtomicUsize::new(0),
            read_count: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
        }
    }

    pub fn inc_new_page(&self) {
        self.new_page.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_read_count(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_write_count(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> (usize, usize, usize) {
        (
            self.new_page.load(Ordering::Relaxed),
            self.read_count.load(Ordering::Relaxed),
            self.write_count.load(Ordering::Relaxed),
        )
    }

    pub fn to_string(&self) -> String {
        format!(
            "New Page: {}\nRead Count: {}\nWrite Count: {}",
            self.new_page.load(Ordering::Relaxed),
            self.read_count.load(Ordering::Relaxed),
            self.write_count.load(Ordering::Relaxed)
        )
    }

    pub fn clear(&self) {
        self.new_page.store(0, Ordering::Relaxed);
        self.read_count.store(0, Ordering::Relaxed);
        self.write_count.store(0, Ordering::Relaxed);
    }
}

pub struct Frames {
    num_frames: usize,
    fast_path_victims: ConcurrentQueue<usize>,
    eviction_candidates: [usize; EVICTION_SCAN_DEPTH],
    frames: Vec<BufferFrame>, // The Vec<frames> is fixed size. If not fixed size, then Pin must be used to ensure that the frame does not move when the vector is resized.
}

impl Frames {
    pub fn new(num_frames: usize) -> Self {
        let fast_path_victims = ConcurrentQueue::unbounded();
        for i in 0..num_frames {
            fast_path_victims.push(i).unwrap();
        }

        Frames {
            num_frames,
            fast_path_victims,
            eviction_candidates: [0; EVICTION_SCAN_DEPTH],
            frames: (0..num_frames)
                .map(|i| BufferFrame::new(i as u32))
                .collect(),
        }
    }

    pub fn reset_free_frames(&self) {
        while self.fast_path_victims.pop().is_ok() {}
        for i in 0..self.num_frames {
            self.fast_path_victims.push(i).unwrap();
        }
    }

    pub fn push_to_eviction_queue(&self, frame_id: usize) {
        self.fast_path_victims.push(frame_id).unwrap();
    }

    /// Choose a victim frame to be evicted.
    /// Return the index of the frame and whether the frame is dirty (requires writing back to disk).
    /// If all the frames are locked, then return None.
    pub fn choose_victim(&mut self) -> Option<FrameWriteGuard> {
        log_debug!("Choosing victim");

        // First, try the fast path victims.
        while let Ok(victim) = self.fast_path_victims.pop() {
            let frame = self.frames[victim].try_write(false);
            if let Some(guard) = frame {
                log_debug!("Fast path victim found @ frame({})", guard.frame_id());
                return Some(guard);
            } else {
                // The frame is latched. Try the next frame.
            }
        }

        // Next, randomly select a few frames and choose the one with the minimum eviction score
        for _ in 0..EVICTION_SCAN_TRIALS {
            // Initialize the eviction candidates with max
            for i in 0..EVICTION_SCAN_DEPTH {
                self.eviction_candidates[i] = usize::MAX;
            }
            if self.num_frames > EVICTION_SCAN_DEPTH {
                // Generate **distinct** random numbers.
                for _ in 0..3 * EVICTION_SCAN_DEPTH {
                    let rand_idx = gen_random_int(0, self.num_frames - 1);
                    self.eviction_candidates[rand_idx % EVICTION_SCAN_DEPTH] = rand_idx;
                    // Use mod to avoid duplicates
                }
            } else {
                // Use all the frames as candidates
                for i in 0..self.num_frames {
                    self.eviction_candidates[i] = i;
                }
            }
            log_debug!("Eviction candidates: {:?}", self.eviction_candidates);

            let mut frame_with_min_score: Option<FrameWriteGuard> = None;
            for i in self.eviction_candidates.iter() {
                if i == &usize::MAX {
                    // Skip the invalid index
                    continue;
                }
                let frame = self.frames[*i].try_write(false);
                if let Some(guard) = frame {
                    if let Some(current_min_score) = frame_with_min_score.as_ref() {
                        if guard.eviction_score() < current_min_score.eviction_score() {
                            frame_with_min_score = Some(guard);
                        } else {
                            // No need to update the min frame
                        }
                    } else {
                        frame_with_min_score = Some(guard);
                    }
                } else {
                    // Could not acquire the lock. Do not consider this frame.
                }
            }

            log_debug!("Frame with min score: {:?}", frame_with_min_score);

            #[allow(clippy::manual_map)]
            if let Some(guard) = frame_with_min_score {
                log_debug!("Victim found @ frame({})", guard.frame_id());
                return Some(guard);
            } else {
                log_debug!("All latched");
                continue;
            }
        }
        None
    }
}

impl Deref for Frames {
    type Target = Vec<BufferFrame>;

    fn deref(&self) -> &Self::Target {
        &self.frames
    }
}

impl DerefMut for Frames {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frames
    }
}

/// Buffer pool that manages the buffer frames.
pub struct BufferPool {
    remove_dir_on_drop: bool,
    path: PathBuf,
    latch: RwLatch,
    frames: UnsafeCell<Frames>,
    id_to_index: UnsafeCell<HashMap<PageKey, usize>>, // (c_id, page_id) -> index
    container_to_file: UnsafeCell<HashMap<ContainerKey, FileManager>>,
    runtime_stats: RuntimeStats,
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        if self.remove_dir_on_drop {
            std::fs::remove_dir_all(&self.path).unwrap();
        } else {
            // Persist all the pages to disk
            self.flush_all_and_reset().unwrap();
        }
    }
}

impl BufferPool {
    /// Create a new buffer pool with the given number of frames.
    /// Directory structure
    /// * bp_dir
    ///    * db_dir
    ///      * container_file
    ///
    /// The buffer pool will create the bp_dir if it does not exist.
    /// The db_dir and container_file are lazily created when a page is evicted and
    /// a file manager is constructed.
    /// If remove_dir_on_drop is true, then the bp_dir will be removed when the buffer pool is dropped.
    pub fn new<P: AsRef<std::path::Path>>(
        bp_dir: P,
        num_frames: usize,
        remove_dir_on_drop: bool,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("Buffer pool created: num_frames: {}", num_frames);

        // Identify all the directories. A directory corresponds to a database.
        // A file in the directory corresponds to a container.
        // Create a FileManager for each file and store it in the container.
        let mut container = HashMap::new();
        create_dir_all(&bp_dir).unwrap();
        for entry in std::fs::read_dir(&bp_dir).unwrap() {
            let entry = entry.unwrap();
            let db_path = entry.path();
            if db_path.is_dir() {
                let db_id = db_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                for entry in std::fs::read_dir(&db_path).unwrap() {
                    let entry = entry.unwrap();
                    let file_path = entry.path();
                    if file_path.is_file() {
                        let c_id = file_path
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .parse()
                            .unwrap();
                        let fm = FileManager::new(&db_path, c_id)?;
                        container.insert(ContainerKey::new(db_id, c_id), fm);
                    }
                }
            }
        }

        Ok(BufferPool {
            remove_dir_on_drop,
            path: bp_dir.as_ref().to_path_buf(),
            latch: RwLatch::default(),
            id_to_index: UnsafeCell::new(HashMap::new()),
            frames: UnsafeCell::new(Frames::new(num_frames)),
            container_to_file: UnsafeCell::new(container),
            runtime_stats: RuntimeStats::new(),
        })
    }

    pub fn eviction_stats(&self) -> String {
        #[cfg(feature = "stat")]
        {
            let stats = GLOBAL_BP_STAT.lock().unwrap();
            LOCAL_BP_STAT.with(|local_stat| {
                stats.merge(&local_stat.stat);
                local_stat.stat.clear();
            });
            stats.to_string()
        }
        #[cfg(not(feature = "stat"))]
        {
            "Stat is disabled".to_string()
        }
    }

    pub fn file_stats(&self) -> String {
        #[cfg(feature = "stat")]
        {
            let container_to_file = unsafe { &*self.container_to_file.get() };
            let mut result = String::new();
            for (c_key, file) in container_to_file.iter() {
                result.push_str(&format!("Container: {:?}\n", c_key));
                result.push_str(&file.get_stats());
                result.push_str("\n");
            }
            result
        }
        #[cfg(not(feature = "stat"))]
        {
            "Stat is disabled".to_string()
        }
    }

    fn shared(&self) {
        self.latch.shared();
    }

    fn exclusive(&self) {
        self.latch.exclusive();
    }

    fn release_shared(&self) {
        self.latch.release_shared();
    }

    fn release_exclusive(&self) {
        self.latch.release_exclusive();
    }

    // The exclusive latch must be held when calling this function
    fn choose_victim(&self) -> Option<FrameWriteGuard> {
        let frames = unsafe { &mut *self.frames.get() };
        frames.choose_victim()
    }

    // The exclusive latch must be held when calling this function
    fn handle_page_fault(
        &self,
        key: PageKey,
        new_page: bool,
        location: &mut FrameWriteGuard,
    ) -> Result<(), MemPoolStatus> {
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        let index = location.frame_id();
        let is_dirty = location.dirty().load(Ordering::Acquire);

        // Evict old page if necessary
        if let Some(old_key) = location.page_key() {
            if is_dirty {
                #[cfg(feature = "stat")]
                inc_local_bp_dirty_victim();
                location.dirty().store(false, Ordering::Release);
                let file = container_to_file
                    .get(&old_key.c_key)
                    .ok_or(MemPoolStatus::FileManagerNotFound)?;

                self.runtime_stats.inc_write_count();
                file.write_page(old_key.page_id, location)?;
            } else {
                #[cfg(feature = "stat")]
                inc_local_bp_clean_victim();
            }
            id_to_index.remove(old_key);
            log_debug!("Page evicted: {}", old_key);
        } else {
            #[cfg(feature = "stat")]
            inc_local_bp_free_victim();
        }

        // Create a new page or read from disk
        if new_page {
            location.set_id(key.page_id);
        } else {
            let file = container_to_file
                .get(&key.c_key)
                .ok_or(MemPoolStatus::FileManagerNotFound)?;

            self.runtime_stats.inc_read_count();
            file.read_page(key.page_id, location)?;
        };

        id_to_index.insert(key, index as usize);
        *location.page_key_mut() = Some(key);
        {
            let mut evict_info = location.evict_info().write().unwrap();
            evict_info.reset();
            evict_info.update();
        }

        log_debug!("Page loaded: key: {}", key);
        Ok(())
    }
}

impl MemPool for BufferPool {
    /// Create a new page for write in memory.
    /// NOTE: This function does not write the page to disk.
    /// See more at `handle_page_fault(key, new_page=true)`
    /// The newly allocated page is not formatted except for the page id.
    /// The caller is responsible for initializing the page.
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, MemPoolStatus> {
        log_debug!("Page create: {}", c_key);
        self.runtime_stats.inc_new_page();

        self.exclusive();

        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        let fm = container_to_file.entry(c_key).or_insert_with(|| {
            match(FileManager::new(self.path.join(c_key.db_id.to_string()), c_key.c_id)){
                Ok(x) => x,
                Err(y) => panic!("{:?}", y),
            }
        });

        let page_id = fm.fetch_add_page_id();
        let key = PageKey::new(c_key, page_id);
        if let Some(mut location) = self.choose_victim() {
            let res = self.handle_page_fault(key, true, &mut location);
            if let Ok(()) = res {
                #[cfg(feature = "stat")]
                inc_local_bp_new_page();
                location.dirty().store(true, Ordering::Release);
            } else {
                #[cfg(feature = "stat")]
                inc_local_bp_latch_failures();
                fm.fetch_sub_page_id();
            }

            self.release_exclusive();
            res.map(|_| location)
        } else {
            self.release_exclusive();
            Err(MemPoolStatus::CannotEvictPage)
        }
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard, MemPoolStatus> {
        log_debug!("Page write: {}", key);

        let location = {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            let frames = unsafe { &*self.frames.get() };
            if (frame_id as usize) < frames.len() {
                let guard = frames[frame_id as usize].try_write(false);
                if let Some(g) = guard {
                    // Check if the page key matches
                    if let Some(page_key) = g.page_key() {
                        if page_key == &key.p_key() {
                            // Update the eviction info
                            g.evict_info().write().unwrap().update();
                            // Mark the page as dirty
                            g.dirty().store(true, Ordering::Release);
                            log_debug!("Page fast path write: {}", key);
                            #[cfg(feature = "stat")]
                            inc_local_bp_fast_path_hit();
                            return Ok(g);
                        } else {
                            // The page key does not match.
                            // Go to the slow path.
                            Some(g)
                        }
                    } else {
                        // The frame is empty.
                        // Go to the slow path.
                        log_debug!("Page fast path write empty frame: {}", key);
                        Some(g)
                    }
                } else {
                    // The frame is latched.
                    // Need to retry.
                    log_debug!("Page fast path write latch failed: {}", key);
                    #[cfg(feature = "stat")]
                    inc_local_bp_latch_failures();
                    None
                }
            } else {
                // The frame id is out of bounds.
                // Go to the slow path.
                log_debug!("Page fast path write frame id out of bounds: {}", key);
                None
            }
        };

        {
            self.shared();
            let id_to_index = unsafe { &mut *self.id_to_index.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = id_to_index.get(&key.p_key()) {
                let guard = frames[index].try_write(true);
                if let Some(g) = &guard {
                    g.evict_info().write().unwrap().update();
                }
                self.release_shared();
                match guard {
                    Some(g) => {
                        log_debug!("Page slow path(shared bp latch) write: {}", key);
                        #[cfg(feature = "stat")]
                        inc_local_bp_slow_path_hit();
                        return Ok(g);
                    }
                    None => {
                        #[cfg(feature = "stat")]
                        inc_local_bp_latch_failures();
                        return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
                    }
                }
            }
            self.release_shared();
        }

        self.exclusive();

        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let frames = unsafe { &mut *self.frames.get() };

        // We need to recheck the id_to_index because another thread might have inserted the page
        let result = match id_to_index.get(&key.p_key()) {
            Some(&index) => {
                let guard = frames[index].try_write(true);
                if let Some(g) = &guard {
                    #[cfg(feature = "stat")]
                    inc_local_bp_slow_path_hit();
                    g.evict_info().write().unwrap().update();
                } else {
                    #[cfg(feature = "stat")]
                    inc_local_bp_latch_failures();
                }
                guard.ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)
            }
            None => {
                let mut victim = if let Some(location) = location {
                    location
                } else if let Some(location) = self.choose_victim() {
                    location
                } else {
                    self.release_exclusive();
                    return Err(MemPoolStatus::CannotEvictPage);
                };
                let res = self.handle_page_fault(key.p_key(), false, &mut victim);
                // If guard is ok, mark the page as dirty
                if let Ok(()) = res {
                    #[cfg(feature = "stat")]
                    inc_local_bp_slow_path_miss();
                    victim.dirty().store(true, Ordering::Release);
                } else {
                    #[cfg(feature = "stat")]
                    inc_local_bp_latch_failures();
                }
                res.map(|_| victim)
            }
        };
        self.release_exclusive();
        result
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard, MemPoolStatus> {
        log_debug!("Page read: {}", key);

        let location = {
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            let frames = unsafe { &*self.frames.get() };
            if (frame_id as usize) < frames.len() {
                let guard = frames[frame_id as usize].try_read();
                if let Some(g) = guard {
                    // Check if the page key matches
                    if let Some(page_key) = g.page_key() {
                        if page_key == &key.p_key() {
                            // Update the eviction info
                            g.evict_info().write().unwrap().update();
                            log_debug!("Page fast path read: {}", key);
                            #[cfg(feature = "stat")]
                            inc_local_bp_fast_path_hit();
                            return Ok(g);
                        } else {
                            // The page key does not match.
                            // Go to the slow path.
                            log_debug!("Page fast path read key mismatch: {}", key);
                            // Return option
                            g.try_upgrade(false).ok()
                        }
                    } else {
                        // The frame is empty.
                        // Go to the slow path.
                        log_debug!("Page fast path read empty frame: {}", key);
                        g.try_upgrade(false).ok()
                    }
                } else {
                    // The frame is latched.
                    // Need to retry.
                    log_debug!("Page fast path read latch failed: {}", key);
                    #[cfg(feature = "stat")]
                    inc_local_bp_latch_failures();
                    None
                }
            } else {
                // The frame id is out of bounds.
                // Go to the slow path.
                log_debug!("Page fast path read frame id out of bounds: {}", key);
                None
            }
        };

        {
            self.shared();
            let id_to_index = unsafe { &mut *self.id_to_index.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = id_to_index.get(&key.p_key()) {
                let guard = frames[index].try_read();
                if let Some(g) = &guard {
                    g.evict_info().write().unwrap().update();
                }
                self.release_shared();
                match guard {
                    Some(g) => {
                        log_debug!("Page slow path(shared bp latch) read: {}", key);
                        #[cfg(feature = "stat")]
                        inc_local_bp_slow_path_hit();
                        return Ok(g);
                    }
                    None => {
                        #[cfg(feature = "stat")]
                        inc_local_bp_latch_failures();
                        return Err(MemPoolStatus::FrameReadLatchGrantFailed);
                    }
                }
            }
            self.release_shared();
        }

        self.exclusive();

        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let frames = unsafe { &mut *self.frames.get() };

        let result = match id_to_index.get(&key.p_key()) {
            Some(&index) => {
                let guard = frames[index].try_read();
                if let Some(g) = &guard {
                    #[cfg(feature = "stat")]
                    inc_local_bp_slow_path_hit();
                    g.evict_info().write().unwrap().update();
                } else {
                    #[cfg(feature = "stat")]
                    inc_local_bp_latch_failures();
                }
                guard.ok_or(MemPoolStatus::FrameReadLatchGrantFailed)
            }
            None => {
                let mut victim = if let Some(location) = location {
                    location
                } else if let Some(location) = self.choose_victim() {
                    location
                } else {
                    self.release_exclusive();
                    return Err(MemPoolStatus::CannotEvictPage);
                };
                let res = self.handle_page_fault(key.p_key(), false, &mut victim);
                #[cfg(feature = "stat")]
                if res.is_ok() {
                    inc_local_bp_slow_path_miss();
                } else {
                    inc_local_bp_latch_failures();
                }
                res.map(|_| victim.downgrade())
            }
        };
        self.release_exclusive();
        result
    }

    fn prefetch_page(&self, key: PageFrameKey) -> Result<(), MemPoolStatus> {
        #[cfg(not(feature = "new_async_write"))]
        return Ok(());

        #[cfg(feature = "new_async_write")]
        {
            log_debug!("Page prefetch: {}", key);
            // Fast path access to the frame using frame_id
            let frame_id = key.frame_id();
            let frames = unsafe { &*self.frames.get() };
            if (frame_id as usize) < frames.len() {
                let guard = frames[frame_id as usize].try_read();
                if let Some(g) = &guard {
                    // Check if the page key matches
                    if let Some(page_key) = g.page_key() {
                        if page_key == &key.p_key() {
                            return Ok(());
                        }
                    }
                }
            }

            self.shared();
            let id_to_index = unsafe { &mut *self.id_to_index.get() };
            let container_to_file = unsafe { &mut *self.container_to_file.get() };

            if id_to_index.contains_key(&key.p_key()) {
                // Do nothing
            } else {
                let file = container_to_file
                    .get(&key.p_key().c_key)
                    .ok_or(MemPoolStatus::FileManagerNotFound)
                    .unwrap();
                file.prefetch_page(key.p_key().page_id).unwrap();
            }

            self.release_shared();
            Ok(())
        }
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.shared();

        let frames = unsafe { &*self.frames.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        for frame in frames.iter() {
            let frame = loop {
                if let Some(guard) = frame.try_read() {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            if frame.dirty().swap(false, Ordering::Acquire) {
                // swap is required to avoid concurrent flushes
                let key = frame.page_key().unwrap();
                if let Some(file) = container_to_file.get(&key.c_key) {
                    self.runtime_stats.inc_write_count();
                    file.write_page(key.page_id, &frame)?;
                } else {
                    self.release_shared();
                    return Err(MemPoolStatus::FileManagerNotFound);
                }
            }
        }

        // Call fsync on all the files
        for file in container_to_file.values() {
            file.flush()?;
        }

        self.release_shared();
        Ok(())
    }

    fn fast_evict(&self, frame_id: u32) -> Result<(), MemPoolStatus> {
        // push_to_eviction_queue can be done without buffer pool latch
        // because it is a lock-free operation
        let frames = unsafe { &*self.frames.get() };
        frames.push_to_eviction_queue(frame_id as usize);
        Ok(())
    }

    // Just return the runtime stats
    fn stats(&self) -> (usize, usize, usize) {
        self.runtime_stats.get()
    }

    // Reset the runtime stats
    fn reset_stats(&self) {
        self.runtime_stats.clear();
    }

    /// Reset the buffer pool to its initial state.
    /// This will write all the dirty pages to disk and flush the files.
    /// After this operation, the buffer pool will have all the frames cleared.
    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        self.exclusive();

        let frames = unsafe { &*self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        for frame in frames.iter() {
            let mut frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            if frame.dirty().load(Ordering::Acquire) {
                let key = frame.page_key().unwrap();
                if let Some(file) = container_to_file.get(&key.c_key) {
                    self.runtime_stats.inc_write_count();
                    file.write_page(key.page_id, &frame)?;
                } else {
                    self.release_exclusive();
                    return Err(MemPoolStatus::FileManagerNotFound);
                }
            }
            frame.clear();
        }

        // Call fsync on all the files
        for file in container_to_file.values() {
            file.flush()?;
        }

        id_to_index.clear();

        frames.reset_free_frames();

        self.release_exclusive();
        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        self.exclusive();

        let frames = unsafe { &*self.frames.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        for frame in frames.iter() {
            let frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            frame.dirty().store(false, Ordering::Release);
        }

        // Call fsync on all the files to ensure that no dirty pages
        // are left in the file manager queue
        for file in container_to_file.values() {
            file.flush()?;
        }

        // container_to_file.clear();
        self.runtime_stats.clear();

        self.release_exclusive();
        Ok(())
    }
}

#[cfg(test)]
impl BufferPool {
    pub fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_id_to_index();
        self.check_frame_id_and_page_id_match();
    }

    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            frame.try_write(false).unwrap();
        }
    }

    // Invariant: id_to_index contains all the pages in the buffer pool
    pub fn check_id_to_index(&self) {
        let id_to_index = unsafe { &*self.id_to_index.get() };
        let mut index_to_id = HashMap::new();
        for (k, &v) in id_to_index.iter() {
            index_to_id.insert(v, k);
        }
        let frames = unsafe { &*self.frames.get() };
        for (i, frame) in frames.iter().enumerate() {
            let frame = frame.read();
            if index_to_id.contains_key(&i) {
                assert_eq!(frame.page_key().unwrap(), *index_to_id[&i]);
            } else {
                assert_eq!(frame.page_key(), &None);
            }
        }
        // println!("id_to_index: {:?}", id_to_index);
    }

    pub fn check_frame_id_and_page_id_match(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            let frame = frame.read();
            if let Some(key) = frame.page_key() {
                let page_id = frame.get_id();
                assert_eq!(key.page_id, page_id);
            }
        }
    }

    pub fn is_in_buffer_pool(&self, key: PageFrameKey) -> bool {
        let id_to_index = unsafe { &*self.id_to_index.get() };
        id_to_index.contains_key(&key.p_key())
    }
}

unsafe impl Sync for BufferPool {}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::log;
    use crate::log_info;

    use super::*;
    use std::thread::{self, sleep};
    use tempfile::TempDir;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        {
            let num_frames = 10;
            let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
            let c_key = ContainerKey::new(db_id, 0);
            let frame = bp.create_new_page_for_write(c_key).unwrap();
            let key = frame.page_frame_key().unwrap();
            drop(frame);

            let num_threads = 3;
            let num_iterations = 80; // Note: u8 max value is 255
            thread::scope(|s| {
                for _ in 0..num_threads {
                    s.spawn(|| {
                        for _ in 0..num_iterations {
                            loop {
                                if let Ok(mut guard) = bp.get_page_for_write(key) {
                                    guard[0] += 1;
                                    break;
                                } else {
                                    // spin
                                    log_info!("Spin");
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    });
                }
            });
            bp.run_checks();
            {
                let guard = bp.get_page_for_read(key).unwrap();
                assert_eq!(guard[0], num_threads * num_iterations);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_write_back_simple() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        {
            let num_frames = 1;
            let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            let key1 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = 1;
                guard.page_frame_key().unwrap()
            };
            let key2 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = 2;
                guard.page_frame_key().unwrap()
            };
            bp.run_checks();
            // check contents of evicted page
            {
                let guard = bp.get_page_for_read(key1).unwrap();
                assert_eq!(guard[0], 1);
            }
            // check contents of the page in the buffer pool
            {
                let guard = bp.get_page_for_read(key2).unwrap();
                assert_eq!(guard[0], 2);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_write_back_many() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        {
            let mut keys = Vec::new();
            let num_frames = 1;
            let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            for i in 0..100 {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = i;
                keys.push(guard.page_frame_key().unwrap());
            }
            bp.run_checks();
            for (i, key) in keys.iter().enumerate() {
                let guard = bp.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_create_new_page() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 2;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let num_traversal = 100;

        let mut count = 0;
        let mut keys = Vec::new();

        for _ in 0..num_traversal {
            let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
            guard1[0] = count;
            count += 1;
            keys.push(guard1.page_frame_key().unwrap());

            let mut guard2 = bp.create_new_page_for_write(c_key).unwrap();
            guard2[0] = count;
            count += 1;
            keys.push(guard2.page_frame_key().unwrap());
        }

        bp.run_checks();

        // Traverse by 2 pages at a time
        for i in 0..num_traversal {
            let guard1 = bp.get_page_for_read(keys[i * 2]).unwrap();
            assert_eq!(guard1[0], i as u8 * 2);
            let guard2 = bp.get_page_for_read(keys[i * 2 + 1]).unwrap();
            assert_eq!(guard2[0], i as u8 * 2 + 1);
        }

        bp.run_checks();
    }

    #[test]
    fn test_bp_all_frames_latched() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 1;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
        guard1[0] = 1;

        // Try to get a new page for write. This should fail because all the frames are latched.
        let res = bp.create_new_page_for_write(c_key);
        assert_eq!(res.unwrap_err(), MemPoolStatus::CannotEvictPage);

        drop(guard1);

        // Now, we should be able to get a new page for write.
        let guard2 = bp.create_new_page_for_write(c_key).unwrap();
        drop(guard2);
    }

    #[test]
    fn test_bp_clear_frames() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 10;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames * 2 {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        bp.run_checks();

        // Clear the buffer pool
        bp.flush_all_and_reset().unwrap();

        bp.run_checks();

        // Check the contents of the pages
        for (i, key) in keys.iter().enumerate() {
            let guard = bp.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        bp.run_checks();
    }

    #[test]
    fn test_bp_clear_frames_durable() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 10;
        let bp1 = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let mut keys = Vec::new();
        for i in 0..num_frames * 10 {
            let mut guard = bp1.create_new_page_for_write(c_key).unwrap();
            guard[0] = i as u8;
            keys.push(guard.page_frame_key().unwrap());
        }

        bp1.run_checks();

        // Clear the buffer pool
        bp1.flush_all_and_reset().unwrap();

        bp1.run_checks();

        drop(bp1); // Drop will also clear the buffer pool

        // Create a new buffer pool
        let bp2 = BufferPool::new(&temp_dir, num_frames * 2, false).unwrap();

        bp2.run_checks();

        // Check the contents of the pages
        for (i, key) in keys.iter().enumerate() {
            let guard = bp2.get_page_for_read(*key).unwrap();
            assert_eq!(guard[0], i as u8);
        }

        bp2.run_checks();
    }

    #[test]
    fn test_bp_stats() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;

        let num_frames = 1;
        let bp = BufferPool::new(&temp_dir, num_frames, false).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let key_1 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 1;
            guard.page_frame_key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        let key_2 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 2;
            guard.page_frame_key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_1).unwrap();
            assert_eq!(guard[0], 1);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_2).unwrap();
            assert_eq!(guard[0], 2);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);
    }
}
