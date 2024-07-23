use std::fs::File;
use std::io::{self, Write, Read, BufReader, BufWriter, BufRead};
use log::debug;
use rayon::prelude::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::Arc;
use fbtree::prelude::{ContainerKey, EvictionPolicy, MemPool, PAGE_SIZE};
use fbtree::bp::{BufferPool, LRUEvictionPolicy};
use fbtree::page::Page;
use fbtree::access_method::append_only_store::prelude::{AppendOnlyStore};

// Converts a slice of u32 into a slice of u8
fn u32_to_u8_slice(input: &[u32]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            input.as_ptr() as *const u8,
            input.len() * std::mem::size_of::<u32>(),
        )
    }
}

// Writes data to a page, ensuring it doesn't exceed the page size
fn write_to_page(page: &mut Page, data: &[u32]) -> Result<(), &'static str> {
    let data_as_u8 = u32_to_u8_slice(data);
    if data_as_u8.len() > PAGE_SIZE {
        return Err("Slice exceeds page size");
    }

    page.write_slice(0, data_as_u8)?;
    Ok(())
}

// Enum representing a buffer that can be in memory or on disk
#[derive(Debug)]
pub enum TupleBuffer {
    InMemory(MemoryBuffer),
    OnDisk(DiskBuffer),
}

impl TupleBuffer {
    pub fn push(&mut self, tuple: u32) {
        match self {
            TupleBuffer::InMemory(buffer) => buffer.push(tuple),
            TupleBuffer::OnDisk(buffer) => buffer.push(tuple),
        }
    }

    pub fn into_iterator(self) -> TupleIterator {
        match self {
            TupleBuffer::InMemory(buffer) => TupleIterator::InMemory(buffer.into_iterator()),
            TupleBuffer::OnDisk(buffer) => TupleIterator::OnDisk(buffer.into_iterator()),
        }
    }

    pub fn get_nth_data(&self, n: usize) -> u32 {
        match self {
            TupleBuffer::InMemory(buffer) => buffer.get_nth_data(n),
            TupleBuffer::OnDisk(buffer) => buffer.get_nth_data(n),
        }
    }

    pub fn get_values(&self, start: u32, end: u32) -> Vec<u32> {
        match self {
            TupleBuffer::InMemory(buffer) => buffer.get_values(start, end),
            TupleBuffer::OnDisk(buffer) => buffer.get_values(start, end),
        }
    }
}

// Iterator enum for TupleBuffer
#[derive(Debug)]
pub enum TupleIterator {
    InMemory(MemoryIterator),
    OnDisk(DiskIterator),
}

impl Iterator for TupleIterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TupleIterator::InMemory(iterator) => iterator.next(),
            TupleIterator::OnDisk(iterator) => iterator.next(),
        }
    }
}

// Struct for managing quantiles
#[derive(Debug)]
pub struct Quantiles {
    num_quantiles: usize,
    quantiles: Vec<Vec<u32>>,
}

impl Quantiles {
    pub fn new(num_quantiles: usize) -> Self {
        Quantiles {
            num_quantiles,
            quantiles: Vec::new(),
        }
    }

    pub fn compute_quantiles(&mut self, run: &[u32]) {
        let run_len = run.len();
        let mut quantile_values = Vec::new();

        for i in 1..self.num_quantiles {
            let quantile_index = (i * run_len) / self.num_quantiles;
            quantile_values.push(run[quantile_index]);
        }

        self.quantiles.push(quantile_values);
    }

    pub fn estimate_global_quantiles_mean(&self) -> Vec<u32> {
        let mut all_quantiles: Vec<u32> = self.quantiles.iter().flatten().cloned().collect();
        all_quantiles.sort_unstable();
        let mut global_quantiles = Vec::new();
        let len = all_quantiles.len();
        for i in 1..self.num_quantiles {
            let index = (i * len) / self.num_quantiles;
            global_quantiles.push(all_quantiles[index]);
        }
        global_quantiles
    }

    pub fn estimate_global_quantiles_median(&self) -> Vec<u32> {
        let mut global_quantiles = Vec::new();
        for i in 1..self.num_quantiles {
            let mut ith_quantiles: Vec<u32> = self.quantiles.iter().map(|q| q[i - 1]).collect();
            ith_quantiles.sort_unstable();
            let median = ith_quantiles[ith_quantiles.len() / 2];
            global_quantiles.push(median);
        }
        global_quantiles
    }
}

// Struct for external sorting using buffer pool and append-only store
pub struct ExternalSorting<T: EvictionPolicy> {
    input: Vec<u32>,
    intermediate_buffers: Vec<TupleBuffer>,
    output: TupleBuffer,
    run_size: usize,
    quantiles: Quantiles,
    buffer_pool: Arc<dyn MemPool<T>>,
    append_store: Arc<AppendOnlyStore<T, BufferPool<T>>>,
}

impl<T: EvictionPolicy> ExternalSorting<T> {
    pub fn new(input: Vec<u32>, run_size: usize, num_quantiles: usize, buffer_pool: Arc<dyn MemPool<T>>, append_store: Arc<AppendOnlyStore<T, BufferPool<T>>>) -> Self {
        ExternalSorting {
            input,
            intermediate_buffers: Vec::new(),
            output: TupleBuffer::InMemory(MemoryBuffer::new()),
            run_size,
            quantiles: Quantiles::new(num_quantiles),
            buffer_pool,
            append_store,
        }
    }

    pub fn execute(&mut self) {
        // Step 1: Read input and create sorted runs in parallel
        let num_runs = (self.input.len() + self.run_size - 1) / self.run_size;
        let runs: Vec<Vec<u32>> = (0..num_runs)
            .into_par_iter()
            .map(|i| {
                let start = i * self.run_size;
                let end = (start + self.run_size).min(self.input.len());
                let mut run: Vec<u32> = self.input[start..end].to_vec();
                run.sort();
                debug!("run added {:?}", run);
                run
            })
            .collect();

        println!("finished reading");
        // Compute quantiles for each run outside the parallel iterator
        for run in &runs {
            self.quantiles.compute_quantiles(run);
        }

        // Write sorted runs to append-only store
        for run in runs {
            for &value in &run {
                let value_bytes = value.to_be_bytes();
                self.append_store.append(&[], &value_bytes).expect("Failed to append to store");
            }
        }

        // Estimate global quantiles
        let global_quantiles = self.quantiles.estimate_global_quantiles_mean();
        debug!("Global quantiles: {:?}", global_quantiles);
        println!("Global quantiles: {:?}", global_quantiles);

        // Step 2: Merge sorted runs in parallel based on global quantiles
        let merged_buffers: Vec<_> = global_quantiles
            .par_iter()
            .enumerate()
            .map(|(i, &quantile)| {
                let lower_bound = if i == 0 { u32::MIN } else { global_quantiles[i - 1] };
                let upper_bound = quantile;
                let mut merged_buffer = MemoryBuffer::new();

                debug!("Merging values in range [{}, {})", lower_bound, upper_bound);

                for buffer in &self.intermediate_buffers {
                    let values = buffer.get_values(lower_bound, upper_bound);
                    debug!("From buffer {:?}, selected values {:?}", buffer, values);
                    for value in values {
                        merged_buffer.push(value);
                    }
                }

                merged_buffer
            })
            .collect();

        // Collect all merged buffers into the final output
        let mut final_output = MemoryBuffer::new();
        for buffer in merged_buffers {
            for value in buffer.into_iterator() {
                final_output.push(value);
            }
        }

        self.output = TupleBuffer::InMemory(final_output);
    }
}

// Struct representing an in-memory buffer
#[derive(Debug)]
pub struct MemoryBuffer {
    data: Vec<u32>,
}

impl MemoryBuffer {
    pub fn new() -> Self {
        MemoryBuffer { data: Vec::new() }
    }

    pub fn push(&mut self, tuple: u32) {
        self.data.push(tuple);
    }

    pub fn into_iterator(self) -> MemoryIterator {
        MemoryIterator {
            data: self.data.into_iter(),
        }
    }

    pub fn from_vec(data: Vec<u32>) -> Self {
        MemoryBuffer { data }
    }

    pub fn get_nth_data(&self, n: usize) -> u32 {
        self.data[n]
    }

    pub fn get_values(&self, start: u32, end: u32) -> Vec<u32> {
        self.data.iter().cloned().filter(|&v| v >= start && v < end).collect()
    }
}

// Iterator for MemoryBuffer
#[derive(Debug)]
pub struct MemoryIterator {
    data: std::vec::IntoIter<u32>,
}

impl Iterator for MemoryIterator {
type Item = u32;

fn next(&mut self) -> Option<Self::Item> {
    self.data.next()
}
}

// Struct representing a buffer stored on disk
#[derive(Debug)]
pub struct DiskBuffer {
file_path: String,
}

impl DiskBuffer {
pub fn new(file_path: String) -> Self {
DiskBuffer { file_path }
}

pub fn push(&mut self, tuple: u32) {
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&self.file_path)
        .expect("Unable to open file");
    writeln!(file, "{}", tuple).expect("Unable to write data to file");
}

pub fn into_iterator(self) -> DiskIterator {
    DiskIterator {
        file: std::fs::File::open(self.file_path).expect("Unable to open file"),
    }
}

pub fn get_nth_data(&self, n: usize) -> u32 {
    let file = File::open(&self.file_path).expect("Unable to open file");
    let reader = BufReader::new(file);
    reader.lines().nth(n).expect("No data at this position").expect("Failed to read line").parse().expect("Failed to parse value")
}

pub fn get_values(&self, start: u32, end: u32) -> Vec<u32> {
    let file = File::open(&self.file_path).expect("Unable to open file");
    let reader = BufReader::new(file);
    reader.lines().filter_map(|line| {
        let value: u32 = line.expect("Failed to read line").parse().expect("Failed to parse value");
        if value >= start && value < end {
            Some(value)
        } else {
            None
        }
    }).collect()
}
}

// Iterator for DiskBuffer
#[derive(Debug)]
pub struct DiskIterator {
file: std::fs::File,
}

impl Iterator for DiskIterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {  // Corrected type
        let mut buffer = String::new();
        if let Ok(bytes_read) = self.file.read_to_string(&mut buffer) {
            if bytes_read == 0 {
                None
            } else {
                Some(buffer.trim().parse().expect("Unable to parse number"))
            }
        } else {
            None
        }
    }
}

// Function to read input from a file
fn read_input_from_file(file_path: &str) -> io::Result<Vec<u32>> {  // Add generic argument
    let file = File::open(file_path)?;
    let buf_reader = BufReader::new(file);
    let mut input = Vec::new();
    for line in buf_reader.lines() {
        let number: u32 = line?.parse().expect("Unable to parse number");
        input.push(number);
    }
    Ok(input)
}

// Function to generate random input
fn generate_random_input(size: usize) -> Vec<u32> {  // Add generic argument
    let mut rng = thread_rng();
    let mut data: Vec<u32> = (1..=size as u32).collect();  // Add generic argument
    data.shuffle(&mut rng);
    data
}

// Writes the input data to a file
fn write_input_to_file(file_path: &str, data: &[u32]) -> io::Result<()> {
    let file = File::create(file_path)?;
    let mut buf_writer = BufWriter::new(file);
    for &value in data {
        writeln!(buf_writer, "{}", value)?;
    }
    Ok(())
}

// Verifies that the output is sorted
fn verify_sorted(output: &[u32]) -> bool {
    for i in 1..output.len() {
        if output[i - 1] > output[i] {
            return false;
        }
    }
    true
}

fn main() {
    env_logger::init();
    let file_path = "100,000,000.txt";
    // Read input from file
    let input_data = read_input_from_file(&file_path).expect("Unable to read input file");

    // Create output buffer
    let output_buffer = MemoryBuffer::new();

    // Create BufferPool
    let buffer_pool: Arc<BufferPool<LRUEvictionPolicy>> = Arc::new(
        BufferPool::new("bp_dir", 100, true).expect("Failed to create buffer pool")
    );

    // Create AppendOnlyStore
    let append_store = Arc::new(AppendOnlyStore::new(ContainerKey::new(1, 1), buffer_pool.clone()));

    // Create ExternalSorting object
    let mut sorter = ExternalSorting::new(input_data, 10000, 500, buffer_pool, append_store);

    // Execute sorting
    sorter.execute();

    // Retrieve sorted output
    if let TupleBuffer::InMemory(output) = sorter.output {
        let sorted_output: Vec<u32> = output.data;
        if verify_sorted(&sorted_output) {
            debug!("sorted output: {:?}", sorted_output);
            println!("The output is correctly sorted.");
        } else {
            println!("The output is NOT correctly sorted.");
            debug!("Sorted output: {:?}", sorted_output);
            println!("Sorted output: {:?}", sorted_output);
        }
    }
}