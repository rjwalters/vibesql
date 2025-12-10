//! External Hash Join (Grace Hash Join) with partition-based disk spilling
//!
//! This module implements a memory-bounded hash join that can handle datasets
//! larger than available memory by using a partition-based approach:
//!
//! 1. **Partition Phase**: Both build and probe sides are partitioned by hash
//! 2. **Build Phase**: For each partition, build an in-memory hash table
//! 3. **Probe Phase**: Probe the hash table with matching partition rows
//!
//! When memory is exhausted, partitions are spilled to disk and processed
//! one at a time during the final join phase.

use std::io::{self, Read};
use std::sync::Arc;

use ahash::AHashMap;
use vibesql_types::SqlValue;

use super::controller::{MemoryController, MemoryReservation};
use super::row_serialization::{deserialize_value, serialize_value};
use super::spill::SpillFile;

/// Configuration for external hash join
#[derive(Debug, Clone)]
pub struct ExternalHashJoinConfig {
    /// Number of partitions (must be power of 2)
    pub num_partitions: usize,
    /// Maximum rows to keep in memory per partition before considering spill
    pub max_rows_per_partition: usize,
}

impl Default for ExternalHashJoinConfig {
    fn default() -> Self {
        Self {
            num_partitions: 64,
            max_rows_per_partition: 10_000,
        }
    }
}

/// Join type for the external hash join
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
}

/// Build-side partition
struct BuildPartition {
    /// In-memory rows: (key_values, full_row)
    rows: Vec<(Vec<SqlValue>, Vec<SqlValue>)>,
    /// Estimated memory usage
    memory_bytes: usize,
    /// Whether this partition has been spilled
    spilled: bool,
    /// Spill file for this partition
    spill_file: Option<SpillFile>,
    /// Number of rows written to spill file
    spilled_row_count: usize,
}

impl BuildPartition {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            memory_bytes: 0,
            spilled: false,
            spill_file: None,
            spilled_row_count: 0,
        }
    }
}

/// Probe-side partition
struct ProbePartition {
    /// In-memory rows: (key_values, full_row)
    rows: Vec<(Vec<SqlValue>, Vec<SqlValue>)>,
    /// Estimated memory usage
    memory_bytes: usize,
    /// Whether this partition has been spilled
    spilled: bool,
    /// Spill file for this partition
    spill_file: Option<SpillFile>,
    /// Number of rows written to spill file
    spilled_row_count: usize,
}

impl ProbePartition {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            memory_bytes: 0,
            spilled: false,
            spill_file: None,
            spilled_row_count: 0,
        }
    }
}

/// External Hash Join operator
pub struct ExternalHashJoin {
    /// Memory reservation
    reservation: MemoryReservation,
    /// Configuration
    #[allow(dead_code)]
    config: ExternalHashJoinConfig,
    /// Build-side partitions
    build_partitions: Vec<BuildPartition>,
    /// Probe-side partitions
    probe_partitions: Vec<ProbePartition>,
    /// Join key column indices for build side
    build_key_indices: Vec<usize>,
    /// Join key column indices for probe side
    probe_key_indices: Vec<usize>,
    /// Partition mask (num_partitions - 1)
    partition_mask: usize,
    /// Join type
    join_type: JoinType,
    /// Total rows on build side
    build_row_count: usize,
    /// Total rows on probe side
    probe_row_count: usize,
}

impl ExternalHashJoin {
    /// Create a new external hash join
    pub fn new(
        controller: Arc<MemoryController>,
        config: ExternalHashJoinConfig,
        build_key_indices: Vec<usize>,
        probe_key_indices: Vec<usize>,
        join_type: JoinType,
    ) -> Self {
        assert!(
            config.num_partitions.is_power_of_two(),
            "num_partitions must be power of 2"
        );
        assert_eq!(
            build_key_indices.len(),
            probe_key_indices.len(),
            "build and probe must have same number of key columns"
        );

        let partition_mask = config.num_partitions - 1;
        let mut build_partitions = Vec::with_capacity(config.num_partitions);
        let mut probe_partitions = Vec::with_capacity(config.num_partitions);

        for _ in 0..config.num_partitions {
            build_partitions.push(BuildPartition::new());
            probe_partitions.push(ProbePartition::new());
        }

        Self {
            reservation: controller.create_reservation(),
            config,
            build_partitions,
            probe_partitions,
            build_key_indices,
            probe_key_indices,
            partition_mask,
            join_type,
            build_row_count: 0,
            probe_row_count: 0,
        }
    }

    /// Add a row to the build side
    pub fn add_build_row(&mut self, row: &[SqlValue]) -> io::Result<()> {
        let key_values: Vec<SqlValue> = self
            .build_key_indices
            .iter()
            .map(|&idx| row.get(idx).cloned().unwrap_or(SqlValue::Null))
            .collect();

        // Skip rows with NULL keys (they never match in equi-joins)
        if key_values.iter().any(|v| v == &SqlValue::Null) {
            return Ok(());
        }

        let partition_idx = self.compute_partition(&key_values);
        let row_size = estimate_row_size(&key_values) + estimate_row_size(row);

        // Check if we need to spill
        if self.reservation.should_spill() {
            self.spill_largest_build_partition()?;
        }

        let partition = &mut self.build_partitions[partition_idx];

        if partition.spilled {
            // Write directly to spill file
            self.write_to_build_spill(partition_idx, &key_values, row)?;
        } else {
            // Try to grow memory reservation
            if !self.reservation.try_grow(row_size) {
                // Need to spill this partition
                self.spill_build_partition(partition_idx)?;
                self.write_to_build_spill(partition_idx, &key_values, row)?;
            } else {
                partition.rows.push((key_values, row.to_vec()));
                partition.memory_bytes += row_size;
            }
        }

        self.build_row_count += 1;
        Ok(())
    }

    /// Add a row to the probe side
    pub fn add_probe_row(&mut self, row: &[SqlValue]) -> io::Result<()> {
        let key_values: Vec<SqlValue> = self
            .probe_key_indices
            .iter()
            .map(|&idx| row.get(idx).cloned().unwrap_or(SqlValue::Null))
            .collect();

        // For inner joins, skip NULL keys. For outer joins, we need them.
        if self.join_type == JoinType::Inner
            && key_values.iter().any(|v| v == &SqlValue::Null)
        {
            return Ok(());
        }

        let partition_idx = self.compute_partition(&key_values);
        let row_size = estimate_row_size(&key_values) + estimate_row_size(row);

        // Check if we need to spill
        if self.reservation.should_spill() {
            self.spill_largest_probe_partition()?;
        }

        let partition = &mut self.probe_partitions[partition_idx];

        if partition.spilled {
            // Write directly to spill file
            self.write_to_probe_spill(partition_idx, &key_values, row)?;
        } else {
            // Try to grow memory reservation
            if !self.reservation.try_grow(row_size) {
                // Need to spill this partition
                self.spill_probe_partition(partition_idx)?;
                self.write_to_probe_spill(partition_idx, &key_values, row)?;
            } else {
                partition.rows.push((key_values, row.to_vec()));
                partition.memory_bytes += row_size;
            }
        }

        self.probe_row_count += 1;
        Ok(())
    }

    /// Compute partition index from key values
    fn compute_partition(&self, key_values: &[SqlValue]) -> usize {
        use std::hash::Hasher;
        let mut hasher = ahash::AHasher::default();
        for v in key_values {
            hash_sql_value(v, &mut hasher);
        }
        (hasher.finish() as usize) & self.partition_mask
    }

    /// Spill the largest build partition to disk
    fn spill_largest_build_partition(&mut self) -> io::Result<()> {
        let largest_idx = self
            .build_partitions
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.spilled && !p.rows.is_empty())
            .max_by_key(|(_, p)| p.memory_bytes)
            .map(|(i, _)| i);

        if let Some(idx) = largest_idx {
            self.spill_build_partition(idx)?;
        }
        Ok(())
    }

    /// Spill a specific build partition
    fn spill_build_partition(&mut self, idx: usize) -> io::Result<()> {
        let partition = &mut self.build_partitions[idx];
        if partition.spilled {
            return Ok(());
        }

        let temp_dir = self.reservation.temp_directory().clone();
        let mut spill_file =
            SpillFile::with_suffix(&temp_dir, &format!("build_part_{}", idx))?;

        // Write existing rows to spill file
        let rows = std::mem::take(&mut partition.rows);
        for (key, row) in rows {
            write_keyed_row(&mut spill_file, &key, &row)?;
        }
        spill_file.flush()?;

        self.reservation.shrink(partition.memory_bytes);
        partition.memory_bytes = 0;
        partition.spilled = true;
        partition.spill_file = Some(spill_file);

        Ok(())
    }

    /// Write a row to build spill file
    fn write_to_build_spill(
        &mut self,
        idx: usize,
        key_values: &[SqlValue],
        row: &[SqlValue],
    ) -> io::Result<()> {
        let partition = &mut self.build_partitions[idx];
        let spill_file = partition
            .spill_file
            .as_mut()
            .expect("spill file should exist");

        write_keyed_row(spill_file, key_values, row)?;
        spill_file.flush()?;

        partition.spilled_row_count += 1;
        Ok(())
    }

    /// Spill the largest probe partition to disk
    fn spill_largest_probe_partition(&mut self) -> io::Result<()> {
        let largest_idx = self
            .probe_partitions
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.spilled && !p.rows.is_empty())
            .max_by_key(|(_, p)| p.memory_bytes)
            .map(|(i, _)| i);

        if let Some(idx) = largest_idx {
            self.spill_probe_partition(idx)?;
        }
        Ok(())
    }

    /// Spill a specific probe partition
    fn spill_probe_partition(&mut self, idx: usize) -> io::Result<()> {
        let partition = &mut self.probe_partitions[idx];
        if partition.spilled {
            return Ok(());
        }

        let temp_dir = self.reservation.temp_directory().clone();
        let mut spill_file =
            SpillFile::with_suffix(&temp_dir, &format!("probe_part_{}", idx))?;

        // Write existing rows to spill file
        let rows = std::mem::take(&mut partition.rows);
        for (key, row) in rows {
            write_keyed_row(&mut spill_file, &key, &row)?;
        }
        spill_file.flush()?;

        self.reservation.shrink(partition.memory_bytes);
        partition.memory_bytes = 0;
        partition.spilled = true;
        partition.spill_file = Some(spill_file);

        Ok(())
    }

    /// Write a row to probe spill file
    fn write_to_probe_spill(
        &mut self,
        idx: usize,
        key_values: &[SqlValue],
        row: &[SqlValue],
    ) -> io::Result<()> {
        let partition = &mut self.probe_partitions[idx];
        let spill_file = partition
            .spill_file
            .as_mut()
            .expect("spill file should exist");

        write_keyed_row(spill_file, key_values, row)?;
        spill_file.flush()?;

        partition.spilled_row_count += 1;
        Ok(())
    }

    /// Get number of spilled build partitions
    pub fn num_spilled_build_partitions(&self) -> usize {
        self.build_partitions.iter().filter(|p| p.spilled).count()
    }

    /// Get number of spilled probe partitions
    pub fn num_spilled_probe_partitions(&self) -> usize {
        self.probe_partitions.iter().filter(|p| p.spilled).count()
    }

    /// Execute the join and return results
    pub fn finish(mut self) -> io::Result<HashJoinResultIterator> {
        let mut results = Vec::new();

        // Process each partition
        for partition_idx in 0..self.build_partitions.len() {
            let partition_results =
                self.process_partition(partition_idx)?;
            results.extend(partition_results);
        }

        Ok(HashJoinResultIterator {
            results: results.into_iter(),
            _reservation: self.reservation,
        })
    }

    /// Process a single partition
    fn process_partition(
        &mut self,
        partition_idx: usize,
    ) -> io::Result<Vec<Vec<SqlValue>>> {
        // Load build side into hash table
        let build_rows = self.load_build_partition(partition_idx)?;

        // Build hash table
        let mut hash_table: AHashMap<Vec<SqlValue>, Vec<Vec<SqlValue>>> =
            AHashMap::new();
        for (key, row) in build_rows {
            hash_table.entry(key).or_default().push(row);
        }

        // Load probe side
        let probe_rows = self.load_probe_partition(partition_idx)?;

        // Perform join
        let mut results = Vec::new();

        match self.join_type {
            JoinType::Inner => {
                for (key, probe_row) in probe_rows {
                    if let Some(build_rows) = hash_table.get(&key) {
                        for build_row in build_rows {
                            let mut result = build_row.clone();
                            result.extend(probe_row.clone());
                            results.push(result);
                        }
                    }
                }
            }
            JoinType::LeftOuter => {
                // Track which build rows were matched
                let build_row_width = if let Some((_, row)) =
                    hash_table.values().next().and_then(|v| v.first().map(|r| ((), r)))
                {
                    row.len()
                } else {
                    0
                };

                for (key, probe_row) in probe_rows {
                    if let Some(build_rows) = hash_table.get(&key) {
                        for build_row in build_rows {
                            let mut result = build_row.clone();
                            result.extend(probe_row.clone());
                            results.push(result);
                        }
                    } else {
                        // No match - emit probe row with NULLs for build side
                        let mut result = vec![SqlValue::Null; build_row_width];
                        result.extend(probe_row);
                        results.push(result);
                    }
                }
            }
            JoinType::RightOuter => {
                // Track which build rows were matched
                let mut matched: std::collections::HashSet<usize> =
                    std::collections::HashSet::new();
                let probe_row_width = probe_rows.first().map(|(_, r)| r.len()).unwrap_or(0);

                // First pass: find matches
                let build_rows_vec: Vec<_> = hash_table
                    .iter()
                    .flat_map(|(k, rows)| rows.iter().map(move |r| (k.clone(), r.clone())))
                    .collect();

                for (key, probe_row) in &probe_rows {
                    if let Some(build_rows) = hash_table.get(key) {
                        for build_row in build_rows {
                            let mut result = build_row.clone();
                            result.extend(probe_row.clone());
                            results.push(result);
                        }
                        // Mark these build rows as matched
                        for (i, (k, _)) in build_rows_vec.iter().enumerate() {
                            if k == key {
                                matched.insert(i);
                            }
                        }
                    }
                }

                // Second pass: emit unmatched build rows with NULLs
                for (i, (_, build_row)) in build_rows_vec.iter().enumerate() {
                    if !matched.contains(&i) {
                        let mut result = build_row.clone();
                        result.extend(vec![SqlValue::Null; probe_row_width]);
                        results.push(result);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Load build partition (from memory or disk)
    fn load_build_partition(
        &mut self,
        idx: usize,
    ) -> io::Result<Vec<(Vec<SqlValue>, Vec<SqlValue>)>> {
        let partition = &mut self.build_partitions[idx];

        if !partition.spilled {
            // Return in-memory rows
            return Ok(std::mem::take(&mut partition.rows));
        }

        // Read from spill file
        let spill_file = partition.spill_file.as_mut().expect("spill file should exist");
        spill_file.prepare_for_read()?;

        let mut rows = Vec::new();
        loop {
            match read_keyed_row(spill_file) {
                Ok(Some((key, row))) => rows.push((key, row)),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(rows)
    }

    /// Load probe partition (from memory or disk)
    fn load_probe_partition(
        &mut self,
        idx: usize,
    ) -> io::Result<Vec<(Vec<SqlValue>, Vec<SqlValue>)>> {
        let partition = &mut self.probe_partitions[idx];

        if !partition.spilled {
            // Return in-memory rows
            return Ok(std::mem::take(&mut partition.rows));
        }

        // Read from spill file
        let spill_file = partition.spill_file.as_mut().expect("spill file should exist");
        spill_file.prepare_for_read()?;

        let mut rows = Vec::new();
        loop {
            match read_keyed_row(spill_file) {
                Ok(Some((key, row))) => rows.push((key, row)),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(rows)
    }
}

/// Write a keyed row to a spill file
fn write_keyed_row(spill_file: &mut SpillFile, key: &[SqlValue], row: &[SqlValue]) -> io::Result<()> {
    // Write key length
    let key_len = key.len() as u16;
    spill_file.write_all(&key_len.to_le_bytes())?;

    // Write key values
    let mut buf = Vec::new();
    for v in key {
        serialize_value(v, &mut buf)?;
    }
    spill_file.write_all(&buf)?;

    // Write row length
    let row_len = row.len() as u16;
    spill_file.write_all(&row_len.to_le_bytes())?;

    // Write row values
    buf.clear();
    for v in row {
        serialize_value(v, &mut buf)?;
    }
    spill_file.write_all(&buf)?;

    Ok(())
}

/// Read a keyed row from a spill file
fn read_keyed_row(spill_file: &mut SpillFile) -> io::Result<Option<(Vec<SqlValue>, Vec<SqlValue>)>> {
    // Read key length
    let mut len_buf = [0u8; 2];
    match spill_file.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let key_len = u16::from_le_bytes(len_buf) as usize;

    // Read key values
    let mut key = Vec::with_capacity(key_len);
    for _ in 0..key_len {
        key.push(deserialize_value_from_spill(spill_file)?);
    }

    // Read row length
    spill_file.read_exact(&mut len_buf)?;
    let row_len = u16::from_le_bytes(len_buf) as usize;

    // Read row values
    let mut row = Vec::with_capacity(row_len);
    for _ in 0..row_len {
        row.push(deserialize_value_from_spill(spill_file)?);
    }

    Ok(Some((key, row)))
}

/// Read a single value from spill file (wrapper around deserialize_value)
fn deserialize_value_from_spill(spill_file: &mut SpillFile) -> io::Result<SqlValue> {
    // We need to wrap SpillFile to implement Read
    struct SpillFileReader<'a>(&'a mut SpillFile);

    impl Read for SpillFileReader<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read(buf)
        }
    }

    let mut reader = SpillFileReader(spill_file);
    deserialize_value(&mut reader)
}

/// Hash a SqlValue for partitioning
fn hash_sql_value<H: std::hash::Hasher>(value: &SqlValue, hasher: &mut H) {
    use std::hash::Hash;
    match value {
        SqlValue::Null => 0u8.hash(hasher),
        SqlValue::Integer(i) => {
            1u8.hash(hasher);
            i.hash(hasher);
        }
        SqlValue::Smallint(i) => {
            2u8.hash(hasher);
            i.hash(hasher);
        }
        SqlValue::Bigint(i) => {
            3u8.hash(hasher);
            i.hash(hasher);
        }
        SqlValue::Unsigned(u) => {
            4u8.hash(hasher);
            u.hash(hasher);
        }
        SqlValue::Numeric(d) => {
            5u8.hash(hasher);
            d.to_string().hash(hasher);
        }
        SqlValue::Float(f) => {
            6u8.hash(hasher);
            f.to_bits().hash(hasher);
        }
        SqlValue::Real(f) => {
            7u8.hash(hasher);
            f.to_bits().hash(hasher);
        }
        SqlValue::Double(f) => {
            8u8.hash(hasher);
            f.to_bits().hash(hasher);
        }
        SqlValue::Character(s) | SqlValue::Varchar(s) => {
            9u8.hash(hasher);
            s.hash(hasher);
        }
        SqlValue::Boolean(b) => {
            10u8.hash(hasher);
            b.hash(hasher);
        }
        SqlValue::Date(d) => {
            11u8.hash(hasher);
            d.hash(hasher);
        }
        SqlValue::Time(t) => {
            12u8.hash(hasher);
            t.hash(hasher);
        }
        SqlValue::Timestamp(ts) => {
            13u8.hash(hasher);
            ts.hash(hasher);
        }
        SqlValue::Interval(iv) => {
            14u8.hash(hasher);
            iv.value.hash(hasher);
        }
        SqlValue::Vector(v) => {
            15u8.hash(hasher);
            for f in v {
                f.to_bits().hash(hasher);
            }
        }
    }
}

/// Estimate memory size of a row
fn estimate_row_size(row: &[SqlValue]) -> usize {
    let base_size = std::mem::size_of::<Vec<SqlValue>>() + row.len() * std::mem::size_of::<SqlValue>();
    let value_size: usize = row
        .iter()
        .map(|v| match v {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.len(),
            SqlValue::Vector(vec) => vec.len() * std::mem::size_of::<f32>(),
            _ => 0,
        })
        .sum();
    base_size + value_size
}

/// Iterator over hash join results
pub struct HashJoinResultIterator {
    results: std::vec::IntoIter<Vec<SqlValue>>,
    #[allow(dead_code)]
    _reservation: MemoryReservation,
}

impl Iterator for HashJoinResultIterator {
    type Item = io::Result<Vec<SqlValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.results.next().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_controller() -> Arc<MemoryController> {
        Arc::new(MemoryController::with_budget(1024 * 1024)) // 1MB
    }

    #[test]
    fn test_inner_join_basic() {
        let controller = make_test_controller();
        let config = ExternalHashJoinConfig {
            num_partitions: 4,
            max_rows_per_partition: 100,
        };

        let mut join = ExternalHashJoin::new(
            controller,
            config,
            vec![0], // Build key column
            vec![0], // Probe key column
            JoinType::Inner,
        );

        // Build side: (id, name)
        join.add_build_row(&[SqlValue::Integer(1), SqlValue::Varchar("Alice".into())])
            .unwrap();
        join.add_build_row(&[SqlValue::Integer(2), SqlValue::Varchar("Bob".into())])
            .unwrap();
        join.add_build_row(&[SqlValue::Integer(3), SqlValue::Varchar("Charlie".into())])
            .unwrap();

        // Probe side: (id, city)
        join.add_probe_row(&[SqlValue::Integer(1), SqlValue::Varchar("NYC".into())])
            .unwrap();
        join.add_probe_row(&[SqlValue::Integer(2), SqlValue::Varchar("LA".into())])
            .unwrap();
        join.add_probe_row(&[SqlValue::Integer(4), SqlValue::Varchar("Chicago".into())])
            .unwrap(); // No match

        let results: Vec<_> = join.finish().unwrap().map(|r| r.unwrap()).collect();

        // Should have 2 results (id=1 and id=2 match)
        assert_eq!(results.len(), 2);

        // Verify Alice, NYC match
        let alice_match = results
            .iter()
            .find(|r| r[1] == SqlValue::Varchar("Alice".into()));
        assert!(alice_match.is_some());
        assert_eq!(alice_match.unwrap()[3], SqlValue::Varchar("NYC".into()));

        // Verify Bob, LA match
        let bob_match = results
            .iter()
            .find(|r| r[1] == SqlValue::Varchar("Bob".into()));
        assert!(bob_match.is_some());
        assert_eq!(bob_match.unwrap()[3], SqlValue::Varchar("LA".into()));
    }

    #[test]
    fn test_left_outer_join() {
        let controller = make_test_controller();
        let config = ExternalHashJoinConfig {
            num_partitions: 4,
            max_rows_per_partition: 100,
        };

        let mut join = ExternalHashJoin::new(
            controller,
            config,
            vec![0],
            vec![0],
            JoinType::LeftOuter,
        );

        // Build side
        join.add_build_row(&[SqlValue::Integer(1), SqlValue::Varchar("A".into())])
            .unwrap();

        // Probe side (left table in left outer)
        join.add_probe_row(&[SqlValue::Integer(1), SqlValue::Varchar("X".into())])
            .unwrap();
        join.add_probe_row(&[SqlValue::Integer(2), SqlValue::Varchar("Y".into())])
            .unwrap(); // No match

        let results: Vec<_> = join.finish().unwrap().map(|r| r.unwrap()).collect();

        // Should have 2 results
        assert_eq!(results.len(), 2);

        // One with match, one with NULLs
        let matched = results.iter().filter(|r| r[0] != SqlValue::Null).count();
        let unmatched = results.iter().filter(|r| r[0] == SqlValue::Null).count();
        assert_eq!(matched, 1);
        assert_eq!(unmatched, 1);
    }

    #[test]
    fn test_multi_key_join() {
        let controller = make_test_controller();
        let config = ExternalHashJoinConfig {
            num_partitions: 4,
            max_rows_per_partition: 100,
        };

        let mut join = ExternalHashJoin::new(
            controller,
            config,
            vec![0, 1], // Two key columns
            vec![0, 1],
            JoinType::Inner,
        );

        // Build: (a, b, val)
        join.add_build_row(&[
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Varchar("X".into()),
        ])
        .unwrap();
        join.add_build_row(&[
            SqlValue::Integer(1),
            SqlValue::Integer(20),
            SqlValue::Varchar("Y".into()),
        ])
        .unwrap();

        // Probe: (a, b, other)
        join.add_probe_row(&[
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Varchar("P".into()),
        ])
        .unwrap();
        join.add_probe_row(&[
            SqlValue::Integer(1),
            SqlValue::Integer(30),
            SqlValue::Varchar("Q".into()),
        ])
        .unwrap(); // No match

        let results: Vec<_> = join.finish().unwrap().map(|r| r.unwrap()).collect();

        // Only (1, 10) matches
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][2], SqlValue::Varchar("X".into()));
        assert_eq!(results[0][5], SqlValue::Varchar("P".into()));
    }

    #[test]
    fn test_null_handling() {
        let controller = make_test_controller();
        let config = ExternalHashJoinConfig {
            num_partitions: 4,
            max_rows_per_partition: 100,
        };

        let mut join = ExternalHashJoin::new(
            controller,
            config,
            vec![0],
            vec![0],
            JoinType::Inner,
        );

        // Build with NULL key
        join.add_build_row(&[SqlValue::Null, SqlValue::Varchar("A".into())])
            .unwrap();
        join.add_build_row(&[SqlValue::Integer(1), SqlValue::Varchar("B".into())])
            .unwrap();

        // Probe with NULL key
        join.add_probe_row(&[SqlValue::Null, SqlValue::Varchar("X".into())])
            .unwrap();
        join.add_probe_row(&[SqlValue::Integer(1), SqlValue::Varchar("Y".into())])
            .unwrap();

        let results: Vec<_> = join.finish().unwrap().map(|r| r.unwrap()).collect();

        // NULLs should not match - only id=1 should match
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][1], SqlValue::Varchar("B".into()));
    }

    #[test]
    fn test_empty_inputs() {
        let controller = make_test_controller();
        let config = ExternalHashJoinConfig::default();

        let join = ExternalHashJoin::new(
            controller,
            config,
            vec![0],
            vec![0],
            JoinType::Inner,
        );

        let results: Vec<_> = join.finish().unwrap().map(|r| r.unwrap()).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_many_partitions() {
        let controller = make_test_controller();
        let config = ExternalHashJoinConfig {
            num_partitions: 16,
            max_rows_per_partition: 10,
        };

        let mut join = ExternalHashJoin::new(
            controller,
            config,
            vec![0],
            vec![0],
            JoinType::Inner,
        );

        // Add many rows to test partitioning
        for i in 0..100 {
            join.add_build_row(&[SqlValue::Integer(i), SqlValue::Integer(i * 10)])
                .unwrap();
            join.add_probe_row(&[SqlValue::Integer(i), SqlValue::Integer(i * 100)])
                .unwrap();
        }

        let results: Vec<_> = join.finish().unwrap().map(|r| r.unwrap()).collect();

        // All 100 rows should match
        assert_eq!(results.len(), 100);
    }

    #[test]
    fn test_duplicate_keys() {
        let controller = make_test_controller();
        let config = ExternalHashJoinConfig {
            num_partitions: 4,
            max_rows_per_partition: 100,
        };

        let mut join = ExternalHashJoin::new(
            controller,
            config,
            vec![0],
            vec![0],
            JoinType::Inner,
        );

        // Build with duplicate keys
        join.add_build_row(&[SqlValue::Integer(1), SqlValue::Varchar("A".into())])
            .unwrap();
        join.add_build_row(&[SqlValue::Integer(1), SqlValue::Varchar("B".into())])
            .unwrap();

        // Probe with duplicate keys
        join.add_probe_row(&[SqlValue::Integer(1), SqlValue::Varchar("X".into())])
            .unwrap();
        join.add_probe_row(&[SqlValue::Integer(1), SqlValue::Varchar("Y".into())])
            .unwrap();

        let results: Vec<_> = join.finish().unwrap().map(|r| r.unwrap()).collect();

        // Should have 2x2 = 4 results (cartesian product of matching rows)
        assert_eq!(results.len(), 4);
    }
}
