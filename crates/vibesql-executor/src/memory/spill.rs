//! Spill file management for external operators
//!
//! This module provides temporary file handling for operators that need to
//! spill data to disk when memory is exhausted.
//!
//! # Design
//!
//! - Files are automatically cleaned up when the `SpillFile` handle is dropped
//! - Files are named with a unique prefix to avoid collisions
//! - Files are created lazily (only when first written to)
//! - Supports both sequential writes and random reads for merge operations

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// Global counter for unique file naming
static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A handle to a temporary spill file
///
/// The file is automatically deleted when this handle is dropped.
/// Files are created lazily - no disk I/O occurs until the first write.
///
/// # Example
///
/// ```rust,ignore
/// use vibesql_executor::memory::SpillFile;
/// use std::path::Path;
///
/// let mut file = SpillFile::new(Path::new("/tmp/vibesql"))?;
///
/// // Write sorted run
/// file.write_all(&serialized_data)?;
/// file.flush()?;
///
/// // Read back
/// file.seek(SeekFrom::Start(0))?;
/// let data = file.read_to_vec()?;
///
/// // File is automatically deleted when `file` is dropped
/// ```
pub struct SpillFile {
    /// Path to the spill file
    path: PathBuf,

    /// Buffered writer (created lazily)
    writer: Option<BufWriter<File>>,

    /// Buffered reader (created lazily)
    reader: Option<BufReader<File>>,

    /// Number of bytes written
    bytes_written: usize,

    /// Whether the file has been created
    created: bool,
}

impl SpillFile {
    /// Create a new spill file in the specified directory
    ///
    /// The file is not actually created until the first write.
    pub fn new(temp_dir: &Path) -> io::Result<Self> {
        // Ensure temp directory exists
        fs::create_dir_all(temp_dir)?;

        // Generate unique filename
        let id = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let filename = format!("vibesql_spill_{}_{}.tmp", pid, id);
        let path = temp_dir.join(filename);

        Ok(Self { path, writer: None, reader: None, bytes_written: 0, created: false })
    }

    /// Create a spill file with a specific name suffix
    ///
    /// Useful for debugging - creates files like "vibesql_spill_12345_0_sort_run.tmp"
    pub fn with_suffix(temp_dir: &Path, suffix: &str) -> io::Result<Self> {
        fs::create_dir_all(temp_dir)?;

        let id = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let filename = format!("vibesql_spill_{}_{}_{}.tmp", pid, id, suffix);
        let path = temp_dir.join(filename);

        Ok(Self { path, writer: None, reader: None, bytes_written: 0, created: false })
    }

    /// Get the path to the spill file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the number of bytes written to this file
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Check if the file has been created on disk
    pub fn is_created(&self) -> bool {
        self.created
    }

    /// Ensure the file is created and return a writer
    fn ensure_writer(&mut self) -> io::Result<&mut BufWriter<File>> {
        if self.writer.is_none() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.path)?;
            self.writer = Some(BufWriter::with_capacity(64 * 1024, file)); // 64KB buffer
            self.created = true;
        }
        Ok(self.writer.as_mut().unwrap())
    }

    /// Write data to the spill file
    pub fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let writer = self.ensure_writer()?;
        writer.write_all(data)?;
        self.bytes_written += data.len();
        Ok(())
    }

    /// Flush buffered data to disk
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Prepare the file for reading
    ///
    /// This flushes any pending writes and switches to read mode.
    pub fn prepare_for_read(&mut self) -> io::Result<()> {
        // Flush and drop writer
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            // Writer is dropped, file handle closed
        }

        // Open for reading
        if self.created {
            let file = File::open(&self.path)?;
            self.reader = Some(BufReader::with_capacity(64 * 1024, file)); // 64KB buffer
        }

        Ok(())
    }

    /// Seek to a position in the file
    pub fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if let Some(reader) = self.reader.as_mut() {
            reader.seek(pos)
        } else if let Some(writer) = self.writer.as_mut() {
            writer.seek(pos)
        } else {
            Ok(0)
        }
    }

    /// Read data from the spill file
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.reader.is_none() {
            self.prepare_for_read()?;
        }
        if let Some(reader) = self.reader.as_mut() {
            reader.read(buf)
        } else {
            Ok(0)
        }
    }

    /// Read exact number of bytes
    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if self.reader.is_none() {
            self.prepare_for_read()?;
        }
        if let Some(reader) = self.reader.as_mut() {
            reader.read_exact(buf)
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "spill file not created"))
        }
    }

    /// Read entire file into a Vec
    pub fn read_to_vec(&mut self) -> io::Result<Vec<u8>> {
        self.seek(SeekFrom::Start(0))?;
        let mut data = Vec::with_capacity(self.bytes_written);
        if let Some(reader) = self.reader.as_mut() {
            reader.read_to_end(&mut data)?;
        }
        Ok(data)
    }

    /// Delete the spill file (called automatically on drop)
    fn delete(&mut self) {
        // Close all handles first
        self.writer = None;
        self.reader = None;

        // Delete the file
        if self.created {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        self.delete();
    }
}

/// A collection of spill files for managing multiple sorted runs
///
/// Used by external sort to manage multiple sorted runs that need
/// to be merged together.
pub struct SpillFileSet {
    /// Directory for spill files
    temp_dir: PathBuf,

    /// Active spill files
    files: Vec<SpillFile>,

    /// Total bytes spilled across all files
    total_bytes: usize,
}

impl SpillFileSet {
    /// Create a new spill file set
    pub fn new(temp_dir: PathBuf) -> Self {
        Self { temp_dir, files: Vec::new(), total_bytes: 0 }
    }

    /// Create a new spill file in this set
    pub fn create_file(&mut self) -> io::Result<&mut SpillFile> {
        let file = SpillFile::new(&self.temp_dir)?;
        self.files.push(file);
        Ok(self.files.last_mut().unwrap())
    }

    /// Create a new spill file with a suffix
    pub fn create_file_with_suffix(&mut self, suffix: &str) -> io::Result<&mut SpillFile> {
        let file = SpillFile::with_suffix(&self.temp_dir, suffix)?;
        self.files.push(file);
        Ok(self.files.last_mut().unwrap())
    }

    /// Get the number of spill files
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Get total bytes spilled
    pub fn total_bytes(&self) -> usize {
        self.files.iter().map(|f| f.bytes_written()).sum()
    }

    /// Get all files for reading
    pub fn files(&self) -> &[SpillFile] {
        &self.files
    }

    /// Get mutable access to all files
    pub fn files_mut(&mut self) -> &mut [SpillFile] {
        &mut self.files
    }

    /// Take ownership of all files (consumes the set)
    pub fn into_files(self) -> Vec<SpillFile> {
        self.files
    }

    /// Prepare all files for reading
    pub fn prepare_all_for_read(&mut self) -> io::Result<()> {
        for file in &mut self.files {
            file.prepare_for_read()?;
        }
        Ok(())
    }

    /// Clear all files (deletes them from disk)
    pub fn clear(&mut self) {
        self.files.clear();
        self.total_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_spill_file_create_and_write() {
        let temp = TempDir::new().unwrap();
        let mut file = SpillFile::new(temp.path()).unwrap();

        // File not created until first write
        assert!(!file.is_created());
        assert_eq!(file.bytes_written(), 0);

        // Write data
        file.write_all(b"hello world").unwrap();
        file.flush().unwrap();

        assert!(file.is_created());
        assert_eq!(file.bytes_written(), 11);
        assert!(file.path().exists());
    }

    #[test]
    fn test_spill_file_read_back() {
        let temp = TempDir::new().unwrap();
        let mut file = SpillFile::new(temp.path()).unwrap();

        let test_data = b"test data 12345";
        file.write_all(test_data).unwrap();
        file.flush().unwrap();

        file.prepare_for_read().unwrap();
        let read_data = file.read_to_vec().unwrap();

        assert_eq!(read_data, test_data);
    }

    #[test]
    fn test_spill_file_auto_delete() {
        let temp = TempDir::new().unwrap();
        let path;
        {
            let mut file = SpillFile::new(temp.path()).unwrap();
            file.write_all(b"data").unwrap();
            file.flush().unwrap();
            path = file.path().to_path_buf();
            assert!(path.exists());
        }
        // File should be deleted after drop
        assert!(!path.exists());
    }

    #[test]
    fn test_spill_file_with_suffix() {
        let temp = TempDir::new().unwrap();
        let file = SpillFile::with_suffix(temp.path(), "sort_run").unwrap();

        assert!(file.path().to_string_lossy().contains("sort_run"));
    }

    #[test]
    fn test_spill_file_set() {
        let temp = TempDir::new().unwrap();
        let mut set = SpillFileSet::new(temp.path().to_path_buf());

        assert!(set.is_empty());

        // Create multiple files
        {
            let file = set.create_file().unwrap();
            file.write_all(b"run1").unwrap();
        }
        {
            let file = set.create_file().unwrap();
            file.write_all(b"run22").unwrap();
        }

        assert_eq!(set.len(), 2);
        assert_eq!(set.total_bytes(), 9); // 4 + 5

        // Clear deletes all files
        set.clear();
        assert!(set.is_empty());
    }

    #[test]
    fn test_spill_file_sequential_read() {
        let temp = TempDir::new().unwrap();
        let mut file = SpillFile::new(temp.path()).unwrap();

        // Write multiple chunks
        file.write_all(b"chunk1").unwrap();
        file.write_all(b"chunk2").unwrap();
        file.write_all(b"chunk3").unwrap();
        file.flush().unwrap();

        // Read back
        file.prepare_for_read().unwrap();

        let mut buf = [0u8; 6];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"chunk1");

        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"chunk2");

        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"chunk3");
    }

    #[test]
    fn test_spill_file_seek() {
        let temp = TempDir::new().unwrap();
        let mut file = SpillFile::new(temp.path()).unwrap();

        file.write_all(b"0123456789").unwrap();
        file.flush().unwrap();
        file.prepare_for_read().unwrap();

        // Seek to position 5
        file.seek(SeekFrom::Start(5)).unwrap();

        let mut buf = [0u8; 5];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"56789");
    }
}
