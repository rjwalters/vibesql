//! Memory tracking utilities for TPC-DS benchmarks
//!
//! Provides platform-specific memory usage monitoring to help diagnose
//! and prevent OOM issues during long benchmark runs.

/// Memory usage statistics
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// Resident set size in bytes (physical memory used)
    pub rss_bytes: usize,
    /// Virtual memory size in bytes
    pub vsize_bytes: usize,
}

impl MemoryStats {
    /// Format RSS as human-readable string
    pub fn rss_mb(&self) -> f64 {
        self.rss_bytes as f64 / (1024.0 * 1024.0)
    }

    /// Format virtual memory as human-readable string
    pub fn vsize_mb(&self) -> f64 {
        self.vsize_bytes as f64 / (1024.0 * 1024.0)
    }
}

impl std::fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RSS: {:.1} MB, VSize: {:.1} MB", self.rss_mb(), self.vsize_mb())
    }
}

/// Get current process memory usage
///
/// Returns None if memory stats couldn't be retrieved (unsupported platform, etc.)
#[cfg(target_os = "macos")]
pub fn get_memory_usage() -> Option<MemoryStats> {
    use std::mem::MaybeUninit;

    // Use mach_task_basic_info to get memory stats
    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(
            target_task: u32,
            flavor: i32,
            task_info_out: *mut libc::c_void,
            task_info_out_cnt: *mut u32,
        ) -> i32;
    }

    const MACH_TASK_BASIC_INFO: i32 = 20;

    #[repr(C)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: [u64; 2],
        system_time: [u64; 2],
        policy: i32,
        suspend_count: i32,
    }

    let mut info = MaybeUninit::<MachTaskBasicInfo>::uninit();
    let mut count = (std::mem::size_of::<MachTaskBasicInfo>() / std::mem::size_of::<u32>()) as u32;

    unsafe {
        let result = task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            info.as_mut_ptr() as *mut libc::c_void,
            &mut count,
        );

        if result == 0 {
            let info = info.assume_init();
            Some(MemoryStats {
                rss_bytes: info.resident_size as usize,
                vsize_bytes: info.virtual_size as usize,
            })
        } else {
            None
        }
    }
}

/// Get current process memory usage (Linux implementation)
#[cfg(target_os = "linux")]
pub fn get_memory_usage() -> Option<MemoryStats> {
    use std::fs;

    // Read /proc/self/statm for memory info
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let parts: Vec<&str> = statm.split_whitespace().collect();

    if parts.len() >= 2 {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let vsize_pages: usize = parts[0].parse().ok()?;
        let rss_pages: usize = parts[1].parse().ok()?;

        Some(MemoryStats { rss_bytes: rss_pages * page_size, vsize_bytes: vsize_pages * page_size })
    } else {
        None
    }
}

/// Fallback for unsupported platforms
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn get_memory_usage() -> Option<MemoryStats> {
    None
}

/// Memory tracking context for batch execution
pub struct MemoryTracker {
    initial_rss: usize,
    peak_rss: usize,
    warning_threshold_mb: f64,
}

impl MemoryTracker {
    /// Create a new memory tracker
    pub fn new(warning_threshold_mb: f64) -> Self {
        let initial_rss = get_memory_usage().map(|s| s.rss_bytes).unwrap_or(0);
        Self { initial_rss, peak_rss: initial_rss, warning_threshold_mb }
    }

    /// Record current memory usage and return stats
    pub fn record(&mut self) -> Option<MemoryStats> {
        let stats = get_memory_usage()?;
        if stats.rss_bytes > self.peak_rss {
            self.peak_rss = stats.rss_bytes;
        }

        // Warn if memory usage exceeds threshold
        if stats.rss_mb() > self.warning_threshold_mb {
            eprintln!(
                "[MEMORY WARNING] RSS {:.1} MB exceeds threshold {:.1} MB",
                stats.rss_mb(),
                self.warning_threshold_mb
            );
        }

        Some(stats)
    }

    /// Get the initial RSS when tracker was created
    pub fn initial_rss_mb(&self) -> f64 {
        self.initial_rss as f64 / (1024.0 * 1024.0)
    }

    /// Get the peak RSS observed
    pub fn peak_rss_mb(&self) -> f64 {
        self.peak_rss as f64 / (1024.0 * 1024.0)
    }

    /// Get memory growth since tracker creation
    pub fn growth_mb(&self) -> f64 {
        (self.peak_rss - self.initial_rss) as f64 / (1024.0 * 1024.0)
    }

    /// Print summary of memory usage
    pub fn print_summary(&self) {
        eprintln!("\n--- Memory Summary ---");
        eprintln!("Initial RSS: {:.1} MB", self.initial_rss_mb());
        eprintln!("Peak RSS:    {:.1} MB", self.peak_rss_mb());
        eprintln!("Growth:      {:.1} MB", self.growth_mb());
    }
}

/// Hint to the allocator that now is a good time to return memory to the OS
///
/// When jemalloc feature is enabled, this uses jemalloc's epoch advancement
/// and arena purging which is much more effective at releasing memory.
///
/// Without jemalloc, falls back to platform-specific hints which may have
/// limited effectiveness.
#[cfg(feature = "jemalloc")]
pub fn hint_memory_release() {
    use tikv_jemalloc_ctl::{epoch, raw};

    // Advance the epoch to update jemalloc's statistics
    // This is necessary before purging to ensure we have accurate data
    if let Err(e) = epoch::advance() {
        eprintln!("[jemalloc] Failed to advance epoch: {}", e);
    }

    // Purge all arenas to release memory back to the OS
    // "arenas.purge" is a write-only command that forces immediate page purging
    let purge_result = unsafe { raw::write(b"arena.0.purge\0", ()) };
    if let Err(e) = purge_result {
        // Arena 0 might not exist; try the global purge
        let _ = unsafe { raw::write(b"arenas.purge\0", ()) };
        let _ = e; // Suppress warning
    }
}

/// Fallback hint_memory_release for when jemalloc is not enabled
#[cfg(not(feature = "jemalloc"))]
pub fn hint_memory_release() {
    // On macOS, we can use malloc_zone_pressure_relief
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn malloc_zone_pressure_relief(zone: *mut libc::c_void, goal: usize) -> usize;
        }
        unsafe {
            // Pass NULL for zone to affect all zones, 0 for goal to release as much as possible
            malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
        }
    }

    // On Linux with glibc, we can use malloc_trim
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        unsafe {
            malloc_trim(0);
        }
    }
}

/// Returns true if jemalloc is being used as the global allocator
pub fn is_jemalloc_enabled() -> bool {
    cfg!(feature = "jemalloc")
}

/// Get jemalloc memory statistics (only available with jemalloc feature)
#[cfg(feature = "jemalloc")]
pub fn get_jemalloc_stats() -> Option<JemallocStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    // Advance epoch to get fresh stats
    epoch::advance().ok()?;

    Some(JemallocStats {
        allocated: stats::allocated::read().ok()?,
        active: stats::active::read().ok()?,
        resident: stats::resident::read().ok()?,
        retained: stats::retained::read().ok()?,
    })
}

#[cfg(not(feature = "jemalloc"))]
pub fn get_jemalloc_stats() -> Option<JemallocStats> {
    None
}

/// jemalloc memory statistics
#[derive(Debug, Clone, Copy)]
pub struct JemallocStats {
    /// Total bytes allocated by the application
    pub allocated: usize,
    /// Total bytes in active pages (may include fragmentation)
    pub active: usize,
    /// Total bytes mapped by jemalloc (RSS contribution)
    pub resident: usize,
    /// Total bytes retained in virtual memory (not returned to OS)
    pub retained: usize,
}

impl std::fmt::Display for JemallocStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "allocated: {:.1} MB, active: {:.1} MB, resident: {:.1} MB, retained: {:.1} MB",
            self.allocated as f64 / (1024.0 * 1024.0),
            self.active as f64 / (1024.0 * 1024.0),
            self.resident as f64 / (1024.0 * 1024.0),
            self.retained as f64 / (1024.0 * 1024.0)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_memory_usage() {
        if let Some(stats) = get_memory_usage() {
            assert!(stats.rss_bytes > 0, "RSS should be > 0");
            assert!(stats.vsize_bytes >= stats.rss_bytes, "VSize should be >= RSS");
            println!("Current memory: {}", stats);
        }
    }

    #[test]
    fn test_memory_tracker() {
        let mut tracker = MemoryTracker::new(1000.0);

        // Allocate some memory
        let _data: Vec<u8> = vec![0; 10 * 1024 * 1024]; // 10 MB

        if let Some(stats) = tracker.record() {
            println!("After allocation: {}", stats);
        }

        tracker.print_summary();
    }
}
