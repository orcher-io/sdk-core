//! What a process costs: its allocations (through a counting allocator),
//! CPU time and memory.
//!
//! The worker process installs [`Counting`] as its global allocator, so
//! every allocation the host, the drivers and the kernel make is counted.
//! Wall-clock time is recorded too, but only so rates can be computed; the
//! suite compares resources, not speed on a laptop.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static FREED_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

/// The system allocator, counting what passes through it.
pub struct Counting;

// SAFETY: every call is forwarded to `System` unchanged; the counters are
// side effects only.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        FREED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = System.realloc(ptr, layout, new_size);
        if !new.is_null() {
            FREED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            allocated(new_size);
        }
        new
    }
}

fn allocated(size: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    let total = ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed) + size as u64;
    let live = total.saturating_sub(FREED_BYTES.load(Ordering::Relaxed));
    PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
}

/// A point-in-time reading of a process's resource use.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ProcessStats {
    /// Allocations made since the process started.
    pub allocations: u64,
    /// Bytes allocated since the process started.
    pub allocated_bytes: u64,
    /// Bytes allocated and not freed.
    pub live_heap_bytes: u64,
    /// The most bytes ever allocated and not freed at once.
    pub peak_live_heap_bytes: u64,
    /// CPU time spent in user mode, in microseconds.
    pub cpu_user_us: u64,
    /// CPU time spent in the kernel on the process's behalf, in microseconds.
    pub cpu_system_us: u64,
    /// The largest resident set the process has had, in bytes.
    pub max_rss_bytes: u64,
    /// Tokio tasks alive in the process's runtime.
    pub tokio_alive_tasks: u64,
    /// Tokio worker threads.
    pub tokio_workers: u64,
}

impl ProcessStats {
    /// Reads this process's resource use now.
    pub fn now() -> Self {
        let freed = FREED_BYTES.load(Ordering::Relaxed);
        let allocated = ALLOCATED_BYTES.load(Ordering::Relaxed);
        let (cpu_user_us, cpu_system_us, max_rss_bytes) = rusage();
        let (tokio_alive_tasks, tokio_workers) = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let metrics = handle.metrics();
                (
                    metrics.num_alive_tasks() as u64,
                    metrics.num_workers() as u64,
                )
            }
            Err(_) => (0, 0),
        };
        Self {
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            allocated_bytes: allocated,
            live_heap_bytes: allocated.saturating_sub(freed),
            peak_live_heap_bytes: PEAK_LIVE_BYTES.load(Ordering::Relaxed),
            cpu_user_us,
            cpu_system_us,
            max_rss_bytes,
            tokio_alive_tasks,
            tokio_workers,
        }
    }

    /// CPU time, user and system, in microseconds.
    pub fn cpu_us(&self) -> u64 {
        self.cpu_user_us + self.cpu_system_us
    }
}

fn rusage() -> (u64, u64, u64) {
    // SAFETY: getrusage fills the struct it is given and reads nothing else.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return (0, 0, 0);
        }
        usage
    };
    let micros = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    // ru_maxrss is in bytes on macOS and in kilobytes on Linux.
    let rss = usage.ru_maxrss as u64;
    let max_rss_bytes = if cfg!(target_os = "macos") {
        rss
    } else {
        rss * 1024
    };
    (
        micros(usage.ru_utime),
        micros(usage.ru_stime),
        max_rss_bytes,
    )
}
