// SPDX-License-Identifier: MIT OR Apache-2.0
//! A WebAssembly-safe read-write lock that papers over platform-specific locking constraints.
//!
//! # The Core Problem
//!
//! Like regular mutexes, **WebAssembly's main thread cannot use blocking locks**. However,
//! read-write locks face an additional challenge: they need to efficiently handle multiple
//! concurrent readers while ensuring exclusive access for writers, all while avoiding the
//! "cannot block on the main thread" panic.
//!
//! # The Solution
//!
//! This crate provides a read-write lock that automatically adapts its locking strategy
//! based on the runtime environment, just like our Mutex, but with the added benefit of
//! allowing multiple concurrent readers:
//!
//! - **Native (any thread)**: Uses efficient thread parking for both readers and writers
//! - **WASM worker threads**: Uses `Atomics.wait` when available for proper blocking
//! - **WASM main thread**: Falls back to spinning (non-blocking busy-wait)
//!
//! This means you get the performance benefits of read-write locks (multiple concurrent readers)
//! while maintaining compatibility across all platforms without worrying about thread restrictions.
//!
//! # Features
//!
//! - **Multiple concurrent readers**: Better performance for read-heavy workloads
//! - **Exclusive writer access**: Ensures data consistency during writes
//! - **Transparent adaptation**: Automatically detects and uses the best locking mechanism
//! - **Main thread safe**: Won't panic on WASM main thread (uses spinning instead)
//! - **Worker thread optimized**: Uses proper blocking when available for efficiency
//! - **Multiple strategies**: Try-lock, spin-lock, blocking lock, and async lock for both read and write
//!
//! # Examples
//!
//! ## Basic Usage
//!
//! ```
//! use wasm_safe_mutex::rwlock::RwLock;
//!
//! let rwlock = RwLock::new(42);
//!
//! // Multiple readers can access simultaneously
//! let guard1 = rwlock.lock_sync_read();
//! let guard2 = rwlock.lock_sync_read();
//! assert_eq!(*guard1, 42);
//! assert_eq!(*guard2, 42);
//! drop(guard1);
//! drop(guard2);
//!
//! // Writer gets exclusive access
//! let mut guard = rwlock.lock_sync_write();
//! *guard = 100;
//! drop(guard);
//!
//! // Read the updated value
//! let guard = rwlock.lock_sync_read();
//! assert_eq!(*guard, 100);
//! ```
//!
//! ## Try Lock
//!
//! ```
//! use wasm_safe_mutex::rwlock::RwLock;
//! use wasm_safe_mutex::NotAvailable;
//!
//! let rwlock = RwLock::new("data");
//!
//! // First read lock succeeds
//! let guard1 = rwlock.try_lock_read().unwrap();
//!
//! // Second read lock also succeeds (multiple readers allowed)
//! let guard2 = rwlock.try_lock_read().unwrap();
//! assert_eq!(*guard1, "data");
//! assert_eq!(*guard2, "data");
//!
//! // Write lock fails while readers are active
//! let result = rwlock.try_lock_write();
//! assert!(matches!(result, Err(NotAvailable)));
//! ```
//!
//! ## Async Usage
//!
//! ```
//! # test_executors::spin_on(async {
//! use wasm_safe_mutex::rwlock::RwLock;
//!
//! let rwlock = RwLock::new(vec![1, 2, 3]);
//!
//! // Async read doesn't block the executor
//! let guard = rwlock.lock_async_read().await;
//! let sum: i32 = guard.iter().sum();
//! drop(guard);
//!
//! // Async write for modifications
//! let mut guard = rwlock.lock_async_read_write().await;
//! guard.push(4);
//! drop(guard);
//!
//! // Using the convenience method
//! let len = rwlock.with_async(|data| data.len()).await;
//! assert_eq!(len, 4);
//! # });
//! ```
//!
//! ## Thread-Safe Sharing with Multiple Readers
//!
//! ```
//! use wasm_safe_mutex::rwlock::RwLock;
//! use std::sync::Arc;
//! # use std::thread;
//!
//! let rwlock = Arc::new(RwLock::new(vec![1, 2, 3, 4, 5]));
//! let mut handles = vec![];
//!
//! // Spawn multiple reader threads
//! for i in 0..3 {
//!     let rwlock = Arc::clone(&rwlock);
//!     handles.push(thread::spawn(move || {
//!         let guard = rwlock.lock_sync_read();
//!         let sum: i32 = guard.iter().sum();
//!         println!("Reader {} calculated sum: {}", i, sum);
//!         sum
//!     }));
//! }
//!
//! // All readers can work concurrently
//! for handle in handles {
//!     assert_eq!(handle.join().unwrap(), 15);
//! }
//! ```

use std::cell::UnsafeCell;
use std::fmt::{Display, Formatter, Pointer};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU8};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::thread;
use crate::{NotAvailable};
use crate::spinlock::Spinlock;

const UNLOCKED: u8 = 0;
const LOCKED_WRITE: u8 = 0b10000000;


/// A reader-writer lock that works across native and WebAssembly targets.
///
/// This lock allows multiple readers to access the data simultaneously, but only
/// one writer at a time. Writers get exclusive access - no readers or other writers
/// can access the data while a write lock is held.
///
/// Like [`Mutex`](crate::Mutex), this lock provides multiple locking strategies:
/// - **`try_lock_read/write`**: Non-blocking attempt to acquire the lock
/// - **`lock_spin_read/write`**: Spin-wait until the lock is acquired
/// - **`lock_block_read/write`**: Blocks on native/WASM workers, spins on WASM main thread
/// - **`lock_sync_read/write`**: Automatically chooses the right strategy for your platform
/// - **`lock_async_read/read_write`**: Always non-blocking, works everywhere including WASM main thread
///
/// ## Platform Behavior
///
/// The RwLock transparently handles platform differences:
/// - **Native (main or worker thread)**: Full blocking with thread parking
/// - **WASM worker threads**: Blocks using `Atomics.wait`
/// - **WASM main thread**: Spins to avoid "cannot block on main thread" panic
///
/// ## When to Use RwLock vs Mutex
///
/// Use `RwLock` when:
/// - Your workload is read-heavy (many reads, few writes)
/// - Multiple threads need to read data simultaneously
/// - You want to maximize concurrent access for readers
///
/// Use [`Mutex`](crate::Mutex) when:
/// - Reads and writes are equally common
/// - Operations are very quick
/// - You want simpler semantics
///
/// # Examples
///
/// ## Basic Reader-Writer Pattern
///
/// ```
/// use wasm_safe_mutex::rwlock::RwLock;
///
/// let rwlock = RwLock::new(0i32);
///
/// // Multiple readers can access simultaneously
/// {
///     let reader1 = rwlock.lock_sync_read();
///     let reader2 = rwlock.lock_sync_read();
///     assert_eq!(*reader1, *reader2);
/// } // Both read locks released here
///
/// // Writer gets exclusive access
/// {
///     let mut writer = rwlock.lock_sync_write();
///     *writer += 1;
/// } // Write lock released here
///
/// // Read the updated value
/// let value = rwlock.with_sync(|data| *data);
/// assert_eq!(value, 1);
/// ```
///
/// ## Concurrent Readers Example
///
/// ```
/// use wasm_safe_mutex::rwlock::RwLock;
/// use std::sync::Arc;
/// # use std::thread;
///
/// let shared_data = Arc::new(RwLock::new(vec![1, 2, 3, 4, 5]));
///
/// // Multiple threads can read simultaneously
/// let data_clone1 = Arc::clone(&shared_data);
/// let handle1 = thread::spawn(move || {
///     data_clone1.with_sync(|vec| {
///         vec.iter().sum::<i32>()
///     })
/// });
///
/// let data_clone2 = Arc::clone(&shared_data);
/// let handle2 = thread::spawn(move || {
///     data_clone2.with_sync(|vec| {
///         vec.len()
///     })
/// });
///
/// // Both readers execute concurrently
/// let sum = handle1.join().unwrap();
/// let len = handle2.join().unwrap();
/// assert_eq!(sum, 15);
/// assert_eq!(len, 5);
/// ```
#[derive(Debug,Default)]
pub struct RwLock<T> {
    inner: UnsafeCell<T>,
    data_lock: AtomicU8,
    waiting_sync_read_threads: Spinlock<Vec<thread::Thread>>,
    waiting_sync_write_threads: Spinlock<Vec<thread::Thread>>,
    waiting_async_read_threads: Spinlock<Vec<r#continue::Sender<()>>>,
    waiting_async_write_threads: Spinlock<Vec<r#continue::Sender<()>>>,
}

impl<T: Display> Display for RwLock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.try_lock_read() {
            Ok(guard) => std::fmt::Display::fmt(&*guard, f),
            Err(_) => write!(f, "Mutex {{ <locked> }}"),
        }
    }
}

impl <T> From<T> for RwLock<T> {
    fn from(value: T) -> Self {
        RwLock::new(value)
    }
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

/// A guard that provides read-only access to the data protected by an [`RwLock`].
///
/// This guard is created by the read locking methods on [`RwLock`]. When the guard
/// is dropped, the read lock is automatically released, allowing writers to acquire
/// the lock if no other readers are active.
///
/// Multiple `ReadGuard`s can exist simultaneously for the same `RwLock`, enabling
/// concurrent read access.
///
/// # Examples
///
/// ```
/// use wasm_safe_mutex::rwlock::RwLock;
///
/// let rwlock = RwLock::new(vec![1, 2, 3]);
///
/// {
///     let guard1 = rwlock.lock_sync_read();
///     let guard2 = rwlock.lock_sync_read();
///
///     // Both guards can read simultaneously
///     assert_eq!(guard1.len(), 3);
///     assert_eq!(guard2[0], 1);
/// } // Both guards dropped, read locks released
/// ```
#[derive(Debug)]
pub struct ReadGuard<'a, T> {
    pub(crate) mutex: &'a RwLock<T>,
}

/// A guard that provides exclusive read-write access to the data protected by an [`RwLock`].
///
/// This guard is created by the write locking methods on [`RwLock`]. When the guard
/// is dropped, the write lock is automatically released, allowing other readers or
/// writers to acquire the lock.
///
/// Only one `WriteGuard` can exist at a time for a given `RwLock`, ensuring exclusive
/// access for modifications.
///
/// # Examples
///
/// ```
/// use wasm_safe_mutex::rwlock::RwLock;
///
/// let rwlock = RwLock::new(String::from("hello"));
///
/// {
///     let mut guard = rwlock.lock_sync_write();
///     guard.push_str(", world!");
///     assert_eq!(&*guard, "hello, world!");
/// } // Guard dropped, write lock released
/// ```
#[derive(Debug)]
pub struct WriteGuard<'a, T> {
    pub(crate) mutex: &'a RwLock<T>,
}

impl<'a, T> AsRef<T> for ReadGuard<'a, T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<'a, T> AsRef<T> for WriteGuard<'a, T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<'a, T> AsMut<T> for WriteGuard<'a, T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self
    }
}

impl <'a, T> Display for ReadGuard<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl <'a, T> Display for WriteGuard<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe{&*self.mutex.inner.get()}
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe {&mut*self.mutex.inner.get()}
    }
}


impl<'a, T> Deref for ReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe{&*self.mutex.inner.get()}
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        let r = self.mutex.data_lock.fetch_sub(1, Release);
        assert!(r > 0);
        self.mutex.did_unlock_read();
    }
}

impl <'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        let old = self.mutex.data_lock.swap(UNLOCKED, Release);
        assert!(old == LOCKED_WRITE);
        self.mutex.did_unlock_write();
    }
}


impl <T> RwLock<T> {
    /// Creates a new read-write lock with the given initial value.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(42);
    /// assert_eq!(*rwlock.lock_sync_read(), 42);
    /// ```
    pub const fn new(value: T) -> RwLock<T> {
        RwLock {
            inner: UnsafeCell::new(value),
            data_lock: AtomicU8::new(UNLOCKED),
            waiting_sync_read_threads: Spinlock::new(vec![]),
            waiting_async_read_threads: Spinlock::new(vec![]),
            waiting_sync_write_threads: Spinlock::new(vec![]),
            waiting_async_write_threads: Spinlock::new(vec![]),

        }
    }

    /// Attempts to acquire a read lock without blocking.
    ///
    /// Returns immediately with either a guard to the protected data or
    /// a `NotAvailable` error if a writer currently holds the lock.
    ///
    /// Multiple readers can hold read locks simultaneously, so this will
    /// only fail if there's an active writer.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use wasm_safe_mutex::NotAvailable;
    ///
    /// let rwlock = RwLock::new("data");
    ///
    /// // Multiple readers can acquire locks
    /// let guard1 = rwlock.try_lock_read().unwrap();
    /// let guard2 = rwlock.try_lock_read().unwrap();
    /// assert_eq!(*guard1, "data");
    /// assert_eq!(*guard2, "data");
    /// ```
    ///
    /// ## Writer Blocks Readers
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use wasm_safe_mutex::NotAvailable;
    ///
    /// let rwlock = RwLock::new(0);
    /// let _writer = rwlock.lock_sync_write();
    ///
    /// // This will fail because a writer holds the lock
    /// assert!(matches!(rwlock.try_lock_read(), Err(NotAvailable)));
    /// ```
    pub fn try_lock_read(&self) -> Result<ReadGuard<'_,T>, NotAvailable> {
        let r = self.data_lock.fetch_update(Acquire, Relaxed, |f| {
            if f & LOCKED_WRITE != 0 {
                None
            } else if f == LOCKED_WRITE - 1 {
                panic!("Too many readers")
            } else {
                Some(f + 1)
            }
        });
        match r {
            Ok(_) => {
                Ok(ReadGuard { mutex: self})

            }
            Err(_) => {
                Err(NotAvailable)
            }
        }
    }

    /// Attempts to acquire a write lock without blocking.
    ///
    /// Returns immediately with either a guard to the protected data or
    /// a `NotAvailable` error if any readers or another writer currently
    /// hold locks.
    ///
    /// Write locks are exclusive - no other readers or writers can be active.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use wasm_safe_mutex::NotAvailable;
    ///
    /// let rwlock = RwLock::new(vec![1, 2, 3]);
    ///
    /// match rwlock.try_lock_write() {
    ///     Ok(mut guard) => {
    ///         guard.push(4);
    ///         println!("Updated: {:?}", *guard);
    ///     }
    ///     Err(NotAvailable) => {
    ///         println!("Could not acquire write lock");
    ///     }
    /// }
    /// ```
    ///
    /// ## Readers Block Writers
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use wasm_safe_mutex::NotAvailable;
    ///
    /// let rwlock = RwLock::new(0);
    /// let _reader = rwlock.lock_sync_read();
    ///
    /// // This will fail because a reader holds a lock
    /// assert!(matches!(rwlock.try_lock_write(), Err(NotAvailable)));
    /// ```
    pub fn try_lock_write(&self) -> Result<WriteGuard<'_,T>, NotAvailable> {
        match self.data_lock.compare_exchange(UNLOCKED, LOCKED_WRITE, Acquire, Relaxed) {
            Ok(_) => {
                Ok(WriteGuard { mutex: self })
            }
            Err(_) => {
                Err(NotAvailable)
            }
        }
    }

    /// Acquires a read lock by spinning until it becomes available.
    ///
    /// This method will continuously check if the lock is available in a tight
    /// loop. While this ensures the lock is eventually acquired, it consumes
    /// CPU cycles while waiting. Use this when you know the lock will be held
    /// only briefly, or when blocking is not possible (e.g., WASM main thread).
    ///
    /// Multiple readers can hold locks simultaneously - this only spins if
    /// a writer currently holds the lock.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(vec![1, 2, 3]);
    ///
    /// let guard1 = rwlock.lock_spin_read();
    /// let guard2 = rwlock.lock_spin_read();
    ///
    /// // Both readers can access the data
    /// assert_eq!(guard1.len(), guard2.len());
    /// ```
    pub fn lock_spin_read(&self) -> ReadGuard<'_,T> {
        // Spin until we can acquire the lock
        loop {
            let r = self.try_lock_read();
            match r {
                Ok(r) => {
                    return r;
                }
                Err(_) => {
                    std::hint::spin_loop();

                }
            }
        }
    }
    /// Acquires a write lock by spinning until it becomes available.
    ///
    /// This method will continuously check if the lock is available in a tight
    /// loop. The lock can only be acquired when no readers or writers are active.
    ///
    /// While spinning ensures the lock is eventually acquired, it consumes CPU
    /// cycles while waiting. Use this when blocking is not possible or when
    /// you know the lock will be available soon.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(String::from("hello"));
    ///
    /// let mut guard = rwlock.lock_spin_write();
    /// guard.push_str(", world!");
    /// assert_eq!(&*guard, "hello, world!");
    /// ```
    pub fn lock_spin_write(&self) -> WriteGuard<'_,T> {
        // Spin until we can acquire the lock
        loop {
            let r = self.try_lock_write();
            match r {
                Ok(r) => {
                    return r;
                }
                Err(_) => {
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Acquires a read lock by blocking the current thread until it becomes available.
    ///
    /// This method will put the thread to sleep if a writer holds the lock,
    /// allowing other threads to run. When the writer releases the lock, waiting
    /// reader threads are woken up to acquire read access.
    ///
    /// Multiple readers can acquire locks simultaneously once no writer is active.
    ///
    /// # Platform Behavior
    ///
    /// - **Native (main or worker)**: Uses thread parking for efficient blocking
    /// - **WASM worker threads**: Blocks using `Atomics.wait` when available
    /// - **WASM main thread**: Falls back to spinning (cannot use blocking primitives)
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(HashMap::from([("key", "value")]));
    ///
    /// // This will block if a writer holds the lock
    /// let guard = rwlock.lock_block_read();
    /// assert_eq!(guard.get("key"), Some(&"value"));
    /// # use std::collections::HashMap;
    /// ```
    pub fn lock_block_read(&self) -> ReadGuard<'_, T> {
        //insert our thread into the waiting list
        loop {
            let r = self.waiting_sync_read_threads.with_mut(|threads| {
                match self.try_lock_read() {
                    Ok(guard) => {
                        // Return the guard
                        Ok(guard)
                    }
                    Err(_) => {
                        let handle = thread::current();
                        threads.push(handle);
                        Err(NotAvailable)
                    }
                }
            });
            match r {
                Ok(guard) => return guard,
                Err(NotAvailable) => thread::park(),
            }
        }
    }

    /// Acquires a write lock by blocking the current thread until it becomes available.
    ///
    /// This method will put the thread to sleep if any readers or another writer
    /// hold locks, allowing other threads to run. When all locks are released,
    /// one waiting writer thread is woken up to acquire exclusive access.
    ///
    /// # Platform Behavior
    ///
    /// - **Native (main or worker)**: Uses thread parking for efficient blocking
    /// - **WASM worker threads**: Blocks using `Atomics.wait` when available
    /// - **WASM main thread**: Falls back to spinning (cannot use blocking primitives)
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let rwlock = Arc::new(RwLock::new(0));
    /// let rwlock_clone = Arc::clone(&rwlock);
    ///
    /// thread::spawn(move || {
    ///     let mut guard = rwlock_clone.lock_block_write();
    ///     *guard = 42;
    /// });
    ///
    /// # thread::sleep(std::time::Duration::from_millis(10));
    /// let guard = rwlock.lock_block_read();
    /// assert_eq!(*guard, 42);
    /// ```
    pub fn lock_block_write(&self) -> WriteGuard<'_, T> {
        loop {
            let r = self.waiting_sync_write_threads.with_mut(|threads| {
                match self.try_lock_write() {
                    Ok(guard) => {
                        Ok(guard)
                    }
                    Err(_) => {
                        let handle = thread::current();
                        threads.push(handle);
                        Err(NotAvailable)
                    }
                }
            });
            match r {
                Ok(guard) => return guard,
                Err(NotAvailable) => thread::park(),
            }
        }
    }

    /// Asynchronously acquires a read lock.
    ///
    /// This method returns a future that resolves to a read guard when the lock
    /// becomes available. Unlike the blocking variants, this doesn't block
    /// the async executor, allowing other tasks to run while waiting.
    ///
    /// Multiple readers can hold locks simultaneously - this only waits if
    /// a writer currently holds the lock.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(vec!["async", "data"]);
    ///
    /// let guard1 = rwlock.lock_async_read().await;
    /// let guard2 = rwlock.lock_async_read().await;
    ///
    /// // Both readers can access simultaneously
    /// assert_eq!(guard1.len(), 2);
    /// assert_eq!(guard2[0], "async");
    /// # });
    /// ```
    pub async fn lock_async_read(&self) -> ReadGuard<'_, T> {
        loop {
            let a = self.waiting_async_read_threads.with_mut(|senders| {
                match self.try_lock_read() {
                    Ok(guard) => Ok(guard),
                    Err(NotAvailable) => {
                        // Create a new channel to signal when the lock is available
                        let (sender, receiver) = r#continue::continuation();
                        senders.push(sender);
                        Err(receiver)
                    }
                }
            });
            match a {
                Ok(guard) => return guard,
                Err(receiver) => {
                    // Wait for the signal that the lock is available
                    receiver.await;
                }
            }
        }
    }

    /// Asynchronously acquires a write lock.
    ///
    /// This method returns a future that resolves to a write guard when the lock
    /// becomes available. Unlike the blocking variants, this doesn't block
    /// the async executor, allowing other tasks to run while waiting.
    ///
    /// The write lock is exclusive - it waits until all readers and any other
    /// writer release their locks.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use std::collections::HashMap;
    ///
    /// let rwlock = RwLock::new(HashMap::new());
    ///
    /// let mut guard = rwlock.lock_async_read_write().await;
    /// guard.insert("key", "value");
    /// drop(guard);
    ///
    /// let guard = rwlock.lock_async_read().await;
    /// assert_eq!(guard.get("key"), Some(&"value"));
    /// # });
    /// ```
    ///
    /// ## Note
    ///
    /// The method is currently named `lock_async_read_write` but functions as
    /// an async write lock. The name may be updated in future versions for clarity.
    pub async fn lock_async_read_write(&self) -> WriteGuard<'_, T> {
        loop {
            let a = self.waiting_async_write_threads.with_mut(|senders| {
                match self.try_lock_write() {
                    Ok(guard) => Ok(guard),
                    Err(NotAvailable) => {
                        // Create a new channel to signal when the lock is available
                        let (sender, receiver) = r#continue::continuation();
                        senders.push(sender);
                        Err(receiver)
                    }
                }
            });
            match a {
                Ok(guard) => return guard,
                Err(receiver) => {
                    // Wait for the signal that the lock is available
                    receiver.await;
                }
            }
        }
    }

    fn did_unlock_write(&self) {
        //pop the waiting READ threads
        let threads = self.waiting_sync_read_threads.with_mut(std::mem::take);
        for thread in threads {
            // Wake up the thread
            thread.unpark();
        }
        let threads = self.waiting_async_read_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.send(())
        }

        //AND the write threads
        let threads = self.waiting_sync_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
        let threads = self.waiting_async_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.send(())
        }
    }

    fn did_unlock_read(&self) {
        //unlock only WRITE threads
        let threads = self.waiting_sync_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
        let threads = self.waiting_async_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.send(())
        }
    }

    /// Automatically chooses the right read locking strategy for your platform.
    ///
    /// This is the recommended method for acquiring read locks as it papers over
    /// all platform differences:
    /// - **Native (any thread)**: Uses efficient thread parking
    /// - **WASM worker threads**: Uses `Atomics.wait` for proper blocking
    /// - **WASM main thread**: Falls back to spinning to avoid panic
    ///
    /// You don't need to worry about "cannot block on main thread" errors -
    /// this method handles that automatically by detecting the environment
    /// and choosing the appropriate strategy.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(vec!["apple", "banana"]);
    ///
    /// // Automatically uses the best strategy for the platform
    /// let guard1 = rwlock.lock_sync_read();
    /// let guard2 = rwlock.lock_sync_read();
    ///
    /// // Multiple readers work on all platforms
    /// assert_eq!(guard1.len(), 2);
    /// assert_eq!(guard2[1], "banana");
    /// ```
    ///
    /// ## Cross-Platform Code
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// fn read_data(rwlock: &RwLock<String>) -> usize {
    ///     // Works efficiently on both native and WASM
    ///     let guard = rwlock.lock_sync_read();
    ///     guard.len()
    /// }
    ///
    /// let rwlock = RwLock::new(String::from("cross-platform"));
    /// assert_eq!(read_data(&rwlock), 14);
    /// ```
    pub fn lock_sync_read(&self) -> ReadGuard<'_, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block_read()
        }
        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::prelude::wasm_bindgen;
            //check if we're on the main thread
            #[wasm_bindgen(inline_js = "
export function supportsAtomicsWait() {
    if (typeof SharedArrayBuffer === 'undefined') return false;
    if (typeof Atomics === 'undefined' || typeof Atomics.wait !== 'function') return false;

    try {
        const sab = new SharedArrayBuffer(4);
        const ia = new Int32Array(sab);
        const result = Atomics.wait(ia, 0, 0, 0);
        return result === 'timed-out' || result === 'not-equal';
    } catch (_) {
        return false;
    }
}
")]
            extern "C" {
                fn supportsAtomicsWait() -> bool;
            }

            if supportsAtomicsWait() {
                self.lock_block_read()
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.lock_spin_read()
            }
        }
    }
    /// Automatically chooses the right write locking strategy for your platform.
    ///
    /// This is the recommended method for acquiring write locks as it papers over
    /// all platform differences:
    /// - **Native (any thread)**: Uses efficient thread parking
    /// - **WASM worker threads**: Uses `Atomics.wait` for proper blocking
    /// - **WASM main thread**: Falls back to spinning to avoid panic
    ///
    /// The write lock provides exclusive access - no readers or other writers
    /// can access the data while this lock is held.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(vec![1, 2, 3]);
    ///
    /// // Automatically uses the best strategy for the platform
    /// let mut guard = rwlock.lock_sync_write();
    /// guard.push(4);
    /// assert_eq!(guard.len(), 4);
    /// ```
    ///
    /// ## Cross-Platform Modifications
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use std::collections::HashMap;
    ///
    /// fn update_config(rwlock: &RwLock<HashMap<String, i32>>) {
    ///     // Works on both native and WASM without changes
    ///     let mut guard = rwlock.lock_sync_write();
    ///     guard.insert("version".to_string(), 2);
    /// }
    ///
    /// let rwlock = RwLock::new(HashMap::new());
    /// update_config(&rwlock);
    ///
    /// let guard = rwlock.lock_sync_read();
    /// assert_eq!(guard.get("version"), Some(&2));
    /// ```
    pub fn lock_sync_write(&self) -> WriteGuard<'_, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block_write()
        }
        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::prelude::wasm_bindgen;
            //check if we're on the main thread
            #[wasm_bindgen(inline_js = "
export function supportsAtomicsWait() {
    if (typeof SharedArrayBuffer === 'undefined') return false;
    if (typeof Atomics === 'undefined' || typeof Atomics.wait !== 'function') return false;

    try {
        const sab = new SharedArrayBuffer(4);
        const ia = new Int32Array(sab);
        const result = Atomics.wait(ia, 0, 0, 0);
        return result === 'timed-out' || result === 'not-equal';
    } catch (_) {
        return false;
    }
}
")]
            extern "C" {
                fn supportsAtomicsWait() -> bool;
            }

            if supportsAtomicsWait() {
                self.lock_block_write()
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.lock_spin_write()
            }
        }
    }

    /// Accesses the data inside the RwLock synchronously with a read-only closure.
    ///
    /// This method acquires a read lock, executes the provided closure with a reference
    /// to the protected data, and immediately releases the lock. This ensures the
    /// critical section is as short as possible.
    ///
    /// Multiple threads can execute read closures simultaneously, making this efficient
    /// for read-heavy workloads.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(vec![1, 2, 3, 4, 5]);
    ///
    /// // Calculate sum without holding the lock longer than needed
    /// let sum = rwlock.with_sync(|data| data.iter().sum::<i32>());
    /// assert_eq!(sum, 15);
    ///
    /// // Multiple operations in one critical section
    /// let (first, last) = rwlock.with_sync(|data| {
    ///     (data.first().copied(), data.last().copied())
    /// });
    /// assert_eq!(first, Some(1));
    /// assert_eq!(last, Some(5));
    /// ```
    pub fn with_sync<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_sync_read();
        f(&guard)
    }

    /// Accesses the data inside the RwLock synchronously with a mutable closure.
    ///
    /// This method acquires a write lock, executes the provided closure with a mutable
    /// reference to the protected data, and immediately releases the lock. This ensures
    /// exclusive access during modifications.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(vec![1, 2, 3]);
    ///
    /// // Modify and return a value
    /// let new_len = rwlock.with_mut_sync(|data| {
    ///     data.push(4);
    ///     data.len()
    /// });
    /// assert_eq!(new_len, 4);
    /// ```
    pub fn with_mut_sync<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_sync_write();
        f(&mut guard)
    }

    /// Accesses the data inside the RwLock asynchronously with a read-only closure.
    ///
    /// This method asynchronously acquires a read lock, executes the provided closure
    /// with a reference to the protected data, and immediately releases the lock.
    /// This ensures the critical section is as short as possible while not blocking
    /// the async executor.
    ///
    /// Multiple async tasks can read simultaneously, making this efficient for
    /// concurrent async read operations.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(String::from("async world"));
    ///
    /// let length = rwlock.with_async(|s| s.len()).await;
    /// assert_eq!(length, 11);
    ///
    /// let uppercase = rwlock.with_async(|s| s.to_uppercase()).await;
    /// assert_eq!(uppercase, "ASYNC WORLD");
    /// # });
    /// ```
    pub async fn with_async<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_async_read().await;
        f(&guard)
    }

    /// Accesses the data inside the RwLock asynchronously with a mutable closure.
    ///
    /// This method asynchronously acquires a write lock, executes the provided closure
    /// with a mutable reference to the protected data, and immediately releases the lock.
    /// This ensures exclusive access for modifications without blocking the async executor.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::rwlock::RwLock;
    /// use std::collections::HashMap;
    ///
    /// let rwlock = RwLock::new(HashMap::new());
    ///
    /// // Add multiple entries in one async operation
    /// rwlock.with_mut_async(|map| {
    ///     map.insert("async", 1);
    ///     map.insert("await", 2);
    /// }).await;
    ///
    /// let sum = rwlock.with_async(|map| {
    ///     map.values().sum::<i32>()
    /// }).await;
    /// assert_eq!(sum, 3);
    /// # });
    /// ```
    pub async fn with_mut_async<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_async_read_write().await;
        f(&mut guard)
    }
}



#[cfg(test)] mod test {
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use std::time::Duration;
    use crate::rwlock::RwLock;

    #[test] fn test_lock_try() {
        let mutex = RwLock::new(0);
        let lock = mutex.try_lock_read();
        assert!(lock.is_ok());
        assert_eq!(lock.as_ref().unwrap().deref(), &0);
        let lock2 = mutex.try_lock_read();
        assert!(lock2.is_ok());
        assert_eq!(lock2.as_ref().unwrap().deref(), &0);

        drop(lock2);
        //fail to acquire write lock
        let lock2 = mutex.try_lock_write();
        assert!(!lock2.is_ok());

        drop(lock);
        let mut write_lock = mutex.try_lock_write();
        assert!(write_lock.is_ok());
        assert_eq!(write_lock.as_ref().unwrap().deref(), &0);

        *write_lock.as_mut().unwrap().deref_mut() = 2;
        assert_eq!(write_lock.as_ref().unwrap().deref(), &2);

        //fail to acquire a new read lock
        let read_lock = mutex.try_lock_read();
        assert!(!read_lock.is_ok());

        drop(write_lock);
        //new read lock
        let read_lock = mutex.try_lock_read();
        assert!(read_lock.is_ok());
        assert_eq!(read_lock.as_ref().unwrap().deref(), &2);
    }

    #[test] fn test_lock_spin() {
        let mutex = RwLock::new(0);
        let lock = mutex.lock_spin_read();
        drop(lock);

        let lock = mutex.lock_spin_write();
        drop(lock);
    }

    #[test] fn test_lock_block() {
        let mutex = Arc::new(RwLock::new(0));
        let lock = mutex.lock_block_read();
        assert_eq!(lock.deref(), &0);
        let lock2 = mutex.lock_block_read();
        assert_eq!(lock.deref(), &0);
        drop(lock2);

        let (tx,rx) = std::sync::mpsc::channel();
        let mutex_clone = mutex.clone();
        std::thread::spawn(move || {
            //indicate thread came up
            tx.send(()).unwrap();
            let lock = mutex_clone.lock_block_write();
            tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(25));
            drop(lock);
        });
        //wait for thread up msg
        rx.recv().unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(10)).is_err());
        drop(lock); //thread should now acquire lock
        rx.recv().unwrap(); //wait for thread to acquire lock
        let time = std::time::Instant::now();
        mutex.lock_block_read();
        //ensure time took >50ms
        assert!(time.elapsed() > Duration::from_millis(10));
    }

    #[test_executors::async_test] async fn test_async() {
        let mutex = Arc::new(RwLock::new(0));
        let lock = mutex.lock_async_read().await;
        assert_eq!(lock.deref(), &0);
        drop(lock);
        let lock = mutex.lock_async_read_write().await;
        assert_eq!(lock.deref(), &0);
        drop(lock);
    }

    #[test] fn test_sync() {
        let mutex = Arc::new(RwLock::new(0));
        let lock = mutex.lock_sync_read();
        assert_eq!(lock.deref(), &0);
        drop(lock);
        let lock = mutex.lock_sync_write();
        assert_eq!(lock.deref(), &0);
        drop(lock);
    }
}