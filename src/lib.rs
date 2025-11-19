// SPDX-License-Identifier: MIT OR Apache-2.0
//! A WebAssembly-safe mutex that papers over platform-specific locking constraints.
//!
//! ![logo](../../../art/logo.png)
//!
//! # The Core Problem
//!
//! **WebAssembly's main thread cannot use blocking locks** - attempting to do so will panic
//! with "cannot block on the main thread". This is a fundamental limitation of the browser
//! environment where blocking the main thread would freeze the entire UI.
//!
//! However, blocking locks ARE allowed in:
//! - **WebAssembly worker threads** (where `Atomics.wait` is available)
//! - **Native platforms** (both main and worker threads)
//! - **Most non-WASM contexts** (traditional OS threads have no such restrictions)
//!
//! # The Solution
//!
//! This crate papers over all these platform differences by automatically adapting its
//! locking strategy based on the runtime environment:
//!
//! - **Native (any thread)**: Uses efficient thread parking (`thread::park`)
//! - **WASM worker threads**: Uses `Atomics.wait` when available
//! - **WASM main thread**: Falls back to spinning (non-blocking busy-wait)
//!
//! This means you can write code once and have it work correctly across all platforms,
//! without worrying about whether you're on the main thread, a worker thread, native or WASM.
//!
//! # Features
//!
//! - **Transparent adaptation**: Automatically detects and uses the best available locking mechanism
//! - **Main thread safe**: Won't panic on WASM main thread (uses spinning instead)
//! - **Worker thread optimized**: Uses proper blocking when available for efficiency
//! - **Native performance**: Full thread parking on native platforms
//! - **Async support**: Non-blocking async methods that work everywhere
//! - **Multiple strategies**: Try-lock, spin-lock, blocking lock, and async lock
//!
//! # Examples
//!
//! ## Basic Usage
//!
//! ```
//! use wasm_safe_mutex::Mutex;
//!
//! let mutex = Mutex::new(42);
//! let mut guard = mutex.lock_sync();
//! *guard = 100;
//! drop(guard);
//!
//! // Value has been updated
//! let guard = mutex.lock_sync();
//! assert_eq!(*guard, 100);
//! ```
//!
//! ## Try Lock
//!
//! ```
//! use wasm_safe_mutex::{Mutex, NotAvailable};
//!
//! let mutex = Mutex::new("data");
//!
//! // First lock succeeds
//! let guard = mutex.try_lock().unwrap();
//! assert_eq!(*guard, "data");
//!
//! // Second lock fails while first is held
//! let result = mutex.try_lock();
//! assert!(matches!(result, Err(NotAvailable)));
//! ```
//!
//! ## Async Usage
//!
//! ```
//! # test_executors::spin_on(async {
//! use wasm_safe_mutex::Mutex;
//!
//! let mutex = Mutex::new(vec![1, 2, 3]);
//!
//! // Async lock doesn't block the executor
//! let mut guard = mutex.lock_async().await;
//! guard.push(4);
//! drop(guard);
//!
//! // Using the convenience method
//! let sum = mutex.with_async(|data| data.iter().sum::<i32>()).await;
//! assert_eq!(sum, 10);
//! # });
//! ```
//!
//! ## Thread-Safe Sharing
//!
//! ```
//! use wasm_safe_mutex::Mutex;
//! use std::sync::Arc;
//! # use std::thread;
//!
//! let mutex = Arc::new(Mutex::new(0));
//! let handles: Vec<_> = (0..4)
//!     .map(|_| {
//!         let mutex = Arc::clone(&mutex);
//!         thread::spawn(move || {
//!             for _ in 0..25 {
//!                 mutex.with_mut_sync(|value| *value += 1);
//!             }
//!         })
//!     })
//!     .collect();
//!
//! for handle in handles {
//!     handle.join().unwrap();
//! }
//!
//! assert_eq!(*mutex.lock_sync(), 100);
//! ```

pub mod condvar;
pub mod guard;
pub mod rwlock;
pub mod spinlock;
#[cfg(test)]
mod tests;

pub use crate::guard::Guard;
use crate::spinlock::Spinlock;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicBool;

#[cfg(not(target_arch = "wasm32"))]
use std::thread;
#[cfg(target_arch = "wasm32")]
#[cfg(target_arch = "wasm32")]
use wasm_thread as thread;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

/// Error returned when a lock cannot be immediately acquired.
///
/// This error is returned by [`Mutex::try_lock`] when the mutex is already
/// locked by another thread.
///
/// # Examples
///
/// ```
/// use wasm_safe_mutex::{Mutex, NotAvailable};
///
/// let mutex = Mutex::new(42);
/// let _guard = mutex.lock_sync();
///
/// // Try to lock while already locked
/// match mutex.try_lock() {
///     Ok(_) => panic!("Should not succeed"),
///     Err(NotAvailable) => println!("Lock is held by another thread"),
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NotAvailable;

// ================================================================================================
// Boilerplate trait implementations
// ================================================================================================

impl std::fmt::Display for NotAvailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lock not available")
    }
}

impl std::error::Error for NotAvailable {}

// ================================================================================================
// Main types
// ================================================================================================

/// A mutual exclusion primitive that works across native and WebAssembly targets.
///
/// This mutex provides multiple locking strategies:
/// - **`try_lock`**: Non-blocking attempt to acquire the lock
/// - **`lock_spin`**: Spin-wait until the lock is acquired
/// - **`lock_block`**: Blocks on native/WASM workers, spins on WASM main thread
/// - **`lock_sync`**: Automatically chooses the right strategy for your platform
/// - **`lock_async`**: Always non-blocking, works everywhere including WASM main thread
///
/// ## Platform Behavior
///
/// The mutex transparently handles platform differences:
/// - **Native (main or worker thread)**: Full blocking with thread parking
/// - **WASM worker threads**: Blocks using `Atomics.wait`
/// - **WASM main thread**: Spins to avoid "cannot block on main thread" panic
///
/// This automatic adaptation means your code works everywhere without modification.
///
/// # Examples
///
/// ## Basic Synchronization
///
/// ```
/// use wasm_safe_mutex::Mutex;
///
/// let mutex = Mutex::new(0i32);
///
/// // Increment the value
/// {
///     let mut guard = mutex.lock_sync();
///     *guard += 1;
/// } // Lock is released here
///
/// // Read the value
/// let value = mutex.with_sync(|data| *data);
/// assert_eq!(value, 1);
/// ```
///
/// ## Shared State Between Threads
///
/// ```
/// use wasm_safe_mutex::Mutex;
/// use std::sync::Arc;
/// # use std::thread;
///
/// let shared_data = Arc::new(Mutex::new(vec![1, 2, 3]));
///
/// let data_clone = Arc::clone(&shared_data);
/// let handle = thread::spawn(move || {
///     data_clone.with_mut_sync(|vec| {
///         vec.push(4);
///         vec.len()
///     })
/// });
///
/// let length = handle.join().unwrap();
/// assert_eq!(length, 4);
/// ```
#[derive(Debug)]
pub struct Mutex<T> {
    inner: UnsafeCell<T>,
    data_lock: AtomicBool,
    waiting_sync_threads: Spinlock<Vec<thread::Thread>>,
    waiting_async_threads: Spinlock<Vec<r#continue::Sender<()>>>,
}
impl<T> Mutex<T> {
    /// Creates a new mutex with the given initial value.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(42);
    /// assert_eq!(*mutex.lock_sync(), 42);
    /// ```
    pub const fn new(value: T) -> Self {
        Mutex {
            inner: UnsafeCell::new(value),
            data_lock: AtomicBool::new(false),
            waiting_sync_threads: Spinlock::new(vec![]),
            waiting_async_threads: Spinlock::new(vec![]),
        }
    }

    /// Attempts to acquire the lock without blocking.
    ///
    /// Returns immediately with either a guard to the protected data or
    /// a `NotAvailable` error if the lock is already held.
    ///
    /// This method is useful when you want to avoid blocking and can
    /// handle the case where the lock is not available.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, NotAvailable};
    ///
    /// let mutex = Mutex::new("data");
    ///
    /// match mutex.try_lock() {
    ///     Ok(guard) => {
    ///         println!("Got the lock: {}", *guard);
    ///     }
    ///     Err(NotAvailable) => {
    ///         println!("Could not acquire lock, doing something else");
    ///     }
    /// }
    /// ```
    ///
    /// ## Handling Contention
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, NotAvailable};
    ///
    /// let mutex = Mutex::new(0);
    /// let _first_guard = mutex.lock_sync();
    ///
    /// // This will fail because the lock is held
    /// assert!(matches!(mutex.try_lock(), Err(NotAvailable)));
    /// ```
    pub fn try_lock(&self) -> Result<Guard<'_, T>, NotAvailable> {
        if self
            .data_lock
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            let data = unsafe { &mut *self.inner.get() };
            Ok(Guard { mutex: self, data })
        } else {
            Err(NotAvailable)
        }
    }

    /// Acquires the lock by spinning until it becomes available.
    ///
    /// This method will continuously check if the lock is available in a tight
    /// loop. While this ensures the lock is eventually acquired, it consumes
    /// CPU cycles while waiting. Use this when you know the lock will be held
    /// only briefly, or when blocking is not possible (e.g., WASM main thread).
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(vec![1, 2, 3]);
    ///
    /// let mut guard = mutex.lock_spin();
    /// guard.push(4);
    /// assert_eq!(guard.len(), 4);
    /// ```
    ///
    /// ## Performance Consideration
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let mutex = Arc::new(Mutex::new(0));
    ///
    /// // Spinning is less efficient than blocking for long waits
    /// let mutex_clone = Arc::clone(&mutex);
    /// thread::spawn(move || {
    ///     // This will spin until the lock is available
    ///     let mut guard = mutex_clone.lock_spin();
    ///     *guard = 42;
    /// });
    /// ```
    pub fn lock_spin(&self) -> Guard<'_, T> {
        // Spin until we can acquire the lock
        while self
            .data_lock
            .swap(true, std::sync::atomic::Ordering::Acquire)
        {
            std::hint::spin_loop();
        }
        // SAFETY: We have exclusive access to the data now
        let data = unsafe { &mut *self.inner.get() };
        Guard { mutex: self, data }
    }

    /// Acquires the lock by spinning until it becomes available or the deadline is reached.
    ///
    /// This method behaves like [`lock_spin`](Self::lock_spin), but will return `None`
    /// if the lock cannot be acquired before the specified deadline.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    ///
    /// let mutex = Mutex::new(0);
    /// let deadline = Instant::now() + Duration::from_millis(100);
    ///
    /// if let Some(mut guard) = mutex.lock_spin_timeout(deadline) {
    ///     *guard = 42;
    /// } else {
    ///     println!("Could not acquire lock in time");
    /// }
    /// ```
    pub fn lock_spin_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        // Spin until we can acquire the lock
        while self
            .data_lock
            .swap(true, std::sync::atomic::Ordering::Acquire)
        {
            if Instant::now() >= deadline {
                return None;
            }
            std::hint::spin_loop();
        }
        // SAFETY: We have exclusive access to the data now
        let data = unsafe { &mut *self.inner.get() };
        Some(Guard { mutex: self, data })
    }

    /// Acquires the lock by blocking the current thread until it becomes available.
    ///
    /// This method will put the thread to sleep if the lock is not available,
    /// allowing other threads to run. When the lock is released, waiting threads
    /// are woken up to try acquiring the lock.
    ///
    /// # Platform Behavior
    ///
    /// - **Native (main or worker)**: Uses thread parking for efficient blocking
    /// - **WASM worker threads**: Blocks using `Atomics.wait` when available
    /// - **WASM main thread**: Falls back to spinning (cannot use blocking primitives)
    ///
    /// This adaptation happens automatically - you don't need to check the platform.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(String::from("hello"));
    ///
    /// // This will block if the lock is held by another thread
    /// let mut guard = mutex.lock_block();
    /// guard.push_str(", world!");
    /// assert_eq!(&*guard, "hello, world!");
    /// ```
    ///
    /// ## Efficient Waiting
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// use std::sync::Arc;
    /// # use std::thread;
    /// # use std::time::Duration;
    ///
    /// let mutex = Arc::new(Mutex::new(0));
    /// let mutex_clone = Arc::clone(&mutex);
    ///
    /// // Start a thread that holds the lock briefly
    /// thread::spawn(move || {
    ///     let mut guard = mutex_clone.lock_block();
    ///     *guard = 100;
    ///     # #[cfg(not(target_arch = "wasm32"))]
    ///     thread::sleep(Duration::from_millis(10));
    /// });
    ///
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// thread::sleep(Duration::from_millis(5));
    ///
    /// // This blocks efficiently until the lock is available
    /// let guard = mutex.lock_block();
    /// assert_eq!(*guard, 100);
    /// ```
    pub fn lock_block(&self) -> Guard<'_, T> {
        //insert our thread into the waiting list
        loop {
            let r = self.waiting_sync_threads.with_mut(|threads| {
                match self.try_lock() {
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
                Err(NotAvailable) => std::thread::park(),
            }
        }
    }

    /// Acquires the lock by blocking with a timeout.
    ///
    /// This method behaves like [`lock_block`](Self::lock_block), but will return `None`
    /// if the lock cannot be acquired before the specified deadline.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    ///
    /// let mutex = Mutex::new(0);
    /// let deadline = Instant::now() + Duration::from_millis(100);
    ///
    /// if let Some(mut guard) = mutex.lock_block_timeout(deadline) {
    ///     *guard = 42;
    /// } else {
    ///     println!("Could not acquire lock in time");
    /// }
    /// ```
    pub fn lock_block_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                // Try one last time
                if let Ok(guard) = self.try_lock() {
                    return Some(guard);
                }
                return None;
            }

            let r = self
                .waiting_sync_threads
                .with_mut(|threads| match self.try_lock() {
                    Ok(guard) => Ok(guard),
                    Err(_) => {
                        let handle = thread::current();
                        threads.push(handle);
                        Err(NotAvailable)
                    }
                });

            match r {
                Ok(guard) => return Some(guard),
                Err(NotAvailable) => {
                    let remaining = deadline - Instant::now();
                    std::thread::park_timeout(remaining);
                }
            }
        }
    }

    /// Asynchronously acquires the lock.
    ///
    /// This method returns a future that resolves to a guard when the lock
    /// becomes available. Unlike the blocking variants, this doesn't block
    /// the async executor, allowing other tasks to run while waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(HashMap::<String, i32>::new());
    ///
    /// let mut guard = mutex.lock_async().await;
    /// guard.insert("key".to_string(), 42);
    /// drop(guard);
    ///
    /// let guard = mutex.lock_async().await;
    /// assert_eq!(guard.get("key"), Some(&42));
    /// # });
    /// # use std::collections::HashMap;
    /// ```
    ///
    /// ## Concurrent Async Tasks
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::Mutex;
    /// use std::sync::Arc;
    ///
    /// let mutex = Arc::new(Mutex::new(0));
    ///
    /// let mutex1 = Arc::clone(&mutex);
    /// let task1 = async move {
    ///     let mut guard = mutex1.lock_async().await;
    ///     *guard += 10;
    /// };
    ///
    /// let mutex2 = Arc::clone(&mutex);
    /// let task2 = async move {
    ///     let mut guard = mutex2.lock_async().await;
    ///     *guard += 20;
    /// };
    ///
    /// task1.await;
    /// task2.await;
    ///
    /// let guard = mutex.lock_async().await;
    /// assert_eq!(*guard, 30);
    /// # });
    /// ```
    pub async fn lock_async(&self) -> Guard<'_, T> {
        loop {
            let a = self.waiting_async_threads.with_mut(|senders| {
                match self.try_lock() {
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

    /// Asynchronously acquires the lock with a timeout.
    ///
    /// This method behaves like [`lock_async`](Self::lock_async), but will return `None`
    /// if the lock cannot be acquired before the specified deadline.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::Mutex;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    ///
    /// let mutex = Mutex::new(0);
    /// let deadline = Instant::now() + Duration::from_millis(100);
    ///
    /// if let Some(mut guard) = mutex.lock_async_timeout(deadline).await {
    ///     *guard = 42;
    /// } else {
    ///     println!("Could not acquire lock in time");
    /// }
    /// # });
    /// ```
    pub async fn lock_async_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                // Try one last time
                if let Ok(guard) = self.try_lock() {
                    return Some(guard);
                }
                return None;
            }

            let a = self.waiting_async_threads.with_mut(|senders| {
                match self.try_lock() {
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
                Ok(guard) => return Some(guard),
                Err(receiver) => {
                    // Create a channel for timeout
                    let (timeout_sender, timeout_receiver) = r#continue::continuation();

                    // Spawn a thread to handle the timeout
                    thread::spawn(move || {
                        let now = Instant::now();
                        if deadline > now {
                            let duration = deadline - now;
                            thread::sleep(duration);
                        }
                        // Send timeout signal
                        timeout_sender.send(());
                    });

                    // Race between notification and timeout
                    struct Race<F1, F2> {
                        notify: Option<F1>,
                        timeout: Option<F2>,
                    }

                    impl<F1: Future + Unpin, F2: Future + Unpin> Future for Race<F1, F2> {
                        type Output = bool; // true if timed out

                        fn poll(
                            mut self: Pin<&mut Self>,
                            cx: &mut Context<'_>,
                        ) -> Poll<Self::Output> {
                            // Poll notification future
                            if let Some(ref mut notify) = self.notify {
                                if Pin::new(notify).poll(cx).is_ready() {
                                    self.notify = None;
                                    return Poll::Ready(false); // Got notification
                                }
                            }

                            // Poll timeout future
                            if let Some(ref mut timeout) = self.timeout {
                                if Pin::new(timeout).poll(cx).is_ready() {
                                    self.timeout = None;
                                    return Poll::Ready(true); // Timed out
                                }
                            }

                            Poll::Pending
                        }
                    }

                    let timed_out = Race {
                        notify: Some(receiver),
                        timeout: Some(timeout_receiver),
                    }
                    .await;

                    if timed_out {
                        return None;
                    }
                    // If not timed out, we loop and try to lock again
                }
            }
        }
    }
    fn did_unlock(&self) {
        //pop the waiting threads
        let threads = self.waiting_sync_threads.with_mut(std::mem::take);
        for thread in threads {
            // Wake up the thread
            thread.unpark();
        }
        // Notify any async tasks waiting on this mutex
        let senders = self.waiting_async_threads.with_mut(std::mem::take);
        for sender in senders {
            // Send a signal to wake up the async task
            sender.send(());
        }
    }

    /// Automatically chooses the right locking strategy for your platform.
    ///
    /// This is the recommended method as it papers over all platform differences:
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
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(vec!["apple", "banana"]);
    ///
    /// // Automatically uses the best strategy for the platform
    /// let mut guard = mutex.lock_sync();
    /// guard.push("cherry");
    /// assert_eq!(guard.len(), 3);
    /// ```
    ///
    /// ## Cross-Platform Code
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// use std::sync::Arc;
    ///
    /// fn process_data(mutex: &Mutex<i32>) -> i32 {
    ///     // Works efficiently on both native and WASM
    ///     let mut guard = mutex.lock_sync();
    ///     *guard *= 2;
    ///     *guard
    /// }
    ///
    /// let mutex = Mutex::new(21);
    /// let result = process_data(&mutex);
    /// assert_eq!(result, 42);
    /// ```
    pub fn lock_sync(&self) -> Guard<'_, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block()
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
                self.lock_block()
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.lock_spin()
            }
        }
    }

    /// Automatically chooses the right locking strategy with a timeout.
    ///
    /// This method behaves like [`lock_sync`](Self::lock_sync), but will return `None`
    /// if the lock cannot be acquired before the specified deadline.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    ///
    /// let mutex = Mutex::new(0);
    /// let deadline = Instant::now() + Duration::from_millis(100);
    ///
    /// if let Some(mut guard) = mutex.lock_sync_timeout(deadline) {
    ///     *guard = 42;
    /// } else {
    ///     println!("Could not acquire lock in time");
    /// }
    /// ```
    pub fn lock_sync_timeout(&self, deadline: Instant) -> Option<Guard<'_, T>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block_timeout(deadline)
        }
        #[cfg(target_arch = "wasm32")]
        {
            if crate::condvar::atomics_wait_supported() {
                self.lock_block_timeout(deadline)
            } else {
                self.lock_spin_timeout(deadline)
            }
        }
    }

    /// Accesses the data inside the mutex synchronously with a read-only closure.
    ///
    /// This method acquires the lock, executes the provided closure with a reference
    /// to the protected data, and immediately releases the lock. This ensures the
    /// critical section is as short as possible.
    ///
    /// Use this method when you need to read data from the mutex without keeping
    /// the lock held longer than necessary.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(vec![1, 2, 3, 4, 5]);
    ///
    /// // Calculate sum without holding the lock longer than needed
    /// let sum = mutex.with_sync(|data| data.iter().sum::<i32>());
    /// assert_eq!(sum, 15);
    ///
    /// // Find maximum value
    /// let max = mutex.with_sync(|data| *data.iter().max().unwrap());
    /// assert_eq!(max, 5);
    /// ```
    ///
    /// ## Complex Operations
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// use std::collections::HashMap;
    ///
    /// let mutex = Mutex::new(HashMap::from([
    ///     ("alice", 100),
    ///     ("bob", 200),
    /// ]));
    ///
    /// // Perform multiple reads in one critical section
    /// let (total, count) = mutex.with_sync(|map| {
    ///     let total: i32 = map.values().sum();
    ///     let count = map.len();
    ///     (total, count)
    /// });
    ///
    /// assert_eq!(total, 300);
    /// assert_eq!(count, 2);
    /// ```
    pub fn with_sync<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_sync();
        f(&guard)
    }
    /// Accesses the data inside the mutex synchronously with a mutable closure.
    ///
    /// This method acquires the lock, executes the provided closure with a mutable
    /// reference to the protected data, and immediately releases the lock. This ensures
    /// the critical section is as short as possible.
    ///
    /// Use this method when you need to modify data in the mutex without keeping
    /// the lock held longer than necessary.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(vec![1, 2, 3]);
    ///
    /// // Modify and return a value
    /// let new_len = mutex.with_mut_sync(|data| {
    ///     data.push(4);
    ///     data.len()
    /// });
    /// assert_eq!(new_len, 4);
    /// ```
    ///
    /// ## In-Place Modifications
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    /// use std::collections::HashMap;
    ///
    /// let mutex = Mutex::new(HashMap::new());
    ///
    /// // Insert multiple values in one critical section
    /// mutex.with_mut_sync(|map| {
    ///     map.insert("temperature", 25);
    ///     map.insert("humidity", 60);
    /// });
    ///
    /// // Update existing values
    /// let old_temp = mutex.with_mut_sync(|map| {
    ///     map.insert("temperature", 28)
    /// });
    ///
    /// assert_eq!(old_temp, Some(25));
    /// ```
    pub fn with_mut_sync<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_sync();
        f(&mut guard)
    }

    /// Accesses the data inside the mutex asynchronously with a read-only closure.
    ///
    /// This method asynchronously acquires the lock, executes the provided closure
    /// with a reference to the protected data, and immediately releases the lock.
    /// This ensures the critical section is as short as possible while not blocking
    /// the async executor.
    ///
    /// Use this method in async contexts when you need to read data from the mutex.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(String::from("async world"));
    ///
    /// let length = mutex.with_async(|s| s.len()).await;
    /// assert_eq!(length, 11);
    ///
    /// let uppercase = mutex.with_async(|s| s.to_uppercase()).await;
    /// assert_eq!(uppercase, "ASYNC WORLD");
    /// # });
    /// ```
    ///
    /// ## Async Task Coordination
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::Mutex;
    /// use std::sync::Arc;
    ///
    /// let shared_state = Arc::new(Mutex::new(vec![1, 2, 3]));
    ///
    /// let state1 = Arc::clone(&shared_state);
    /// let task1 = async move {
    ///     state1.with_async(|data| data.iter().sum::<i32>()).await
    /// };
    ///
    /// let state2 = Arc::clone(&shared_state);
    /// let task2 = async move {
    ///     state2.with_async(|data| data.len()).await
    /// };
    ///
    /// let sum = task1.await;
    /// let len = task2.await;
    ///
    /// assert_eq!(sum, 6);
    /// assert_eq!(len, 3);
    /// # });
    /// ```
    pub async fn with_async<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        let guard = self.lock_async().await;
        f(&guard)
    }

    /// Accesses the data inside the mutex asynchronously with a mutable closure.
    ///
    /// This method asynchronously acquires the lock, executes the provided closure
    /// with a mutable reference to the protected data, and immediately releases the lock.
    /// This ensures the critical section is as short as possible while not blocking
    /// the async executor.
    ///
    /// Use this method in async contexts when you need to modify data in the mutex.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(vec!["async", "programming"]);
    ///
    /// // Add an element and return the new length
    /// let new_len = mutex.with_mut_async(|data| {
    ///     data.push("rocks");
    ///     data.len()
    /// }).await;
    ///
    /// assert_eq!(new_len, 3);
    /// # });
    /// ```
    ///
    /// ## Async State Updates
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::Mutex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let shared_config = Arc::new(Mutex::new(HashMap::new()));
    ///
    /// // Update configuration from multiple async tasks
    /// let config1 = Arc::clone(&shared_config);
    /// let update1 = async move {
    ///     config1.with_mut_async(|cfg| {
    ///         cfg.insert("max_connections", 100);
    ///         cfg.insert("timeout", 30);
    ///     }).await;
    /// };
    ///
    /// let config2 = Arc::clone(&shared_config);
    /// let update2 = async move {
    ///     config2.with_mut_async(|cfg| {
    ///         cfg.insert("buffer_size", 8192);
    ///     }).await;
    /// };
    ///
    /// update1.await;
    /// update2.await;
    ///
    /// let final_size = shared_config.with_async(|cfg| cfg.len()).await;
    /// assert_eq!(final_size, 3);
    /// # });
    /// ```
    pub async fn with_mut_async<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        let mut guard = self.lock_async().await;
        f(&mut guard)
    }
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

// Boilerplate trait implementations for Mutex
impl<T: Default> Default for Mutex<T> {
    /// Creates a new mutex with the default value of the wrapped type.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex: Mutex<i32> = Mutex::default();
    /// assert_eq!(*mutex.lock_sync(), 0);
    ///
    /// let mutex: Mutex<Vec<String>> = Mutex::default();
    /// assert!(mutex.lock_sync().is_empty());
    /// ```
    fn default() -> Self {
        Mutex::new(T::default())
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Mutex<T> {
    /// Formats the mutex by attempting to acquire the lock and formatting the contained value.
    ///
    /// If the lock cannot be acquired immediately, displays a placeholder message.
    /// This prevents blocking during formatting operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(42);
    /// println!("{}", mutex); // Prints "42" or "Mutex { <locked> }"
    /// ```
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.try_lock() {
            Ok(guard) => std::fmt::Display::fmt(&*guard, f),
            Err(_) => write!(f, "Mutex {{ <locked> }}"),
        }
    }
}

impl<T> From<T> for Mutex<T> {
    /// Creates a new mutex from the given value.
    ///
    /// This is equivalent to [`Mutex::new`] but can be more convenient
    /// in generic contexts or when using turbofish syntax.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::Mutex;
    ///
    /// let mutex = Mutex::from(42);
    /// assert_eq!(*mutex.lock_sync(), 42);
    ///
    /// // Useful in generic contexts
    /// fn create_mutex<T>(value: T) -> Mutex<T> {
    ///     Mutex::from(value)
    /// }
    /// ```
    fn from(value: T) -> Self {
        Mutex::new(value)
    }
}
