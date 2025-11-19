// SPDX-License-Identifier: MIT OR Apache-2.0
//! A WebAssembly-safe condition variable implementation.
//!
//! This module provides a condition variable that works across native and WebAssembly targets,
//! automatically adapting its waiting strategy based on the runtime environment.

use crate::{Guard, Spinlock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

#[cfg(target_arch = "wasm32")]
use web_time::Instant;

#[cfg(not(target_arch = "wasm32"))]
use std::thread;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;
#[cfg(target_arch = "wasm32")]
use wasm_thread as thread;

#[cfg(target_arch = "wasm32")]
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

#[cfg(target_arch = "wasm32")]
fn atomics_wait_supported() -> bool {
    supportsAtomicsWait()
}

/// A wrapper for async waiters that includes a unique ID for identification
#[derive(Debug)]
struct AsyncWaiter {
    id: u64,
    sender: r#continue::Sender<()>,
}

/// Counter for generating unique IDs for async waiters
static ASYNC_WAITER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A condition variable that works across native and WebAssembly targets.
///
/// A condition variable allows threads to wait for a particular condition to become true,
/// and to notify other threads when the condition changes. This implementation automatically
/// adapts to the platform:
///
/// - **Native (any thread)**: Uses efficient thread parking
/// - **WASM worker threads**: Uses `Atomics.wait` when available
/// - **WASM main thread**: Falls back to spinning to avoid panic
///
/// # Examples
///
/// ## Basic Usage
///
/// ```
/// use wasm_safe_mutex::{Mutex, condvar::Condvar};
/// use std::sync::Arc;
/// # use std::thread;
///
/// let pair = Arc::new((Mutex::new(false), Condvar::new()));
/// let pair_clone = Arc::clone(&pair);
///
/// thread::spawn(move || {
///     let (mutex, condvar) = &*pair_clone;
///     let mut ready = mutex.lock_sync();
///     *ready = true;
///     drop(ready);
///     condvar.notify_one();
/// });
///
/// let (mutex, condvar) = &*pair;
/// let mut ready = mutex.lock_sync();
/// while !*ready {
///     ready = condvar.wait_sync(ready);
/// }
/// assert!(*ready);
/// ```
///
/// ## Producer-Consumer Pattern
///
/// ```
/// use wasm_safe_mutex::{Mutex, condvar::Condvar};
/// use std::sync::Arc;
/// use std::collections::VecDeque;
/// # use std::thread;
///
/// let shared = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
/// let producer = Arc::clone(&shared);
///
/// // Producer thread
/// thread::spawn(move || {
///     let (mutex, condvar) = &*producer;
///     for i in 0..5 {
///         let mut queue = mutex.lock_sync();
///         queue.push_back(i);
///         drop(queue);
///         condvar.notify_one();
///     }
/// });
///
/// // Consumer
/// let (mutex, condvar) = &*shared;
/// let mut collected = Vec::new();
/// for _ in 0..5 {
///     let mut queue = mutex.lock_sync();
///     while queue.is_empty() {
///         queue = condvar.wait_sync(queue);
///     }
///     if let Some(value) = queue.pop_front() {
///         collected.push(value);
///     }
/// }
/// assert_eq!(collected, vec![0, 1, 2, 3, 4]);
/// ```
#[derive(Debug)]
pub struct Condvar {
    waiting_sync_threads: Spinlock<Vec<thread::Thread>>,
    waiting_async_threads: Spinlock<Vec<AsyncWaiter>>,
    waiting_spin_threads: Spinlock<Vec<Arc<AtomicBool>>>,
}

impl Condvar {
    /// Creates a new condition variable.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::condvar::Condvar;
    ///
    /// let condvar = Condvar::new();
    /// ```
    pub const fn new() -> Self {
        Condvar {
            waiting_sync_threads: Spinlock::new(vec![]),
            waiting_async_threads: Spinlock::new(vec![]),
            waiting_spin_threads: Spinlock::new(vec![]),
        }
    }

    /// Waits by spinning until this condition variable receives a notification.
    ///
    /// This method will atomically unlock the mutex specified by the guard and
    /// spin in a tight loop until notified. When a notification is received, the
    /// mutex will be re-acquired before returning.
    ///
    /// While this ensures the wait completes, it consumes CPU cycles. Use this
    /// when you know notifications will arrive quickly, or when blocking is not
    /// possible (e.g., WASM main thread).
    ///
    /// # Spurious Wakeups
    ///
    /// This method may return spuriously (without a notification). Always use it
    /// in a loop that checks the condition.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut ready = mutex.lock_sync();
    ///     *ready = true;
    ///     drop(ready);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_sync();
    /// while !*ready {
    ///     ready = condvar.wait_spin(ready);
    /// }
    /// assert!(*ready);
    /// ```
    pub fn wait_spin<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        let wake = Arc::new(AtomicBool::new(false));
        let mutex = guard.mutex;
        //insert into wait queue
        self.waiting_spin_threads.with_mut(|e| e.push(wake.clone()));
        eprintln!("Pushed a waiting_spin_thread");

        // Release the mutex
        drop(guard);

        while !wake.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        eprintln!("Spin complete");

        // Re-acquire the mutex before returning
        mutex.lock_sync()
    }

    /// Waits by spinning while the predicate remains `true`.
    ///
    /// This helper repeatedly evaluates `condition`, calling [`wait_spin`]
    /// whenever more progress is required. The guard returned at the end
    /// always satisfies `condition == false`.
    pub fn wait_spin_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            guard = self.wait_spin(guard);
        }
        guard
    }

    /// Waits by spinning until this condition variable receives a notification or the deadline is reached.
    ///
    /// This method will atomically unlock the mutex specified by the guard and
    /// spin in a tight loop until notified or the deadline is reached. When a notification is received
    /// or the timeout expires, the mutex will be re-acquired before returning.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut ready = mutex.lock_sync();
    ///     *ready = true;
    ///     drop(ready);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_sync();
    /// let deadline = Instant::now() + Duration::from_secs(1);
    /// while !*ready {
    ///     let result;
    ///     (ready, result) = condvar.wait_spin_until(ready, deadline);
    ///     if result.timed_out() {
    ///         break;
    ///     }
    /// }
    /// ```
    pub fn wait_spin_until<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        let wake = Arc::new(AtomicBool::new(false));
        let mutex = guard.mutex;
        //insert into wait queue
        self.waiting_spin_threads.with_mut(|e| e.push(wake.clone()));

        // Release the mutex
        drop(guard);

        loop {
            if wake.load(Ordering::Acquire) {
                // Re-acquire the mutex before returning
                return (mutex.lock_sync(), WaitTimeoutResult(false));
            }
            if Instant::now() >= deadline {
                // We timed out. We need to remove ourselves from the wait list.
                // It's possible we were notified just now, so we check one last time after locking.
                let notified = self.waiting_spin_threads.with_mut(|threads| {
                    // Find our wake arc and remove it
                    if let Some(pos) = threads.iter().position(|x| Arc::ptr_eq(x, &wake)) {
                        threads.remove(pos);
                        false // We removed ourselves, so we were NOT notified by someone else popping us
                    } else {
                        true // We were not in the list, so we MUST have been notified/popped
                    }
                });

                if notified {
                    // We were notified, so we shouldn't return timeout.
                    // We still need to wait for the wake flag to be set to true by the notifier
                    // effectively behaving as a normal wait_spin completion.
                    while !wake.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    return (mutex.lock_sync(), WaitTimeoutResult(false));
                } else {
                    return (mutex.lock_sync(), WaitTimeoutResult(true));
                }
            }
            std::hint::spin_loop();
        }
    }

    /// Waits by spinning while the predicate remains `true` or until the deadline is reached.
    ///
    /// This helper repeatedly evaluates `condition`, calling [`wait_spin_until`]
    /// whenever more progress is required. Returns when either `condition` evaluates
    /// to `false` or the deadline is reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(0), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut value = mutex.lock_sync();
    ///     *value = 10;
    ///     drop(value);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut guard = mutex.lock_sync();
    /// let deadline = Instant::now() + Duration::from_secs(1);
    /// let (guard, result) = condvar.wait_spin_until_while(guard, deadline, |v| *v < 10);
    /// if !result.timed_out() {
    ///     assert_eq!(*guard, 10);
    /// }
    /// ```
    pub fn wait_spin_until_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        deadline: Instant,
        mut condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            let result;
            (guard, result) = self.wait_spin_until(guard, deadline);
            if result.timed_out() {
                return (guard, result);
            }
        }
        (guard, WaitTimeoutResult(false))
    }
}

/// A type indicating whether a timed wait on a condition variable returned
/// due to a time out or not.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct WaitTimeoutResult(bool);

impl WaitTimeoutResult {
    /// Returns `true` if the wait was known to have timed out.
    pub fn timed_out(&self) -> bool {
        self.0
    }
}

impl Condvar {
    /// Blocks the current thread until this condition variable receives a notification.
    ///
    /// This method will atomically unlock the mutex specified by the guard and block
    /// the current thread. When a notification is received, the thread will wake up
    /// and re-acquire the lock before returning.
    ///
    /// # Platform Behavior
    ///
    /// - **Native (main or worker)**: Uses thread parking for efficient blocking
    /// - **WASM worker threads**: Blocks using `Atomics.wait` when available
    /// - **WASM main thread**: Falls back to spinning (cannot use blocking primitives)
    ///
    /// # Spurious Wakeups
    ///
    /// This method may return spuriously (without a notification). Always use it
    /// in a loop that checks the condition.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut ready = mutex.lock_sync();
    ///     *ready = true;
    ///     drop(ready);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_sync();
    /// while !*ready {
    ///     ready = condvar.wait_block(ready);
    /// }
    /// assert!(*ready);
    /// ```
    pub fn wait_block<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        let mutex = guard.mutex;

        // Register this thread as waiting before releasing the lock
        self.waiting_sync_threads.with_mut(|threads| {
            threads.push(thread::current());
        });

        // Explicitly drop the guard to release the mutex
        drop(guard);

        // Park this thread until notified
        std::thread::park();

        // Re-acquire the mutex before returning
        mutex.lock_sync()
    }

    /// Blocks the thread while the predicate returns `true`.
    ///
    /// Equivalent to manually looping on [`wait_block`] with a predicate check.
    pub fn wait_block_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            guard = self.wait_block(guard);
        }
        guard
    }

    /// Blocks the current thread until this condition variable receives a notification or the deadline is reached.
    ///
    /// This method will atomically unlock the mutex specified by the guard and block
    /// the current thread. When a notification is received or the timeout expires,
    /// the thread will wake up and re-acquire the lock before returning.
    ///
    /// # Platform Behavior
    ///
    /// - **Native (main or worker)**: Uses thread parking with timeout
    /// - **WASM worker threads**: Blocks using `Atomics.wait` with timeout when available
    /// - **WASM main thread**: Falls back to spinning (cannot use blocking primitives)
    ///
    /// # Spurious Wakeups
    ///
    /// This method may return spuriously (without a notification). Always use it
    /// in a loop that checks the condition.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut ready = mutex.lock_sync();
    ///     *ready = true;
    ///     drop(ready);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_sync();
    /// let deadline = Instant::now() + Duration::from_secs(1);
    /// while !*ready {
    ///     let result;
    ///     (ready, result) = condvar.wait_block_until(ready, deadline);
    ///     if result.timed_out() {
    ///         break;
    ///     }
    /// }
    /// assert!(*ready);
    /// ```
    pub fn wait_block_until<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        let mutex = guard.mutex;

        // Register this thread as waiting before releasing the lock
        self.waiting_sync_threads.with_mut(|threads| {
            threads.push(thread::current());
        });

        // Explicitly drop the guard to release the mutex
        drop(guard);

        loop {
            let now = Instant::now();
            if now >= deadline {
                // We timed out. We need to remove ourselves from the wait list.
                // It's possible we were notified just now, so we check one last time after locking.
                let notified = self.waiting_sync_threads.with_mut(|threads| {
                    // Find our thread and remove it
                    let current = thread::current();
                    if let Some(pos) = threads.iter().position(|x| x.id() == current.id()) {
                        threads.remove(pos);
                        false // We removed ourselves, so we were NOT notified by someone else popping us
                    } else {
                        true // We were not in the list, so we MUST have been notified/popped
                    }
                });

                if notified {
                    // We were notified, so we shouldn't return timeout.
                    // We just need to re-acquire the lock.
                    return (mutex.lock_sync(), WaitTimeoutResult(false));
                } else {
                    return (mutex.lock_sync(), WaitTimeoutResult(true));
                }
            }

            let timeout = deadline - now;
            // Park this thread until notified or timeout
            std::thread::park_timeout(timeout);

            // Check if we were notified
            let notified = self.waiting_sync_threads.with_mut(|threads| {
                let current = thread::current();
                if threads.iter().any(|x| x.id() == current.id()) {
                    // We are still in the list, so we were NOT notified (spurious wakeup or timeout)
                    false
                } else {
                    // We are not in the list, so we MUST have been notified/popped
                    true
                }
            });

            if notified {
                return (mutex.lock_sync(), WaitTimeoutResult(false));
            }
        }
    }

    /// Blocks the thread while the predicate remains `true` or until the deadline is reached.
    ///
    /// This loops on [`wait_block_until`] until `condition` evaluates to `false` or
    /// the deadline is reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(0), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut value = mutex.lock_sync();
    ///     *value = 10;
    ///     drop(value);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let guard = mutex.lock_sync();
    /// let deadline = Instant::now() + Duration::from_secs(1);
    /// let (guard, result) = condvar.wait_block_until_while(guard, deadline, |v| *v < 10);
    /// if !result.timed_out() {
    ///     assert_eq!(*guard, 10);
    /// }
    /// ```
    pub fn wait_block_until_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        deadline: Instant,
        mut condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            let result;
            (guard, result) = self.wait_block_until(guard, deadline);
            if result.timed_out() {
                return (guard, result);
            }
        }
        (guard, WaitTimeoutResult(false))
    }

    /// Automatically chooses the right waiting strategy for your platform.
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
    /// # Spurious Wakeups
    ///
    /// This method may return spuriously (without a notification). Always use it
    /// in a loop that checks the condition.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut ready = mutex.lock_sync();
    ///     *ready = true;
    ///     drop(ready);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_sync();
    /// let deadline = Instant::now() + Duration::from_secs(1);
    /// while !*ready {
    ///     let result;
    ///     (ready, result) = condvar.wait_sync_until(ready, deadline);
    ///     if result.timed_out() {
    ///         break;
    ///     }
    /// }
    /// assert!(*ready);
    /// ```
    pub fn wait_sync_until<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.wait_block_until(guard, deadline)
        }
        #[cfg(target_arch = "wasm32")]
        {
            if atomics_wait_supported() {
                self.wait_block_until(guard, deadline)
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.wait_spin_until(guard, deadline)
            }
        }
    }

    /// Automatically waits while the predicate is `true` or until the deadline is reached,
    /// choosing the best strategy per platform.
    ///
    /// This is the recommended method as it papers over all platform differences:
    /// - **Native (any thread)**: Uses efficient thread parking
    /// - **WASM worker threads**: Uses `Atomics.wait` for proper blocking
    /// - **WASM main thread**: Falls back to spinning to avoid panic
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(0), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut value = mutex.lock_sync();
    ///     *value = 10;
    ///     drop(value);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let guard = mutex.lock_sync();
    /// let deadline = Instant::now() + Duration::from_secs(1);
    /// let (guard, result) = condvar.wait_sync_until_while(guard, deadline, |v| *v < 10);
    /// if !result.timed_out() {
    ///     assert_eq!(*guard, 10);
    /// }
    /// ```
    pub fn wait_sync_until_while<'a, T, F>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
        condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.wait_block_until_while(guard, deadline, condition)
        }
        #[cfg(target_arch = "wasm32")]
        {
            if atomics_wait_supported() {
                self.wait_block_until_while(guard, deadline, condition)
            } else {
                self.wait_spin_until_while(guard, deadline, condition)
            }
        }
    }

    /// Automatically chooses the right waiting strategy for your platform.
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
    /// # Spurious Wakeups
    ///
    /// This method may return spuriously (without a notification). Always use it
    /// in a loop that checks the condition.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut ready = mutex.lock_sync();
    ///     *ready = true;
    ///     drop(ready);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_sync();
    /// while !*ready {
    ///     ready = condvar.wait_sync(ready);
    /// }
    /// assert!(*ready);
    /// ```
    pub fn wait_sync<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.wait_block(guard)
        }
        #[cfg(target_arch = "wasm32")]
        {
            if atomics_wait_supported() {
                self.wait_block(guard)
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.wait_spin(guard)
            }
        }
    }

    /// Automatically waits while the predicate is `true`, choosing the best strategy per platform.
    pub fn wait_sync_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            while condition(&mut guard) {
                guard = self.wait_block(guard);
            }
            guard
        }
        #[cfg(target_arch = "wasm32")]
        {
            if atomics_wait_supported() {
                while condition(&mut guard) {
                    guard = self.wait_block(guard);
                }
                guard
            } else {
                while condition(&mut guard) {
                    guard = self.wait_spin(guard);
                }
                guard
            }
        }
    }

    /// Asynchronously waits until this condition variable receives a notification.
    ///
    /// This method will atomically unlock the mutex specified by the guard and
    /// await a notification. When a notification is received, the mutex will be
    /// re-acquired before the future resolves.
    ///
    /// This method is non-blocking and works everywhere, including WASM main thread.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// // Spawn a thread that will notify us
    /// thread::spawn(move || {
    ///     # #[cfg(not(target_arch = "wasm32"))]
    ///     std::thread::sleep(std::time::Duration::from_millis(10));
    ///     test_executors::spin_on(async {
    ///         let (mutex, condvar) = &*pair_clone;
    ///         let mut ready = mutex.lock_async().await;
    ///         *ready = true;
    ///         drop(ready);
    ///         condvar.notify_one();
    ///     });
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_async().await;
    /// while !*ready {
    ///     ready = condvar.wait_async(ready).await;
    /// }
    /// assert!(*ready);
    /// # });
    /// ```
    pub async fn wait_async<'a, T>(&self, guard: Guard<'a, T>) -> Guard<'a, T> {
        let mutex = guard.mutex;

        // Create a channel to receive the notification
        let receiver = self.waiting_async_threads.with_mut(|waiters| {
            let (sender, receiver) = r#continue::continuation();
            let id = ASYNC_WAITER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
            waiters.push(AsyncWaiter { id, sender });
            receiver
        });

        // Release the mutex
        drop(guard);

        // Wait for notification
        receiver.await;

        // Re-acquire the mutex
        mutex.lock_async().await
    }

    /// Asynchronously waits while the predicate remains `true`.
    ///
    /// This loops on [`wait_async`] until `condition` evaluates to `false`.
    pub async fn wait_async_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        mut condition: F,
    ) -> Guard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            guard = self.wait_async(guard).await;
        }
        guard
    }

    /// Asynchronously waits until this condition variable receives a notification or the deadline is reached.
    ///
    /// This method will atomically unlock the mutex specified by the guard and
    /// await a notification or timeout. When a notification is received or the timeout expires,
    /// the mutex will be re-acquired before the future resolves.
    ///
    /// This method is non-blocking and works everywhere, including WASM main thread.
    ///
    /// # Examples
    ///
    /// ```
    /// # test_executors::spin_on(async {
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # #[cfg(target_arch = "wasm32")]
    /// use web_time::{Duration, Instant};
    /// # #[cfg(not(target_arch = "wasm32"))]
    /// # use std::time::{Duration, Instant};
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// // Spawn a thread that will notify us
    /// thread::spawn(move || {
    ///     # #[cfg(not(target_arch = "wasm32"))]
    ///     std::thread::sleep(std::time::Duration::from_millis(10));
    ///     test_executors::spin_on(async {
    ///         let (mutex, condvar) = &*pair_clone;
    ///         let mut ready = mutex.lock_async().await;
    ///         *ready = true;
    ///         drop(ready);
    ///         condvar.notify_one();
    ///     });
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_async().await;
    /// let deadline = Instant::now() + Duration::from_secs(1);
    /// while !*ready {
    ///     let result;
    ///     (ready, result) = condvar.wait_async_until(ready, deadline).await;
    ///     if result.timed_out() {
    ///         break;
    ///     }
    /// }
    /// assert!(*ready);
    /// # });
    /// ```
    pub async fn wait_async_until<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        let mutex = guard.mutex;

        // Create a unique ID for this waiter
        let waiter_id = ASYNC_WAITER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Create two channels - one for normal notification, one for timeout
        let (notify_sender, notify_receiver) = r#continue::continuation();
        let (timeout_sender, timeout_receiver) = r#continue::continuation();

        // Add to waiting list
        self.waiting_async_threads.with_mut(|waiters| {
            waiters.push(AsyncWaiter { id: waiter_id, sender: notify_sender });
        });

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

        // Release the mutex
        drop(guard);

        // Race between notification and timeout
        // We'll poll both futures and see which completes first
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct Race<F1, F2> {
            notify: Option<F1>,
            timeout: Option<F2>,
        }

        impl<F1: Future + Unpin, F2: Future + Unpin> Future for Race<F1, F2> {
            type Output = bool; // true if timed out

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
            notify: Some(notify_receiver),
            timeout: Some(timeout_receiver),
        }
        .await;

        // If we timed out, remove ourselves from the list
        if timed_out {
            self.waiting_async_threads.with_mut(|waiters| {
                if let Some(pos) = waiters.iter().position(|w| w.id == waiter_id) {
                    let waiter = waiters.remove(pos);
                    // Send the notification to complete the receiver
                    waiter.sender.send(());
                }
            });
        }

        // Re-acquire the mutex
        let guard = mutex.lock_async().await;

        // Return the result
        (guard, WaitTimeoutResult(timed_out))
    }

    /// Asynchronously waits while the predicate remains `true` or until the deadline is reached.
    ///
    /// This loops on [`wait_async_until`] until `condition` evaluates to `false` or timeout occurs.
    pub async fn wait_async_until_while<'a, T, F>(
        &self,
        mut guard: Guard<'a, T>,
        deadline: Instant,
        mut condition: F,
    ) -> (Guard<'a, T>, WaitTimeoutResult)
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            let result;
            (guard, result) = self.wait_async_until(guard, deadline).await;
            if result.timed_out() {
                return (guard, result);
            }
        }
        (guard, WaitTimeoutResult(false))
    }

    /// Wakes up one blocked thread on this condition variable.
    ///
    /// If there are multiple threads waiting, one will be woken up (unspecified which one).
    /// If no threads are waiting, this is a no-op.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = Arc::clone(&pair);
    ///
    /// thread::spawn(move || {
    ///     let (mutex, condvar) = &*pair_clone;
    ///     let mut ready = mutex.lock_sync();
    ///     *ready = true;
    ///     drop(ready);
    ///     condvar.notify_one();
    /// });
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut ready = mutex.lock_sync();
    /// while !*ready {
    ///     ready = condvar.wait_sync(ready);
    /// }
    /// ```
    pub fn notify_one(&self) {
        //Try to wake one spinlock first

        let thread = self.waiting_spin_threads.with_mut(|threads| threads.pop());
        if let Some(thread) = thread {
            eprintln!("Popped a waiting_spin_thread");
            thread.store(true, Ordering::Release);
            return;
        }
        // Try to wake one sync thread first
        let thread = self.waiting_sync_threads.with_mut(|threads| threads.pop());
        if let Some(thread) = thread {
            thread.unpark();
            return;
        }

        // If no sync threads, wake one async task
        let waiter = self.waiting_async_threads.with_mut(|waiters| waiters.pop());
        if let Some(waiter) = waiter {
            waiter.sender.send(());
        }
    }

    /// Wakes up all blocked threads on this condition variable.
    ///
    /// All waiting threads will be woken up. If no threads are waiting, this is a no-op.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::{Mutex, condvar::Condvar};
    /// use std::sync::Arc;
    /// # use std::thread;
    ///
    /// let pair = Arc::new((Mutex::new(0), Condvar::new()));
    /// let mut handles = vec![];
    ///
    /// for _ in 0..3 {
    ///     let pair = Arc::clone(&pair);
    ///     handles.push(thread::spawn(move || {
    ///         let (mutex, condvar) = &*pair;
    ///         let mut count = mutex.lock_sync();
    ///         while *count < 10 {
    ///             count = condvar.wait_sync(count);
    ///         }
    ///     }));
    /// }
    ///
    /// let (mutex, condvar) = &*pair;
    /// let mut count = mutex.lock_sync();
    /// *count = 10;
    /// drop(count);
    /// condvar.notify_all();
    ///
    /// for handle in handles {
    ///     handle.join().unwrap();
    /// }
    /// ```
    pub fn notify_all(&self) {
        //wake all spin threads
        let threads = self.waiting_spin_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.store(true, Ordering::Release);
        }

        // Wake all sync threads
        let threads = self.waiting_sync_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }

        // Wake all async tasks
        let waiters = self.waiting_async_threads.with_mut(std::mem::take);
        for waiter in waiters {
            waiter.sender.send(());
        }
    }
}

impl Default for Condvar {
    /// Creates a new condition variable with default settings.
    ///
    /// # Examples
    ///
    /// ```
    /// use wasm_safe_mutex::condvar::Condvar;
    ///
    /// let condvar: Condvar = Default::default();
    /// ```
    fn default() -> Self {
        Condvar::new()
    }
}

unsafe impl Send for Condvar {}
unsafe impl Sync for Condvar {}

#[cfg(test)]
mod tests;
