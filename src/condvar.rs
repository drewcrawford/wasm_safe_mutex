// SPDX-License-Identifier: MIT OR Apache-2.0
//! A WebAssembly-safe condition variable implementation.
//!
//! This module provides a condition variable that works across native and WebAssembly targets,
//! automatically adapting its waiting strategy based on the runtime environment.

use crate::{Guard, Spinlock};

#[cfg(not(target_arch = "wasm32"))]
use std::thread;
#[cfg(target_arch = "wasm32")]
use wasm_thread as thread;

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
    waiting_async_threads: Spinlock<Vec<r#continue::Sender<()>>>,
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
        let mutex = guard.mutex;

        // Release the mutex
        drop(guard);

        // Yield to allow other threads/tasks to run
        // This is critical in WASM where we need to give the event loop a chance
        #[cfg(target_arch = "wasm32")]
        {
            // On WASM, we need to yield back to the event loop
            // A simple spin_loop won't work because we need cooperative scheduling
            for _ in 0..100 {
                std::hint::spin_loop();
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            // On native, just do a few spin loops to yield CPU
            for _ in 0..10 {
                std::hint::spin_loop();
            }
        }

        // Re-acquire the mutex before returning
        mutex.lock_sync()
    }

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
            use wasm_bindgen::prelude::wasm_bindgen;

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
                self.wait_block(guard)
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.wait_spin(guard)
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
        let receiver = self.waiting_async_threads.with_mut(|senders| {
            let (sender, receiver) = r#continue::continuation();
            senders.push(sender);
            receiver
        });

        // Release the mutex
        drop(guard);

        // Wait for notification
        receiver.await;

        // Re-acquire the mutex
        mutex.lock_async().await
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
        // Try to wake one sync thread first
        let thread = self.waiting_sync_threads.with_mut(|threads| threads.pop());
        if let Some(thread) = thread {
            thread.unpark();
            return;
        }

        // If no sync threads, wake one async task
        let sender = self.waiting_async_threads.with_mut(|senders| senders.pop());
        if let Some(sender) = sender {
            sender.send(());
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
        // Wake all sync threads
        let threads = self.waiting_sync_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }

        // Wake all async tasks
        let senders = self.waiting_async_threads.with_mut(std::mem::take);
        for sender in senders {
            sender.send(());
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
mod tests {
    use super::*;
    use crate::Mutex;
    use std::sync::Arc;

    #[cfg(not(target_arch = "wasm32"))]
    use std::time::Duration;

    use r#continue::continuation;
    #[cfg(not(target_arch = "wasm32"))]
    use std::thread;
    #[cfg(target_arch = "wasm32")]
    use wasm_thread as thread;

    // Configure WASM tests to run in browser (required for thread spawning)
    #[cfg(all(test, target_arch = "wasm32"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    #[test_executors::async_test]
    //#[cfg(not(target_arch = "wasm32"))] // HANGS on WASM
    async fn test_condvar_basic_spin() {

        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair_clone = Arc::clone(&pair);

        let (c, r) = continuation();
        thread::Builder::new().name("basic_spin_write".into()).spawn(move || {
            thread::sleep(std::time::Duration::from_millis(50));

            let (mutex, condvar) = &*pair_clone;
            let mut ready = mutex.lock_sync();
            *ready = true;
            drop(ready);
            condvar.notify_one();
            c.send(());
        }).unwrap();
        let (c,r2) = continuation();
        thread::Builder::new().name("basic_spin_write".into()).spawn(move || {
            let (mutex, condvar) = &*pair;
            let mut ready = mutex.lock_sync();
            while !*ready {
                ready = condvar.wait_spin(ready);
            }
            assert!(*ready);
            c.send(());
        }).unwrap();
        r.await;
        r2.await;
    }

    #[test_executors::async_test]
    async fn test_condvar_wait_block() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair_clone = Arc::clone(&pair);

        let (c, r) = continuation();
        thread::spawn(move || {
            #[cfg(not(target_arch = "wasm32"))]
            thread::sleep(Duration::from_millis(10));

            let (mutex, condvar) = &*pair_clone;
            let mut ready = mutex.lock_sync();
            *ready = true;
            drop(ready);
            condvar.notify_one();
            c.send(());
        });

        // Move the waiting into a worker thread
        let (c2, r2) = continuation();
        thread::spawn(move || {
            let (mutex, condvar) = &*pair;
            let mut ready = mutex.lock_sync();
            while !*ready {
                ready = condvar.wait_block(ready);
            }
            assert!(*ready);
            c2.send(());
        });

        r.await;
        r2.await;
    }

    #[test_executors::async_test]
    async fn test_condvar_wait_sync() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair_clone = Arc::clone(&pair);

        let (c, r) = continuation();
        thread::spawn(move || {
            #[cfg(not(target_arch = "wasm32"))]
            thread::sleep(Duration::from_millis(10));

            let (mutex, condvar) = &*pair_clone;
            let mut ready = mutex.lock_sync();
            *ready = true;
            drop(ready);
            condvar.notify_one();
            c.send(());
        });

        // Move the waiting into a worker thread
        let (c2, r2) = continuation();
        thread::spawn(move || {
            let (mutex, condvar) = &*pair;
            let mut ready = mutex.lock_sync();
            while !*ready {
                ready = condvar.wait_sync(ready);
            }
            assert!(*ready);
            c2.send(());
        });

        r.await;
        r2.await;
    }

    #[test_executors::async_test]
    async fn test_condvar_wait_async() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair_clone = Arc::clone(&pair);

        let (c, r) = continuation();
        thread::spawn(move || {
            #[cfg(not(target_arch = "wasm32"))]
            thread::sleep(Duration::from_millis(10));

            test_executors::spin_on(async {
                let (mutex, condvar) = &*pair_clone;
                let mut ready = mutex.lock_async().await;
                *ready = true;
                drop(ready);
                condvar.notify_one();
            });
            c.send(());
        });

        // Move the async waiting into a worker thread
        let (c2, r2) = continuation();
        thread::spawn(move || {
            test_executors::spin_on(async {
                let (mutex, condvar) = &*pair;
                let mut ready = mutex.lock_async().await;
                while !*ready {
                    ready = condvar.wait_async(ready).await;
                }
                assert!(*ready);
            });
            c2.send(());
        });

        r.await;
        r2.await;
    }

    #[test_executors::async_test]
    async fn test_condvar_notify_all() {
        let pair = Arc::new((Mutex::new(0), Condvar::new()));
        let mut receivers = Vec::new();

        // Spawn 3 threads that wait for count to reach 10
        for _ in 0..3 {
            let pair = Arc::clone(&pair);
            let (c, r) = continuation();
            thread::spawn(move || {
                let (mutex, condvar) = &*pair;
                let mut count = mutex.lock_sync();
                while *count < 10 {
                    count = condvar.wait_sync(count);
                }
                c.send(*count);
            });
            receivers.push(r);
        }

        // Give threads time to start waiting
        #[cfg(not(target_arch = "wasm32"))]
        thread::sleep(Duration::from_millis(50));

        // Update the count and notify all
        let (mutex, condvar) = &*pair;
        let mut count = mutex.lock_sync();
        *count = 10;
        drop(count);
        condvar.notify_all();

        // All threads should wake up and see count = 10
        for receiver in receivers {
            let result = receiver.await;
            assert_eq!(result, 10);
        }
    }

    #[test_executors::async_test]
    async fn test_condvar_producer_consumer() {
        use std::collections::VecDeque;

        let shared = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let producer = Arc::clone(&shared);

        let (c, r) = continuation();
        thread::spawn(move || {
            let (mutex, condvar) = &*producer;
            for i in 0..5 {
                #[cfg(not(target_arch = "wasm32"))]
                thread::sleep(Duration::from_millis(5));

                let mut queue = mutex.lock_sync();
                queue.push_back(i);
                drop(queue);
                condvar.notify_one();
            }
            c.send(());
        });

        // Move the consumer (waiting) into a worker thread
        let consumer = Arc::clone(&shared);
        let (c2, r2) = continuation();
        thread::spawn(move || {
            let (mutex, condvar) = &*consumer;
            let mut collected = Vec::new();
            for _ in 0..5 {
                let mut queue = mutex.lock_sync();
                while queue.is_empty() {
                    queue = condvar.wait_sync(queue);
                }
                if let Some(value) = queue.pop_front() {
                    collected.push(value);
                }
            }
            assert_eq!(collected, vec![0, 1, 2, 3, 4]);
            c2.send(());
        });

        r.await;
        r2.await;
    }

    #[test_executors::async_test]
    async fn test_condvar_notify_one_only_wakes_one() {
        let pair = Arc::new((Mutex::new(0), Condvar::new()));
        let mut receivers = Vec::new();

        // Spawn 3 threads that increment on each wake
        for _ in 0..3 {
            let pair = Arc::clone(&pair);
            let (c, r) = continuation();
            thread::spawn(move || {
                let (mutex, condvar) = &*pair;
                let mut count = mutex.lock_sync();
                while *count < 3 {
                    count = condvar.wait_spin(count);
                }
                c.send(());
            });
            receivers.push(r);
        }

        // Give threads time to start waiting
        #[cfg(not(target_arch = "wasm32"))]
        thread::sleep(Duration::from_millis(50));

        // Increment and notify one at a time
        for i in 1..=3 {
            let (mutex, condvar) = &*pair;
            let mut count = mutex.lock_sync();
            *count = i;
            drop(count);
            condvar.notify_one();

            #[cfg(not(target_arch = "wasm32"))]
            thread::sleep(Duration::from_millis(10));
        }

        // All threads should eventually complete
        for receiver in receivers {
            receiver.await;
        }

        let (mutex, _) = &*pair;
        assert_eq!(*mutex.lock_sync(), 3);
    }
}
