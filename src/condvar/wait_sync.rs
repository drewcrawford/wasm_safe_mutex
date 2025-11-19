use super::{Condvar, WaitTimeoutResult};
use super::Instant;
use crate::Guard;

#[cfg(target_arch = "wasm32")]
use super::atomics_wait_supported;

impl Condvar {
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
    ///     (ready, result) = condvar.wait_sync_timeout(ready, deadline);
    ///     if result.timed_out() {
    ///         break;
    ///     }
    /// }
    /// assert!(*ready);
    /// ```
    pub fn wait_sync_timeout<'a, T>(
        &self,
        guard: Guard<'a, T>,
        deadline: Instant,
    ) -> (Guard<'a, T>, WaitTimeoutResult) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.wait_block_timeout(guard, deadline)
        }
        #[cfg(target_arch = "wasm32")]
        {
            if atomics_wait_supported() {
                self.wait_block_timeout(guard, deadline)
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.wait_spin_timeout(guard, deadline)
            }
        }
    }

    /// Automatically waits while the predicate is `true`, bounded by the deadline,
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
    /// let (guard, result) = condvar.wait_sync_timeout_while(guard, deadline, |v| *v < 10);
    /// if !result.timed_out() {
    ///     assert_eq!(*guard, 10);
    /// }
    /// ```
    pub fn wait_sync_timeout_while<'a, T, F>(
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
            self.wait_block_timeout_while(guard, deadline, condition)
        }
        #[cfg(target_arch = "wasm32")]
        {
            if atomics_wait_supported() {
                self.wait_block_timeout_while(guard, deadline, condition)
            } else {
                self.wait_spin_timeout_while(guard, deadline, condition)
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
}
