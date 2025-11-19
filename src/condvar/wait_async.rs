use super::{AsyncWaiter, Condvar, WaitTimeoutResult, ASYNC_WAITER_ID_COUNTER};
use super::Instant;
use super::thread;
use crate::Guard;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

impl Condvar {
    /// Asynchronously waits for a notification from this condition variable.
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
    /// This loops on [`wait_async`] while `condition` evaluates to `true`.
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

    /// Asynchronously waits for a notification from this condition variable with a deadline.
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
    ///     (ready, result) = condvar.wait_async_timeout(ready, deadline).await;
    ///     if result.timed_out() {
    ///         break;
    ///     }
    /// }
    /// assert!(*ready);
    /// # });
    /// ```
    pub async fn wait_async_timeout<'a, T>(
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

    /// Asynchronously waits while the predicate remains `true`, bounded by the deadline.
    ///
    /// This loops on [`wait_async_timeout`] while `condition` evaluates to `true` and stops when it is `false` or timing out.
    pub async fn wait_async_timeout_while<'a, T, F>(
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
            (guard, result) = self.wait_async_timeout(guard, deadline).await;
            if result.timed_out() {
                return (guard, result);
            }
        }
        (guard, WaitTimeoutResult(false))
    }
}
