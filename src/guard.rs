//! Guard type for mutex locks.
//!
//! This module provides the `Guard` type that wraps access to mutex-protected data.

use crate::Mutex;

/// A guard that provides access to the data protected by a `Mutex`.
///
/// This guard is created by the locking methods on [`Mutex`]. When the guard
/// is dropped, the lock is automatically released, allowing other threads to
/// acquire it.
///
/// The guard implements `Deref` and `DerefMut`, allowing you to access the
/// protected data directly.
///
/// # Examples
///
/// ```
/// use wasm_safe_mutex::Mutex;
///
/// let mutex = Mutex::new(String::from("hello"));
/// 
/// // The guard provides access to the protected data
/// let mut guard = mutex.lock_sync();
/// guard.push_str(", world!");
/// assert_eq!(&*guard, "hello, world!");
/// 
/// // Lock is released when guard is dropped
/// drop(guard);
/// ```
pub struct Guard<'a, T> {
    pub(crate) mutex: &'a Mutex<T>,
    pub(crate) data: &'a mut T,
}

impl<T> std::ops::Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<T> std::ops::DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        // Release the lock
        self.mutex
            .data_lock
            .store(false, std::sync::atomic::Ordering::Release);
        // Notify any waiting threads
        self.mutex.did_unlock();
    }
}

// ================================================================================================
// Boilerplate trait implementations
// ================================================================================================

impl<T: std::fmt::Debug> std::fmt::Debug for Guard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("data", &**self)
            .finish_non_exhaustive()
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Guard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&**self, f)
    }
}

impl<T> AsRef<T> for Guard<'_, T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for Guard<'_, T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}