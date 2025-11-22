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
pub mod mpsc;
pub mod mutex;
pub mod rwlock;
pub mod spinlock;

#[cfg(test)]
mod tests;

pub use crate::guard::Guard;
pub use mutex::{Mutex, NotAvailable};
