// SPDX-License-Identifier: MIT OR Apache-2.0
//! A suite of WebAssembly-safe synchronization primitives that paper over platform-specific locking constraints.
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
//! This crate provides synchronization primitives that automatically adapt their
//! locking strategy based on the runtime environment:
//!
//! - **Native (any thread)**: Uses efficient thread parking (`thread::park`)
//! - **WASM worker threads**: Uses `Atomics.wait` when available
//! - **WASM main thread**: Falls back to spinning (non-blocking busy-wait)
//!
//! This means you can write code once and have it work correctly across all platforms,
//! without worrying about whether you're on the main thread, a worker thread, native or WASM.
//!
//! # Primitives
//!
//! This crate provides the following primitives, all of which support the adaptive behavior:
//!
//! - **[`Mutex`]**: A mutual exclusion primitive for protecting shared data.
//! - **[`RwLock`](rwlock::RwLock)**: A reader-writer lock that allows multiple concurrent readers or one exclusive writer.
//! - **[`Condvar`](condvar::Condvar)**: A condition variable for blocking a thread while waiting for an event.
//! - **[`mpsc`](mpsc)**: A multi-producer, single-consumer channel for message passing.
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
//! ## Mutex
//!
//! ```
//! use wasm_safe_mutex::Mutex;
//!
//! let mutex = Mutex::new(42);
//! let mut guard = mutex.lock_sync();
//! *guard = 100;
//! drop(guard);
//!
//! assert_eq!(*mutex.lock_sync(), 100);
//! ```
//!
//! ## RwLock
//!
//! ```
//! use wasm_safe_mutex::rwlock::RwLock;
//!
//! let rwlock = RwLock::new(vec![1, 2, 3]);
//!
//! // Multiple readers
//! let r1 = rwlock.lock_sync_read();
//! let r2 = rwlock.lock_sync_read();
//! assert_eq!(r1.len(), 3);
//! assert_eq!(r2.len(), 3);
//! drop(r1);
//! drop(r2);
//!
//! // Exclusive writer
//! let mut w = rwlock.lock_sync_write();
//! w.push(4);
//! ```
//!
//! ## Async Usage
//!
//! ```
//! # test_executors::spin_on(async {
//! use wasm_safe_mutex::Mutex;
//!
//! let mutex = Mutex::new(0);
//!
//! // Async lock works everywhere, including WASM main thread
//! let mut guard = mutex.lock_async().await;
//! *guard += 1;
//! # });
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
