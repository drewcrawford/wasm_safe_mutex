# wasm_safe_mutex

![logo](art/logo.png)

A WebAssembly-safe mutex that papers over platform-specific locking constraints.

## The Core Problem

**WebAssembly's main thread cannot use blocking locks** - attempting to do so will panic with "cannot block on the main thread". This is a fundamental limitation of the browser environment where blocking the main thread would freeze the entire UI.

However, blocking locks ARE allowed in:
- **WebAssembly worker threads** (where `Atomics.wait` is available)
- **Native platforms** (both main and worker threads)
- **Most non-WASM contexts** (traditional OS threads have no such restrictions)

## The Solution

This crate papers over all these platform differences by automatically adapting its locking strategy based on the runtime environment:

- **Native (any thread)**: Uses efficient thread parking (`thread::park`)
- **WASM worker threads**: Uses `Atomics.wait` when available
- **WASM main thread**: Falls back to spinning (non-blocking busy-wait)

This means you can write code once and have it work correctly across all platforms, without worrying about whether you're on the main thread, a worker thread, native or WASM.

## Features

- **Transparent adaptation**: Automatically detects and uses the best available locking mechanism
- **Main thread safe**: Won't panic on WASM main thread (uses spinning instead)
- **Worker thread optimized**: Uses proper blocking when available for efficiency
- **Native performance**: Full thread parking on native platforms
- **Async support**: Non-blocking async methods that work everywhere
- **Multiple strategies**: Try-lock, spin-lock, blocking lock, and async lock

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
wasm_safe_mutex = "0.1.0"
```

## Examples

### Basic Usage

```rust
use wasm_safe_mutex::Mutex;

let mutex = Mutex::new(42);
let mut guard = mutex.lock_sync();
*guard = 100;
drop(guard);

// Value has been updated
let guard = mutex.lock_sync();
assert_eq!(*guard, 100);
```

### Try Lock

```rust
use wasm_safe_mutex::{Mutex, NotAvailable};

let mutex = Mutex::new("data");

// First lock succeeds
let guard = mutex.try_lock().unwrap();
assert_eq!(*guard, "data");

// Second lock fails while first is held
let result = mutex.try_lock();
assert!(matches!(result, Err(NotAvailable)));
```

### Async Usage

```rust
use wasm_safe_mutex::Mutex;

let mutex = Mutex::new(vec![1, 2, 3]);

// Async lock doesn't block the executor
let mut guard = mutex.lock_async().await;
guard.push(4);
drop(guard);

// Using the convenience method
let sum = mutex.with_async(|data| data.iter().sum::<i32>()).await;
assert_eq!(sum, 10);
```

### Thread-Safe Sharing

```rust
use wasm_safe_mutex::Mutex;
use std::sync::Arc;
use std::thread;

let mutex = Arc::new(Mutex::new(0));
let handles: Vec<_> = (0..4)
    .map(|_| {
        let mutex = Arc::clone(&mutex);
        thread::spawn(move || {
            for _ in 0..25 {
                mutex.with_mut_sync(|value| *value += 1);
            }
        })
    })
    .collect();

for handle in handles {
    handle.join().unwrap();
}

assert_eq!(*mutex.lock_sync(), 100);
```

## API Overview

The `Mutex<T>` type provides multiple locking strategies:

### Locking Methods

- **`try_lock()`**: Non-blocking attempt to acquire the lock
- **`lock_spin()`**: Spin-wait until the lock is acquired
- **`lock_block()`**: Blocks on native/WASM workers, spins on WASM main thread
- **`lock_sync()`**: Automatically chooses the right strategy for your platform (recommended)
- **`lock_async()`**: Always non-blocking, works everywhere including WASM main thread

### Convenience Methods

- **`with_sync()`**: Execute a read-only closure with the lock
- **`with_mut_sync()`**: Execute a mutable closure with the lock
- **`with_async()`**: Execute a read-only closure with the lock asynchronously
- **`with_mut_async()`**: Execute a mutable closure with the lock asynchronously

## Platform Behavior

The mutex transparently handles platform differences:

- **Native (main or worker thread)**: Full blocking with thread parking
- **WASM worker threads**: Blocks using `Atomics.wait`
- **WASM main thread**: Spins to avoid "cannot block on main thread" panic

This automatic adaptation means your code works everywhere without modification.
