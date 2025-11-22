# wasm_safe_mutex

![logo](art/logo.png)

A suite of WebAssembly-safe synchronization primitives that paper over platform-specific locking constraints.

## The Core Problem

**WebAssembly's main thread cannot use blocking locks** - attempting to do so will panic with "cannot block on the main thread". This is a fundamental limitation of the browser environment where blocking the main thread would freeze the entire UI.

However, blocking locks ARE allowed in:
- **WebAssembly worker threads** (where `Atomics.wait` is available)
- **Native platforms** (both main and worker threads)
- **Most non-WASM contexts** (traditional OS threads have no such restrictions)

## The Solution

This crate provides synchronization primitives that automatically adapt their locking strategy based on the runtime environment:

- **Native (any thread)**: Uses efficient thread parking (`thread::park`)
- **WASM worker threads**: Uses `Atomics.wait` when available
- **WASM main thread**: Falls back to spinning (non-blocking busy-wait)

This means you can write code once and have it work correctly across all platforms, without worrying about whether you're on the main thread, a worker thread, native or WASM.

## Primitives

This crate provides the following primitives, all of which support the adaptive behavior:

- **`Mutex`**: A mutual exclusion primitive for protecting shared data.
- **`RwLock`**: A reader-writer lock that allows multiple concurrent readers or one exclusive writer.
- **`Condvar`**: A condition variable for blocking a thread while waiting for an event.
- **`mpsc`**: A multi-producer, single-consumer channel for message passing.

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

### Mutex

```rust
use wasm_safe_mutex::Mutex;

let mutex = Mutex::new(42);
let mut guard = mutex.lock_sync();
*guard = 100;
drop(guard);

assert_eq!(*mutex.lock_sync(), 100);
```

### RwLock

```rust
use wasm_safe_mutex::rwlock::RwLock;

let rwlock = RwLock::new(vec![1, 2, 3]);

// Multiple readers
let r1 = rwlock.lock_sync_read();
let r2 = rwlock.lock_sync_read();
assert_eq!(r1.len(), 3);
assert_eq!(r2.len(), 3);
drop(r1);
drop(r2);

// Exclusive writer
let mut w = rwlock.lock_sync_write();
w.push(4);
```

### Async Usage

```rust
use wasm_safe_mutex::Mutex;

let mutex = Mutex::new(0);

// Async lock works everywhere, including WASM main thread
let mut guard = mutex.lock_async().await;
*guard += 1;
```

## Platform Behavior

The primitives transparently handle platform differences:

- **Native (main or worker thread)**: Full blocking with thread parking
- **WASM worker threads**: Blocks using `Atomics.wait`
- **WASM main thread**: Spins to avoid "cannot block on main thread" panic

This automatic adaptation means your code works everywhere without modification.
