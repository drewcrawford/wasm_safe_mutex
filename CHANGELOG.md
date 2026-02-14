# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Switched WASM threading integration to `wasm_safe_thread` for runtime behavior tailored to this crate's portability goals
- Refreshed crate and README documentation to keep examples and release guidance aligned with current usage

## [0.2.0] - 2026-02-01

### Added

- `WaitTimeoutResult` now implements `Default` and `Hash` traits for better ergonomics
- MPSC error types (`RecvError`, `RecvTimeoutError`, `TryRecvError`) now implement `Error`, `Display`, and `Hash` traits
- `IntoIter<T>` now implements `Debug` for better diagnostics

### Changed

- MPSC error enums are now marked `#[non_exhaustive]` for future extensibility—handle those match arms defensively!
  - This is a breaking change.
- Improved documentation throughout, especially for `_block` methods
- Enhanced test behaviors for better reliability
- Updated CI configuration for more robust builds
- Bumped dependencies to latest compatible versions

### Fixed

- Disabled certain doctests on `wasm32` targets where `std::thread` is not supported—no more confusing test failures in WASM land

## [0.1.2] - 2025-11-28

A quick dependency housekeeping stop—nothing to see here, move along!

### Changed

- Relaxed `wasm-bindgen` dependency constraint to allow version 0.2.106 (previously pinned to exactly 0.2.105)—because sometimes newer is better, and we trust the semver gods

No API changes, no behavior changes, just a little more breathing room for your dependency tree.

## [0.1.1] - 2025-11-21

This release brings three major new synchronization primitives to the family, making `wasm_safe_mutex` a comprehensive concurrency toolkit for WebAssembly.

### Added

#### Condition Variables (`Condvar`)
The mutex now has a friend to wait with! Full condition variable support with multiple wait strategies that adapt to your environment:

- `wait_spin` / `wait_spin_while` / `wait_spin_until` - Busy-waits for the condition (main thread friendly)
- `wait_block` / `wait_block_while` / `wait_block_until` - Parks the thread when available (efficient on workers)
- `wait_sync` / `wait_sync_while` / `wait_sync_until` - Automatically picks the best strategy for your context
- `wait_async` / `wait_async_while` / `wait_async_until` - Async/await compatible waiting
- `notify_one` / `notify_all` - Wake up waiting threads when conditions change

#### Read-Write Lock (`RwLock`)
When you need many readers but only one writer, we've got you covered:

- `lock_sync_read` / `lock_sync_write` - Adaptive blocking that works everywhere
- `lock_async_read` / `lock_async_write` - Async variants for the `await` enthusiasts
- `try_read` / `try_write` - Non-blocking attempts for the impatient
- Multiple readers can hold the lock simultaneously, while writers get exclusive access

#### MPSC Channel
Message passing between threads, now with that WASM-safe goodness:

- `channel()` - Create a new channel with sender and receiver
- `send` / `recv` - Send messages across threads
- `try_recv` - Non-blocking receive for when you can't wait around
- Hangup detection - Know when the other end has disconnected

#### Mutex Enhancements
The OG mutex learned some new tricks:

- `lock_spin_until` / `lock_block_until` / `lock_sync_until` - Timeout variants so you're not waiting forever
- `lock_async_until` - Async timeout support

### Changed

- Reorganized internal code structure for better maintainability (split mutex, rwlock, and condvar into dedicated modules)
- Documentation improvements throughout

### Fixed

- Various spinlock edge cases that could cause issues under contention
- Write behavior correctness in RwLock

### Internal

- Added SPDX license headers to all source files
- Updated CI matrix for better test coverage
- Addressed clippy suggestions for cleaner code

## [0.1.0] - Initial Release

The beginning! A WebAssembly-safe mutex that automatically adapts its locking strategy based on the runtime environment.

- `Mutex` with adaptive locking (spin on main thread, block on workers)
- `Guard` for RAII-style lock management
- Basic spinlock implementation
- Support for both native and WASM targets

[Unreleased]: https://github.com/drewcrawford/wasm_safe_mutex/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/drewcrawford/wasm_safe_mutex/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/drewcrawford/wasm_safe_mutex/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/drewcrawford/wasm_safe_mutex/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/drewcrawford/wasm_safe_mutex/releases/tag/v0.1.0
