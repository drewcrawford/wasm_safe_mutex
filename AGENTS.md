# Repository Guidelines

## Project Structure & Module Organization
- `src/lib.rs` exposes the public API (Mutex, RwLock, Condvar, mpsc, Spinlock) and crate docs.
- Each primitive has a top-level module plus a helper directory (`src/mutex.rs` + `src/mutex/`, `src/rwlock.rs` + `src/rwlock/`, etc.); shared pieces live in `src/guard.rs` and `src/spinlock.rs`.
- Tests reside in `src/tests.rs`; assets in `art/`; licensing and metadata at the repo root.

## Build, Test, and Development Commands
- `cargo build` — compile for the host target.
- `cargo test` — run host tests (sync + `test_executors` async).
- `cargo +nightly test --target wasm32-unknown-unknown` — exercise the WASM path; `wasm_bindgen_test` will drive browser-based runs when configured.
- `cargo check` for quick validation; `cargo doc --no-deps` to preview docs; `cargo clippy --all-targets --all-features` to lint.

## Coding Style & Naming Conventions
- Rust 2024 edition; format with `cargo fmt` (4-space indent, trailing commas enabled). Prefer snake_case modules/items and concise, behavior-focused names.
- Method suffixes communicate strategy: `_sync` (auto chooses best), `_block`, `_spin`, `_async`, and `_timeout` variants—follow these when adding APIs.
- On `wasm32`, prefer `web_time::{Instant, Duration}` over `std::time`; keep platform-specific imports behind `#[cfg]`.
- Avoid blocking the WASM main thread; use RAII guards and helper methods instead of manual flag juggling.

## Testing Guidelines
- Use `#[test]` for sync cases and `#[test_executors::async_test]` for async; annotate WASM-aware tests with `#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]`.
- For browser/WASM concurrency, move work into spawned threads/tasks and await handles; do not spin the main thread while waiting for workers.
- Prefer deterministic deadlines and the existing `_timeout` helpers; avoid `thread::sleep` on WASM (use spin loops with `Instant` when needed).
- Name tests `test_<behavior>` and keep them cross-platform unless explicitly `cfg`-gated.

## Commit & Pull Request Guidelines
- Recent history favors short, imperative summaries (e.g., “Add docs”, “Update ci matrix”); keep one concern per commit and run relevant tests before pushing.
- PRs should describe behavior changes, note which commands were run (`cargo test`, WASM target when applicable), and call out platform-sensitive logic (main thread vs worker) or time-handling choices.
- Link related issues and include logs or screenshots for browser-based runs when debugging WASM.
