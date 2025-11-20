use crate::{Mutex, condvar::Condvar};
use std::collections::VecDeque;
use std::sync::Arc;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

/// The shared state of the channel.
struct Shared<T> {
    queue: Mutex<VecDeque<T>>,
    condvar: Condvar,
}

/// The sending half of the channel.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// The receiving half of the channel.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

/// Creates a new asynchronous channel, returning the sender/receiver halves.
///
/// All data sent on the `Sender` will become available on the `Receiver` in
/// the same order as it was sent, and no `send` will block the calling thread
/// (this channel has an "infinite buffer", unlike `sync_channel`, which will
/// block after its buffer limit is reached). `recv` will block until a message
/// is available.
///
/// The `Sender` can be cloned to `send` to the same channel multiple times, but
/// only one `Receiver` is supported.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::new()),
        condvar: Condvar::new(),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

impl<T> Sender<T> {
    /// Sends a value on this channel, spinning if the lock is contended.
    pub fn send_spin(&self, t: T) {
        let mut queue = self.shared.queue.lock_spin();
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
    }

    /// Sends a value on this channel, blocking if the lock is contended.
    pub fn send_block(&self, t: T) {
        let mut queue = self.shared.queue.lock_block();
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
    }

    /// Sends a value on this channel, using the appropriate strategy for the platform.
    pub fn send_sync(&self, t: T) {
        let mut queue = self.shared.queue.lock_sync();
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
    }

    /// Sends a value on this channel asynchronously.
    pub async fn send_async(&self, t: T) {
        let mut queue = self.shared.queue.lock_async().await;
        queue.push_back(t);
        drop(queue);
        self.shared.condvar.notify_one();
    }
}

impl<T> Receiver<T> {
    /// Receives a value from the channel, spinning if empty.
    pub fn recv_spin(&self) -> T {
        let mut queue = self.shared.queue.lock_spin();
        loop {
            if let Some(t) = queue.pop_front() {
                return t;
            }
            queue = self.shared.condvar.wait_spin(queue);
        }
    }

    /// Receives a value from the channel, blocking if empty.
    pub fn recv_block(&self) -> T {
        let mut queue = self.shared.queue.lock_block();
        loop {
            if let Some(t) = queue.pop_front() {
                return t;
            }
            queue = self.shared.condvar.wait_block(queue);
        }
    }

    /// Receives a value from the channel, using the appropriate strategy for the platform.
    pub fn recv_sync(&self) -> T {
        let mut queue = self.shared.queue.lock_sync();
        loop {
            if let Some(t) = queue.pop_front() {
                return t;
            }
            queue = self.shared.condvar.wait_sync(queue);
        }
    }

    /// Receives a value from the channel asynchronously.
    pub async fn recv_async(&self) -> T {
        let mut queue = self.shared.queue.lock_async().await;
        loop {
            if let Some(t) = queue.pop_front() {
                return t;
            }
            queue = self.shared.condvar.wait_async(queue).await;
        }
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Send for Receiver<T> {}

#[cfg(test)]
mod tests;
