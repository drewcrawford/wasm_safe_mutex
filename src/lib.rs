mod spinlock;

use crate::spinlock::Spinlock;
use std::cell::UnsafeCell;
use std::sync::Condvar;
use std::sync::atomic::AtomicBool;
use std::thread;

pub struct Guard<'a, T> {
    mutex: &'a Mutex<T>,
    data: &'a mut T,
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

pub struct NotAvailable;

pub struct Mutex<T> {
    inner: UnsafeCell<T>,
    data_lock: AtomicBool,
    waiting_sync_threads: Spinlock<Vec<thread::Thread>>,
    waiting_async_threads: Spinlock<Vec<r#continue::Sender<()>>>,
}
impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Mutex {
            inner: UnsafeCell::new(value),
            data_lock: AtomicBool::new(false),
            waiting_sync_threads: Spinlock::new(vec![]),
            waiting_async_threads: Spinlock::new(vec![]),
        }
    }

    pub fn try_lock(&self) -> Result<Guard<'_, T>, NotAvailable> {
        if self
            .data_lock
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            let data = unsafe { &mut *self.inner.get() };
            Ok(Guard { mutex: self, data })
        } else {
            Err(NotAvailable)
        }
    }

    pub fn lock_spin(&self) -> Guard<'_, T> {
        // Spin until we can acquire the lock
        while self
            .data_lock
            .swap(true, std::sync::atomic::Ordering::Acquire)
        {
            std::hint::spin_loop();
        }
        // SAFETY: We have exclusive access to the data now
        let data = unsafe { &mut *self.inner.get() };
        Guard { mutex: self, data }
    }

    pub fn lock_block(&self) -> Guard<'_, T> {
        //insert our thread into the waiting list
        loop {
            let r = self.waiting_sync_threads.with_mut(|threads| {
                match self.try_lock() {
                    Ok(guard) => {
                        // Return the guard
                        Ok(guard)
                    }
                    Err(_) => {
                        let handle = thread::current();
                        threads.push(handle);
                        Err(NotAvailable)
                    }
                }
            });
            match r {
                Ok(guard) => return guard,
                Err(NotAvailable) => thread::park(),
            }
        }
    }

    pub async fn lock_async(&self) -> Guard<'_, T> {
        loop {
            let a = self.waiting_async_threads.with_mut(|senders| {
                match self.try_lock() {
                    Ok(guard) => Ok(guard),
                    Err(NotAvailable) => {
                        // Create a new channel to signal when the lock is available
                        let (sender, receiver) = r#continue::continuation();
                        senders.push(sender);
                        Err(receiver)
                    }
                }
            });
            match a {
                Ok(guard) => return guard,
                Err(receiver) => {
                    // Wait for the signal that the lock is available
                    receiver.await;
                }
            }
        }
    }
    fn did_unlock(&self) {
        //pop the waiting threads
        let threads = self
            .waiting_sync_threads
            .with_mut(|threads| threads.drain(..).collect::<Vec<_>>());
        for thread in threads {
            // Wake up the thread
            thread.unpark();
        }
        // Notify any async tasks waiting on this mutex
        let senders = self
            .waiting_async_threads
            .with_mut(|senders| senders.drain(..).collect::<Vec<_>>());
        for sender in senders {
            // Send a signal to wake up the async task
            sender.send(());
        }
    }

    /**
    Performs an appropriate lock operation based on the context.

    Usually this is implemented with [lock_block], but in contexts where
    blocking is forbidden, such as the main thread on wasm32, it is implemented with
    [lock_spin].
    */
    pub fn lock_sync(&self) -> Guard<'_, T> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.lock_block()
        }
        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::prelude::wasm_bindgen;
            //check if we're on the main thread
            #[wasm_bindgen(inline_js = "
export function supportsAtomicsWait() {
    if (typeof SharedArrayBuffer === 'undefined') return false;
    if (typeof Atomics === 'undefined' || typeof Atomics.wait !== 'function') return false;

    try {
        const sab = new SharedArrayBuffer(4);
        const ia = new Int32Array(sab);
        const result = Atomics.wait(ia, 0, 0, 0);
        return result === 'timed-out' || result === 'not-equal';
    } catch (_) {
        return false;
    }
}
")]
            extern "C" {
                fn supportsAtomicsWait() -> bool;
            }

            if supportsAtomicsWait() {
                self.lock_block()
            } else {
                // Fallback to spin lock if Atomics.wait is not supported
                self.lock_spin()
            }
        }
    }
}
