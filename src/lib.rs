mod spinlock;

use crate::spinlock::Spinlock;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicBool;
use std::thread;

pub struct Guard<'a, T> {
    mutex: &'a Mutex<T>,
    data: &'a mut T,
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

#[derive(Debug)]
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

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[cfg(not(target_arch = "wasm32"))]
    use std::time::Duration;

    #[cfg(target_arch = "wasm32")]
    use wasm_thread as thread;
    #[cfg(not(target_arch = "wasm32"))]
    use std::thread;
    use r#continue::continuation;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[test]
    fn test_spinlock_basic() {
        let spinlock = Spinlock::new(42);
        let result = spinlock.with_mut(|data| {
            *data += 1;
            *data
        });
        assert_eq!(result, 43);
    }
    #[test_executors::async_test]
    async fn test_spinlock_concurrent_access() {
        let spinlock = Arc::new(Spinlock::new(0));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let spinlock = Arc::clone(&spinlock);
                let (c,r) = continuation();
                thread::spawn(move || {
                    for _ in 0..100 {
                        spinlock.with_mut(|data| *data += 1);
                    }
                    c.send(());
                });
                r
            })
            .collect();

        for h in handles {
            h.await;
        }
        assert_eq!(spinlock.with_mut(|data| *data), 1000);

    }
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[test]
    fn test_mutex_try_lock_success() {
        let mutex = Mutex::new(42);
        let guard = mutex.try_lock().unwrap();
        assert_eq!(*guard, 42);
    }
    #[test_executors::async_test]
    async fn test_mutex_try_lock_contention() {
        //for the time being, wasm_thread only works in browser
        //see https://github.com/rustwasm/wasm-bindgen/issues/4534,
        //though we also need wasm_thread support.
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
        let mutex = Arc::new(Mutex::new(42));
        let guard = mutex.try_lock().unwrap();
        
        let mutex_clone = Arc::clone(&mutex);
        let (c, r) = continuation();
        thread::spawn(move || {
            let failed = mutex_clone.try_lock().is_err();
            c.send(failed);
        });
        
        let failed = r.await;
        assert!(failed);
        drop(guard);
    }
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[test]
    fn test_mutex_lock_spin() {
        let mutex = Mutex::new(0);
        let mut guard = mutex.lock_spin();
        *guard = 42;
        drop(guard);
        
        let guard = mutex.lock_spin();
        assert_eq!(*guard, 42);
    }
    #[test_executors::async_test]
    async fn test_mutex_lock_block() {
        //for the time being, wasm_thread only works in browser
        //see https://github.com/rustwasm/wasm-bindgen/issues/4534,
        //though we also need wasm_thread support.
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
        let mutex = Arc::new(Mutex::new(0));
        let mutex_clone = Arc::clone(&mutex);
        
        let (c, r) = continuation();
        thread::spawn(move || {
            let mut guard = mutex_clone.lock_block();
            *guard = 42;
            // Don't use thread::sleep in WASM as it calls Atomics.wait
            #[cfg(not(target_arch = "wasm32"))]
            thread::sleep(Duration::from_millis(10));
            c.send(());
        });
        
        // Wait for the spawned thread to complete first
        r.await;
        
        // Don't use thread::sleep in WASM as it calls Atomics.wait  
        #[cfg(not(target_arch = "wasm32"))]
        thread::sleep(Duration::from_millis(5));
        
        let guard = mutex.lock_block();
        assert_eq!(*guard, 42);
    }
    #[test_executors::async_test]
    async fn test_mutex_concurrent_increment() {
        //for the time being, wasm_thread only works in browser
        //see https://github.com/rustwasm/wasm-bindgen/issues/4534,
        //though we also need wasm_thread support.
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

        let mutex = Arc::new(Mutex::new(0));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let mutex = Arc::clone(&mutex);
                let (c, r) = continuation();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let mut guard = mutex.lock_spin();
                        *guard += 1;
                    }
                    c.send(());
                });
                r
            })
            .collect();

        for handle in handles {
            handle.await;
        }

        let guard = mutex.lock_spin();
        assert_eq!(*guard, 1000);
    }
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[test]
    fn test_mutex_lock_async() {
        test_executors::spin_on(async {
            let mutex = Mutex::new(42);
            let guard = mutex.lock_async().await;
            assert_eq!(*guard, 42);
        });
    }
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[test]
    fn test_mutex_async_contention() {
        test_executors::spin_on(async {
            let mutex = Arc::new(Mutex::new(0));
            
            let mutex1 = Arc::clone(&mutex);
            let task1 = async move {
                let mut guard = mutex1.lock_async().await;
                *guard += 1;
                drop(guard);
            };
            
            let mutex2 = Arc::clone(&mutex);
            let task2 = async move {
                let mut guard = mutex2.lock_async().await;
                *guard += 10;
            };
            
            task1.await;
            task2.await;
            
            let guard = mutex.lock_async().await;
            assert_eq!(*guard, 11);
        });
    }
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[test]
    fn test_guard_drop_releases_lock() {
        let mutex = Arc::new(Mutex::new(42));
        {
            let _guard = mutex.lock_spin();
        }
        
        let guard = mutex.try_lock().unwrap();
        assert_eq!(*guard, 42);
    }
}