use std::cell::UnsafeCell;
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU8};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::thread;
use crate::{Guard, NotAvailable};
use crate::spinlock::Spinlock;

const UNLOCKED: u8 = 0;
const LOCKED_READ: u8 = 0b1;
const LOCKED_WRITE: u8 = 0b10000000;


pub struct RwLock<T> {
    inner: UnsafeCell<T>,
    data_lock: AtomicU8,
    waiting_sync_read_threads: Spinlock<Vec<thread::Thread>>,
    waiting_sync_write_threads: Spinlock<Vec<thread::Thread>>,
    waiting_async_read_threads: Spinlock<Vec<r#continue::Sender<()>>>,
    waiting_async_write_threads: Spinlock<Vec<r#continue::Sender<()>>>,

}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

pub struct ReadGuard<'a, T> {
    pub(crate) mutex: &'a RwLock<T>,
}

pub struct WriteGuard<'a, T> {
    pub(crate) mutex: &'a RwLock<T>,
}

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe{&*self.mutex.inner.get()}
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe {&mut*self.mutex.inner.get()}
    }
}


impl<'a, T> Deref for ReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe{&*self.mutex.inner.get()}
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        let r = self.mutex.data_lock.fetch_sub(1, Release);
        assert!(r > 0);
        self.mutex.did_unlock_read();
    }
}

impl <'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        let old = self.mutex.data_lock.swap(UNLOCKED, Release);
        assert!(old == LOCKED_WRITE);
        self.mutex.did_unlock_write();
    }
}


impl <T> RwLock<T> {
    pub const fn new(value: T) -> RwLock<T> {
        RwLock {
            inner: UnsafeCell::new(value),
            data_lock: AtomicU8::new(UNLOCKED),
            waiting_sync_read_threads: Spinlock::new(vec![]),
            waiting_async_read_threads: Spinlock::new(vec![]),
            waiting_sync_write_threads: Spinlock::new(vec![]),
            waiting_async_write_threads: Spinlock::new(vec![]),

        }
    }

    pub fn try_lock_read(&self) -> Result<ReadGuard<T>, NotAvailable> {
        let r = self.data_lock.fetch_update(Acquire, Relaxed, |f| {
            if f & LOCKED_WRITE != 0 {
                None
            } else if f == LOCKED_WRITE - 1 {
                panic!("Too many readers")
            } else {
                Some(f + 1)
            }
        });
        match r {
            Ok(e) => {
                Ok(ReadGuard { mutex: self})

            }
            Err(_) => {
                Err(NotAvailable)
            }
        }
    }

    pub fn try_lock_write(&self) -> Result<WriteGuard<T>, NotAvailable> {
        match self.data_lock.compare_exchange(UNLOCKED, LOCKED_WRITE, Acquire, Relaxed) {
            Ok(e) => {
                Ok(WriteGuard { mutex: self })
            }
            Err(_) => {
                Err(NotAvailable)
            }
        }
    }

    pub fn lock_spin_read(&self) -> ReadGuard<T> {
        // Spin until we can acquire the lock
        loop {
            let r = self.try_lock_read();
            match r {
                Ok(r) => {
                    return r;
                }
                Err(_) => {
                    std::hint::spin_loop();

                }
            }
        }
    }
    pub fn lock_spin_write(&self) -> WriteGuard<T> {
        // Spin until we can acquire the lock
        loop {
            let r = self.try_lock_write();
            match r {
                Ok(r) => {
                    return r;
                }
                Err(_) => {
                    std::hint::spin_loop();
                }
            }
        }
    }

    pub fn lock_block_read(&self) -> ReadGuard<'_, T> {
        //insert our thread into the waiting list
        loop {
            let r = self.waiting_sync_read_threads.with_mut(|threads| {
                match self.try_lock_read() {
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

    pub fn lock_block_write(&self) -> WriteGuard<'_, T> {
        loop {
            let r = self.waiting_sync_write_threads.with_mut(|threads| {
                match self.try_lock_write() {
                    Ok(guard) => {
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

    pub async fn lock_async_read(&self) -> ReadGuard<'_, T> {
        loop {
            let a = self.waiting_async_read_threads.with_mut(|senders| {
                match self.try_lock_read() {
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

    pub async fn lock_async_read_write(&self) -> WriteGuard<'_, T> {
        loop {
            let a = self.waiting_async_write_threads.with_mut(|senders| {
                match self.try_lock_write() {
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

    pub fn did_unlock_write(&self) {
        //pop the waiting READ threads
        let threads = self.waiting_sync_read_threads.with_mut(std::mem::take);
        for thread in threads {
            // Wake up the thread
            thread.unpark();
        }
        //AND the write threads
        let threads = self.waiting_sync_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
    }

    pub fn did_unlock_read(&self) {
        //unlock only WRITE threads
        let threads = self.waiting_sync_write_threads.with_mut(std::mem::take);
        for thread in threads {
            thread.unpark();
        }
    }


}


#[cfg(test)] mod test {
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use std::time::Duration;
    use crate::rwlock::{RwLock, LOCKED_WRITE};

    #[test] fn test_lock_try() {
        let mutex = RwLock::new(0);
        let lock = mutex.try_lock_read();
        assert!(lock.is_ok());
        assert_eq!(lock.as_ref().unwrap().deref(), &0);
        let lock2 = mutex.try_lock_read();
        assert!(lock2.is_ok());
        assert_eq!(lock2.as_ref().unwrap().deref(), &0);

        drop(lock2);
        //fail to acquire write lock
        let lock2 = mutex.try_lock_write();
        assert!(!lock2.is_ok());

        drop(lock);
        let mut write_lock = mutex.try_lock_write();
        assert!(write_lock.is_ok());
        assert_eq!(write_lock.as_ref().unwrap().deref(), &0);

        *write_lock.as_mut().unwrap().deref_mut() = 2;
        assert_eq!(write_lock.as_ref().unwrap().deref(), &2);

        //fail to acquire a new read lock
        let read_lock = mutex.try_lock_read();
        assert!(!read_lock.is_ok());

        drop(write_lock);
        //new read lock
        let read_lock = mutex.try_lock_read();
        assert!(read_lock.is_ok());
        assert_eq!(read_lock.as_ref().unwrap().deref(), &2);
    }

    #[test] fn test_lock_spin() {
        let mutex = RwLock::new(0);
        let lock = mutex.lock_spin_read();
        drop(lock);

        let lock = mutex.lock_spin_write();
        drop(lock);
    }

    #[test] fn test_lock_block() {
        let mutex = Arc::new(RwLock::new(0));
        let lock = mutex.lock_block_read();
        assert_eq!(lock.deref(), &0);
        let lock2 = mutex.lock_block_read();
        assert_eq!(lock.deref(), &0);
        drop(lock2);

        let (tx,rx) = std::sync::mpsc::channel();
        let mutex_clone = mutex.clone();
        std::thread::spawn(move || {
            //indicate thread came up
            tx.send(()).unwrap();
            let lock = mutex_clone.lock_block_write();
            tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(25));
            drop(lock);
        });
        //wait for thread up msg
        rx.recv().unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(10)).is_err());
        drop(lock); //thread should now acquire lock
        rx.recv().unwrap(); //wait for thread to acquire lock
        let time = std::time::Instant::now();
        mutex.lock_block_read();
        //ensure time took >50ms
        assert!(time.elapsed() > Duration::from_millis(10));
    }

    #[test_executors::async_test] async fn test_async() {
        let mutex = Arc::new(RwLock::new(0));
        let lock = mutex.lock_async_read().await;
        assert_eq!(lock.deref(), &0);
        drop(lock);
        let lock = mutex.lock_async_read_write().await;
        assert_eq!(lock.deref(), &0);
        drop(lock);
    }
}