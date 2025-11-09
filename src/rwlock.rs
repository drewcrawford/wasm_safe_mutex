use std::cell::UnsafeCell;
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU8};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use crate::{Guard, NotAvailable};

const UNLOCKED: u8 = 0;
const LOCKED_READ: u8 = 0b1;
const LOCKED_WRITE: u8 = 0b10000000;


pub struct RwLock<T> {
    inner: UnsafeCell<T>,
    data_lock: AtomicU8,
}

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
    }
}

impl <'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        let old = self.mutex.data_lock.swap(UNLOCKED, Release);
        assert!(old == LOCKED_WRITE);
    }
}


impl <T> RwLock<T> {
    pub const fn new(value: T) -> RwLock<T> {
        RwLock {
            inner: UnsafeCell::new(value),
            data_lock: AtomicU8::new(UNLOCKED),
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
}


#[cfg(test)] mod test {
    use std::ops::{Deref, DerefMut};
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
}