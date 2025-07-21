use std::cell::UnsafeCell;

pub struct Spinlock<T> {
    data: UnsafeCell<T>,
    locked: std::sync::atomic::AtomicBool,
}

impl<T> Spinlock<T> {
    pub fn new(data: T) -> Self {
        Spinlock {
            data: UnsafeCell::new(data),
            locked: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /**
    By design, we only expose this func to ensure all accesses are short-lived.
    */
    pub fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        // Spin until we can acquire the lock
        while self.locked.swap(true, std::sync::atomic::Ordering::Acquire) {
            std::hint::spin_loop();
        }

        // SAFETY: We have exclusive access to the data now
        let result = unsafe { f(&mut *self.data.get()) };

        // Release the lock
        self.locked
            .store(false, std::sync::atomic::Ordering::Release);

        result
    }
}
