use super::*;
use std::thread;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

#[cfg(target_arch = "wasm32")]
use web_time::Instant;

#[test]
fn test_send_recv_spin() {
    let (tx, rx) = channel();
    tx.send_spin(1);
    tx.send_spin(2);
    assert_eq!(rx.recv_spin(), 1);
    assert_eq!(rx.recv_spin(), 2);
}

#[test]
fn test_send_recv_block() {
    let (tx, rx) = channel();
    tx.send_block(1);
    tx.send_block(2);
    assert_eq!(rx.recv_block(), 1);
    assert_eq!(rx.recv_block(), 2);
}

#[test]
fn test_send_recv_sync() {
    let (tx, rx) = channel();
    tx.send_sync(1);
    tx.send_sync(2);
    assert_eq!(rx.recv_sync(), 1);
    assert_eq!(rx.recv_sync(), 2);
}

#[test]
fn test_send_recv_async() {
    test_executors::spin_on(async {
        let (tx, rx) = channel();
        tx.send_async(1).await;
        tx.send_async(2).await;
        assert_eq!(rx.recv_async().await, 1);
        assert_eq!(rx.recv_async().await, 2);
    });
}

#[test]
fn test_multiple_senders() {
    let (tx, rx) = channel();
    let tx1 = tx.clone();
    let tx2 = tx.clone();

    thread::spawn(move || {
        tx1.send_sync(1);
    });
    thread::spawn(move || {
        tx2.send_sync(2);
    });

    let mut results = vec![rx.recv_sync(), rx.recv_sync()];
    results.sort();
    assert_eq!(results, vec![1, 2]);
}

#[test]
fn test_ordering() {
    let (tx, rx) = channel();
    for i in 0..10 {
        tx.send_sync(i);
    }
    for i in 0..10 {
        assert_eq!(rx.recv_sync(), i);
    }
}

#[test]
fn test_blocking_behavior() {
    let (tx, rx) = channel();
    let t = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        tx.send_sync(42);
    });

    assert_eq!(rx.recv_sync(), 42);
    t.join().unwrap();
}

#[test]
fn test_try_recv() {
    let (tx, rx) = channel();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    tx.send_sync(1);
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn test_recv_timeout() {
    let (tx, rx) = channel();
    let deadline = Instant::now() + Duration::from_millis(100);
    assert_eq!(
        rx.recv_sync_timeout(deadline),
        Err(RecvTimeoutError::Timeout)
    );

    tx.send_sync(1);
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(rx.recv_sync_timeout(deadline), Ok(1));
}

#[test]
fn test_recv_spin_timeout() {
    let (tx, rx) = channel();
    let deadline = Instant::now() + Duration::from_millis(100);
    assert_eq!(
        rx.recv_spin_timeout(deadline),
        Err(RecvTimeoutError::Timeout)
    );

    tx.send_sync(1);
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(rx.recv_spin_timeout(deadline), Ok(1));
}

#[test]
fn test_recv_block_timeout() {
    let (tx, rx) = channel();
    let deadline = Instant::now() + Duration::from_millis(100);
    assert_eq!(
        rx.recv_block_timeout(deadline),
        Err(RecvTimeoutError::Timeout)
    );

    tx.send_sync(1);
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(rx.recv_block_timeout(deadline), Ok(1));
}

#[test]
fn test_recv_async_timeout() {
    test_executors::spin_on(async {
        let (tx, rx) = channel();
        let deadline = Instant::now() + Duration::from_millis(100);
        assert_eq!(
            rx.recv_async_timeout(deadline).await,
            Err(RecvTimeoutError::Timeout)
        );

        tx.send_async(1).await;
        let deadline = Instant::now() + Duration::from_secs(1);
        assert_eq!(rx.recv_async_timeout(deadline).await, Ok(1));
    });
}

#[test]
fn test_debug() {
    let (tx, rx) = channel::<i32>();
    assert_eq!(format!("{:?}", tx), "Sender");
    assert_eq!(format!("{:?}", rx), "Receiver");
}

#[test]
fn test_into_iter() {
    let (tx, rx) = channel();
    thread::spawn(move || {
        tx.send_sync(1);
        tx.send_sync(2);
        tx.send_sync(3);
        // No disconnect signal yet, so iterator would block forever if we tried to take 4
    });

    let mut iter = rx.into_iter();
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), Some(2));
    assert_eq!(iter.next(), Some(3));
}

#[test]
fn test_sender_sync() {
    fn is_sync<T: Sync>() {}
    is_sync::<Sender<i32>>();
}
