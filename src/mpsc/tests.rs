use super::*;
use std::thread;
use std::time::Duration;

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
