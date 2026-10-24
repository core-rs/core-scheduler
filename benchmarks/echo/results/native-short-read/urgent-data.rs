// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(target_os = "linux")]

use core_scheduler::{
    Cancellation, SocketRecvFlags, SocketRegistration, SocketStreamReader, SocketStreamReading,
};
use core_scheduler_iouring::IoUringSelector;
use core_scheduler_selector::{SelectorDriver, SelectorWorker};
use futures_lite::future::{block_on, poll_once};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    pin::pin,
    time::{Duration, Instant},
};

#[test]
fn ordinary_bytes_after_an_urgent_mark_remain_readable() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let mut worker = driver.enter();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = selector
        .register_socket(listener.accept().unwrap().0)
        .unwrap();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    let never = Cancellation::never();
    let mut buffer = [0; 64];
    peer.write_all(b"seed").unwrap();
    assert_eq!(block_on(reader.read_into(&mut buffer, &never)).unwrap(), 4);
    {
        let mut reading = pin!(reader.read_into(&mut buffer, &never));
        assert!(block_on(poll_once(&mut reading)).is_none());
        worker.turn(Some(Duration::ZERO)).unwrap();
        peer.write_all(b"before").unwrap();
        assert_eq!(
            socket2::SockRef::from(&peer)
                .send_with_flags(b"!", libc::MSG_OOB)
                .unwrap(),
            1
        );
        peer.write_all(b"after").unwrap();
        std::thread::sleep(Duration::from_millis(10));
        // Publish queued notifications before consuming the prefix, just as an event-loop ready batch does.
        for _ in 0..10 {
            worker.turn(Some(Duration::ZERO)).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Some(result) = block_on(poll_once(&mut reading)) {
                assert_eq!(result.unwrap(), 6);
                break;
            }
            assert!(Instant::now() < deadline, "prefix stalled");
            worker.turn(Some(Duration::from_millis(1))).unwrap();
        }
    }
    assert_eq!(&buffer[..6], b"before");
    let mut result = None;
    {
        let mut reading = pin!(reader.read_into(&mut buffer, &never));
        let deadline = Instant::now() + Duration::from_millis(100);
        while Instant::now() < deadline {
            if let Some(value) = block_on(poll_once(&mut reading)) {
                result = Some(value.unwrap());
                break;
            }
            worker.turn(Some(Duration::from_millis(1))).unwrap();
        }
    }
    if result.is_none() {
        let count = socket
            .try_recv(&mut buffer, SocketRecvFlags::default())
            .unwrap();
        assert_eq!(&buffer[..count], b"after");
        panic!("bound reader stalled for 100 ms while direct recv retrieved the queued suffix");
    }
    assert_eq!(result.unwrap(), 5);
    assert_eq!(&buffer[..5], b"after");
    println!("bound reader retrieved both ordinary payloads without stalling");
}
