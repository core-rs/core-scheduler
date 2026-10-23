// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use monoio::{
    buf::IoBuf,
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::TcpStream,
};
use std::net::{TcpListener, TcpStream as StdTcpStream};

/// Assign persistent connections before the timed workload. Every connection stays on one local runtime; no cross-thread operation queue is added to Monoio's hot path.
pub(crate) fn run(listener: TcpListener, workers: usize, connections: usize, legacy: bool) {
    let mut sockets: Vec<Vec<StdTcpStream>> = (0..workers).map(|_| Vec::new()).collect();
    for index in 0..connections {
        let (socket, _) = listener.accept().expect("accept connection");
        socket.set_nodelay(true).expect("TCP_NODELAY");
        // Match Monoio's own accept path: readiness syscalls need nonblocking sockets, while completion reads use blocking socket semantics.
        socket.set_nonblocking(legacy).expect("Monoio socket mode");
        sockets[index % workers].push(socket);
    }
    let threads: Vec<_> = sockets
        .into_iter()
        .map(|sockets| {
            std::thread::spawn(move || {
                if legacy {
                    monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
                        .build()
                        .expect("Monoio epoll runtime")
                        .block_on(serve(sockets));
                } else {
                    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                        .with_entries(256)
                        .build()
                        .expect("Monoio io_uring runtime")
                        .block_on(serve(sockets));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("Monoio worker failed");
    }
}

async fn serve(sockets: Vec<StdTcpStream>) {
    let tasks: Vec<_> = sockets
        .into_iter()
        .map(|socket| {
            let socket = TcpStream::from_std(socket).expect("Monoio socket registration");
            monoio::spawn(echo(socket))
        })
        .collect();
    for task in tasks {
        task.await;
    }
}

async fn echo(mut socket: TcpStream) {
    let mut buffer = vec![0; crate::BUFFER_SIZE];
    loop {
        let (result, returned) = socket.read(buffer).await;
        buffer = returned;
        let count = result.expect("echo read");
        if count == 0 {
            return;
        }
        let (result, returned) = socket.write_all(buffer.slice(..count)).await;
        buffer = returned.into_inner();
        result.expect("echo write");
    }
}
