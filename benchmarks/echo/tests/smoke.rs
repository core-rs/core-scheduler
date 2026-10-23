// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::net::TcpStream;

const BINARY: &str = env!("CARGO_BIN_EXE_core-scheduler-echo-benchmark");
struct Process(Child);
impl Process {
    fn new(args: &[&str]) -> Self {
        Self(
            Command::new(BINARY)
                .args(args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        )
    }
    fn address(&mut self) -> String {
        let stdout = self.0.stdout.take().unwrap();
        let (send, recv) = mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            BufReader::new(stdout).read_line(&mut line).unwrap();
            send.send(line).unwrap();
        });
        recv.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .trim()
            .to_owned()
    }
    fn finish(&mut self, success: bool) -> String {
        let until = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < until, "benchmark process timed out");
            std::thread::sleep(Duration::from_millis(5));
        };
        let mut stderr = String::new();
        self.0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert_eq!(status.success(), success, "{stderr}");
        let mut stdout = String::new();
        if let Some(mut stream) = self.0.stdout.take() {
            stream.read_to_string(&mut stdout).unwrap();
        }
        stdout
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn smoke(backend: &str) {
    let mut server = Process::new(&["server", backend, "2", "4"]);
    let address = server.address();
    let mut client = Process::new(&["client", address.trim(), "2", "4", "4096", "0.03", "0.05"]);
    let output = client.finish(true);
    let stats: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert!(stats["requests"].as_u64().unwrap() > 0);
    assert_eq!(stats["bytes"], 4096);
    assert_eq!(stats["connections"], 4);
    assert!(stats["rtt_p99_us"].as_f64().unwrap() >= stats["rtt_p50_us"].as_f64().unwrap());
    server.finish(true);
}
#[test]
fn pure_tokio() {
    smoke("tokio");
}
#[test]
fn tokio_adapter() {
    smoke("tokio-adapter");
}
#[test]
fn tokio_adapter_poll() {
    smoke("tokio-adapter-poll");
}
#[cfg(target_os = "linux")]
#[test]
fn native_iouring() {
    smoke("iouring");
}
#[cfg(target_os = "linux")]
#[test]
fn native_iouring_poll() {
    smoke("iouring-poll");
}
#[cfg(target_os = "linux")]
#[test]
fn native_iouring_multishot() {
    smoke("iouring-multishot");
}

#[test]
fn invalid_configuration_fails_before_running_a_workload() {
    for args in [
        vec![],
        vec!["server"],
        vec!["server", "missing", "1", "1"],
        vec!["server", "tokio", "0", "1"],
        vec!["server", "tokio", "invalid", "1"],
        vec!["client", "127.0.0.1:0", "1", "1", "65537", "0", "1"],
        vec!["client", "127.0.0.1:0", "1", "1", "1", "0", "0"],
        vec!["client", "127.0.0.1:0", "1", "1", "1", "-1", "1"],
        vec!["client", "127.0.0.1:0", "1", "1", "1", "NaN", "1"],
        vec!["client", "127.0.0.1:0", "1", "1", "1", "0", "1"],
    ] {
        Process::new(&args).finish(false);
    }
}

#[test]
fn client_rejects_corrupt_echoes() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let thread = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut byte = [0];
        socket.read_exact(&mut byte).unwrap();
        socket.write_all(&[byte[0].wrapping_add(1)]).unwrap();
    });
    Process::new(&["client", &address, "1", "1", "1", "0", "0.1"]).finish(false);
    thread.join().unwrap();
}

#[test]
fn client_rejects_an_empty_measurement() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let thread = std::thread::spawn(move || {
        let _ = listener.accept().unwrap();
    });
    Process::new(&["client", &address, "1", "1", "1", "0", "0.000000001"]).finish(false);
    thread.join().unwrap();
}

#[test]
fn every_client_field_is_validated_and_server_connection_count_must_be_positive() {
    for (field, invalid) in [
        (1, "invalid-address"),
        (2, "0"),
        (3, "0"),
        (4, "0"),
        (5, "invalid-seconds"),
        (6, "invalid-seconds"),
    ] {
        let mut args = vec!["client", "127.0.0.1:0", "1", "1", "1", "0", "1"];
        args[field] = invalid;
        Process::new(&args).finish(false);
    }
    Process::new(&["server", "tokio", "1", "0"]).finish(false);
}

#[test]
fn tokio_adapter_monitor() {
    smoke("tokio-adapter-monitor");
}
#[cfg(target_os = "linux")]
#[test]
fn native_iouring_monitor() {
    smoke("iouring-monitor");
}

#[cfg(target_os = "linux")]
#[test]
fn native_readiness_monitor() {
    smoke("native-monitor");
}

#[cfg(target_os = "linux")]
#[test]
fn monoio_iouring() {
    smoke("monoio");
}

#[cfg(target_os = "linux")]
#[test]
fn monoio_epoll() {
    smoke("monoio-legacy");
}

#[cfg(target_os = "linux")]
#[test]
fn monoio_serves_an_active_connection_while_another_is_idle() {
    for backend in ["monoio", "monoio-legacy"] {
        let mut server = Process::new(&["server", backend, "1", "2"]);
        let address = server.address();
        let idle = TcpStream::connect(&address).unwrap();
        let mut active = TcpStream::connect(&address).unwrap();
        active
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        active.write_all(b"active").unwrap();
        let mut bytes = [0; 6];
        active.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"active");
        drop((active, idle));
        server.finish(true);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn monoio_exits_when_there_are_fewer_connections_than_workers() {
    for backend in ["monoio", "monoio-legacy"] {
        let mut server = Process::new(&["server", backend, "4", "1"]);
        let address = server.address();
        let mut client = Process::new(&["client", &address, "1", "1", "65536", "0.01", "0.02"]);
        let output: serde_json::Value = serde_json::from_str(&client.finish(true)).unwrap();
        assert!(output["requests"].as_u64().unwrap() > 0);
        server.finish(true);
    }
}
