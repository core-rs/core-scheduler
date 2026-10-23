// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod client;
#[cfg(target_os = "linux")]
mod monoio_server;
mod server;

use std::{error::Error, time::Duration};
const BUFFER_SIZE: usize = 65536;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const USAGE: &str = "Usage: core-scheduler-echo-benchmark server BACKEND WORKERS CONNECTIONS\n       core-scheduler-echo-benchmark client ADDRESS WORKERS CONNECTIONS BYTES WARMUP_SECONDS MEASURE_SECONDS\nBackends: tokio, tokio-adapter, tokio-adapter-poll, tokio-adapter-monitor, native-monitor, iouring, iouring-poll, iouring-multishot, iouring-monitor, monoio, monoio-legacy (Native and Monoio benchmark modes require Linux). Server binds 127.0.0.1:0 and prints its address.";

fn positive(value: &str) -> Result<usize> {
    let value = value.parse::<usize>()?;
    if value == 0 {
        return Err("expected a positive integer".into());
    }
    Ok(value)
}

fn seconds(value: &str) -> Result<Duration> {
    Ok(Duration::try_from_secs_f64(value.parse::<f64>()?)?)
}

fn runtime(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("Tokio runtime setup")
}

/// Run a server or client using the command-line arguments, including the executable name.
pub fn run(args: &[String]) -> Result<()> {
    match args.get(1).map(String::as_str) {
        Some("server") if args.len() == 5 => {
            server::run(&args[2], positive(&args[3])?, positive(&args[4])?)
        }
        Some("client") if args.len() == 8 => {
            let bytes = positive(&args[5])?;
            let warmup = seconds(&args[6])?;
            let duration = seconds(&args[7])?;
            if bytes > 65536 || duration.is_zero() {
                return Err("use 1..=65536 bytes and a positive measurement duration".into());
            }
            runtime(positive(&args[3])?).block_on(client::run(
                args[2].parse()?,
                positive(&args[4])?,
                bytes,
                warmup,
                duration,
            ))
        }
        _ => Err(USAGE.into()),
    }
}
