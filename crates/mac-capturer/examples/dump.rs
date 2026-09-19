//! `cargo run -p liyanlabs-relay-mac-capturer --example dump > /tmp/native.h265`
//!
//! Stream raw NAL bytes from the Swift capturer to stdout for ~3s, then exit.
//! Use ffprobe to validate the output afterwards.

#[cfg(target_os = "macos")]
use std::io::{self, Write};
#[cfg(target_os = "macos")]
use std::time::{Duration, Instant};

#[cfg(target_os = "macos")]
fn main() {
    eprintln!("starting native mac capturer (1920x1080 @30fps, 4 Mbps)");
    let rx =
        liyanlabs_relay_mac_capturer::start(1920, 1080, 30, 4000, 0).expect("start mac-capturer");
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut bytes = 0u64;
    let mut nals = 0u64;
    while let Ok(nal) = rx.recv_timeout(Duration::from_secs(2)) {
        out.write_all(&nal.data).expect("write");
        bytes += nal.data.len() as u64;
        nals += 1;
        if Instant::now() >= deadline {
            break;
        }
    }
    eprintln!("dumped {nals} NALs, {bytes} bytes");
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("the native mac capturer example requires macOS");
}
