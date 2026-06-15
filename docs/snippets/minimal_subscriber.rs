// Minimal waitbus broadcast subscriber (Rust, stdlib only).
//
// Mirror of minimal_subscriber.py; wire contract documented there.
//
// Build & run:
//     rustc minimal_subscriber.rs -o /tmp/minimal_subscriber
//     /tmp/minimal_subscriber
//
// Constraint: single .rs file, no Cargo.toml, no external crates.
// serde_json is not available, so JSON is parsed with a tiny hand-rolled
// string-extractor that scans for the three fields we need. Frames are
// well-formed JSON from a single trusted producer, so byte scanning is
// safe; this is not a general-purpose JSON parser.

use std::env;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::ExitCode;

const MAX_FRAME_BYTES: usize = 65_536;

fn default_socket_path() -> PathBuf {
    if let Ok(override_path) = env::var("WAITBUS_BROADCAST_SOCKET") {
        return PathBuf::from(override_path);
    }
    if cfg!(target_os = "macos") {
        let home = env::var("HOME").unwrap_or_else(|_| String::from("/"));
        return PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("waitbus")
            .join("broadcast.sock");
    }
    let runtime = env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| {
        // SAFETY: getuid() is always safe (no errors defined by POSIX).
        let uid = unsafe { libc_getuid() };
        format!("/run/user/{}", uid)
    });
    PathBuf::from(runtime).join("waitbus").join("broadcast.sock")
}

// Minimal getuid binding to avoid the libc crate dependency.
extern "C" {
    #[link_name = "getuid"]
    fn libc_getuid() -> u32;
}

fn recv_exactly(stream: &mut UnixStream, n: usize) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = vec![0u8; n];
    let mut filled = 0;
    while filled < n {
        let got = stream.read(&mut buf[filled..])?;
        if got == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("short read: expected {} bytes, got {}", n, filled),
            ));
        }
        filled += got;
    }
    Ok(Some(buf))
}

fn read_frame(stream: &mut UnixStream) -> std::io::Result<Option<Vec<u8>>> {
    let prefix = match recv_exactly(stream, 4)? {
        Some(p) => p,
        None => return Ok(None),
    };
    let length = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame length {} out of bounds", length),
        ));
    }
    match recv_exactly(stream, length)? {
        Some(payload) => Ok(Some(payload)),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "EOF inside frame payload",
        )),
    }
}

fn write_frame(stream: &mut UnixStream, payload: &[u8]) -> std::io::Result<()> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "payload {} bytes exceeds {}",
                payload.len(),
                MAX_FRAME_BYTES
            ),
        ));
    }
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(payload)?;
    Ok(())
}

// Extract the string value of a top-level JSON key from `bytes`.
// Returns None if not found. Handles only well-formed JSON from the
// trusted producer; does not interpret escapes beyond \" and \\.
fn extract_string(bytes: &[u8], key: &str) -> Option<String> {
    let needle = format!("\"{}\"", key);
    let key_bytes = needle.as_bytes();
    let mut i = 0;
    while i + key_bytes.len() < bytes.len() {
        if &bytes[i..i + key_bytes.len()] == key_bytes {
            // Skip past key, whitespace, colon, whitespace, opening quote.
            let mut j = i + key_bytes.len();
            while j < bytes.len() && (bytes[j] == b' ' || bytes[j] == b'\t') {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] != b':' {
                i += 1;
                continue;
            }
            j += 1;
            while j < bytes.len() && (bytes[j] == b' ' || bytes[j] == b'\t') {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] != b'"' {
                return None;
            }
            j += 1;
            let start = j;
            let mut out = Vec::new();
            while j < bytes.len() {
                let c = bytes[j];
                if c == b'\\' && j + 1 < bytes.len() {
                    let esc = bytes[j + 1];
                    match esc {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'n' => out.push(b'\n'),
                        b't' => out.push(b'\t'),
                        b'r' => out.push(b'\r'),
                        b'/' => out.push(b'/'),
                        _ => {
                            out.push(b'\\');
                            out.push(esc);
                        }
                    }
                    j += 2;
                    continue;
                }
                if c == b'"' {
                    return Some(String::from_utf8_lossy(&out).into_owned());
                }
                out.push(c);
                j += 1;
            }
            let _ = start;
            return None;
        }
        i += 1;
    }
    None
}

// Extract a string field nested inside the `"fields": { ... }` object.
fn extract_fields_string(bytes: &[u8], key: &str) -> Option<String> {
    let fields_key = b"\"fields\"";
    let mut i = 0;
    while i + fields_key.len() < bytes.len() {
        if &bytes[i..i + fields_key.len()] == fields_key {
            let mut j = i + fields_key.len();
            while j < bytes.len() && (bytes[j] == b' ' || bytes[j] == b'\t') {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] != b':' {
                i += 1;
                continue;
            }
            j += 1;
            while j < bytes.len() && (bytes[j] == b' ' || bytes[j] == b'\t') {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] != b'{' {
                return None;
            }
            // Walk to matching closing brace, respecting strings.
            let start = j;
            let mut depth = 0i32;
            let mut in_str = false;
            let mut escape = false;
            while j < bytes.len() {
                let c = bytes[j];
                if in_str {
                    if escape {
                        escape = false;
                    } else if c == b'\\' {
                        escape = true;
                    } else if c == b'"' {
                        in_str = false;
                    }
                } else {
                    match c {
                        b'"' => in_str = true,
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                return extract_string(&bytes[start..=j], key);
                            }
                        }
                        _ => {}
                    }
                }
                j += 1;
            }
            return None;
        }
        i += 1;
    }
    None
}

fn run() -> std::io::Result<i32> {
    let socket_path = default_socket_path();
    let mut stream = match UnixStream::connect(&socket_path) {
        Ok(s) => s,
        Err(err) => {
            eprintln!(
                "error: broadcast socket {} unavailable ({}). \
                 Start the daemon via `systemctl --user start waitbus-broadcast.service`.",
                socket_path.display(),
                err.kind()
            );
            return Ok(2);
        }
    };

    // Subscribe envelope: proto=1 is mandatory. Empty filters means "all
    // repos, all event types, from now". Add "filters" or "event_types"
    // keys to narrow.
    write_frame(&mut stream, b"{\"proto\":1}")?;

    loop {
        let frame = match read_frame(&mut stream)? {
            Some(f) => f,
            None => return Ok(0),
        };

        if extract_string(&frame, "kind").as_deref() == Some("subscribe_rejected") {
            let reason = extract_string(&frame, "reason").unwrap_or_else(|| "unknown".to_string());
            eprintln!("error: subscribe_rejected: {}", reason);
            if let Some(remediation) = extract_string(&frame, "remediation") {
                if !remediation.is_empty() {
                    eprintln!("remediation: {}", remediation);
                }
            }
            std::process::exit(2);
        }

        // A "truncated" frame is a DATA frame (it carries an event_id and
        // advances the resume cursor), not a control frame: the event's
        // payload exceeded the wire cap, so only its identity rides the
        // socket. Surface it -- silently dropping it makes a large event
        // invisible -- and re-fetch the full row out of band.
        match extract_string(&frame, "kind").as_deref() {
            Some("event") => {}
            Some("truncated") => {
                let event_id = extract_string(&frame, "event_id").unwrap_or_else(|| "?".to_string());
                println!("{}\t[truncated; re-fetch full payload via `waitbus read-events`]", event_id);
                continue;
            }
            _ => continue,
        }

        let delivery_id = extract_string(&frame, "delivery_id").unwrap_or_else(|| "?".to_string());
        let source = extract_fields_string(&frame, "source").unwrap_or_else(|| "?".to_string());
        let event_type = extract_string(&frame, "event_type").unwrap_or_else(|| "?".to_string());

        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        writeln!(
            handle,
            "{}\tsource={}\ttype={}",
            delivery_id, source, event_type
        )?;
        handle.flush()?;
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code as u8),
        Err(err) => {
            eprintln!("error: {}", err);
            ExitCode::from(1)
        }
    }
}
