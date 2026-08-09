//! End-to-end tests against a local stub that speaks enough of S3 to exercise
//! the real SDK: real HTTP, real ranged GETs, real exit codes.
//!
//! The stub is a small Python script (`tests/s3stub.py`). Tests are skipped if
//! no `python3` is available rather than failing the suite.

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};

struct Stub {
    child: Child,
    port: u16,
    sha256: String,
}

impl Drop for Stub {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts the stub on an ephemeral port. `None` means python3 is unavailable.
fn start_stub() -> Option<Stub> {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/s3stub.py");
    let mut child = Command::new("python3")
        .arg(script)
        .arg("0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    let stdout = child.stdout.take().expect("piped");
    let mut banner = String::new();
    BufReader::new(stdout).read_line(&mut banner).ok()?;

    let field = |key: &str| -> Option<String> {
        banner
            .split_whitespace()
            .find_map(|f| f.strip_prefix(key).map(str::to_string))
    };
    let port = field("port=")?.parse().ok()?;
    let sha256 = field("sha256=")?;
    Some(Stub {
        child,
        port,
        sha256,
    })
}

fn s3get(stub: &Stub) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3get"));
    cmd.env(
        "AWS_ENDPOINT_URL",
        format!("http://127.0.0.1:{}", stub.port),
    )
    .env("AWS_ACCESS_KEY_ID", "test")
    .env("AWS_SECRET_ACCESS_KEY", "test")
    .env("AWS_REGION", "us-east-1")
    .env("AWS_EC2_METADATA_DISABLED", "true");
    cmd
}

fn sha256_hex(bytes: &[u8]) -> String {
    // Shelling out keeps the test free of a hashing dependency.
    use std::io::Write;
    let mut child = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("sha256sum");
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    let out = child.wait_with_output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .unwrap()
        .to_string()
}

macro_rules! stub_or_skip {
    () => {
        match start_stub() {
            Some(stub) => stub,
            None => {
                eprintln!("skipping: python3 unavailable");
                return;
            }
        }
    };
}

#[test]
fn downloads_to_stdout_byte_exactly() {
    let stub = stub_or_skip!();
    for threads in ["1", "6", "16"] {
        let out = s3get(&stub)
            .args(["s3://b/ok", "--block-size", "256kb", "-t", threads])
            .output()
            .unwrap();
        assert!(out.status.success(), "threads={threads}: {:?}", out.status);
        assert_eq!(sha256_hex(&out.stdout), stub.sha256, "threads={threads}");
    }
}

#[test]
fn downloads_to_a_file_and_leaves_no_staging_artefacts() {
    let stub = stub_or_skip!();
    let dir = std::env::temp_dir().join(format!("s3get-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("out.bin");

    let status = s3get(&stub)
        .args(["s3://b/ok", "--block-size", "512kb", "-o"])
        .arg(&target)
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(sha256_hex(&std::fs::read(&target).unwrap()), stub.sha256);

    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".part"))
        .collect();
    assert!(leftovers.is_empty(), "staging files left: {leftovers:?}");
    std::fs::remove_dir_all(&dir).ok();
}

/// The server truncates each range by one byte on first request. The per-block
/// length check must catch it and the retry must recover.
#[test]
fn recovers_from_a_short_response_body() {
    let stub = stub_or_skip!();
    let out = s3get(&stub)
        .args(["s3://b/short", "--block-size", "512kb", "-t", "2"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(sha256_hex(&out.stdout), stub.sha256);
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("expected"),
        "the short body should have been reported"
    );
}

/// A 412 means the object changed under us. Retrying cannot help, so it must
/// fail immediately rather than working through the retry budget.
#[test]
fn fails_fast_when_the_object_changed() {
    let stub = stub_or_skip!();
    let started = std::time::Instant::now();
    let out = s3get(&stub)
        .args([
            "s3://b/precon",
            "--block-size",
            "512kb",
            "--max-retries",
            "6",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "a 412 must not exit 0");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "took {:?}; a non-retryable error should not consume the retry budget",
        started.elapsed()
    );
}

/// A missing ETag disables version pinning, which must be said out loud.
#[test]
fn warns_when_the_object_has_no_etag() {
    let stub = stub_or_skip!();
    let out = s3get(&stub)
        .args(["s3://b/noetag", "--block-size", "512kb"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(sha256_hex(&out.stdout), stub.sha256);
    assert!(String::from_utf8_lossy(&out.stderr).contains("ETag"));
}

/// `s3get ... | head` is ordinary usage: the reader leaving first is not a
/// failure of ours.
#[test]
fn closing_the_pipe_early_is_not_an_error() {
    let stub = stub_or_skip!();
    let mut child = s3get(&stub)
        .args(["s3://b/ok", "--block-size", "256kb"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout = child.stdout.take().unwrap();
    let mut buf = [0u8; 1000];
    stdout.read_exact(&mut buf).unwrap();
    drop(stdout);

    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "a closed consumer should exit 0, got {:?}",
        out.status
    );
    assert!(
        out.stderr.is_empty(),
        "expected no diagnostics, got {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A stalled body produces no error of its own; only the timeout ends it.
#[test]
fn bounds_a_stalled_response() {
    let stub = stub_or_skip!();
    let started = std::time::Instant::now();
    let out = s3get(&stub)
        .args([
            "s3://b/stall",
            "--block-size",
            "512kb",
            "-t",
            "2",
            "--stall-timeout",
            "2",
            "--max-retries",
            "1",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "a stalled download must not exit 0");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(60),
        "the stall timeout did not fire: {:?}",
        started.elapsed()
    );
}

/// Regression test for the silent-truncation bug: a write failure on the final
/// bytes must not be discarded. `/dev/full` fails at write(2), so this covers
/// both the write path and the explicit flush.
#[test]
fn a_failing_output_device_is_not_reported_as_success() {
    let stub = stub_or_skip!();

    let status = s3get(&stub)
        .args(["s3://b/ok", "--block-size", "512kb", "-o", "/dev/full"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "-o /dev/full must not exit 0");

    let out = s3get(&stub)
        .args(["s3://b/ok", "--block-size", "512kb"])
        .stdout(std::fs::File::create("/dev/full").unwrap())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success(), "stdout to /dev/full must not exit 0");
}

/// The completeness check is what stops a torn-down download from looking like
/// a finished one. A permanently failing chunk must never exit 0, and must
/// never leave a destination file behind.
#[test]
fn an_incomplete_download_never_exits_zero() {
    let stub = stub_or_skip!();
    let dir = std::env::temp_dir().join(format!("s3get-incomplete-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("out.bin");

    let out = s3get(&stub)
        .args(["s3://b/precon", "--block-size", "256kb", "-t", "4", "-o"])
        .arg(&target)
        .output()
        .unwrap();

    assert!(!out.status.success(), "a failed download must not exit 0");
    assert!(
        !target.exists(),
        "a failed download must not leave a destination file"
    );
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".part"))
        .collect();
    assert!(leftovers.is_empty(), "staging files left: {leftovers:?}");
    std::fs::remove_dir_all(&dir).ok();
}
