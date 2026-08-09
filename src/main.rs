use aws_sdk_s3 as s3;
use clap::Parser;
use s3::error::{ProvideErrorMetadata, SdkError};
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// S3 redirects a request aimed at the wrong region, naming the right one in
/// the `x-amz-bucket-region` header.
///
/// Defined locally to avoid a dependency on `http`, whose major version would
/// then have to match the copy vendored in the smithy runtime.
const HTTP_MOVED_PERMANENTLY: u16 = 301;

/// Upper bound on retry backoff. Uncapped, a high `--max-retries` schedules
/// sleeps measured in days.
const MAX_BACKOFF_SECS: u64 = 60;

/// Transient S3 error codes. Status alone does not identify them:
/// `RequestTimeout` comes back as HTTP 400, which is otherwise permanent.
const RETRYABLE_ERROR_CODES: &[&str] = &[
    "BandwidthLimitExceeded",
    "InternalError",
    "PriorRequestNotComplete",
    "RequestLimitExceeded",
    "RequestThrottled",
    "RequestThrottledException",
    "RequestTimeout",
    "RequestTimeoutException",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
];

/// Exponential backoff with equal jitter, capped at `MAX_BACKOFF_SECS`. The
/// jitter keeps workers that fail together from retrying in lockstep.
fn backoff_seconds(retry_count: u32) -> u64 {
    let capped = 2_u64.saturating_pow(retry_count).min(MAX_BACKOFF_SECS);
    let half = capped / 2;
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    half + jitter % (half + 1)
}

/// A `--output` destination.
///
/// A regular file is staged in a sibling temp file and renamed on success, so
/// an aborted run leaves nothing partial at the destination. Other targets
/// (`/dev/null`, FIFOs, device nodes) are written directly, since renaming
/// over them would replace them.
struct FileOutput {
    file: std::fs::File,
    /// Set while staging. Cleared after the rename, removed on drop.
    temp_path: Option<PathBuf>,
    destination: PathBuf,
}

impl FileOutput {
    fn create(destination: &Path) -> std::io::Result<Self> {
        // symlink_metadata does not follow links, so a symlink is written
        // through instead of being replaced by the rename.
        let stage = match std::fs::symlink_metadata(destination) {
            Ok(metadata) => metadata.is_file(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => return Err(e),
        };

        if !stage {
            return Self::in_place(destination);
        }

        // Creating a sibling needs write access to the directory, which
        // write access to the destination does not imply, and the longer name
        // can exceed NAME_MAX. Writing in place works in both cases but gives
        // up the abort guarantee, so warn rather than degrade silently.
        Self::staged(destination).or_else(|e| {
            eprintln!(
                "Warning: cannot stage alongside {}: {}. Writing in place, so an \
                 interrupted download will leave a partial file.",
                destination.display(),
                e
            );
            Self::in_place(destination)
        })
    }

    fn in_place(destination: &Path) -> std::io::Result<Self> {
        Ok(Self {
            file: std::fs::File::create(destination)?,
            temp_path: None,
            destination: destination.to_path_buf(),
        })
    }

    fn staged(destination: &Path) -> std::io::Result<Self> {
        let name = destination
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();

        // create_new fails on anything that already exists, symlinks
        // included, so a planted name in a shared directory cannot redirect
        // the write. The random suffix makes guessing impractical as well.
        let mut last_error = None;
        for _ in 0..16 {
            let suffix = std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish();
            // Sibling path keeps the rename on one filesystem.
            let temp_path = destination.with_file_name(format!(".{name}.s3get-{suffix:x}.part"));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
            {
                Ok(file) => {
                    // Without this the new file takes the umask default and
                    // can widen the mode of the file being replaced. Only mode
                    // bits carry over; ACLs, xattrs and hard links belong to
                    // the old inode.
                    if let Ok(metadata) = std::fs::metadata(destination) {
                        let _ = file.set_permissions(metadata.permissions());
                    }
                    return Ok(Self {
                        file,
                        temp_path: Some(temp_path),
                        destination: destination.to_path_buf(),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last_error = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "could not create a staging file",
            )
        }))
    }
}

impl Drop for FileOutput {
    fn drop(&mut self) {
        // Still set means finish() never ran, so the staged bytes are
        // incomplete.
        if let Some(temp_path) = &self.temp_path {
            let _ = std::fs::remove_file(temp_path);
        }
    }
}

/// Where downloaded blocks are written.
///
/// The stdout lock is taken once and held. Nothing else writes to it, and
/// re-locking per block is pure overhead.
enum Output {
    File(FileOutput),
    Stdout(std::io::StdoutLock<'static>),
}

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Output::File(out) => out.file.write(buf),
            Output::Stdout(stdout) => stdout.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Output::File(out) => out.file.flush(),
            Output::Stdout(stdout) => stdout.flush(),
        }
    }
}

impl Output {
    /// Completes the write and reports errors that would otherwise be lost.
    ///
    /// `File::flush` is a no-op and the close result is discarded on drop, so
    /// filesystems that defer errors to close (NFS, or a quota hit under
    /// delayed allocation) need the `sync_all` to fail the run.
    fn finish(&mut self) -> std::io::Result<()> {
        self.flush()?;
        if let Output::File(out) = self {
            // Only regular files defer errors to close. fsync on a FIFO,
            // socket or character device returns EINVAL, which would fail
            // `-o /dev/null` and `-o <fifo>` after a correct download.
            if out.file.metadata()?.is_file() {
                out.file.sync_all()?;
            }
            // Cleared only after the rename succeeds, so a failure here
            // leaves Drop armed to remove the staging file.
            if let Some(temp_path) = out.temp_path.as_ref() {
                std::fs::rename(temp_path, &out.destination)?;
                out.temp_path = None;
            }
        }
        Ok(())
    }

    /// Whether the reader on the other end of the pipe has gone away, as in
    /// `s3get s3://... | head`. Restricted to stdout: the same error against
    /// `--output` is a real write failure and stays fatal.
    fn is_closed_consumer(&self, e: &std::io::Error) -> bool {
        matches!(self, Output::Stdout(_)) && e.kind() == std::io::ErrorKind::BrokenPipe
    }
}

/// Reports failures for blocks that were fetched but never written.
///
/// A closed consumer exits 0, so these never reach the exit code. They are
/// still worth printing: a 412 here means the object was replaced mid-download.
fn report_abandoned_errors(pending: &BTreeMap<usize, Result<Vec<u8>, anyhow::Error>>) {
    for (idx, outcome) in pending {
        if let Err(e) = outcome {
            eprintln!(
                "Warning: chunk {} had failed before the output closed: {:#}",
                idx, e
            );
        }
    }
}

/// Splits `s3://bucket/key` into its two parts.
fn parse_s3_path(path: &str) -> anyhow::Result<(String, String)> {
    let rest = path
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("S3 path has to start with 's3://'"))?;
    match rest.split_once('/') {
        None => anyhow::bail!("S3 path should be 's3://bucket/key'"),
        Some((bucket, key)) if bucket.is_empty() || key.is_empty() => {
            anyhow::bail!("S3 path should be 's3://bucket/key'")
        }
        Some((bucket, key)) => Ok((bucket.to_string(), key.to_string())),
    }
}

/// The half-open byte ranges the object is fetched in.
fn block_ranges(size: i64, block_size: usize) -> impl Iterator<Item = (i64, i64)> {
    (0..size)
        .step_by(block_size)
        .map(move |start| (start, size.min(start + block_size as i64)))
}

/// How many ranges `block_ranges` yields. Computed independently so the
/// completeness check does not depend on the iterator it validates.
fn block_count(size: i64, block_size: usize) -> usize {
    (size as u64).div_ceil(block_size as u64) as usize
}

/// Why the download stopped.
enum Completion {
    /// Every block was written.
    Finished,
    /// The process reading stdout closed the pipe first.
    ConsumerClosed,
}

/// Carries worker diagnostics to a dedicated thread.
///
/// Writing to stderr blocks. If the reader stops draining, a worker doing it
/// inline parks inside its own poll, where neither cancellation nor the stall
/// timeout can reach it. Reporting is best-effort: messages are dropped rather
/// than allowed to hold up the download.
#[derive(Clone)]
struct Diagnostics(std::sync::mpsc::SyncSender<String>);

impl Diagnostics {
    fn spawn() -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<String>(256);
        // Detached, so the process can exit while this thread is still
        // parked in a write that never returns.
        std::thread::spawn(move || {
            for message in receiver {
                eprintln!("{message}");
            }
        });
        Self(sender)
    }

    fn report(&self, message: String) {
        let _ = self.0.try_send(message);
    }
}

/// A failed download attempt, carrying whether another attempt could succeed.
struct AttemptError {
    retryable: bool,
    source: anyhow::Error,
}

impl AttemptError {
    fn retryable(source: anyhow::Error) -> Self {
        Self {
            retryable: true,
            source,
        }
    }
}

/// Upper bound on a parsed size. Above any useful block size, and low enough
/// that block offsets stay inside `i64`.
const MAX_SIZE: usize = 4 * 1024 * 1024 * 1024;

/// Byte-count units, longest suffix first so that `gb` is not read as `b`.
const SIZE_UNITS: &[(&str, usize)] = &[
    ("gb", 1024 * 1024 * 1024),
    ("g", 1024 * 1024 * 1024),
    ("mb", 1024 * 1024),
    ("m", 1024 * 1024),
    ("kb", 1024),
    ("k", 1024),
    ("b", 1),
];

/// Parses a byte count, with or without a unit: `32MB`, `512kb`, `1g`,
/// `1048576`. Case-insensitive. Whole numbers only, and never zero.
fn parse_size(x: &str) -> anyhow::Result<usize> {
    let lowered = x.trim().to_ascii_lowercase();
    let (digits, unit) = SIZE_UNITS
        .iter()
        .find_map(|(suffix, unit)| lowered.strip_suffix(suffix).map(|d| (d, *unit)))
        .unwrap_or((lowered.as_str(), 1));

    let value = usize::from_str(digits.trim()).map_err(|_| {
        anyhow::anyhow!(
            "Cannot parse size '{x}': expected a whole number, e.g. 32MB, 512kb or 1048576"
        )
    })?;

    // A zero block size would make `step_by` panic before any request is sent.
    if value == 0 {
        anyhow::bail!("Size must be greater than zero, got '{x}'");
    }

    let size = value
        .checked_mul(unit)
        .ok_or_else(|| anyhow::anyhow!("Size '{x}' is too large to represent"))?;

    // Offsets are i64 and blocks are buffered whole, so an unbounded value
    // wraps the range arithmetic negative and then asks for a huge allocation.
    if size > MAX_SIZE {
        anyhow::bail!("Size '{x}' exceeds the {MAX_SIZE}-byte maximum");
    }

    Ok(size)
}

/// Largest accepted concurrency. Past the point of diminishing returns, and
/// low enough that the channel capacity stays inside tokio's limits.
const MAX_THREADS: usize = 1024;

/// Rejects zero, which spawns no workers, and values large enough to exhaust
/// the runtime instead of the network.
fn parse_threads(x: &str) -> anyhow::Result<usize> {
    let value = usize::from_str(x.trim())
        .map_err(|_| anyhow::anyhow!("Cannot parse thread count '{x}'"))?;
    if value == 0 {
        anyhow::bail!("At least one thread is required");
    }
    if value > MAX_THREADS {
        anyhow::bail!("At most {MAX_THREADS} threads are supported, got {value}");
    }
    Ok(value)
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// S3 path to download from
    s3_path: String,

    /// Output file name, print to stdout otherwise
    #[arg(long, short)]
    output: Option<PathBuf>,

    /// Block size used for data downloads. Peak memory is roughly
    /// 2 * threads * block-size, so 384MB at the defaults
    #[arg(long, default_value = "32MB", value_parser = parse_size)]
    block_size: usize,

    /// Number of chunks to download concurrently
    #[arg(long, short, default_value = "6", value_parser = parse_threads)]
    threads: usize,

    /// Print verbose information, statistics, etc
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Determines how often each chunk should be retried before giving up.
    /// The AWS SDK makes up to 3 attempts of its own inside each of these, so
    /// the default of 6 permits roughly 18 requests for a single chunk
    #[arg(long, default_value = "6")]
    max_retries: u32,

    /// Give up on a chunk and retry it if no data arrives for this many
    /// seconds. Bounds stalled connections, which report no error of their own
    #[arg(long, default_value = "30", value_parser = clap::value_parser!(u64).range(1..))]
    stall_timeout: u64,
}

/// Fetches the half-open byte range `start..end` of an object.
///
/// `etag` pins the request to one version. If the object is replaced mid
/// download S3 answers 412, rather than serving bytes that would be spliced
/// together with blocks from the previous version.
///
/// `stall_timeout` bounds the wait for the response and for each body chunk. A
/// connection that establishes and then goes quiet reports no error of its
/// own, so without this the block never completes.
async fn download(
    client: &s3::Client,
    bucket: &str,
    key: &str,
    etag: Option<&str>,
    start: i64,
    end: i64,
    stall_timeout: Duration,
) -> Result<Vec<u8>, AttemptError> {
    let request = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .set_if_match(etag.map(str::to_owned))
        .range(format!("bytes={}-{}", start, end - 1))
        .send();

    let mut object = match tokio::time::timeout(stall_timeout, request).await {
        Err(_) => {
            return Err(AttemptError::retryable(anyhow::anyhow!(
                "no response within {}s",
                stall_timeout.as_secs()
            )))
        }
        Ok(Err(e)) => {
            let retryable = match &e {
                // A failed if_match arrives here as 412. Retrying cannot
                // bring back the version we started with, so it is terminal,
                // as are 403 and 404.
                SdkError::ServiceError(context) => {
                    let status = context.raw().status().as_u16();
                    let code = context.err().code().unwrap_or_default();
                    status == 408
                        || status == 429
                        || (500..600).contains(&status)
                        || RETRYABLE_ERROR_CODES.contains(&code)
                }
                // is_user covers requests the client could not form.
                // Connection, DNS and TLS failures come back as io/other and
                // stay retryable.
                SdkError::DispatchFailure(failure) => !failure.is_user(),
                SdkError::ConstructionFailure(_) => false,
                // SdkError is non-exhaustive. Unknown failures are treated as
                // transient: a wasted retry costs a delay, giving up wrongly
                // abandons the download.
                _ => true,
            };
            return Err(AttemptError {
                retryable,
                source: e.into(),
            });
        }
        Ok(Ok(x)) => x,
    };

    let expected = (end - start) as usize;
    // The size is known, so allocate once instead of growing and copying.
    let mut result = Vec::with_capacity(expected);
    loop {
        let chunk = match tokio::time::timeout(stall_timeout, object.body.try_next()).await {
            Err(_) => {
                return Err(AttemptError::retryable(anyhow::anyhow!(
                    "no data for {}s after {} of {} bytes",
                    stall_timeout.as_secs(),
                    result.len(),
                    expected
                )))
            }
            Ok(Err(e)) => return Err(AttemptError::retryable(e.into())),
            Ok(Ok(chunk)) => chunk,
        };
        match chunk {
            Some(chunk) => result.extend(chunk),
            None => break,
        }
    }

    if result.len() != expected {
        return Err(AttemptError::retryable(anyhow::anyhow!(
            "expected {} bytes, received {}",
            expected,
            result.len()
        )));
    }

    Ok(result)
}

/// Result of a successful `HeadObject`: the config that reached the object
/// (possibly after a region redirect), its length, and the version tag the
/// ranged requests are pinned to.
struct ObjectInfo {
    config: aws_config::SdkConfig,
    size: i64,
    etag: Option<String>,
}

async fn probe_object(bucket: &str, key: &str, verbose: u8) -> anyhow::Result<ObjectInfo> {
    let config = aws_config::load_from_env().await;
    let region = config.region().cloned();
    let mut config = config
        .into_builder()
        .region(region.or_else(|| Some(s3::config::Region::new("us-east-2"))))
        // WhenSupported asks S3 for a checksum and validates the body
        // against it, but the validator has no notion of ranged requests and
        // every request here is a range. Its only guard is the "-N" suffix of
        // a composite multipart checksum, which S3 need not send on a partial
        // response, so a whole-object checksum can end up compared against one
        // block. Integrity comes from TLS and the length check in download().
        .response_checksum_validation(s3::config::ResponseChecksumValidation::WhenRequired)
        .build();

    for _ in 0..3 {
        let client = s3::Client::new(&config);
        let head = match client.head_object().bucket(bucket).key(key).send().await {
            Ok(x) => x,
            Err(e) => {
                if verbose > 1 {
                    eprintln!("{:?}", e);
                }
                if let s3::error::SdkError::ServiceError(response) = &e {
                    if response.raw().status().as_u16() == HTTP_MOVED_PERMANENTLY {
                        if let Some(x) = response.raw().headers().get("x-amz-bucket-region") {
                            config = config
                                .into_builder()
                                .region(Some(s3::config::Region::new(x.to_string())))
                                .build();
                            if verbose > 0 {
                                eprintln!("Redirected to {}", x);
                            }
                            continue;
                        }
                    }
                }
                return Err(e.into());
            }
        };

        let size = match head.content_length {
            None => anyhow::bail!("Could not get content size"),
            Some(x) => x,
        };

        return Ok(ObjectInfo {
            config,
            size,
            etag: head.e_tag,
        });
    }
    anyhow::bail!("Stopped following redirects after 3 hops")
}

/// Drives the download.
///
/// Only the setup is async. The ordering loop runs on this thread, outside the
/// runtime, because it blocks: a write to a slow consumer can park it for a
/// long time, and a parked runtime thread stops servicing timers.
fn run(rt: &tokio::runtime::Runtime, args: &Args) -> anyhow::Result<Completion> {
    let (bucket, key) = parse_s3_path(&args.s3_path)?;

    let ObjectInfo { config, size, etag } =
        rt.block_on(probe_object(&bucket, &key, args.verbose))?;
    let stall_timeout = Duration::from_secs(args.stall_timeout);

    if etag.is_none() {
        eprintln!(
            "Warning: object reports no ETag, so blocks cannot be pinned to one \
             version. A concurrent overwrite would go undetected."
        );
    }

    if args.verbose > 0 {
        eprintln!("Downloading {} bytes", size);
    }

    let mut output = match &args.output {
        Some(path) => match FileOutput::create(path) {
            Err(e) => {
                eprintln!("Failed to open output file: {}", e);
                std::process::exit(1);
            }
            Ok(out) => Output::File(out),
        },
        None => Output::Stdout(std::io::stdout().lock()),
    };

    let diagnostics = Diagnostics::spawn();

    // Bounds blocks in flight, and so peak memory.
    let num_tokens = (2 * args.threads).max(1);
    let mut blocks = block_ranges(size, args.block_size).enumerate().fuse();
    let total_blocks = block_count(size, args.block_size);

    let (iter_sender, iter_receiver) = mpsc::channel(num_tokens);
    let (data_sender, mut data_receiver) = mpsc::channel(num_tokens);
    // An mpsc receiver has a single consumer, so the workers share it behind
    // a lock. Held only while taking an item, never across a download.
    let iter_receiver = Arc::new(tokio::sync::Mutex::new(iter_receiver));

    // Spawning needs a runtime context; this thread is otherwise outside one.
    // Dropped before the blocking ordering loop starts.
    let spawn_guard = rt.enter();

    let mut workers = JoinSet::new();
    for thread in 0..args.threads {
        let data_sender = data_sender.clone();
        let iter_receiver = iter_receiver.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let etag = etag.clone();
        let max_retries = args.max_retries;
        let config = config.clone();
        let diagnostics = diagnostics.clone();
        workers.spawn(async move {
            let data_sender = data_sender;
            let mut client = s3::Client::new(&config);
            loop {
                let Some((i, (start, end))) = ({ iter_receiver.lock().await.recv().await }) else {
                    break;
                };
                let mut retry_count = 0;
                let data = loop {
                    let attempt = download(
                        &client,
                        &bucket,
                        &key,
                        etag.as_deref(),
                        start,
                        end,
                        stall_timeout,
                    )
                    .await;
                    match attempt {
                        Err(e) => {
                            if !e.retryable {
                                break Err(e.source);
                            }
                            retry_count += 1;
                            if retry_count > max_retries {
                                break Err(e.source);
                            }
                            let waiting_time = backoff_seconds(retry_count);
                            diagnostics.report(format!(
                                "Thread {thread}: Failed to download chunk {i}: {}, retrying in {}s",
                                e.source, waiting_time
                            ));
                            tokio::time::sleep(Duration::from_secs(waiting_time)).await;
                            // Re-initialize client in case something fundamental changed
                            client = s3::Client::new(&config);
                        }
                        Ok(x) => break Ok(x),
                    }
                };
                if data_sender.send((i, data)).await.is_err() {
                    break;
                }
            }
        });
    }
    // Leave only the workers' clones, so the last one to exit closes the
    // channel and ends the ordering loop.
    drop(data_sender);

    // A worker that dies without delivering its block leaves the ordering
    // loop waiting on an index that never arrives, and the survivors waiting
    // on a queue that is never refilled. Tearing the rest down closes the data
    // channel, which ends the loop and lets the completeness check report what
    // is missing.
    //
    // This only works because the workers await on the queue. Tasks can be
    // cancelled at an await point, never inside a blocking call.
    tokio::spawn(async move {
        while let Some(outcome) = workers.join_next().await {
            if let Err(e) = outcome {
                if e.is_panic() {
                    eprintln!("Download task terminated unexpectedly: {}", e);
                }
                workers.abort_all();
            }
        }
    });

    drop(spawn_guard);

    let mut iter_sender = Some(iter_sender);
    let mut request_next_block = move || {
        let Some(sender) = iter_sender.as_ref() else {
            return Ok(());
        };
        match blocks.next() {
            None => {
                iter_sender = None;
            }
            Some(x) => {
                if sender.blocking_send(x).is_err() {
                    anyhow::bail!("Aborted during communication");
                }
            }
        }
        Ok(())
    };
    let mut pending = BTreeMap::new();
    let mut next_idx = 0;
    for _ in 0..num_tokens {
        request_next_block()?;
    }
    while let Some((idx, payload)) = data_receiver.blocking_recv() {
        pending.insert(idx, payload);
        while let Some(data) = pending.remove(&next_idx) {
            request_next_block()?;
            next_idx += 1;
            if let Err(e) = output.write_all(&data?) {
                if output.is_closed_consumer(&e) {
                    report_abandoned_errors(&pending);
                    return Ok(Completion::ConsumerClosed);
                }
                return Err(e.into());
            }
        }
    }

    // A closed data channel does not prove every block arrived; it also
    // closes when the workers are torn down. Comparing emitted against planned
    // is what stops a short download from exiting 0.
    if next_idx != total_blocks {
        anyhow::bail!("Incomplete download: wrote {next_idx} of {total_blocks} blocks");
    }

    // Dropping the writer flushes but discards the result, so a failure on
    // the final bytes has to be reported here.
    if let Err(e) = output.finish() {
        if output.is_closed_consumer(&e) {
            return Ok(Completion::ConsumerClosed);
        }
        return Err(e.into());
    }

    Ok(Completion::Finished)
}

fn main() {
    let args = Args::parse();

    // Concurrency comes from the task count, not the thread count, so the
    // pool stays at tokio's default. Nothing on the download path blocks a
    // pool thread.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    match run(&rt, &args) {
        Err(e) => {
            // The alternate form prints the source chain. An SdkError alone
            // renders as just "service error".
            eprintln!("Error: {:#}", e);
            std::process::exit(1);
        }
        // `s3get ... | head` closes the pipe deliberately. The reader's exit
        // status is the one that matters in a pipeline.
        Ok(Completion::ConsumerClosed) => std::process::exit(0),
        Ok(Completion::Finished) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_accepts_every_unit_spelling() {
        assert_eq!(parse_size("1048576").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("32MB").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_size("32mb").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_size("32M").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_size("512kb").unwrap(), 512 * 1024);
        assert_eq!(parse_size("512k").unwrap(), 512 * 1024);
        assert_eq!(parse_size("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("4096b").unwrap(), 4096);
    }

    /// `gb` must not match as a `b` suffix on the digits "2g", which would
    /// then fail to parse.
    #[test]
    fn parse_size_prefers_the_longest_unit() {
        assert_eq!(parse_size("2gb").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("32kb").unwrap(), 32 * 1024);
        assert_eq!(parse_size("32mb").unwrap(), 32 * 1024 * 1024);
    }

    /// A zero block size reaches step_by, which panics.
    #[test]
    fn parse_size_rejects_zero() {
        for input in ["0", "0MB", "0kb", "0g"] {
            assert!(parse_size(input).is_err(), "{input} should be rejected");
        }
    }

    /// Without `checked_mul` this wraps and yields a small, plausible size.
    #[test]
    fn parse_size_rejects_overflow_instead_of_wrapping() {
        assert!(parse_size("20000000000GB").is_err());
        assert!(parse_size("99999999999999999999999").is_err());
    }

    /// Past the cap the i64 range arithmetic goes negative and the block
    /// allocation panics.
    #[test]
    fn parse_size_rejects_values_past_the_cap() {
        assert!(parse_size("9223372036854775808").is_err());
        assert!(parse_size("8589934592gb").is_err());
        assert_eq!(parse_size("4gb").unwrap(), MAX_SIZE);
    }

    #[test]
    fn parse_size_rejects_malformed_input() {
        for input in ["", "  ", "abc", "1.5GB", "-5MB", "5tb", "mb", "b", "0x10"] {
            assert!(parse_size(input).is_err(), "{input:?} should be rejected");
        }
    }

    /// Zero workers download nothing.
    #[test]
    fn parse_threads_rejects_zero() {
        assert!(parse_threads("0").is_err());
        assert!(parse_threads("abc").is_err());
        assert_eq!(parse_threads("6").unwrap(), 6);
    }

    #[test]
    fn parse_s3_path_splits_bucket_from_key() {
        let (bucket, key) = parse_s3_path("s3://my-bucket/my-key.tar").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key.tar");

        // Keys legitimately contain slashes; only the first one separates.
        let (bucket, key) = parse_s3_path("s3://b/a/nested/key").unwrap();
        assert_eq!(bucket, "b");
        assert_eq!(key, "a/nested/key");
    }

    #[test]
    fn parse_s3_path_rejects_malformed_input() {
        for input in [
            "",
            "my-bucket/key",
            "http://b/k",
            "s3://",
            "s3://bucket",
            "s3://bucket/",
            "s3:///key",
        ] {
            assert!(
                parse_s3_path(input).is_err(),
                "{input:?} should be rejected"
            );
        }
    }

    /// The range header is inclusive, so a block's last byte is `end - 1`. An
    /// off-by-one here drops or duplicates a byte per block.
    #[test]
    fn block_ranges_tile_the_object_exactly() {
        for (size, block) in [
            (0, 10),
            (1, 10),
            (9, 10),
            (10, 10),
            (11, 10),
            (100, 10),
            (101, 10),
        ] {
            let ranges: Vec<_> = block_ranges(size, block).collect();
            assert_eq!(
                ranges.len(),
                block_count(size, block),
                "count for {size}/{block}"
            );

            let mut expected_start = 0;
            for (start, end) in &ranges {
                assert_eq!(*start, expected_start, "gap or overlap at {size}/{block}");
                assert!(end > start, "empty block at {size}/{block}");
                expected_start = *end;
            }
            assert_eq!(expected_start, size, "ranges must cover the whole object");
        }
    }

    #[test]
    fn block_count_handles_edges() {
        assert_eq!(block_count(0, 32), 0);
        assert_eq!(block_count(1, 32), 1);
        assert_eq!(block_count(32, 32), 1);
        assert_eq!(block_count(33, 32), 2);
    }

    /// An uncapped backoff schedules sleeps of days, and an unchecked pow
    /// overflows.
    #[test]
    fn backoff_is_bounded_at_every_retry_count() {
        for retry in [1u32, 2, 6, 20, 63, 64, u32::MAX] {
            let delay = backoff_seconds(retry);
            assert!(delay <= MAX_BACKOFF_SECS, "retry {retry} gave {delay}s");
        }
        // Growth is still exponential early on.
        assert!(backoff_seconds(1) <= 2);
        assert!(backoff_seconds(6) >= MAX_BACKOFF_SECS / 2);
    }

    /// Reporting must not block the caller. A worker parked writing to a
    /// stderr nobody drains sits inside its own poll, out of reach of both
    /// cancellation and the stall timeout, and the download stops.
    #[test]
    fn diagnostics_drop_messages_rather_than_block() {
        // Stands in for a logger thread parked in a write that never returns
        let (sender, receiver) = std::sync::mpsc::sync_channel::<String>(4);
        let diagnostics = Diagnostics(sender);

        let started = std::time::Instant::now();
        for i in 0..100_000 {
            diagnostics.report(format!("message {i}"));
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "reporting blocked for {:?}",
            started.elapsed()
        );

        // Reporting after the consumer is gone must not panic.
        drop(receiver);
        diagnostics.report("after shutdown".to_string());
    }

    /// Exiting 0 on a closed pipe must not apply to an explicit --output.
    #[test]
    fn closed_consumer_is_stdout_only() {
        let broken = std::io::Error::from(std::io::ErrorKind::BrokenPipe);
        let other = std::io::Error::from(std::io::ErrorKind::OutOfMemory);

        let stdout = Output::Stdout(std::io::stdout().lock());
        assert!(stdout.is_closed_consumer(&broken));
        assert!(!stdout.is_closed_consumer(&other));

        let dir = std::env::temp_dir().join(format!("s3get-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = Output::File(FileOutput::create(&dir.join("out.bin")).unwrap());
        assert!(!file.is_closed_consumer(&broken));
        drop(file);
        std::fs::remove_dir_all(&dir).ok();
    }
}
