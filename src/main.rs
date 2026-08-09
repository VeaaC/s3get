use aws_sdk_s3 as s3;
use clap::Parser;
use s3::error::{ProvideErrorMetadata, SdkError};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// S3 answers a request aimed at the wrong region with a permanent redirect
/// carrying an `x-amz-bucket-region` header.
///
/// Defined here so the crate needs no dependency on `http`, whose major
/// version would have to track the one vendored inside the smithy runtime.
const HTTP_MOVED_PERMANENTLY: u16 = 301;

/// Ceiling on retry backoff, so a large `--max-retries` cannot schedule a
/// wake-up years away.
const MAX_BACKOFF_SECS: u64 = 60;

/// Error codes S3 returns for conditions that clear on their own. Status alone
/// is not enough to recognise them: `RequestTimeout` arrives as an HTTP 400,
/// which is otherwise the signature of a request that will never succeed.
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

/// Delay before the `retry_count`-th retry: exponential growth, capped, with
/// equal jitter so that workers failing together do not return in lockstep.
fn backoff_seconds(retry_count: u32) -> u64 {
    let capped = 2_u64.saturating_pow(retry_count).min(MAX_BACKOFF_SECS);
    let half = capped / 2;
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    half + jitter % (half + 1)
}

/// Where downloaded blocks are written.
enum Output {
    File(std::fs::File),
    Stdout(std::io::Stdout),
}

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Output::File(file) => file.write(buf),
            Output::Stdout(stdout) => stdout.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Output::File(file) => file.flush(),
            Output::Stdout(stdout) => stdout.flush(),
        }
    }
}

impl Output {
    /// Completes the write, surfacing errors that are otherwise discarded.
    ///
    /// A file's `flush` is a no-op and the result of its `close` is dropped
    /// along with the handle, so on a filesystem that defers error reporting --
    /// NFS, or a quota hit under delayed allocation -- `sync_all` is what turns
    /// a failed write into a failed exit.
    fn finish(&mut self) -> std::io::Result<()> {
        self.flush()?;
        if let Output::File(file) = self {
            file.sync_all()?;
        }
        Ok(())
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

fn parse_size(x: &str) -> anyhow::Result<usize> {
    let x = x.to_ascii_lowercase();
    if let Some(value) = x.strip_suffix("gb") {
        return Ok(usize::from_str(value)? * 1024 * 1024 * 1024);
    }
    if let Some(value) = x.strip_suffix("mb") {
        return Ok(usize::from_str(value)? * 1024 * 1024);
    }
    if let Some(value) = x.strip_suffix("kb") {
        return Ok(usize::from_str(value)? * 1024);
    }
    anyhow::bail!("Cannot parse size: '{}'", x)
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// S3 path to download from
    s3_path: String,

    /// Output file name, print to stdout otherwise
    #[arg(long, short)]
    output: Option<PathBuf>,

    /// Block size used for data downloads
    #[arg(long, default_value = "32MB", value_parser = parse_size)]
    block_size: usize,

    /// Number of threads to use, defaults to number of logical cores
    #[arg(long, short, default_value = "6")]
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
/// `etag` pins the request to one specific version of the object: if it is
/// replaced while a download is in flight, S3 answers 412 instead of serving
/// bytes that would be spliced together with blocks from the previous version.
///
/// `stall_timeout` bounds the wait for the response and for each subsequent
/// body chunk. A connection that establishes and then goes quiet produces no
/// error of its own, so without this the block would never complete.
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
                // 412 lands here when `if_match` fails, i.e. the object was
                // replaced. Retrying cannot recover the version we started
                // with, so it is deliberately terminal, as are 403 and 404.
                SdkError::ServiceError(context) => {
                    let status = context.raw().status().as_u16();
                    let code = context.err().code().unwrap_or_default();
                    status == 408
                        || status == 429
                        || (500..600).contains(&status)
                        || RETRYABLE_ERROR_CODES.contains(&code)
                }
                // A dispatch failure caused by configuration -- an unresolvable
                // endpoint, an untrusted certificate chain -- fails the same
                // way every time, so only transport-level ones are retried.
                SdkError::DispatchFailure(failure) => !failure.is_user(),
                SdkError::ConstructionFailure(_) => false,
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
    let mut result = Vec::new();
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

/// What a successful `HeadObject` tells us: the config that reached the
/// object (possibly after a region redirect), its length, and the version tag
/// every subsequent ranged request is pinned to.
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
        // `WhenSupported` asks S3 for a checksum and validates the body
        // against it, but the validator has no notion of ranged requests and
        // every request here is a range. Its only guard is spotting the `-N`
        // suffix of a composite multipart checksum, which S3 need not include
        // on a partial response, so a whole-object checksum can end up
        // compared against a single block. Integrity comes from TLS and the
        // per-block length check in `download`.
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
/// Only the setup is asynchronous. The ordering loop runs on this thread,
/// outside the runtime, because it makes blocking calls -- a write to a slow
/// consumer can park it indefinitely, and a runtime thread parked in a
/// blocking call is a runtime thread that cannot service timers.
fn run(rt: &tokio::runtime::Runtime, args: &Args) -> anyhow::Result<()> {
    let (bucket, key) = match args.s3_path.strip_prefix("s3://") {
        None => anyhow::bail!("S3 path has to start with 's3://'"),
        Some(x) => match x.split_once('/') {
            None => anyhow::bail!("S3 path should be 's3://bucket/key'"),
            Some((bucket, key)) => (bucket.to_string(), key.to_string()),
        },
    };

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
        Some(path) => match std::fs::File::create(path) {
            Err(e) => {
                eprintln!("Failed to open output file: {}", e);
                std::process::exit(1);
            }
            Ok(file) => Output::File(file),
        },
        None => Output::Stdout(std::io::stdout()),
    };

    // Bounds how many blocks may be in flight, and so the peak memory.
    let num_tokens = (2 * args.threads).max(1);
    let mut blocks = (0..size)
        .step_by(args.block_size)
        .map(move |start| (start, size.min(start + args.block_size as i64)))
        .enumerate()
        .fuse();
    let total_blocks = (size as u64).div_ceil(args.block_size as u64) as usize;

    let (iter_sender, iter_receiver) = mpsc::channel(num_tokens);
    let (data_sender, mut data_receiver) = mpsc::channel(num_tokens);
    // The work queue has many consumers, which an mpsc receiver does not allow
    // on its own. The lock is held only across taking an item, never across a
    // download.
    let iter_receiver = Arc::new(tokio::sync::Mutex::new(iter_receiver));

    // Spawning requires a runtime context, which this thread is otherwise
    // outside of. Dropped before the blocking ordering loop begins.
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
                            eprintln!(
                                "Thread {thread}: Failed to download chunk {i}: {}, retrying in {}s",
                                e.source, waiting_time
                            );
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
    // Only the workers' clones should keep this open, so that the last worker
    // to exit closes it and ends the ordering loop.
    drop(data_sender);

    // A worker that dies without delivering its block leaves the ordering loop
    // waiting on an index that can never arrive, while the survivors wait on a
    // queue that is never refilled. Tearing the rest down closes the data
    // channel, which ends that loop and lets the completeness check report
    // what is missing.
    //
    // This works only because the workers are fully asynchronous: a task can
    // be cancelled at an await point, never inside a blocking call.
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
        pending.insert(Reverse(idx), payload);
        while let Some(data) = pending.remove(&Reverse(next_idx)) {
            request_next_block()?;
            next_idx += 1;
            output.write_all(&data?)?;
        }
    }

    // The data channel closing is not by itself proof that every block was
    // written: it also closes when the workers are torn down. Comparing what
    // was emitted against what was planned is what keeps a short download from
    // exiting successfully.
    if next_idx != total_blocks {
        anyhow::bail!("Incomplete download: wrote {next_idx} of {total_blocks} blocks");
    }

    // Dropping the writer would flush but discard the result, so a failure on
    // the final bytes has to be surfaced explicitly.
    output.finish()?;

    Ok(())
}

fn main() {
    let args = Args::parse();

    // Concurrency comes from the number of tasks, not the number of threads,
    // so the pool is left at tokio's default. Nothing running on it blocks.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    if let Err(e) = run(&rt, &args) {
        // Alternate form prints the source chain; an SdkError on its own
        // renders as just "service error".
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}
