use aws_sdk_s3 as s3;
use clap::Parser;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

    /// Number of concurrent downloads
    #[arg(long, short, default_value = "6")]
    threads: usize,

    /// Print verbose information, statistics, etc
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Determines how often each chunk should be retried before giving up
    #[arg(long, default_value = "6")]
    max_retries: u32,
}

async fn download(
    client: &s3::Client,
    bucket: &str,
    key: &str,
    start: i64,
    end: i64,
) -> anyhow::Result<Vec<u8>> {
    let object = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={}-{}", start, end - 1))
        .send()
        .await?;

    Ok(object.body.collect().await?.into_bytes().to_vec())
}

async fn config_and_size(
    bucket: &str,
    key: &str,
    verbose: u8,
) -> anyhow::Result<(aws_config::SdkConfig, i64)> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let region = config.region().cloned();
    let mut config = config
        .into_builder()
        .region(region.or_else(|| Some(s3::config::Region::new("us-east-2"))))
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
                    if response.raw().status().as_u16() == 301 {
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

        let size = head.content_length.unwrap_or(0);
        if size == 0 {
            anyhow::bail!("Could not get content size (or file is empty)");
        }

        return Ok((config, size));
    }
    anyhow::bail!("Stopped following redirects after 3 hops")
}

async fn run(args: &Args) -> anyhow::Result<()> {
    let (bucket, key) = match args.s3_path.strip_prefix("s3://") {
        None => anyhow::bail!("S3 path has to start with 's3://'"),
        Some(x) => match x.split_once('/') {
            None => anyhow::bail!("S3 path should be 's3://bucket/key'"),
            Some((bucket, key)) => (bucket.to_string(), key.to_string()),
        },
    };

    let (config, size) = config_and_size(&bucket, &key, args.verbose).await?;

    if args.verbose > 0 {
        eprintln!("Downloading {} bytes", size);
    }

    let mut output: Box<dyn Write + Send + Sync> = if let Some(file) = &args.output {
        Box::new(std::io::BufWriter::new(std::fs::File::create(file)?))
    } else {
        Box::new(std::io::BufWriter::new(std::io::stdout()))
    };

    let blocks: Vec<(usize, (i64, i64))> = (0..size)
        .step_by(args.block_size)
        .map(|start| (start, size.min(start + args.block_size as i64)))
        .enumerate()
        .collect();

    let total_blocks = blocks.len();
    let num_buffer = 2 * args.threads;
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<(usize, anyhow::Result<Vec<u8>>)>(num_buffer);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(args.threads));
    let cancelled = Arc::new(AtomicBool::new(false));
    let client = s3::Client::new(&config);

    let dispatcher = {
        let tx = tx;
        let semaphore = semaphore.clone();
        let cancelled = cancelled.clone();
        let client = client.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let config = config.clone();
        let max_retries = args.max_retries;
        let verbose = args.verbose;
        tokio::spawn(async move {
            for (i, (start, end)) in blocks {
                if cancelled.load(Ordering::Relaxed) || tx.is_closed() {
                    break;
                }
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let tx = tx.clone();
                let cancelled = cancelled.clone();
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    let mut current_client = client;
                    let mut retry_count = 0u32;
                    let data = loop {
                        match download(&current_client, &bucket, &key, start, end).await {
                            Ok(x) => break Ok(x),
                            Err(e) => {
                                retry_count += 1;
                                if retry_count > max_retries
                                    || cancelled.load(Ordering::Relaxed)
                                {
                                    break Err(e);
                                }
                                let waiting_time = 2u64.pow(retry_count);
                                if verbose > 0 {
                                    eprintln!(
                                        "Failed to download chunk {i}: {e}, retrying in {waiting_time}s"
                                    );
                                }
                                tokio::time::sleep(Duration::from_secs(waiting_time)).await;
                                current_client = s3::Client::new(&config);
                            }
                        }
                    };
                    let _ = tx.send((i, data)).await;
                });
            }
        })
    };

    let mut pending: BTreeMap<usize, anyhow::Result<Vec<u8>>> = BTreeMap::new();
    let mut next_idx = 0usize;

    while let Some((i, data)) = rx.recv().await {
        pending.insert(i, data);
        while let Some(data) = pending.remove(&next_idx) {
            next_idx += 1;
            match data {
                Ok(bytes) => {
                    if let Err(e) = output.write_all(&bytes) {
                        cancelled.store(true, Ordering::Relaxed);
                        semaphore.close();
                        return Err(e.into());
                    }
                }
                Err(e) => {
                    cancelled.store(true, Ordering::Relaxed);
                    semaphore.close();
                    return Err(e);
                }
            }
        }
        if next_idx >= total_blocks {
            break;
        }
    }

    output.flush()?;
    let _ = dispatcher.await;

    Ok(())
}

fn main() {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    if let Err(e) = rt.block_on(run(&args)) {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            if io_err.kind() == std::io::ErrorKind::BrokenPipe {
                std::process::exit(0);
            }
        }
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
