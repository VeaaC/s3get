use aws_sdk_s3 as s3;
use clap::Parser;
use crossbeam::channel;
use http::StatusCode;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
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

    /// Number of threads to use, defaults to number of logical cores
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
    let mut object = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={}-{}", start, end - 1))
        .send()
        .await?;

    let mut result = Vec::new();
    while let Some(chunk) = object.body.try_next().await? {
        result.extend(chunk);
    }

    Ok(result)
}

async fn config_and_size(
    bucket: &str,
    key: &str,
    verbose: u8,
) -> anyhow::Result<(aws_config::SdkConfig, i64)> {
    let config = aws_config::load_from_env().await;
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
                    if response.raw().status().as_u16() == StatusCode::MOVED_PERMANENTLY {
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

    let mut output: Box<dyn std::io::Write + Send + Sync> = if let Some(file) = &args.output {
        Box::new(match std::fs::File::create(file) {
            Err(e) => {
                eprintln!("Failed to open output file: {}", e);
                std::process::exit(1);
            }
            Ok(x) => x,
        })
    } else {
        Box::new(std::io::stdout())
    };

    let num_tokens = 2 * args.threads;
    let mut blocks = (0..size)
        .step_by(args.block_size)
        .map(move |start| (start, size.min(start + args.block_size as i64)))
        .enumerate()
        .fuse();

    let (iter_sender, iter_receiver) = channel::bounded(num_tokens);
    let (data_sender, data_receiver) = channel::bounded(num_tokens);

    for thread in 0..args.threads {
        let data_sender = data_sender.clone();
        let iter_receiver = iter_receiver.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let max_retries = args.max_retries;
        let config = config.clone();
        tokio::spawn(async move {
            let data_sender = data_sender;
            let mut client = s3::Client::new(&config);
            while let Ok((i, (start, end))) = iter_receiver.recv() {
                let mut retry_count = 0;
                let data = loop {
                    match download(&client, &bucket, &key, start, end).await {
                        Err(e) => {
                            retry_count += 1;
                            if retry_count > max_retries {
                                break Err(e);
                            }
                            let waiting_time = 2_u64.pow(retry_count);
                            eprintln!(
                                "Thread {thread}: Failed to download chunk {i}: {}, retrying in {}s",
                                e, waiting_time
                            );
                            tokio::time::sleep(Duration::from_secs(waiting_time)).await;
                            // Re-initialize client in case something fundamental changed
                            client = s3::Client::new(&config);
                        }
                        Ok(x) => break Ok(x),
                    }
                };
                if data_sender.send((i, data)).is_err() {
                    break;
                }
            }
        });
    }
    drop(data_sender); // drop to make sure iteration will finish once all senders are out of scope

    let mut iter_sender = Some(iter_sender);
    let mut request_next_block = move || match &iter_sender {
        None => Ok(()),
        Some(sender) => {
            match blocks.next() {
                None => {
                    iter_sender = None;
                }
                Some(x) => {
                    if sender.send(x).is_err() {
                        anyhow::bail!("Aborted during communication");
                    }
                }
            }
            Ok(())
        }
    };
    let mut pending = BTreeMap::new();
    let mut next_idx = 0;
    for _ in 0..num_tokens {
        request_next_block()?;
    }
    for result in data_receiver {
        pending.insert(Reverse(result.0), result.1);
        while let Some(data) = pending.remove(&Reverse(next_idx)) {
            request_next_block()?;
            next_idx += 1;
            output.write_all(&data?)?;
        }
    }

    Ok(())
}

fn main() {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads + 2) // we need 2 extra threads for blocking I/O
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    if let Err(e) = rt.block_on(async move { run(&args).await }) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
