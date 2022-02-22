use crossbeam::channel;
use futures::future;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, S3Client, S3};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use structopt::StructOpt;

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

#[derive(StructOpt, Debug)]
#[structopt(name = "s3get")]
struct Args {
    /// S3 path to download from
    s3_path: String,

    /// output file name
    #[structopt(long, short = "o")]
    output: Option<PathBuf>,

    /// block size used for data downloads
    #[structopt(long, default_value = "32MB", parse(try_from_str=parse_size))]
    block_size: usize,

    /// number of threads to use, defaults to number of logical cores
    #[structopt(long, short = "t", default_value = "6")]
    threads: usize,

    /// Print verbose information, statistics, etc
    #[structopt(long, short = "v")]
    verbose: bool,
}

fn parallel_process<Data, Iter, Item, Producer, Consumer>(
    threads: usize,
    iter: Iter,
    produce: Producer,
    mut consume: Consumer,
) -> Result<(), anyhow::Error>
where
    Iter: Iterator<Item = Item> + Send + 'static,
    Item: Send + 'static,
    Producer: Clone + Fn(S3Client, Item) -> BoxFuture<'static, Data> + Send + Sync + 'static,
    Data: Send + 'static,
    Consumer: FnMut(Data) -> anyhow::Result<()> + Send + 'static,
{
    let num_tokens = 2 * threads;

    let (token_sender, token_reciver) = channel::bounded(num_tokens);
    let (iter_sender, iter_receiver) = channel::bounded(num_tokens);
    let (data_sender, data_receiver) = channel::bounded(num_tokens);

    tokio::spawn(future::lazy(move |_| {
        for x in iter.enumerate() {
            if token_reciver.recv().is_err() {
                break;
            }
            if iter_sender.send(x).is_err() {
                break;
            }
        }
        std::mem::drop(iter_sender);
    }));

    for _ in 0..threads {
        let data_sender = data_sender.clone();
        let iter_receiver = iter_receiver.clone();
        let produce = produce.clone();
        tokio::spawn(async move {
            let data_sender = data_sender;
            let client = S3Client::new(Default::default());
            while let Ok((i, item)) = iter_receiver.recv() {
                let data = produce(client.clone(), item).await;
                if data_sender.send((i, data)).is_err() {
                    break;
                }
            }
        });
    }
    drop(data_sender); // drop to make sure iteration will finish once all senders are out of scope

    // we need to move these into the scope so they are dropped on failure
    let token_sender = token_sender;
    let data_receiver = data_receiver;
    let mut pending = BTreeMap::new();
    let mut next_idx = 0;
    for _ in 0..num_tokens {
        if token_sender.send(()).is_err() {
            anyhow::bail!("Aborted during initialization");
        }
    }
    for result in data_receiver {
        pending.insert(Reverse(result.0), result.1);
        while let Some(data) = pending.remove(&Reverse(next_idx)) {
            if token_sender.send(()).is_err() {
                // It's ok, we will just consume the remaining data
            }

            next_idx += 1;
            consume(data)?;
        }
    }
    Ok(())
}

async fn download(
    client: &S3Client,
    bucket: &str,
    key: &str,
    start: i64,
    end: i64,
) -> anyhow::Result<Vec<u8>> {
    let mut object = client
        .get_object(GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            range: Some(format!("bytes={}-{}", start, end - 1)),
            ..Default::default()
        })
        .await?;

    let mut body = object.body.take().expect("The object has no body");

    let mut result = Vec::new();
    while let Some(chunk) = body.try_next().await? {
        result.extend(chunk);
    }

    Ok(result)
}

async fn run(args: &Args) -> anyhow::Result<()> {
    let (bucket, key) = match args.s3_path.strip_prefix("s3://") {
        None => anyhow::bail!("S3 path has to start with 's3://'"),
        Some(x) => match x.split_once('/') {
            None => anyhow::bail!("S3 path should be 's3://bucket/key'"),
            Some((bucket, key)) => (bucket.to_string(), key.to_string()),
        },
    };

    let client = S3Client::new(Default::default());
    let head = client
        .head_object(HeadObjectRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            ..Default::default()
        })
        .await?;

    let size = match head.content_length {
        None => anyhow::bail!("Could not get content size"),
        Some(x) => x,
    };

    if args.verbose {
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

    let block_size = args.block_size;
    parallel_process(
        args.threads,
        (0..size)
            .step_by(args.block_size)
            .map(move |start| (start, size.min(start + block_size as i64))),
        move |client, (start, end)| {
            let key = key.clone();
            let bucket = bucket.clone();
            Box::pin(async move { download(&client, &bucket, &key, start, end).await })
        },
        move |data: anyhow::Result<Vec<u8>>| -> anyhow::Result<()> {
            output.write_all(&data?)?;
            Ok(())
        },
    )?;

    Ok(())
}

fn main() {
    let args = Args::from_args();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads + 2) // we need 2 extra threads for blocking I/O
        .enable_io()
        .build()
        .unwrap();

    if let Err(e) = rt.block_on(async move { run(&args).await }) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
