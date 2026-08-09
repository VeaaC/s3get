# S3Get

[<img alt="build" src="https://img.shields.io/github/actions/workflow/status/VeaaC/s3get/cargo.yml?branch=main&style=for-the-badge">](https://github.com/VeaaC/s3get/actions?query=branch%3Amain)

Download a single file from S3 using parallel downloads.

## Usage Examples

Download a compressed archive and unpack it on the fly

```sh
s3get s3://my-bucket/my-key.tar.zstd -t 6 | pzstd -d | tar -xvf -
```

Download to a file instead of stdout

```sh
s3get s3://my-bucket/my-key.tar.zstd -o my-key.tar.zstd
```

## Why S3Get?

Because neither s5cmd, s3cmd, nor aws-cli can offer fast parallel downloads while piping to stdout

## Installation

The CLI app can be installed with [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html):

```sh
cargo install s3get
```

Requires Rust 1.94.1 or newer.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `-o`, `--output <PATH>` | stdout | Write to a file instead of stdout. |
| `--block-size <SIZE>` | `32MB` | Size of each ranged request. Accepts `32MB`, `512kb`, `1g` or a plain byte count, up to 4GB. |
| `-t`, `--threads <N>` | `6` | Number of chunks downloaded concurrently. |
| `--max-retries <N>` | `6` | Attempts per chunk before giving up. The AWS SDK retries up to 3 times inside each attempt, so the default permits roughly 18 requests for one chunk. |
| `--stall-timeout <SECS>` | `30` | Give up on a chunk and retry it if no data arrives for this long. Bounds connections that establish and then go quiet, which report no error of their own. |
| `-v`, `--verbose` | off | Print progress information. Repeat (`-vv`) for full error detail. |

Peak memory is roughly `2 * threads * block-size` — about 384MB at the
defaults. `-t 32 --block-size 128MB` would ask for 8GB.

## Credentials and region

Credentials and region are resolved by the AWS SDK in its standard order:
environment variables, then the shared config and credentials files, then
container and instance metadata. If no region is configured, `us-east-2` is
assumed; if the bucket lives elsewhere, S3's redirect is followed
automatically.

`AWS_ENDPOINT_URL` / `AWS_ENDPOINT_URL_S3` are honoured, which is useful for
S3-compatible stores, and so are `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`.

## Integrity

Every chunk is requested with `If-Match` against the ETag reported when the
download started, so an object replaced mid-transfer fails with a precondition
error rather than yielding a file spliced from two versions. Each chunk is
checked against its expected length, and the run fails unless every block was
written. If the object reports no ETag, version pinning is unavailable and a
warning is printed.

SDK-side response checksum validation is disabled deliberately: it has no
notion of ranged requests, and every request this tool makes is a range.

## Behaviour worth knowing

- **Piping to a consumer that exits early** — `s3get ... | head` exits 0
  silently. The reader's status is what matters in a pipeline. Any chunk
  errors already collected are reported as warnings first.
- **`--output` is staged** — the download is written to a hidden sibling file
  and renamed into place once complete, so an interrupted run never leaves a
  truncated file that looks finished. A run killed by a signal leaves a
  `.<name>.s3get-<id>.part` file behind, which is safe to delete.
- Staging applies only when the destination is a regular file. `/dev/null`, a
  FIFO, a device node or a symlink is written through directly.
- Replacing an existing file swaps in a new inode, so its mode is preserved but
  ACLs, extended attributes and hard links are not.
