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

Peak memory is roughly `2 * threads * block-size`, so about 384MB at the
defaults. `-t 32 --block-size 128MB` asks for 8GB.

## Credentials and region

Credentials and region are resolved by the AWS SDK in its standard order:
environment variables, then the shared config and credentials files, then
container and instance metadata. Without a configured region `us-east-2` is
assumed, and S3's redirect to the bucket's actual region is followed.

`AWS_ENDPOINT_URL` / `AWS_ENDPOINT_URL_S3` are honoured, which is useful for
S3-compatible stores, and so are `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`.

If no credential source responds (no environment variables, no config file,
unreachable instance metadata) the SDK takes about 20 seconds to give up.

## Integrity

Every chunk is requested with `If-Match` against the ETag read at the start,
so an object replaced mid-transfer fails with a precondition error instead of
producing a file spliced from two versions. Each chunk is checked against its
expected length, and the run fails unless every block was written. If the
object reports no ETag there is nothing to pin to, and a warning is printed.

SDK-side response checksum validation is disabled on purpose: it has no notion
of ranged requests, and every request here is a range.

## Behaviour notes

- `s3get ... | head` exits 0 silently. The reader's status is what matters in
  a pipeline. Chunk errors collected before that point are printed as
  warnings. This waives the all-or-nothing guarantee for that case:
  `s3get ... | true` also exits 0, having written nothing.
- `--output` is held to the stricter rule, so `s3get ... -o /dev/stdout | head`
  exits non-zero where the plain pipe form exits 0.
- `--output` is staged: the download goes to a hidden sibling file and is
  renamed into place once complete, so an interrupted run leaves no truncated
  file at the destination. A run killed by a signal leaves a
  `.<name>.s3get-<id>.part` file, which is safe to delete.
- Staging applies only to regular files. `/dev/null`, FIFOs, device nodes and
  symlinks are written through directly. If the sibling cannot be created (a
  read-only directory, or a name too long with the suffix) the download falls
  back to writing in place and warns on stderr; that run can leave a partial
  file.
- Replacing a file swaps in a new inode. The mode is preserved; ACLs, extended
  attributes and hard links are not, and a privileged run becomes the owner.
- Retry diagnostics are best-effort. If stderr is not being read, messages are
  dropped rather than stalling the download.
