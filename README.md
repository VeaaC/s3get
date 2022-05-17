# S3Get

[<img alt="build" src="https://img.shields.io/github/workflow/status/VeaaC/s3get/Shuffly%20CI/main?style=for-the-badge">](https://github.com/Veaac/s3get/actions?query=branch%3Amain)

Download a single file from S3 using parallel downloads.

## Usage Examples

Download a compressed archive and unpack it on the fly

```sh
s3get s3://my-bucket/my-key.tar.zstd -t 6 | pzstd -d | tar -xvf -
```

## Installation

The CLI app can be installed with [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html):

```sh
cargo install s3get
```

## Why S3Get?

Because neither s5cmd, s3cmd, nor aws-cli can offer fast parallel downloads while piping to stdout