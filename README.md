# S3 Shard Ingest Worker

This repository is designed to be **cloned and integrated into your own project**.
It provides reusable Python logic for distributed ingest jobs coordinated through S3.

## What It Does

Each worker:

1. claims one shard DB file from S3 (`pending -> running`) atomically,
2. processes pending batch jobs from that shard,
3. writes parquet output files to S3,
4. uploads the updated shard DB to `completed`.

Run many workers in parallel against the same shard prefix for horizontal scaling.

## Shard Prefix Layout

- `<shard-prefix>/pending/<shard-file>`
- `<shard-prefix>/running/<shard-file>`
- `<shard-prefix>/completed/<shard-file>`
- `<shard-prefix>/failed/<shard-file>` (optional custom handling)

## Integrate Into Your Project

Clone this repo (or copy `src/kalshi_ingest`) and use the API directly:

```python
from pathlib import Path
from kalshi_ingest import WorkerConfig, run_worker

config = WorkerConfig(
    shard_bucket="your-shard-bucket",
    shard_prefix="shards/live",
    s3_bucket="your-output-bucket",
    s3_prefix="candlesticks_raw",
    out_dir=Path("/tmp/parquet"),
    log_dir=Path("./log"),
    aws_region="us-east-1",
    max_rps=4.0,
    threads=2,
    memory_limit="512MB",
    delete_local=True,
)

run_worker(config)
```

## Minimal Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
```

## Worker Job Table

Shard DB files are DuckDB databases containing `candlestick_job_batches`.
The worker ensures schema if missing, and updates `status/error_message/rows_written/output_file`.

## Optional Terraform

`terraform/` includes an example AWS deployment that launches EC2 workers using this repo.
See `/Users/lucas/coding/projects/alpha_ops_aws/terraform/README.md`.

## Compatibility Note

Legacy command usage (`python3 -m ingest_shards_ec2 ...`) still works via a thin wrapper.
