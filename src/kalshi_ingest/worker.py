from __future__ import annotations

import argparse
import json
import os
import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Optional, Dict, Any

import duckdb
import requests
import boto3
from botocore.exceptions import ClientError, BotoCoreError

# ============================================================
# CONSTANTS (API semantics)
# ============================================================

PERIOD_INTERVAL = 1440  # minutes (1 day candles)

MAX_RETRIES = 6
BASE_BACKOFF_S = 0.5
MAX_BACKOFF_S = 20.0

DEFAULT_USER_AGENT = "s3-shard-ingest-worker/1.0"

# ============================================================
# CLI
# ============================================================


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "S3 shard ingest worker (claims shard DB from S3, processes batches, "
            "and uploads parquet outputs to S3)."
        )
    )

    # shard source
    p.add_argument("--shard-bucket", required=True)
    p.add_argument("--shard-prefix", required=True, help="e.g. shards/test")

    # parquet destination
    p.add_argument("--s3-bucket", required=True)
    p.add_argument(
        "--s3-prefix", default="candlesticks_raw", help="e.g. candlesticks_raw"
    )

    # local staging
    p.add_argument("--out-dir", required=True)
    p.add_argument("--log-dir", required=True)

    # perf + http
    p.add_argument("--max-rps", type=float, default=4.0)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--threads", type=int, default=1)
    p.add_argument("--memory-limit", type=str, default="512MB")
    p.add_argument("--aws-region", type=str, default=os.getenv("AWS_REGION"))
    p.add_argument("--user-agent", type=str, default=DEFAULT_USER_AGENT)
    p.add_argument("--delete-local", action="store_true")
    p.add_argument("--log-level", type=str, default="INFO")

    # optional: limit batches per shard (useful for testing)
    p.add_argument("--batch-limit", type=int, default=None)

    # optional: limit shards per worker (useful for testing)
    p.add_argument(
        "--shard-limit",
        type=int,
        default=None,
        help="Max number of shards to process in this worker run",
    )

    return p.parse_args()


@dataclass(frozen=True)
class WorkerConfig:
    shard_bucket: str
    shard_prefix: str
    s3_bucket: str
    s3_prefix: str = "candlesticks_raw"
    out_dir: Path = Path("/tmp/parquet")
    log_dir: Path = Path("./log")
    max_rps: float = 4.0
    timeout: int = 60
    threads: int = 1
    memory_limit: str = "512MB"
    aws_region: Optional[str] = None
    user_agent: str = DEFAULT_USER_AGENT
    delete_local: bool = False
    log_level: str = "INFO"
    batch_limit: Optional[int] = None
    shard_limit: Optional[int] = None


def config_from_args(args: argparse.Namespace) -> WorkerConfig:
    return WorkerConfig(
        shard_bucket=args.shard_bucket,
        shard_prefix=args.shard_prefix,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        out_dir=Path(args.out_dir),
        log_dir=Path(args.log_dir),
        max_rps=args.max_rps,
        timeout=args.timeout,
        threads=args.threads,
        memory_limit=args.memory_limit,
        aws_region=args.aws_region,
        user_agent=args.user_agent,
        delete_local=args.delete_local,
        log_level=args.log_level,
        batch_limit=args.batch_limit,
        shard_limit=args.shard_limit,
    )


# ============================================================
# PATHS + LOGGING
# ============================================================


@dataclass(frozen=True)
class Paths:
    db_path: Path
    out_dir: Path
    log_dir: Path


def resolve_paths(config: WorkerConfig) -> Paths:
    out_dir = config.out_dir
    log_dir = config.log_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    return Paths(
        db_path=Path("/tmp/jobs.duckdb"),
        out_dir=out_dir,
        log_dir=log_dir,
    )


def setup_logging(log_dir: Path, level: str) -> logging.Logger:
    log_file = log_dir / f"shard_worker_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger("shard_worker")


# ============================================================
# RATE LIMITER + HTTP
# ============================================================


class RateLimiter:
    def __init__(self, max_rps: float):
        self.interval = 1.0 / max(max_rps, 0.01)
        self.last = 0.0

    def wait(self):
        now = time.monotonic()
        delay = self.interval - (now - self.last)
        if delay > 0:
            time.sleep(delay)
        self.last = time.monotonic()


def _sleep_backoff(attempt: int, retry_after: Optional[str] = None):
    if retry_after:
        try:
            ra = float(retry_after)
            time.sleep(min(ra, MAX_BACKOFF_S))
            return
        except ValueError:
            pass
    backoff = min(BASE_BACKOFF_S * (2**attempt), MAX_BACKOFF_S)
    time.sleep(backoff + random.uniform(0, backoff * 0.25))


def fetch_json(
    url: str,
    *,
    timeout: int,
    limiter: RateLimiter,
    user_agent: str,
) -> Dict[str, Any]:
    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    for attempt in range(MAX_RETRIES + 1):
        try:
            limiter.wait()
            r = requests.get(url, headers=headers, timeout=timeout)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 400:
                raise RuntimeError(f"HTTP 400: {r.text[:1200]}")

            if r.status_code == 429 and attempt < MAX_RETRIES:
                _sleep_backoff(attempt, r.headers.get("Retry-After"))
                continue

            if r.status_code in (500, 502, 503, 504) and attempt < MAX_RETRIES:
                _sleep_backoff(attempt)
                continue

            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:1200]}")

        except (requests.Timeout, requests.ConnectionError):
            if attempt < MAX_RETRIES:
                _sleep_backoff(attempt)
                continue
            raise

    raise RuntimeError("Exceeded max retries")


# ============================================================
# DUCKDB HELPERS (same as local)
# ============================================================


def ensure_schema(con: duckdb.DuckDBPyConnection):
    # shards should already have this table, but keep safety
    con.execute("""
        CREATE TABLE IF NOT EXISTS candlestick_job_batches (
            batch_id BIGINT PRIMARY KEY,
            market_tickers TEXT NOT NULL,
            start_ts BIGINT NOT NULL,
            end_ts BIGINT NOT NULL,
            resolution_s INTEGER NOT NULL,
            expected_candles INTEGER NOT NULL,
            url TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            error_message TEXT,
            last_attempt_ts TIMESTAMP,
            attempt_count INTEGER DEFAULT 0,
            completed_ts TIMESTAMP,
            rows_written BIGINT,
            output_file TEXT
        );
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_batches_pending
        ON candlestick_job_batches(status, batch_id);
    """)


def claim_batch(con):
    q = """
    UPDATE candlestick_job_batches
    SET status='running', last_attempt_ts=CURRENT_TIMESTAMP,
        attempt_count=attempt_count+1, error_message=NULL
    WHERE batch_id = (
        SELECT batch_id FROM candlestick_job_batches
        WHERE status='pending'
        ORDER BY batch_id
        LIMIT 1
    )
    RETURNING *
    """
    row = con.execute(q).fetchone()
    if not row:
        return None
    cols = [c[0] for c in con.execute("DESCRIBE candlestick_job_batches").fetchall()]
    return dict(zip(cols, row))


def mark_batch(
    con,
    *,
    batch_id: int,
    status: str,
    error: Optional[str],
    rows: int,
    out_file: Optional[str],
):
    con.execute(
        """
        UPDATE candlestick_job_batches
        SET status=?, error_message=?, rows_written=?, output_file=?,
            completed_ts=CASE WHEN ?='completed' THEN CURRENT_TIMESTAMP ELSE completed_ts END
        WHERE batch_id=?
        """,
        (status, error, rows, out_file, status, batch_id),
    )


# ============================================================
# JSON → PARQUET (same as local)
# ============================================================


def write_parquet(con, response: dict, out_file: Path) -> int:
    tmp = out_file.with_suffix(".json")
    tmp.write_text(json.dumps(response), encoding="utf-8")

    sql = f"""
    COPY (
        WITH src AS (
            SELECT *
            FROM read_json_auto('{tmp.as_posix()}', maximum_depth=8)
        ),
        m AS (
            SELECT unnest(markets) AS market FROM src
        ),
        c AS (
            SELECT
                market.market_ticker AS market_ticker,
                unnest(COALESCE(market.candlesticks, [])) AS candle
            FROM m
            WHERE array_length(COALESCE(market.candlesticks, [])) > 0
        )
        SELECT
            market_ticker,
            try_cast(candle.end_period_ts AS BIGINT) AS end_period_ts,
            {PERIOD_INTERVAL}::INTEGER AS period_interval,
            try_cast(candle.open_interest AS BIGINT) AS open_interest,
            try_cast(candle.volume AS BIGINT) AS volume,

            json_extract_string(candle.price, '$.open_dollars')     AS price_open_dollars,
            json_extract_string(candle.price, '$.close_dollars')    AS price_close_dollars,
            json_extract_string(candle.price, '$.high_dollars')     AS price_high_dollars,
            json_extract_string(candle.price, '$.low_dollars')      AS price_low_dollars,
            json_extract_string(candle.price, '$.mean_dollars')     AS price_mean_dollars,
            json_extract_string(candle.price, '$.previous_dollars') AS price_previous_dollars,

            json_extract_string(candle.yes_bid, '$.open_dollars')   AS yes_bid_open_dollars,
            json_extract_string(candle.yes_bid, '$.close_dollars')  AS yes_bid_close_dollars,
            json_extract_string(candle.yes_bid, '$.high_dollars')   AS yes_bid_high_dollars,
            json_extract_string(candle.yes_bid, '$.low_dollars')    AS yes_bid_low_dollars,

            json_extract_string(candle.yes_ask, '$.open_dollars')   AS yes_ask_open_dollars,
            json_extract_string(candle.yes_ask, '$.close_dollars')  AS yes_ask_close_dollars,
            json_extract_string(candle.yes_ask, '$.high_dollars')   AS yes_ask_high_dollars,
            json_extract_string(candle.yes_ask, '$.low_dollars')    AS yes_ask_low_dollars
        FROM c
    )
    TO '{out_file.as_posix()}'
    (FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL 22);
    """

    con.execute(sql)
    tmp.unlink(missing_ok=True)

    return con.execute(
        "SELECT COUNT(*) FROM parquet_scan(?)",
        (str(out_file),),
    ).fetchone()[0]


# ============================================================
# S3 SHARD CLAIMING
# ============================================================


def s3_client(region: Optional[str]):
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")


def _norm_prefix(p: str) -> str:
    return p.strip("/")


def claim_shard(
    s3,
    *,
    bucket: str,
    prefix: str,
    log: logging.Logger,
) -> Optional[str]:
    """
    Atomically claims a shard by copying:
        pending/<shard> → running/<shard>
    using If-None-Match='*' as the lock.

    Returns shard filename if claimed, else None.
    """

    prefix = prefix.strip("/")
    pending_prefix = f"{prefix}/pending/"
    running_prefix = f"{prefix}/running/"

    # 1. List ONE pending shard
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=pending_prefix,
        MaxKeys=1,
    )

    items = resp.get("Contents", [])
    if not items:
        return None

    src_key = items[0]["Key"]
    shard_name = src_key.rsplit("/", 1)[-1]
    dst_key = f"{running_prefix}{shard_name}"

    # 2. Atomic claim: copy ONLY if running/<shard> does not exist
    try:
        s3.copy_object(
            Bucket=bucket,
            Key=dst_key,
            CopySource={"Bucket": bucket, "Key": src_key},
            IfNoneMatch="*",  # 🔐 atomic lock
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("PreconditionFailed", "412"):
            # Another worker already claimed it
            return None
        raise

    # 3. Remove from pending immediately
    s3.delete_object(Bucket=bucket, Key=src_key)

    log.info(f"CLAIMED shard={shard_name}")
    return shard_name


def finalize_shard(
    s3,
    bucket: str,
    prefix: str,
    shard_name: str,
    status: str,
    local_db: Path,
    log: logging.Logger,
):
    prefix = _norm_prefix(prefix)
    key = f"{prefix}/{status}/{shard_name}"
    s3.upload_file(str(local_db), bucket, key)
    log.info(f"UPLOAD shard_db -> s3://{bucket}/{key}")


# ============================================================
# S3 PARQUET UPLOAD
# ============================================================
def debug_list_s3(s3, bucket: str, prefix: str, log: logging.Logger):
    log.warning("=== S3 DEBUG LIST ===")
    log.warning(f"Bucket = {bucket}")
    log.warning(f"Prefix = {prefix}")

    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=10,
    )

    log.warning("Raw response:")
    log.warning(json.dumps(resp, default=str, indent=2))

    contents = resp.get("Contents", [])
    log.warning(f"Found {len(contents)} objects")

    for obj in contents:
        log.warning(f" - {obj['Key']}  size={obj['Size']}")

    log.warning("=== END S3 DEBUG ===")


def upload_parquet(
    s3, bucket: str, prefix: str, batch_id: int, local_path: Path
) -> str:
    prefix = _norm_prefix(prefix)
    dt = datetime.now(timezone.utc)
    key = f"{prefix}/{dt:%Y/%m/%d}/batch_id={batch_id:09d}/{local_path.name}"
    s3.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def process_one_shard(
    *,
    s3,
    config: WorkerConfig,
    paths: Paths,
    shard_name: str,
    log: logging.Logger,
):
    running_key = f"{_norm_prefix(config.shard_prefix)}/running/{shard_name}"
    log.info(f"DOWNLOAD shard_db <- s3://{config.shard_bucket}/{running_key}")
    s3.download_file(config.shard_bucket, running_key, str(paths.db_path))

    con = duckdb.connect(str(paths.db_path))
    try:
        con.execute(f"PRAGMA threads={int(config.threads)};")
    except Exception:
        pass
    try:
        con.execute(f"PRAGMA memory_limit='{config.memory_limit}';")
    except Exception:
        pass

    ensure_schema(con)
    limiter = RateLimiter(config.max_rps)

    completed = 0
    start = perf_counter()
    shard_failed = False

    while True:
        if config.batch_limit is not None and completed >= config.batch_limit:
            break

        batch = claim_batch(con)
        if not batch:
            break

        batch_id = int(batch["batch_id"])
        url = str(batch["url"])
        log.info(f"START batch_id={batch_id}")

        out_local: Optional[Path] = None
        try:
            data = fetch_json(
                url,
                timeout=config.timeout,
                limiter=limiter,
                user_agent=config.user_agent,
            )

            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(json.dumps(data["error"])[:1200])

            markets = data.get("markets") or []
            has_candles = any((m.get("candlesticks") or []) for m in markets)

            rows = 0
            out_ref: Optional[str] = None

            if has_candles:
                out_local = (
                    paths.out_dir
                    / f"candles_batch{batch_id:09d}_{uuid.uuid4().hex}.parquet"
                )
                rows = write_parquet(con, data, out_local)
                out_ref = upload_parquet(
                    s3, config.s3_bucket, config.s3_prefix, batch_id, out_local
                )

                if config.delete_local:
                    out_local.unlink(missing_ok=True)
                    out_local = None

            mark_batch(
                con,
                batch_id=batch_id,
                status="completed",
                error=None,
                rows=rows,
                out_file=out_ref,
            )
            log.info(f"SUCCESS batch_id={batch_id} rows={rows}")

        except Exception as e:
            log.exception(f"FAILED batch_id={batch_id}")
            mark_batch(
                con,
                batch_id=batch_id,
                status="failed",
                error=str(e)[:4000],
                rows=0,
                out_file=None,
            )

        completed += 1

    elapsed = perf_counter() - start
    log.info(f"DONE shard={shard_name} batches={completed} elapsed_s={elapsed:.1f}")

    try:
        finalize_shard(
            s3,
            config.shard_bucket,
            config.shard_prefix,
            shard_name,
            "completed",
            paths.db_path,
            log,
        )
    except (BotoCoreError, ClientError) as e:
        shard_failed = True
        log.error(f"Failed to upload completed shard DB: {e}")

    con.close()

    if shard_failed:
        raise SystemExit(2)


# ============================================================
# PUBLIC API + MAIN
# ============================================================


def run_worker(config: WorkerConfig) -> int:
    paths = resolve_paths(config)
    log = setup_logging(paths.log_dir, config.log_level)

    log.info(
        f"Worker starting (shard_limit={config.shard_limit}, batch_limit={config.batch_limit})"
    )

    s3 = s3_client(config.aws_region)
    shards_processed = 0

    while True:
        if config.shard_limit is not None and shards_processed >= config.shard_limit:
            log.info(f"Shard limit reached ({config.shard_limit}); exiting worker.")
            break

        shard_name = claim_shard(
            s3,
            bucket=config.shard_bucket,
            prefix=config.shard_prefix,
            log=log,
        )

        if not shard_name:
            if shards_processed == 0:
                log.info("No shards available; exiting.")
            else:
                log.info(
                    f"No more shards available; processed {shards_processed} shard(s)."
                )
            break

        shards_processed += 1
        log.info(f"PROCESSING shard #{shards_processed}: {shard_name}")

        process_one_shard(
            s3=s3,
            config=config,
            paths=paths,
            shard_name=shard_name,
            log=log,
        )

    return shards_processed


def main():
    config = config_from_args(parse_args())
    run_worker(config)


if __name__ == "__main__":
    main()
