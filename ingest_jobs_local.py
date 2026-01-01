# ingest/market_candlesticks/ingest_batches.py
from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Optional, Dict, Any

import duckdb
import requests

from config.config import load_config

# ============================================================
# CONSTANTS (API semantics)
# ============================================================

PERIOD_INTERVAL = 1440  # minutes (1 day candles)

MAX_RETRIES = 6
BASE_BACKOFF_S = 0.5
MAX_BACKOFF_S = 20.0

BASE_HEADERS = {
    "User-Agent": "kalshi-candlestick-batch-worker/1.0",
    "Accept": "application/json",
}

# ============================================================
# CLI
# ============================================================


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Kalshi candlestick batch worker (one batch per job)"
    )
    p.add_argument("--batch-start", type=int)
    p.add_argument("--batch-end", type=int)
    p.add_argument("--limit", type=int)
    p.add_argument("--reset-running", action="store_true")
    p.add_argument("--count-rows", action="store_true")
    p.add_argument("--max-rps", type=float, default=4.0)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--memory-limit", type=str, default="8GB")
    p.add_argument("--log-level", type=str, default="INFO")
    return p.parse_args()


# ============================================================
# CONFIG
# ============================================================

CFG = load_config()
PATHS = CFG.paths

DB_PATH = Path(PATHS["duckdb_dir"]) / "kalshi.duckdb"
PARQUET_OUT = Path(PATHS["parquet_data_dir"]) / "candlesticks_raw"
LOG_DIR = Path(PATHS["log_dir"])

PARQUET_OUT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================


def setup_logging(level: str) -> logging.Logger:
    log_file = (
        LOG_DIR
        / f"candlestick_batches_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger("candlestick_batches")


# ============================================================
# RATE LIMITER
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


def _sleep_backoff(attempt: int, retry_after: Optional[float] = None):
    if retry_after:
        time.sleep(min(retry_after, MAX_BACKOFF_S))
        return
    backoff = min(BASE_BACKOFF_S * (2**attempt), MAX_BACKOFF_S)
    time.sleep(backoff + random.uniform(0, backoff * 0.25))


def fetch_json(url: str, *, timeout: int, limiter: RateLimiter) -> Dict[str, Any]:
    for attempt in range(MAX_RETRIES + 1):
        try:
            limiter.wait()
            r = requests.get(url, headers=BASE_HEADERS, timeout=timeout)

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
# DUCKDB HELPERS
# ============================================================


def ensure_schema(con: duckdb.DuckDBPyConnection):
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


def reset_running(con):
    con.execute(
        "UPDATE candlestick_job_batches SET status='pending' WHERE status='running'"
    )


def claim_batch(con, start: Optional[int], end: Optional[int]):
    if start is not None and end is not None:
        q = """
        UPDATE candlestick_job_batches
        SET status='running', last_attempt_ts=CURRENT_TIMESTAMP,
            attempt_count=attempt_count+1, error_message=NULL
        WHERE batch_id = (
            SELECT batch_id FROM candlestick_job_batches
            WHERE status='pending' AND batch_id BETWEEN ? AND ?
            ORDER BY batch_id LIMIT 1
        )
        RETURNING *
        """
        row = con.execute(q, (start, end)).fetchone()
    else:
        q = """
        UPDATE candlestick_job_batches
        SET status='running', last_attempt_ts=CURRENT_TIMESTAMP,
            attempt_count=attempt_count+1, error_message=NULL
        WHERE batch_id = (
            SELECT batch_id FROM candlestick_job_batches
            WHERE status='pending'
            ORDER BY batch_id LIMIT 1
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
# JSON → PARQUET
# ============================================================


def write_parquet(con, response: dict, out_file: Path) -> int:
    tmp = out_file.with_suffix(".json")
    tmp.write_text(json.dumps(response), encoding="utf-8")

    def dollars(struct, dollars_key, cents_key):
        return f"""
        CASE
          WHEN struct_extract({struct}, '{dollars_key}') IS NOT NULL
            THEN try_cast(struct_extract({struct}, '{dollars_key}') AS DOUBLE)
          WHEN struct_extract({struct}, '{cents_key}') IS NOT NULL
            THEN try_cast(struct_extract({struct}, '{cents_key}') AS DOUBLE) / 100.0
          ELSE NULL
        END
        """

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

            -- PRICE (JSON-safe)
            json_extract_string(candle.price, '$.open_dollars')     AS price_open_dollars,
            json_extract_string(candle.price, '$.close_dollars')    AS price_close_dollars,
            json_extract_string(candle.price, '$.high_dollars')     AS price_high_dollars,
            json_extract_string(candle.price, '$.low_dollars')      AS price_low_dollars,
            json_extract_string(candle.price, '$.mean_dollars')     AS price_mean_dollars,
            json_extract_string(candle.price, '$.previous_dollars') AS price_previous_dollars,

            -- YES BID
            json_extract_string(candle.yes_bid, '$.open_dollars')   AS yes_bid_open_dollars,
            json_extract_string(candle.yes_bid, '$.close_dollars')  AS yes_bid_close_dollars,
            json_extract_string(candle.yes_bid, '$.high_dollars')   AS yes_bid_high_dollars,
            json_extract_string(candle.yes_bid, '$.low_dollars')    AS yes_bid_low_dollars,

            -- YES ASK
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
# MAIN
# ============================================================


def main():
    args = parse_args()
    log = setup_logging(args.log_level)

    # If user provides only one side of range, treat as "no range"
    if (args.batch_start is None) ^ (args.batch_end is None):
        raise SystemExit("Provide BOTH --batch-start and --batch-end, or neither.")

    limiter = RateLimiter(args.max_rps)
    con = duckdb.connect(str(DB_PATH))

    try:
        con.execute(f"PRAGMA threads={int(args.threads)};")
    except Exception:
        pass
    try:
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}';")
    except Exception:
        pass

    ensure_schema(con)

    if args.reset_running:
        reset_running(con)
        log.warning("Reset running -> pending")

    completed = 0
    start = perf_counter()

    while True:
        if args.limit is not None and completed >= args.limit:
            break

        batch = claim_batch(con, args.batch_start, args.batch_end)
        if not batch:
            break

        batch_id = int(batch["batch_id"])
        url = str(batch["url"])
        log.info(f"START batch_id={batch_id}")

        try:
            data = fetch_json(url, timeout=args.timeout, limiter=limiter)

            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(json.dumps(data["error"])[:1200])

            markets = data.get("markets") or []
            has_candles = any((m.get("candlesticks") or []) for m in markets)

            out = (
                PARQUET_OUT / f"candles_batch{batch_id:09d}_{uuid.uuid4().hex}.parquet"
            )
            rows = 0
            if has_candles:
                rows = write_parquet(con, data, out)

            mark_batch(
                con,
                batch_id=batch_id,
                status="completed",
                error=None,
                rows=rows,
                out_file=str(out),
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
    log.info(f"Done {completed} batches in {elapsed / 60:.1f} min")
    con.close()


if __name__ == "__main__":
    main()

