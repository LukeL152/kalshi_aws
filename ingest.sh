#!/usr/bin/env bash
set -euo pipefail

############################################
# CONFIG (edit as needed)
############################################

PYTHON_VERSION="python3"
VENV_DIR=".venv"

DB_PATH="/data/kalshi.duckdb"
OUT_DIR="/data/parquet"
LOG_DIR="/data/logs"

S3_BUCKET="lucas-laszacs-kalshi"
S3_PREFIX="candlesticks_raw"

MAX_RPS="4"
THREADS="4"
MEMORY_LIMIT="2GB"

############################################
# SYSTEM SETUP
############################################

echo "=== [1/6] Updating system packages ==="
sudo dnf update -y

echo "=== [2/6] Installing Python and system deps ==="
sudo dnf install -y \
  python3 \
  python3-pip \
  python3-virtualenv \
  git \
  gcc \
  gcc-c++ \
  make \
  openssl-devel \
  libffi-devel

############################################
# PYTHON ENVIRONMENT
############################################

echo "=== [3/6] Creating virtual environment ==="
$PYTHON_VERSION -m venv "$VENV_DIR"

echo "=== [4/6] Activating virtual environment ==="
source "$VENV_DIR/bin/activate"

echo "Upgrading pip..."
pip install --upgrade pip setuptools wheel

############################################
# DEPENDENCIES
############################################

echo "=== [5/6] Installing Python dependencies ==="
pip install -r requirements.txt

############################################
# RUNTIME DIRECTORIES
############################################

echo "Creating runtime directories..."
mkdir -p "$OUT_DIR" "$LOG_DIR"

############################################
# RUN INGEST
############################################

echo "=== [6/6] Starting ingestion ==="

exec python ingest_jobs_ec2.py \
  --db-path "$DB_PATH" \
  --out-dir "$OUT_DIR" \
  --log-dir "$LOG_DIR" \
  --s3-bucket "$S3_BUCKET" \
  --s3-prefix "$S3_PREFIX" \
  --max-rps "$MAX_RPS" \
  --threads "$THREADS" \
  --memory-limit "$MEMORY_LIMIT"
