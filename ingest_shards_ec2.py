"""Backward-compatible entrypoint for existing deployments."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if SRC.exists():
    sys.path.insert(0, str(SRC))

from kalshi_ingest.worker import main


if __name__ == "__main__":
    main()
