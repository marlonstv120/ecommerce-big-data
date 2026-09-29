"""Command-line entry point for the Phase 2 local Spark pipeline."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline import main


if __name__ == "__main__":
    raise SystemExit(main())
