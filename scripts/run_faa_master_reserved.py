"""Load FAA MASTER.txt and RESERVED.txt into PostgreSQL.

Reads from the *latest* date folder under store/raw/faa/ (e.g. 2026-01-23):
  - store/raw/faa/<date>/extracted/MASTER.txt
  - store/raw/faa/<date>/extracted/RESERVED.txt

Usage:
  cd etl-pipeline
  python scripts/run_faa_master_reserved.py              # Skip 8756 rows of MASTER (default), load rest + RESERVED
  python scripts/run_faa_master_reserved.py 0            # Load all MASTER + RESERVED (no skip)
  python scripts/run_faa_master_reserved.py 8756       # Skip first 8756 rows of MASTER, load rest + RESERVED
  python scripts/run_faa_master_reserved.py --help       # Show this help

Equivalent direct command (from etl-pipeline):
  python runners/run_database_loader.py --faa-only --faa-master-offset 8756 --log-level INFO
"""

import sys
from pathlib import Path

# Run from etl-pipeline directory
etl_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(etl_root))

# Default: skip first 8756 rows of MASTER (already saved)
DEFAULT_OFFSET = 8756


def main():
    offset = DEFAULT_OFFSET
    args = sys.argv[1:]
    if args and args[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    if args and args[0].lstrip("-").isdigit():
        offset = int(args[0])
    if "--offset" in args:
        i = args.index("--offset")
        if i + 1 < len(args) and args[i + 1].isdigit():
            offset = int(args[i + 1])

    sys.argv = [
        "run_database_loader.py",
        "--faa-only",
        "--faa-master-offset", str(offset),
        "--log-level", "INFO",
    ]
    from runners.run_database_loader import main as loader_main
    return loader_main()


if __name__ == "__main__":
    sys.exit(main() or 0)
