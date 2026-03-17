"""Run AircraftPost HTML extractor to JSON."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import setup_logging, get_logger
from scrapers.aircraftpost_fleet_extractor import write_extracted_json


def main():
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "aircraftpost_extract_log.txt"
    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)

    # Default: latest scraped date folder path can be passed as arg
    base = Path(__file__).parent.parent / "store" / "raw" / "aircraftpost"
    date_dir = None
    if len(sys.argv) >= 2:
        date_dir = Path(sys.argv[1])
    else:
        # Pick most recent date folder if exists
        candidates = [p for p in base.glob("*") if p.is_dir() and p.name[:4].isdigit()]
        date_dir = sorted(candidates, key=lambda p: p.name)[-1] if candidates else None

    if not date_dir or not date_dir.exists():
        raise SystemExit("Provide aircraftpost date dir path, e.g. store/raw/aircraftpost/2026-03-17")

    html_dir = date_dir / "html"
    out_path = date_dir / "fleet_extracted.json"
    logger.info("Extracting from %s", html_dir)
    logger.info("Writing %s", out_path)

    payload = write_extracted_json(html_dir, out_path)
    logger.info("Done. counts=%s", payload.get("counts"))


if __name__ == "__main__":
    main()

