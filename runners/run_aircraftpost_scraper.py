"""Run AircraftPost fleet HTML scraper.

Logs into AircraftPost and downloads fleet detail HTML pages for make_model IDs.
Saves files to store/raw/aircraftpost/<date>/html/.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.aircraftpost_fleet_scraper import AircraftPostFleetScraper, AircraftPostScraperError
from utils.logger import setup_logging, get_logger


def main():
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "aircraftpost_log.txt"

    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)
    logger.info("Log file: %s", log_file)

    try:
        scraper = AircraftPostFleetScraper()
        result = scraper.scrape(start_model_id=1, end_model_id=92, headless=True)

        logger.info("=" * 60)
        logger.info("AircraftPost scraper completed")
        logger.info("Output dir: %s", result["output_dir"])
        logger.info("HTML dir: %s", result["html_dir"])
        logger.info("Saved pages: %s", result["pages_saved"])
        if result.get("failed_models"):
            logger.warning("Failures: %s", len(result["failed_models"]))
        logger.info("Metadata: %s", result["metadata_file"])
        logger.info("Duration: %.2f seconds", result["scrape_duration_seconds"])
        logger.info("=" * 60)
        return result
    except AircraftPostScraperError as e:
        logger.error("AircraftPost scraper failed: %s", e, exc_info=True)
        raise
    except Exception as e:
        logger.error("AircraftPost scraper failed: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()

