"""Run Aviacost scraper script.

Fetches all aircraft data from https://aviacost.com/api/home/GetAircraftDetails
and saves to store/raw/aviacost/<date>/ as JSON.
"""

import sys
from pathlib import Path

# Add parent directory to path so imports work from runners/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.aviacost_scraper import AviacostScraper, AviacostScraperError
from utils.logger import setup_logging, get_logger


def main():
    """Run Aviacost scraper."""
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "aviacost_log.txt"

    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)

    logger.info("Log file: %s", log_file)

    try:
        logger.info("Starting Aviacost aircraft data scraper...")

        scraper = AviacostScraper()
        result = scraper.scrape()

        logger.info("=" * 60)
        logger.info("Aviacost scraper completed successfully")
        logger.info("Date: %s", result["date"])
        logger.info("Output dir: %s", result["output_dir"])
        logger.info("Aircraft details: %s", result["aircraft_details_file"])
        logger.info("Metadata: %s", result["metadata_file"])
        if result.get("aircraft_count") is not None:
            logger.info("Aircraft count: %s", result["aircraft_count"])
        if result.get("raw_keys"):
            logger.info("Response keys: %s", result["raw_keys"])
        logger.info("Duration: %.2f seconds", result["scrape_duration_seconds"])
        logger.info("=" * 60)

        return result

    except AviacostScraperError as e:
        logger.error("Aviacost scraper failed: %s", e, exc_info=True)
        raise
    except Exception as e:
        logger.error("Aviacost scraper failed: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
