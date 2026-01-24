"""Run Controller detail scraper script.

Scrapes detailed information from individual Controller.com listing pages.
Reads listing URLs from index scraper output. Uses undetected-chromedriver
with human-like behavior (no bot-like speed). Visible browser by default.
"""

import argparse
import sys
from pathlib import Path

from scrapers.controller_detail_scraper_undetected import ControllerDetailScraperUndetected
from utils.logger import setup_logging, get_logger


class FilteredStderr:
    """Filters out harmless Chrome driver cleanup errors from terminal output."""

    def __init__(self, original_stderr):
        self.original_stderr = original_stderr
        self.suppressing = False
        self.suppress_lines_remaining = 0

    def write(self, text):
        text_lower = text.lower()
        if "exception ignored" in text_lower and ("chrome.__del__" in text_lower or "chrome" in text_lower):
            self.suppressing = True
            self.suppress_lines_remaining = 20
            return
        if self.suppressing:
            self.suppress_lines_remaining -= 1
            if any(p in text_lower for p in [
                "oserror: [winerror 6]", "handle is invalid", "undetected_chromedriver",
                "__init__.py", "line 843", "line 798"
            ]) and self.suppress_lines_remaining > 0:
                return
            if self.suppress_lines_remaining > 0:
                return
            self.suppressing = False
        self.original_stderr.write(text)

    def flush(self):
        self.original_stderr.flush()

    def __getattr__(self, name):
        return getattr(self.original_stderr, name)


def main():
    """Run Controller detail scraper (undetected Chrome, human-like)."""
    parser = argparse.ArgumentParser(description="Controller detail scraper (undetected Chrome)")
    parser.add_argument("--max-listings", type=int, default=None, help="Max listings to scrape this run (default: all)")
    parser.add_argument("--start-from", type=int, default=1, help="1-based index to resume from (e.g. 373 = skip 1..372, scrape from 373)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--rate-limit", type=float, default=6.0, help="Base delay between listings in seconds (default: 6)")
    args = parser.parse_args()

    original_stderr = sys.stderr
    sys.stderr = FilteredStderr(original_stderr)

    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "controller_log.txt"

    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)

    try:
        logger.info("=" * 60)
        logger.info("Controller.com detail scraper (undetected Chrome, human-like)")
        logger.info("Log file: %s (overwrite each run)", log_file)
        if args.start_from > 1:
            logger.info("Resuming from listing %s", args.start_from)
        if args.max_listings:
            logger.info("Limiting to %s listings this run", args.max_listings)
        logger.info("=" * 60)

        scraper = ControllerDetailScraperUndetected(rate_limit=args.rate_limit, headless=args.headless)
        result = scraper.scrape_details(
            max_listings=args.max_listings,
            start_from=args.start_from,
        )

        logger.info("=" * 60)
        logger.info("Controller Detail Scraper Completed!")
        logger.info("Date: %s", result["date"])
        logger.info("Total URLs: %s", result["total_urls"])
        logger.info("Listings scraped: %s", result["listings_scraped"])
        logger.info("Listings failed: %s", result["listings_failed"])
        logger.info("HTML files saved: %s", len(result["html_files"]))
        logger.info("Scrape duration: %.2f seconds", result["scrape_duration"])
        if result["errors"]:
            logger.warning("Errors: %s", len(result["errors"]))
        logger.info("=" * 60)

        return result
    except Exception as e:
        logger.error("Controller detail scraper failed: %s", e, exc_info=True)
        raise
    finally:
        sys.stderr = original_stderr


if __name__ == "__main__":
    main()
