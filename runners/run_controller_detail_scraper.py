"""Run Controller detail scraper script.

Scrapes detailed information from individual Controller.com listing pages.
Reads listing URLs from index scraper output. Uses undetected-chromedriver
with human-like behavior (no bot-like speed). Visible browser by default.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path so imports work from runners/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

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
    parser.add_argument("--max-listings", type=int, default=None, help="Max *new* detail pages to scrape this run; existing are skipped (default: all)")
    parser.add_argument("--start-from", type=int, default=1, help="Ignored (skip-if-exists). Kept for compat.")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--rate-limit", type=float, default=6.0, help="Base delay between listings in seconds (default: 6)")
    parser.add_argument("--cooldown-every", type=int, default=50, help="After N successfully scraped listings, rest (default: 50). Use 0 to disable.")
    parser.add_argument("--cooldown-min", type=float, default=10.0, help="Cooldown rest min minutes (default: 10)")
    parser.add_argument("--cooldown-max", type=float, default=30.0, help="Cooldown rest max minutes (default: 30)")
    parser.add_argument("--no-cooldown-restart", action="store_true", help="Disable browser restart after cooldown (default: restart)")
    parser.add_argument("--date", type=str, default=None, help="Use data for YYYY-MM-DD (default: today). Used for index/output paths.")
    parser.add_argument("--profiles-dir", type=str, default=None, help="Chrome profiles dir for multi-profile rotation (default: store/chrome_profiles/controller)")
    parser.add_argument("--num-profiles", type=int, default=3, help="Number of profiles to rotate (default: 3 = different browser ID each cooldown). Use 0 to disable.")
    parser.add_argument("--no-multi-profile", action="store_true", help="Disable multi-profile rotation (use single browser session)")
    parser.add_argument("--proxy", type=str, default=None, help="Proxy host:port (different IP). No auth; use IP whitelist or provider.")
    parser.add_argument("--timezones", type=str, default=None, help="Comma-separated IANA timezones (e.g. America/New_York,Europe/London). Default: NY, London, Tokyo.")
    args = parser.parse_args()

    original_stderr = sys.stderr
    sys.stderr = FilteredStderr(original_stderr)

    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "controller_log.txt"

    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)

    try:
        # Multi-profile: enabled by default (3 profiles) to reduce CAPTCHA
        num_profiles = 0 if args.no_multi_profile else args.num_profiles
        
        logger.info("=" * 60)
        logger.info("Controller.com detail scraper (undetected Chrome, human-like)")
        logger.info("Log file: %s (overwrite each run)", log_file)
        if args.max_listings:
            logger.info("Limiting to %s new scrapes this run (existing skipped)", args.max_listings)
        if args.cooldown_every and args.cooldown_every > 0:
            logger.info(
                "Cooldown: every %s listings, rest %.1f-%.1f mins, restart browser=%s",
                args.cooldown_every,
                args.cooldown_min,
                args.cooldown_max,
                not args.no_cooldown_restart,
            )
        if args.date:
            logger.info("Using date: %s", args.date)
        if num_profiles > 0:
            logger.info("Multi-profile: %d profiles (different browser ID each cooldown) - CAPTCHA mitigation", num_profiles)
        else:
            logger.info("Multi-profile: disabled (single browser session)")
        if args.proxy:
            logger.info("Proxy: %s (different IP)", args.proxy)
        if args.timezones:
            logger.info("Timezones: %s", args.timezones)
        logger.info("=" * 60)

        date_arg = None
        if args.date:
            try:
                date_arg = datetime.strptime(args.date, "%Y-%m-%d")
            except ValueError:
                logger.error("Invalid --date %s; use YYYY-MM-DD", args.date)
                raise SystemExit(1)

        profiles_dir = None
        if num_profiles > 0:
            if args.profiles_dir:
                profiles_dir = Path(args.profiles_dir)
            else:
                profiles_dir = Path(__file__).parent.parent / "store" / "chrome_profiles" / "controller"
            profiles_dir.mkdir(parents=True, exist_ok=True)

        timezones_arg = None
        if args.timezones:
            timezones_arg = [t.strip() for t in args.timezones.split(",") if t.strip()]

        scraper = ControllerDetailScraperUndetected(
            rate_limit=args.rate_limit,
            headless=args.headless,
            profiles_dir=profiles_dir,
            num_profiles=num_profiles,
            proxy=args.proxy,
            timezones=timezones_arg,
        )
        result = scraper.scrape_details(
            max_listings=args.max_listings,
            start_from=args.start_from,
            date=date_arg,
            cooldown_every=args.cooldown_every,
            cooldown_min_minutes=args.cooldown_min,
            cooldown_max_minutes=args.cooldown_max,
            cooldown_restart_browser=not args.no_cooldown_restart,
        )

        logger.info("=" * 60)
        logger.info("Controller Detail Scraper Completed!")
        logger.info("Date: %s", result["date"])
        logger.info("Total URLs: %s", result["total_urls"])
        logger.info("Listings scraped: %s", result["listings_scraped"])
        logger.info("Listings skipped (already scraped): %s", result.get("listings_skipped", 0))
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
