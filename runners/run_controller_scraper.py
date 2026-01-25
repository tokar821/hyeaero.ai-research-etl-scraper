"""Run Controller scraper script.

Scrapes Controller.com aircraft listings and saves raw HTML to local storage.
Uses undetected-chromedriver for better bot detection evasion with human-like behavior.
"""

import argparse
import io
import os
import sys
import warnings
from contextlib import redirect_stderr
from datetime import datetime
from pathlib import Path

# Add parent directory to path so imports work from runners/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.controller_scraper_undetected import ControllerScraperUndetected
from utils.logger import setup_logging, get_logger


class FilteredStderr:
    """Stderr wrapper that filters out harmless Chrome driver cleanup errors."""
    
    def __init__(self, original_stderr):
        self.original_stderr = original_stderr
        self.suppressing = False
        self.suppress_lines_remaining = 0
    
    def write(self, text):
        # Check for any variation of the Chrome cleanup error
        text_lower = text.lower()
        
        # Check if this is the start of an "Exception ignored" message for Chrome cleanup
        if "exception ignored" in text_lower and ("chrome.__del__" in text_lower or "chrome" in text_lower):
            self.suppressing = True
            self.suppress_lines_remaining = 20  # Suppress next ~20 lines (full traceback)
            return  # Suppress this line
        
        # If we're suppressing, continue suppressing traceback lines
        if self.suppressing:
            self.suppress_lines_remaining -= 1
            
            # Check if this is the error message line (various formats)
            if any(phrase in text_lower for phrase in [
                "oserror: [winerror 6]",
                "handle is invalid",
                "undetected_chromedriver",
                "__init__.py",
                "line 843",
                "line 798"
            ]):
                # Still part of the traceback, continue suppressing
                if self.suppress_lines_remaining > 0:
                    return
            
            # Suppress intermediate traceback lines
            if self.suppress_lines_remaining > 0:
                return  # Suppress this line
            else:
                # Stop suppressing if we've exceeded expected lines
                self.suppressing = False
        
        # Pass through all other messages
        self.original_stderr.write(text)
    
    def flush(self):
        self.original_stderr.flush()
    
    def __getattr__(self, name):
        # Delegate all other attributes to original stderr
        return getattr(self.original_stderr, name)


def main():
    """Run Controller scraper with human-like behavior."""
    parser = argparse.ArgumentParser(
        description="Run Controller.com scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all pages from the beginning
  python run_controller_scraper.py
  
  # Resume from page 174
  python run_controller_scraper.py --start-page 174
  
  # Run in headless mode
  python run_controller_scraper.py --headless
        """
    )
    
    parser.add_argument(
        '--start-page',
        type=int,
        default=1,
        help='Ignored (skip-if-exists). Kept for compat.'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=None,
        help='Max pages to fetch this run (existing HTML skipped). Default: all'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Use data for YYYY-MM-DD (default: today). Index paths use this date.'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        help='Run browser in headless mode'
    )
    parser.add_argument(
        '--rate-limit',
        type=float,
        default=6.0,
        help='Base rate limit in seconds between requests (default: 6.0)'
    )
    
    args = parser.parse_args()
    
    # Setup filtered stderr to suppress cleanup errors
    original_stderr = sys.stderr
    filtered_stderr = FilteredStderr(original_stderr)
    sys.stderr = filtered_stderr
    
    # Also suppress warnings about ignored exceptions
    warnings.filterwarnings('ignore', category=RuntimeWarning)
    
    # Setup logging with file output
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "controller_log.txt"
    
    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)
    
    date_arg = None
    if args.date:
        try:
            date_arg = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid --date %s; use YYYY-MM-DD", args.date)
            raise SystemExit(1)
        logger.info("Using date: %s", args.date)
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Controller.com aircraft listings scraper...")
        logger.info("Using undetected-chromedriver with human-like behavior")
        logger.info("Skip-if-exists + backfill: same date re-runs safe; no overwrite/delete.")
        logger.info(f"Log file: {log_file}")
        logger.info("=" * 60)
        
        scraper = ControllerScraperUndetected(rate_limit=args.rate_limit, headless=args.headless)
        result = scraper.scrape_listings(
            date=date_arg,
            max_pages=args.max_pages,
            start_page=args.start_page,
        )
        
        logger.info("=" * 60)
        logger.info("Controller Scraper Completed!")
        logger.info(f"Date: {result['date']}")
        logger.info(f"Pages scraped this run: {result['pages_scraped']}")
        logger.info(f"Pages skipped (existing HTML): {result.get('pages_skipped', 0)}")
        logger.info(f"Total listings: {result['total_listings']}")
        logger.info(f"HTML files saved: {len(result['html_files'])}")
        logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
        if result["errors"]:
            logger.warning(f"Errors: {len(result['errors'])}")
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        logger.error(f"Controller scraper failed: {e}", exc_info=True)
        raise
    finally:
        # Keep filtered stderr active during cleanup
        # The filter will catch any cleanup errors
        pass


if __name__ == "__main__":
    try:
        main()
    finally:
        # Keep stderr filtered until process exits to catch late cleanup errors
        # Don't restore original stderr - let the filter handle everything
        pass
