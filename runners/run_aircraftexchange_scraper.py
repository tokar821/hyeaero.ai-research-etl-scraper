"""Run AircraftExchange scraper script.

Scrapes AircraftExchange.com aircraft listings using multiple strategies:
1. Index scraper - scrapes main listings page (/aircraft-for-sale/all)
2. Manufacturer scraper - scrapes manufacturer list (/aircraft-manufacturers)
3. Manufacturer detail scraper - scrapes manufacturer-based details (manufacturer -> models -> listings -> details)
4. Detail scraper - scrapes detail pages from index listings

Uses undetected-chromedriver for better bot detection evasion with human-like behavior.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path so imports work from runners/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.aircraftexchange_index_scraper_undetected import AircraftExchangeIndexScraperUndetected
from scrapers.aircraftexchange_manufacturer_scraper_undetected import AircraftExchangeManufacturerScraperUndetected
from scrapers.aircraftexchange_manufacturer_detail_scraper_undetected import AircraftExchangeManufacturerDetailScraperUndetected
from scrapers.aircraftexchange_detail_scraper_undetected import AircraftExchangeDetailScraperUndetected
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


def run_index_scraper(headless: bool = False, rate_limit: float = 6.0, max_pages: int = None, date=None):
    """Run AircraftExchange index scraper."""
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("Starting AircraftExchange INDEX scraper...")
    logger.info("Scraping: https://aircraftexchange.com/aircraft-for-sale/all")
    logger.info("=" * 60)
    
    scraper = AircraftExchangeIndexScraperUndetected(headless=headless, rate_limit=rate_limit)
    result = scraper.scrape_listings(date=date or datetime.now(), max_pages=max_pages)
    
    logger.info("=" * 60)
    logger.info("INDEX Scraper Completed!")
    logger.info(f"Date: {result['date']}")
    logger.info(f"Pages scraped: {result['pages_scraped']}")
    logger.info(f"Total listings: {result['total_listings']}")
    logger.info(f"HTML files saved: {len(result['html_files'])}")
    logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
    if result["errors"]:
        logger.warning(f"Errors: {len(result['errors'])}")
        for error in result["errors"][:5]:  # Show first 5 errors
            logger.warning(f"  - {error}")
    logger.info("=" * 60)
    
    return result


def run_manufacturer_scraper(headless: bool = False, rate_limit: float = 6.0, date=None):
    """Run AircraftExchange manufacturer scraper."""
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("Starting AircraftExchange MANUFACTURER scraper...")
    logger.info("Scraping: https://aircraftexchange.com/aircraft-manufacturers")
    logger.info("=" * 60)
    
    run_date = date or datetime.now()
    start_wall = datetime.now()
    scraper = AircraftExchangeManufacturerScraperUndetected(headless=headless, rate_limit=rate_limit)
    manufacturers = scraper.scrape_manufacturers_list(date=run_date)
    
    duration = (datetime.now() - start_wall).total_seconds()
    date_str = run_date.strftime("%Y-%m-%d")
    
    logger.info("=" * 60)
    logger.info("MANUFACTURER Scraper Completed!")
    logger.info(f"Date: {date_str}")
    logger.info(f"Total manufacturers found: {len(manufacturers)}")
    logger.info(f"Scrape duration: {duration:.2f} seconds")
    logger.info("=" * 60)
    
    result = {
        'date': date_str,
        'manufacturers': manufacturers,
        'scrape_duration': duration
    }
    
    return result


def run_manufacturer_detail_scraper(
    headless: bool = False,
    rate_limit: float = 6.0,
    max_manufacturers: int = None,
    max_pages_per_manufacturer: int = None,
    max_listings_per_manufacturer: int = None,
    date=None,
):
    """Run AircraftExchange manufacturer detail scraper."""
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("Starting AircraftExchange MANUFACTURER DETAIL scraper...")
    logger.info("Workflow: Manufacturer -> Model Categories -> Listings -> Detail Pages")
    logger.info("=" * 60)
    
    scraper = AircraftExchangeManufacturerDetailScraperUndetected(headless=headless, rate_limit=rate_limit)
    result = scraper.scrape_all_manufacturer_details(
        date=date or datetime.now(),
        max_manufacturers=max_manufacturers,
        max_pages_per_manufacturer=max_pages_per_manufacturer,
        max_listings_per_manufacturer=max_listings_per_manufacturer
    )
    
    logger.info("=" * 60)
    logger.info("MANUFACTURER DETAIL Scraper Completed!")
    logger.info(f"Date: {result['date']}")
    logger.info(f"Manufacturers processed: {result['manufacturers_processed']}")
    logger.info(f"Total listings found: {result['total_listings_found']}")
    logger.info(f"Total details scraped: {result['total_details_scraped']}")
    logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
    if result["errors"]:
        logger.warning(f"Errors: {len(result['errors'])}")
        for error in result["errors"][:5]:
            logger.warning(f"  - {error}")
    logger.info("=" * 60)
    
    return result


def run_detail_scraper(headless: bool = False, rate_limit: float = 6.0, max_listings: int = None, date=None, start_from: int = 1):
    """Run AircraftExchange detail scraper (from index listings)."""
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("Starting AircraftExchange DETAIL scraper...")
    logger.info("Reading listing URLs from index scraper output")
    logger.info("=" * 60)
    
    scraper = AircraftExchangeDetailScraperUndetected(headless=headless, rate_limit=rate_limit)
    result = scraper.scrape_details(
        date=date or datetime.now(),
        max_listings=max_listings,
        start_from=start_from,
    )
    
    logger.info("=" * 60)
    logger.info("DETAIL Scraper Completed!")
    logger.info(f"Date: {result['date']}")
    logger.info(f"Listings scraped: {result['listings_scraped']}")
    logger.info(f"HTML files saved: {len(result['html_files'])}")
    logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
    if result["errors"]:
        logger.warning(f"Errors: {len(result['errors'])}")
        for error in result["errors"][:5]:
            logger.warning(f"  - {error}")
    logger.info("=" * 60)
    
    return result


def main():
    """Main entry point for AircraftExchange scraper."""
    parser = argparse.ArgumentParser(
        description="Run AircraftExchange scraper modules",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modules (run one or more with --index, --manufacturer, --manufacturer-detail, --detail):

  # Run ONLY index (main listings page)
  python run_aircraftexchange_scraper.py --index

  # Run ONLY manufacturer list
  python run_aircraftexchange_scraper.py --manufacturer

  # Run ONLY manufacturer detail (manufacturer -> models -> listings -> details)
  python run_aircraftexchange_scraper.py --manufacturer-detail

  # Run ONLY detail scraper (detail pages from index; requires index run first)
  python run_aircraftexchange_scraper.py --detail

  # Run all modules in sequence
  python run_aircraftexchange_scraper.py --all

  # With limits (for testing)
  python run_aircraftexchange_scraper.py --index --max-pages 2
  python run_aircraftexchange_scraper.py --manufacturer-detail --max-manufacturers 3 --max-listings 5

  # Headless mode
  python run_aircraftexchange_scraper.py --index --headless
        """
    )
    
    parser.add_argument(
        '--index',
        action='store_true',
        help='Run index scraper (main listings page)'
    )
    parser.add_argument(
        '--manufacturer',
        action='store_true',
        help='Run manufacturer scraper (manufacturer list)'
    )
    parser.add_argument(
        '--manufacturer-detail',
        action='store_true',
        help='Run manufacturer detail scraper (manufacturer -> models -> listings -> details)'
    )
    parser.add_argument(
        '--detail',
        action='store_true',
        help='Run detail scraper (from index listings)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Run all modules in sequence'
    )
    
    # Options
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
    
    # Limits
    parser.add_argument(
        '--max-pages',
        type=int,
        default=None,
        help='Maximum pages to scrape (index scraper)'
    )
    parser.add_argument(
        '--max-manufacturers',
        type=int,
        default=None,
        help='Maximum manufacturers to process (manufacturer detail scraper)'
    )
    parser.add_argument(
        '--max-pages-per-manufacturer',
        type=int,
        default=None,
        help='Maximum pages per manufacturer (manufacturer detail scraper)'
    )
    parser.add_argument(
        '--max-listings-per-manufacturer',
        type=int,
        default=None,
        help='Maximum detail pages per manufacturer (manufacturer detail scraper)'
    )
    parser.add_argument(
        '--max-listings',
        type=int,
        default=None,
        help='Maximum detail pages to scrape (detail scraper)'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Use data for YYYY-MM-DD (default: today). Index/details paths use this date.'
    )
    parser.add_argument(
        '--start-from',
        type=int,
        default=1,
        help='Detail scraper: 1-based index to resume from (e.g. 73 = skip 1..72, scrape from 73). Default: 1.'
    )
    
    args = parser.parse_args()
    
    # If no specific module selected, show help
    if not any([args.index, args.manufacturer, args.manufacturer_detail, args.detail, args.all]):
        parser.print_help()
        return

    # Suppress Chrome driver cleanup errors in terminal
    original_stderr = sys.stderr
    sys.stderr = FilteredStderr(original_stderr)

    # Setup logging with file output
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "aircraftexchange_log.txt"

    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)

    try:
        date_arg = None
        if args.date:
            try:
                date_arg = datetime.strptime(args.date, "%Y-%m-%d")
            except ValueError:
                logger.error("Invalid --date %s; use YYYY-MM-DD", args.date)
                raise SystemExit(1)
            logger.info("Using date: %s", args.date)
        if args.start_from and args.start_from > 1 and (args.all or args.detail):
            logger.info("Detail scraper: resuming from listing %s", args.start_from)
        
        logger.info("=" * 60)
        logger.info("AircraftExchange Scraper - Starting")
        logger.info(f"Rate limit: {args.rate_limit} seconds")
        logger.info(f"Headless mode: {args.headless}")
        logger.info(f"Log file: {log_file}")
        logger.info("=" * 60)
        
        results = {}
        
        if args.all or args.index:
            logger.info("\n")
            results['index'] = run_index_scraper(
                headless=args.headless,
                rate_limit=args.rate_limit,
                max_pages=args.max_pages,
                date=date_arg,
            )
        
        if args.all or args.manufacturer:
            logger.info("\n")
            results['manufacturer'] = run_manufacturer_scraper(
                headless=args.headless,
                rate_limit=args.rate_limit,
                date=date_arg,
            )
        
        if args.all or args.manufacturer_detail:
            logger.info("\n")
            results['manufacturer_detail'] = run_manufacturer_detail_scraper(
                headless=args.headless,
                rate_limit=args.rate_limit,
                max_manufacturers=args.max_manufacturers,
                max_pages_per_manufacturer=args.max_pages_per_manufacturer,
                max_listings_per_manufacturer=args.max_listings_per_manufacturer,
                date=date_arg,
            )
        
        if args.all or args.detail:
            logger.info("\n")
            results['detail'] = run_detail_scraper(
                headless=args.headless,
                rate_limit=args.rate_limit,
                max_listings=args.max_listings,
                date=date_arg,
                start_from=args.start_from or 1,
            )
        
        # Final summary
        logger.info("\n")
        logger.info("=" * 60)
        logger.info("AIRCRAFTEXCHANGE SCRAPER - FINAL SUMMARY")
        logger.info("=" * 60)
        
        total_listings = 0
        total_details = 0
        total_duration = 0
        
        if 'index' in results:
            logger.info(f"Index Scraper: {results['index']['total_listings']} listings found")
            total_listings += results['index']['total_listings']
            total_duration += results['index']['scrape_duration']
        
        if 'manufacturer' in results:
            logger.info(f"Manufacturer Scraper: {len(results['manufacturer']['manufacturers'])} manufacturers found")
            total_duration += results['manufacturer']['scrape_duration']
        
        if 'manufacturer_detail' in results:
            logger.info(f"Manufacturer Detail Scraper: {results['manufacturer_detail']['total_details_scraped']} details scraped")
            total_details += results['manufacturer_detail']['total_details_scraped']
            total_duration += results['manufacturer_detail']['scrape_duration']
        
        if 'detail' in results:
            logger.info(f"Detail Scraper: {results['detail']['listings_scraped']} details scraped")
            total_details += results['detail']['listings_scraped']
            total_duration += results['detail']['scrape_duration']
        
        logger.info(f"\nTotal listings found: {total_listings}")
        logger.info(f"Total details scraped: {total_details}")
        logger.info(f"Total duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        logger.info("=" * 60)
        logger.info("All scrapers completed successfully!")
        logger.info("=" * 60)
        
        return results
        
    except Exception as e:
        logger.error(f"AircraftExchange scraper failed: {e}", exc_info=True)
        raise
    finally:
        sys.stderr = original_stderr


if __name__ == "__main__":
    main()
