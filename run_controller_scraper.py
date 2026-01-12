"""Run Controller scraper script.

Scrapes Controller.com aircraft listings and saves raw HTML to local storage.
Uses undetected-chromedriver for better bot detection evasion with human-like behavior.
"""

from scrapers.controller_scraper_undetected import ControllerScraperUndetected
from utils.logger import setup_logging, get_logger


def main():
    """Run Controller scraper with human-like behavior."""
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting Controller.com aircraft listings scraper...")
        logger.info("Using undetected-chromedriver with human-like behavior")
        logger.info("Priority: Bot Detection Bypass > Speed")
        
        # Initialize scraper with human-like rate limit (6 seconds base, 6-12s actual)
        # Slower but more human-like to avoid bot detection
        scraper = ControllerScraperUndetected(rate_limit=6.0, headless=False)
        
        # Scrape ALL pages (no limit) - stops automatically when Y = Z (current_end >= total_listings)
        # Pagination pattern: "X - Y of Z Listings" - stops when Y >= Z
        result = scraper.scrape_listings(max_pages=None)  # Scrape all pages
        
        logger.info("=" * 60)
        logger.info("Controller Scraper Completed!")
        logger.info(f"Date: {result['date']}")
        logger.info(f"Pages scraped: {result['pages_scraped']}")
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


if __name__ == "__main__":
    main()
