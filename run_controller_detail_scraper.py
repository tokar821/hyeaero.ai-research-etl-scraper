"""Run Controller detail scraper script.

Scrapes detailed information from individual Controller.com listing pages.
Reads listing URLs from index scraper output and extracts detailed fields.
"""

from scrapers.controller_detail_scraper import ControllerDetailScraper
from utils.logger import setup_logging, get_logger
from pathlib import Path
from datetime import datetime


def main():
    """Run Controller detail scraper."""
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting Controller.com aircraft listing detail scraper...")
        
        # Initialize scraper with 2 second rate limit
        scraper = ControllerDetailScraper(rate_limit=2.0)
        
        # Scrape all listings (set max_listings=None for all listings, or a number to limit)
        result = scraper.scrape_details(max_listings=None)  # Scrape all listings
        
        logger.info("=" * 60)
        logger.info("Controller Detail Scraper Completed!")
        logger.info(f"Date: {result['date']}")
        logger.info(f"Total URLs: {result['total_urls']}")
        logger.info(f"Listings scraped: {result['listings_scraped']}")
        logger.info(f"Listings failed: {result['listings_failed']}")
        logger.info(f"HTML files saved: {len(result['html_files'])}")
        logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
        if result["errors"]:
            logger.warning(f"Errors: {len(result['errors'])}")
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        logger.error(f"Controller detail scraper failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
