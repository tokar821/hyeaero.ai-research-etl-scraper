"""Test script for AircraftExchange index scraper."""

from datetime import datetime
from scrapers.aircraftexchange_index_scraper_undetected import AircraftExchangeIndexScraperUndetected
from utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    # Initialize scraper
    scraper = AircraftExchangeIndexScraperUndetected(
        headless=False,  # Set to True for headless mode
        rate_limit=6.0
    )
    
    # Scrape listings (limit to 2 pages for testing)
    result = scraper.scrape_listings(
        date=datetime.now(),
        max_pages=2  # Remove or set to None to scrape all pages
    )
    
    logger.info("=" * 60)
    logger.info("Test Complete")
    logger.info(f"Pages scraped: {result['pages_scraped']}")
    logger.info(f"Total listings: {result['total_listings']}")
    logger.info(f"Errors: {len(result['errors'])}")
    logger.info("=" * 60)
