"""Test script for AircraftExchange manufacturer scraper."""

from datetime import datetime
from scrapers.aircraftexchange_manufacturer_scraper_undetected import AircraftExchangeManufacturerScraperUndetected
from utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    # Initialize scraper
    scraper = AircraftExchangeManufacturerScraperUndetected(
        headless=False,  # Set to True for headless mode
        rate_limit=6.0
    )
    
    # Option 1: Scrape just the manufacturer list
    logger.info("=" * 60)
    logger.info("Testing: Scraping manufacturer list only")
    logger.info("=" * 60)
    manufacturers = scraper.scrape_manufacturers_list(date=datetime.now())
    logger.info(f"Found {len(manufacturers)} manufacturers")
    
    # Option 2: Scrape a single manufacturer (uncomment to test)
    # if manufacturers:
    #     logger.info("=" * 60)
    #     logger.info(f"Testing: Scraping listings for {manufacturers[0]['name']}")
    #     logger.info("=" * 60)
    #     result = scraper.scrape_manufacturer_listings(
    #         manufacturers[0],
    #         date=datetime.now(),
    #         max_pages=2  # Limit to 2 pages for testing
    #     )
    #     logger.info(f"Pages scraped: {result['pages_scraped']}")
    #     logger.info(f"Total listings: {result['total_listings']}")
    
    # Option 3: Scrape all manufacturers (uncomment to run full scrape)
    # logger.info("=" * 60)
    # logger.info("Testing: Scraping all manufacturers")
    # logger.info("=" * 60)
    # overall_result = scraper.scrape_all_manufacturers(
    #     date=datetime.now(),
    #     max_manufacturers=3,  # Limit to 3 manufacturers for testing
    #     max_pages_per_manufacturer=2  # Limit to 2 pages per manufacturer
    # )
    # logger.info(f"Manufacturers scraped: {overall_result['manufacturers_scraped']}")
    # logger.info(f"Total listings: {overall_result['total_listings']}")
    
    logger.info("=" * 60)
    logger.info("Test Complete")
    logger.info("=" * 60)
