"""Run Controller scraper script.

Scrapes Controller.com aircraft listings and saves raw HTML to local storage.
"""

from scrapers.controller_scraper import ControllerScraper
from utils.logger import setup_logging, get_logger


def main():
    """Run Controller scraper."""
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting Controller.com aircraft listings scraper...")
        
        # Initialize scraper with 2 second rate limit
        scraper = ControllerScraper(rate_limit=2.0)
        
        # Scrape all pages (set max_pages=None for all pages, or a number to limit)
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
