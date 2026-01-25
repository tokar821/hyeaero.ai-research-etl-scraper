"""Test script for Controller detail scraper."""

import sys
from pathlib import Path

# Add parent directory to path so imports work from scripts/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.controller_detail_scraper_undetected import ControllerDetailScraperUndetected
from utils.logger import setup_logging, get_logger
import json


def main():
    """Test Controller detail scraper with 2 listings (using undetected version)."""
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting Controller.com aircraft listing detail scraper test (undetected version)...")
        
        # Initialize scraper with 6 second rate limit (human-like delays)
        scraper = ControllerDetailScraperUndetected(rate_limit=6.0, headless=False)
        
        # Path to index metadata
        index_path = Path('store/raw/controller/2026-01-12/index/listings_metadata.json')
        
        # Scrape all listings (set max_listings=None for all, or a number to limit)
        result = scraper.scrape_details(
            index_metadata_path=index_path,
            max_listings=None  # Scrape all 4 listings
        )
        
        # Print results
        print("\n" + "=" * 60)
        print("TEST RESULTS")
        print("=" * 60)
        print(f"Total URLs: {result['total_urls']}")
        print(f"Listings scraped: {result['listings_scraped']}")
        print(f"Listings failed: {result['listings_failed']}")
        print(f"HTML files saved: {len(result['html_files'])}")
        print(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
        
        if result["errors"]:
            print(f"\nErrors encountered: {len(result['errors'])}")
            for error in result['errors']:
                print(f"  - {error}")
        
        print("\n" + "=" * 60)
        print("EXTRACTED DATA")
        print("=" * 60)
        print(json.dumps(result['detail_data'], indent=2, ensure_ascii=False))
        
        return result
        
    except Exception as e:
        logger.error(f"Controller detail scraper test failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
