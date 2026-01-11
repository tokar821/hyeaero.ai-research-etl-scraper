"""Run FAA scraper script.

Downloads FAA Aircraft Registration Database and saves to local storage.
"""

from scrapers.faa_scraper import FAAScraper
from utils.logger import setup_logging, get_logger


def main():
    """Run FAA scraper."""
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting FAA Aircraft Registration Database scraper...")
        
        scraper = FAAScraper()
        result = scraper.download_database(download_docs=True)
        
        logger.info("=" * 60)
        logger.info("FAA Scraper Completed Successfully!")
        logger.info(f"Date: {result['date']}")
        logger.info(f"Database file: {result['database_file']}")
        logger.info(f"Database size: {result['database_size']:,} bytes ({result['database_size'] / 1024 / 1024:.2f} MB)")
        logger.info(f"Database hash: {result['database_hash']}")
        logger.info(f"Total records: {sum(result['record_counts'].values()):,}")
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        logger.error(f"FAA scraper failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
