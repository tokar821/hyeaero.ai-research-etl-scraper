"""Run database loader script.

Loads scraped data from store/ directory into PostgreSQL database.
Only processes the latest date data for each source.
"""

import sys
from pathlib import Path

# Add parent directory to path so imports work from runners/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.data_loader import DataLoader
from utils.logger import setup_logging, get_logger


def main():
    """Run database loader."""
    # Setup logging
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "database_loader_log.txt"
    
    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("Database Loader - Loading latest scraped data to PostgreSQL")
        logger.info("=" * 60)
        
        # PostgreSQL connection (from environment or defaults)
        connection_string = (
            "postgres://avnadmin:AVNS_IT0JkCtP0vz1x-an3Aj@"
            "pg-134dedd1-allevi8marketing-47f2.c.aivencloud.com:13079/"
            "defaultdb?sslmode=require"
        )
        
        # Initialize database client
        logger.info("Connecting to PostgreSQL...")
        db_client = PostgresClient(connection_string=connection_string)
        
        # Test connection
        if not db_client.test_connection():
            logger.error("Failed to connect to database. Exiting.")
            return
        
        # Check/create schema
        if not db_client.table_exists('aircraft'):
            logger.info("Creating database schema...")
            db_client.create_schema()
        else:
            logger.info("Database schema already exists")
        
        # Initialize data loader
        loader = DataLoader(db_client)
        
        # Load all latest data
        logger.info("Loading latest data from all sources...")
        summary = loader.load_all_latest()
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Database Loader Completed!")
        logger.info("=" * 60)
        logger.info(f"Controller: {summary.get('controller')}")
        logger.info(f"AircraftExchange: {summary.get('aircraftexchange')}")
        logger.info(f"Internal DB: {summary.get('internaldb')}")
        logger.info(f"Total Inserted: {summary['total_inserted']}")
        logger.info(f"Total Updated: {summary['total_updated']}")
        logger.info(f"Total Skipped: {summary['total_skipped']}")
        logger.info("=" * 60)
        
        return summary
        
    except Exception as e:
        logger.error(f"Database loader failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
