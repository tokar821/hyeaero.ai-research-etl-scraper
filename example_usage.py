"""Example usage of the ETL pipeline components.

This script demonstrates how to use the config, storage, and logging modules.
"""

from datetime import datetime
from config import get_config
from storage import AkamaiStorageClient
from utils import setup_logging, get_logger

# Setup logging first
setup_logging()
logger = get_logger(__name__)


def main():
    """Example usage of ETL pipeline components."""
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = get_config()
        logger.info(f"Environment: {config.environment.value}")
        logger.info(f"Dry-run mode: {config.is_dry_run()}")
        logger.info(f"Bucket: {config.akamai.bucket_name}")
        
        # Initialize storage client
        logger.info("Initializing storage client...")
        storage = AkamaiStorageClient()
        
        # Example: Upload raw data
        logger.info("Example: Uploading raw data...")
        sample_data = b'{"aircraft": "Phenom 300", "year": 2017, "hours": 1200}'
        raw_path = storage.upload_raw_data(
            source="controller",
            filename="listing_12345.json",
            data=sample_data,
            content_type="application/json"
        )
        logger.info(f"Raw data uploaded to: {raw_path}")
        
        # Example: Upload snapshot
        logger.info("Example: Uploading snapshot...")
        snapshot_data = b'{"entity": "aircraft", "model": "Phenom 300", "normalized": true}'
        snapshot_path = storage.upload_snapshot(
            entity="aircraft",
            filename="phenom300_2017.json",
            data=snapshot_data,
            content_type="application/json"
        )
        logger.info(f"Snapshot uploaded to: {snapshot_path}")
        
        # Example: Upload with specific date
        logger.info("Example: Uploading with specific date...")
        specific_date = datetime(2024, 1, 15)
        dated_path = storage.upload_raw_data(
            source="aircraftexchange",
            filename="listing_67890.json",
            data=sample_data,
            date=specific_date,
            content_type="application/json"
        )
        logger.info(f"Dated upload path: {dated_path}")
        
        logger.info("Example completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in example: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
