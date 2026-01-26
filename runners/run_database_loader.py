"""Run database loader script.

Loads scraped data from store/ directory into PostgreSQL database.
Only processes the latest date data for each source.
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path so imports work from runners/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.postgres_client import PostgresClient
from database.data_loader import DataLoader
from utils.logger import setup_logging, get_logger
from config.config_loader import get_config


def main():
    """Run database loader."""
    # Setup logging
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "database_loader_log.txt"
    
    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Load scraped data into PostgreSQL database')
    parser.add_argument('--limit-controller', type=int, default=None, 
                       help='Limit number of Controller listings to process (for testing)')
    parser.add_argument('--limit-aircraftexchange', type=int, default=None,
                       help='Limit number of AircraftExchange listings to process (for testing)')
    parser.add_argument('--limit-faa', type=int, default=None,
                       help='Limit number of FAA records to process (for testing)')
    parser.add_argument('--limit-internal', type=int, default=None,
                       help='Limit number of Internal DB records to process (for testing)')
    parser.add_argument('--test', action='store_true',
                       help='Test mode: process only 10 records from each source')
    parser.add_argument('--controller-only', action='store_true',
                       help='Process only Controller data (skip AircraftExchange, FAA, Internal DB)')
    parser.add_argument('--aircraftexchange-only', action='store_true',
                       help='Process only AircraftExchange data (skip Controller, FAA, Internal DB)')
    parser.add_argument('--faa-only', action='store_true',
                       help='Process only FAA data (skip Controller, AircraftExchange, Internal DB)')
    parser.add_argument('--internal-only', action='store_true',
                       help='Process only Internal DB data (skip Controller, AircraftExchange, FAA)')
    
    args = parser.parse_args()
    
    # If --test flag, set small limits
    if args.test:
        args.limit_controller = 10
        args.limit_aircraftexchange = 10
        args.limit_faa = 10
        args.limit_internal = 10
    
    # If --controller-only flag, skip other sources
    if args.controller_only:
        args.limit_aircraftexchange = -1  # Use -1 as sentinel to skip
        args.limit_faa = -1
        args.limit_internal = -1
        # If no limit specified for Controller, process all (set to None which means no limit)
        if args.limit_controller is None:
            args.limit_controller = None  # None means process all
    
    # If --aircraftexchange-only flag, skip other sources
    if args.aircraftexchange_only:
        args.limit_controller = -1
        args.limit_faa = -1
        args.limit_internal = -1
        # If no limit specified for AircraftExchange, process all (set to None which means no limit)
        if args.limit_aircraftexchange is None:
            args.limit_aircraftexchange = None  # None means process all
    
    # If --faa-only flag, skip other sources
    if args.faa_only:
        args.limit_controller = -1  # Use -1 as sentinel to skip
        args.limit_aircraftexchange = -1
        args.limit_internal = -1
        # If no limit specified for FAA, process all (set to None which means no limit)
        if args.limit_faa is None:
            args.limit_faa = None  # None means process all
    
    # If --internal-only flag, skip other sources
    if args.internal_only:
        args.limit_controller = -1  # Use -1 as sentinel to skip
        args.limit_aircraftexchange = -1
        args.limit_faa = -1
        # If no limit specified for Internal, process all (set to None which means no limit)
        if args.limit_internal is None:
            args.limit_internal = None  # None means process all
    
    # Auto-detect single source mode: if only one source has a limit specified,
    # automatically skip the others (unless --test flag or explicit --*-only flags are used)
    if not args.test and not args.controller_only and not args.aircraftexchange_only and not args.faa_only and not args.internal_only:
        specified_limits = [
            (args.limit_controller is not None, 'controller'),
            (args.limit_aircraftexchange is not None, 'aircraftexchange'),
            (args.limit_faa is not None, 'faa'),
            (args.limit_internal is not None, 'internal'),
        ]
        specified_count = sum(1 for has_limit, _ in specified_limits if has_limit)
        
        # If exactly one source has a limit, skip the others
        if specified_count == 1:
            for has_limit, source_name in specified_limits:
                if has_limit:
                    logger.info(f"Single source mode detected: {source_name} only (skipping other sources)")
                    # Set others to -1 to skip
                    if source_name == 'controller':
                        args.limit_aircraftexchange = -1
                        args.limit_faa = -1
                        args.limit_internal = -1
                    elif source_name == 'aircraftexchange':
                        args.limit_controller = -1
                        args.limit_faa = -1
                        args.limit_internal = -1
                    elif source_name == 'faa':
                        args.limit_controller = -1
                        args.limit_aircraftexchange = -1
                        args.limit_internal = -1
                    elif source_name == 'internal':
                        args.limit_controller = -1
                        args.limit_aircraftexchange = -1
                        args.limit_faa = -1
                    break
    
    try:
        logger.info("=" * 60)
        logger.info("Database Loader - Loading latest scraped data to PostgreSQL")
        if args.test or any([args.limit_controller and args.limit_controller != -1, 
                             args.limit_aircraftexchange and args.limit_aircraftexchange != -1,
                             args.limit_faa and args.limit_faa != -1, 
                             args.limit_internal and args.limit_internal != -1]):
            logger.info("TEST MODE - Processing limited data")
            if args.limit_controller and args.limit_controller != -1:
                logger.info(f"  Controller limit: {args.limit_controller}")
            if args.limit_aircraftexchange and args.limit_aircraftexchange != -1:
                logger.info(f"  AircraftExchange limit: {args.limit_aircraftexchange}")
            if args.limit_faa and args.limit_faa != -1:
                logger.info(f"  FAA limit: {args.limit_faa}")
            if args.limit_internal and args.limit_internal != -1:
                logger.info(f"  Internal DB limit: {args.limit_internal}")
        
        # Log which sources are being skipped
        if args.controller_only:
            logger.info("CONTROLLER-ONLY MODE: Processing only Controller data")
        elif args.aircraftexchange_only:
            logger.info("AIRCRAFTEXCHANGE-ONLY MODE: Processing only AircraftExchange data")
        elif args.faa_only:
            logger.info("FAA-ONLY MODE: Processing only FAA data")
        elif args.internal_only:
            logger.info("INTERNAL-ONLY MODE: Processing only Internal DB data")
        elif any([args.limit_controller == -1, args.limit_aircraftexchange == -1, 
                  args.limit_faa == -1, args.limit_internal == -1]):
            skipped = []
            if args.limit_controller == -1:
                skipped.append("Controller")
            if args.limit_aircraftexchange == -1:
                skipped.append("AircraftExchange")
            if args.limit_faa == -1:
                skipped.append("FAA")
            if args.limit_internal == -1:
                skipped.append("Internal DB")
            if skipped:
                logger.info(f"Skipping sources: {', '.join(skipped)}")
        logger.info("=" * 60)
        
        # Load configuration
        config = get_config()
        
        # PostgreSQL connection (from environment or fallback to hardcoded)
        if config.postgres_connection_string:
            connection_string = config.postgres_connection_string
            logger.info("Using PostgreSQL connection string from environment")
        elif config.postgres_host and config.postgres_user and config.postgres_password:
            # Build connection string from individual components
            port = config.postgres_port or 5432
            database = config.postgres_database or "defaultdb"
            connection_string = (
                f"host={config.postgres_host} port={port} dbname={database} "
                f"user={config.postgres_user} password={config.postgres_password} sslmode=require"
            )
            logger.info(f"Using PostgreSQL connection from environment (host: {config.postgres_host})")
        else:
            # Fallback to hardcoded (for backward compatibility)
            connection_string = (
                "postgres://avnadmin:AVNS_IT0JkCtP0vz1x-an3Aj@"
                "pg-134dedd1-allevi8marketing-47f2.c.aivencloud.com:13079/"
                "defaultdb?sslmode=require"
            )
            logger.warning("Using hardcoded PostgreSQL connection (set POSTGRES_CONNECTION_STRING or individual env vars)")
        
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
            # Check if we need to fix serial_number constraint
            try:
                check_nullable = db_client.execute_query(
                    "SELECT is_nullable FROM information_schema.columns WHERE table_name = 'aircraft' AND column_name = 'serial_number'"
                )
                if check_nullable and check_nullable[0]['is_nullable'] == 'NO':
                    logger.info("Fixing serial_number constraint to allow NULL...")
                    fix_sql = """
                        ALTER TABLE aircraft ALTER COLUMN serial_number DROP NOT NULL;
                        ALTER TABLE aircraft DROP CONSTRAINT IF EXISTS aircraft_serial_number_key;
                        ALTER TABLE aircraft DROP CONSTRAINT IF EXISTS at_least_one_identifier;
                        ALTER TABLE aircraft ADD CONSTRAINT at_least_one_identifier 
                            CHECK (serial_number IS NOT NULL OR registration_number IS NOT NULL);
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_aircraft_serial_number_unique 
                            ON aircraft(serial_number) WHERE serial_number IS NOT NULL;
                    """
                    db_client.execute_update(fix_sql)
                    logger.info("serial_number constraint fixed")
            except Exception as e:
                logger.warning(f"Could not check/fix serial_number constraint: {e}")
            
            # Check if we need to add number_of_passengers to aircraft_listings
            try:
                check_column = db_client.execute_query(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'aircraft_listings' AND column_name = 'number_of_passengers'"
                )
                if not check_column:
                    logger.info("Adding number_of_passengers column to aircraft_listings...")
                    db_client.execute_update(
                        "ALTER TABLE aircraft_listings ADD COLUMN number_of_passengers INTEGER;"
                    )
                    logger.info("number_of_passengers column added")
            except Exception as e:
                logger.warning(f"Could not check/add number_of_passengers column: {e}")
            
            # Check if we need to fix make_model columns (VARCHAR(255) -> TEXT)
            try:
                # Check aircraft_engines.make_model
                if db_client.table_exists('aircraft_engines'):
                    check_engines = db_client.execute_query(
                        "SELECT data_type, character_maximum_length FROM information_schema.columns "
                        "WHERE table_name = 'aircraft_engines' AND column_name = 'make_model'"
                    )
                    if check_engines and check_engines[0]['data_type'] == 'character varying':
                        logger.info("Changing aircraft_engines.make_model from VARCHAR to TEXT...")
                        db_client.execute_update(
                            "ALTER TABLE aircraft_engines ALTER COLUMN make_model TYPE TEXT;"
                        )
                        logger.info("aircraft_engines.make_model updated to TEXT")
                
                # Check aircraft_apus.make_model
                if db_client.table_exists('aircraft_apus'):
                    check_apus = db_client.execute_query(
                        "SELECT data_type, character_maximum_length FROM information_schema.columns "
                        "WHERE table_name = 'aircraft_apus' AND column_name = 'make_model'"
                    )
                    if check_apus and check_apus[0]['data_type'] == 'character varying':
                        logger.info("Changing aircraft_apus.make_model from VARCHAR to TEXT...")
                        db_client.execute_update(
                            "ALTER TABLE aircraft_apus ALTER COLUMN make_model TYPE TEXT;"
                        )
                        logger.info("aircraft_apus.make_model updated to TEXT")
            except Exception as e:
                logger.warning(f"Could not check/fix make_model columns: {e}")
            
            # Check if we need to make n_number nullable in faa_registrations
            try:
                if db_client.table_exists('faa_registrations'):
                    check_nullable = db_client.execute_query(
                        "SELECT is_nullable FROM information_schema.columns "
                        "WHERE table_name = 'faa_registrations' AND column_name = 'n_number'"
                    )
                    if check_nullable and check_nullable[0]['is_nullable'] == 'NO':
                        logger.info("Making faa_registrations.n_number nullable...")
                        db_client.execute_update(
                            "ALTER TABLE faa_registrations ALTER COLUMN n_number DROP NOT NULL;"
                        )
                        logger.info("faa_registrations.n_number updated to nullable")
            except Exception as e:
                logger.warning(f"Could not check/fix n_number nullable: {e}")
            
            # Check if we need to increase registration_number column size
            try:
                check_size = db_client.execute_query(
                    "SELECT character_maximum_length FROM information_schema.columns WHERE table_name = 'aircraft' AND column_name = 'registration_number'"
                )
                if check_size and check_size[0]['character_maximum_length'] and check_size[0]['character_maximum_length'] < 50:
                    logger.info("Increasing registration_number column size to 50...")
                    db_client.execute_update(
                        "ALTER TABLE aircraft ALTER COLUMN registration_number TYPE VARCHAR(50);"
                    )
                    logger.info("registration_number column size increased")
            except Exception as e:
                logger.warning(f"Could not check/increase registration_number column size: {e}")
        
        # Initialize data loader
        loader = DataLoader(db_client)
        
        # Load all latest data with limits
        logger.info("Loading latest data from all sources...")
        # Keep -1 as sentinel for skipping (don't convert to None)
        limits = {
            'controller': args.limit_controller,
            'aircraftexchange': args.limit_aircraftexchange,
            'faa': args.limit_faa,
            'internal': args.limit_internal,
        }
        summary = loader.load_all_latest(limits=limits)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Database Loader Completed!")
        logger.info("=" * 60)
        logger.info(f"Controller: {summary.get('controller')}")
        logger.info(f"AircraftExchange: {summary.get('aircraftexchange')}")
        if summary.get('aircraftexchange'):
            ae_stats = summary['aircraftexchange']
            logger.info(f"  - Main Index: {ae_stats.get('listings', 0)} listings")
            logger.info(f"  - Main Details: {ae_stats.get('details', 0)} details")
            logger.info(f"  - Manufacturer Listings: {ae_stats.get('manufacturer_listings', 0)} listings")
            logger.info(f"  - Manufacturer Details: {ae_stats.get('manufacturer_details', 0)} details")
        logger.info(f"FAA: {summary.get('faa')}")
        if summary.get('faa'):
            faa_stats = summary['faa']
            logger.info(f"  - MASTER: {faa_stats.get('master', {})}")
            logger.info(f"  - ACFTREF: {faa_stats.get('acftref', {})}")
            logger.info(f"  - DEALER: {faa_stats.get('dealer', {})}")
            logger.info(f"  - DEREG: {faa_stats.get('dereg', {})}")
            logger.info(f"  - ENGINE: {faa_stats.get('engine', {})}")
            logger.info(f"  - DOCINDEX: {faa_stats.get('docindex', {})}")
            logger.info(f"  - RESERVED: {faa_stats.get('reserved', {})}")
            logger.info(f"  - PDFs: {faa_stats.get('pdfs', {})}")
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
