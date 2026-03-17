"""Run AircraftPost extracted JSON loader into PostgreSQL."""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config_loader import get_config
from database.postgres_client import PostgresClient
from database.loaders.aircraftpost_loader import AircraftPostLoader
from utils.logger import setup_logging, get_logger


def main():
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "aircraftpost_loader_log.txt"
    setup_logging(log_file=str(log_file), log_file_overwrite=True)
    logger = get_logger(__name__)

    config = get_config()
    db = PostgresClient(
        host=config.postgres_host,
        port=config.postgres_port,
        database=config.postgres_database,
        user=config.postgres_user,
        password=config.postgres_password,
        connection_string=config.postgres_connection_string,
    )

    # Ensure table exists (migration)
    if not db.table_exists("aircraftpost_fleet_aircraft"):
        migration_sql = Path(__file__).parent.parent / "database" / "migrations" / "ensure_aircraftpost_fleet_aircraft.sql"
        logger.info("Creating aircraftpost_fleet_aircraft table (migration): %s", migration_sql)
        sql_content = migration_sql.read_text(encoding="utf-8")
        # Use raw connection execute
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_content)
        logger.info("Migration applied.")

    # Date arg optional; defaults to today's date folder
    if len(sys.argv) >= 2:
        ingestion_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        ingestion_date = datetime.now().date()

    loader = AircraftPostLoader(db)
    stats = loader.load_aircraftpost_data(ingestion_date=ingestion_date, limit=None, store_raw=True)
    logger.info("AircraftPost loader finished. stats=%s", stats)


if __name__ == "__main__":
    main()

