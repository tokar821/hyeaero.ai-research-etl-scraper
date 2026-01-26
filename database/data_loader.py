"""Data loader module for ETL pipeline.

Loads scraped data from store/ directory into PostgreSQL database.
Only processes the latest date data for each source.
Handles insert/update logic with change tracking.

This module orchestrates all data source loaders.
"""

from datetime import date
from pathlib import Path
from typing import Dict, Optional, Any
import logging

from .postgres_client import PostgresClient
from .loaders.controller_loader import ControllerLoader
from .loaders.aircraftexchange_loader import AircraftExchangeLoader
from .loaders.faa_loader import FAALoader
from .loaders.internal_loader import InternalLoader

logger = logging.getLogger(__name__)


class DataLoader:
    """Orchestrates all data loaders to load scraped data into PostgreSQL database."""

    def __init__(self, db_client: PostgresClient, store_base_path: Optional[Path] = None):
        """Initialize data loader.

        Args:
            db_client: PostgreSQL client instance
            store_base_path: Base path to store/ directory. Defaults to ./store/raw
        """
        self.db = db_client
        if store_base_path is None:
            # Default to parent of etl-pipeline/store
            store_base_path = Path(__file__).parent.parent / "store" / "raw"
        
        # Initialize all source loaders
        self.controller_loader = ControllerLoader(db_client, store_base_path)
        self.aircraftexchange_loader = AircraftExchangeLoader(db_client, store_base_path)
        self.faa_loader = FAALoader(db_client, store_base_path)
        self.internal_loader = InternalLoader(db_client, store_base_path)
        
        logger.info(f"DataLoader initialized with store base path: {store_base_path}")

    def find_latest_date(self, source: str) -> Optional[date]:
        """Find the latest date directory for a source.

        Args:
            source: Source name (controller, aircraftexchange, internaldb, faa)

        Returns:
            Latest date or None if no data found
        """
        # Use any loader's find_latest_date method (they all inherit from BaseLoader)
        return self.controller_loader.find_latest_date(source)

    def load_all_latest(self, limits: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        """Load latest date data from all sources.

        Args:
            limits: Optional dict with limits for each source:
                - 'controller': Limit controller listings
                - 'aircraftexchange': Limit aircraftexchange listings
                - 'faa': Limit FAA records
                - 'internal': Limit internal DB records

        Returns:
            Dict with summary statistics
        """
        if limits is None:
            limits = {}
        summary = {
            'controller': None,
            'aircraftexchange': None,
            'faa': None,
            'internaldb': None,
            'total_inserted': 0,
            'total_updated': 0,
            'total_skipped': 0,
        }

        # Load Controller (skip if limit is -1, process if None or number)
        controller_limit = limits.get('controller')
        if controller_limit != -1:  # -1 means skip, None means process all, number means limit
            controller_date = self.find_latest_date('controller')
            if controller_date:
                stats = self.controller_loader.load_controller_data(controller_date, limit=controller_limit)
                summary['controller'] = {'date': controller_date.isoformat(), **stats}
                summary['total_inserted'] += stats.get('inserted', 0)
                summary['total_updated'] += stats.get('updated', 0)
                summary['total_skipped'] += stats.get('skipped', 0)
        else:
            logger.info("Skipping Controller data (limit set to -1)")

        # Load AircraftExchange (skip if limit is -1, process if None or number)
        aircraftexchange_limit = limits.get('aircraftexchange')
        if aircraftexchange_limit != -1:
            aircraftexchange_date = self.find_latest_date('aircraftexchange')
            if aircraftexchange_date:
                stats = self.aircraftexchange_loader.load_aircraftexchange_data(aircraftexchange_date, limit=aircraftexchange_limit)
                summary['aircraftexchange'] = {'date': aircraftexchange_date.isoformat(), **stats}
                summary['total_inserted'] += stats.get('inserted', 0)
                summary['total_updated'] += stats.get('updated', 0)
                summary['total_skipped'] += stats.get('skipped', 0)
        else:
            logger.info("Skipping AircraftExchange data (limit set to -1)")

        # Load FAA (skip if limit is -1, process if None or number)
        faa_limit = limits.get('faa')
        if faa_limit != -1:  # -1 means skip, None means process all, number means limit
            faa_date = self.find_latest_date('faa')
            if faa_date:
                stats = self.faa_loader.load_faa_data(faa_date, limit=faa_limit)
                summary['faa'] = {'date': faa_date.isoformat(), **stats}
                summary['total_inserted'] += stats.get('total_inserted', 0)
                summary['total_updated'] += stats.get('total_updated', 0)
                summary['total_skipped'] += stats.get('total_skipped', 0)
        else:
            logger.info("Skipping FAA data (limit set to -1)")

        # Load Internal DB (skip if limit is -1, process if None or number)
        internal_limit = limits.get('internal')
        if internal_limit != -1:
            stats = self.internal_loader.load_internal_db_data(limit=internal_limit)
            summary['internaldb'] = stats
            summary['total_inserted'] += stats.get('inserted', 0)
            summary['total_updated'] += stats.get('updated', 0)
            summary['total_skipped'] += stats.get('skipped', 0)
        else:
            logger.info("Skipping Internal DB data (limit set to -1)")

        return summary
