"""Data loader module for ETL pipeline.

Loads scraped data from store/ directory into PostgreSQL database.
Only processes the latest date data for each source.
Handles insert/update logic with change tracking.
"""

import json
import csv
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, InvalidOperation
import logging
import hashlib

from .postgres_client import PostgresClient

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads scraped data into PostgreSQL database."""

    def __init__(self, db_client: PostgresClient, store_base_path: Optional[Path] = None):
        """Initialize data loader.

        Args:
            db_client: PostgreSQL client instance
            store_base_path: Base path to store/ directory. Defaults to ./store
        """
        self.db = db_client
        if store_base_path is None:
            # Default to parent of etl-pipeline/store
            self.store_base = Path(__file__).parent.parent / "store" / "raw"
        else:
            self.store_base = Path(store_base_path)
        
        logger.info(f"Store base path: {self.store_base}")

    def find_latest_date(self, source: str) -> Optional[date]:
        """Find the latest date directory for a source.

        Args:
            source: Source name (controller, aircraftexchange, internaldb, faa)

        Returns:
            Latest date or None if no data found
        """
        source_path = self.store_base / source
        if not source_path.exists():
            logger.warning(f"Source directory not found: {source_path}")
            return None

        dates = []
        for item in source_path.iterdir():
            if item.is_dir():
                try:
                    d = datetime.strptime(item.name, "%Y-%m-%d").date()
                    dates.append(d)
                except ValueError:
                    continue

        if not dates:
            logger.warning(f"No date directories found for {source}")
            return None

        latest = max(dates)
        logger.info(f"Latest date for {source}: {latest}")
        return latest

    def load_controller_data(self, ingestion_date: date) -> Dict[str, int]:
        """Load Controller.com data for a specific date.

        Args:
            ingestion_date: Date to load

        Returns:
            Dict with counts: {'listings': X, 'details': Y, 'inserted': Z, 'updated': W}
        """
        date_str = ingestion_date.strftime("%Y-%m-%d")
        base_path = self.store_base / "controller" / date_str

        stats = {
            'listings': 0,
            'details': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
        }

        # Load index listings
        index_file = base_path / "index" / "listings_metadata.json"
        if index_file.exists():
            logger.info(f"Loading Controller index data from {index_file}")
            with open(index_file, 'r', encoding='utf-8') as f:
                listings = json.load(f)
            stats['listings'] = len(listings)
            # Store raw data first
            self._store_raw_data('controller', 'index', ingestion_date, index_file, listings)
            # Process listings (basic info, will be enriched by details)
            for listing in listings:
                result = self._upsert_controller_listing(listing, ingestion_date)
                if result == 'inserted':
                    stats['inserted'] += 1
                elif result == 'updated':
                    stats['updated'] += 1
                else:
                    stats['skipped'] += 1

        # Load detail listings
        details_file = base_path / "details" / "details_metadata.json"
        if details_file.exists():
            logger.info(f"Loading Controller detail data from {details_file}")
            with open(details_file, 'r', encoding='utf-8') as f:
                details = json.load(f)
            stats['details'] = len(details)
            # Store raw data first
            self._store_raw_data('controller', 'detail', ingestion_date, details_file, details)
            for detail in details:
                result = self._upsert_controller_detail(detail, ingestion_date)
                if result == 'inserted':
                    stats['inserted'] += 1
                elif result == 'updated':
                    stats['updated'] += 1
                else:
                    stats['skipped'] += 1

        return stats

    def load_aircraftexchange_data(self, ingestion_date: date) -> Dict[str, int]:
        """Load AircraftExchange.com data for a specific date.

        Args:
            ingestion_date: Date to load

        Returns:
            Dict with counts
        """
        date_str = ingestion_date.strftime("%Y-%m-%d")
        base_path = self.store_base / "aircraftexchange" / date_str

        stats = {
            'listings': 0,
            'details': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
        }

        # Load index listings
        index_file = base_path / "index" / "listings_metadata.json"
        if index_file.exists():
            logger.info(f"Loading AircraftExchange index data from {index_file}")
            with open(index_file, 'r', encoding='utf-8') as f:
                listings = json.load(f)
            stats['listings'] = len(listings)
            # Store raw data first
            self._store_raw_data('aircraftexchange', 'index', ingestion_date, index_file, listings)
            for listing in listings:
                result = self._upsert_aircraftexchange_listing(listing, ingestion_date)
                if result == 'inserted':
                    stats['inserted'] += 1
                elif result == 'updated':
                    stats['updated'] += 1
                else:
                    stats['skipped'] += 1

        # Load detail listings
        details_file = base_path / "details" / "details_metadata.json"
        if details_file.exists():
            logger.info(f"Loading AircraftExchange detail data from {details_file}")
            with open(details_file, 'r', encoding='utf-8') as f:
                details = json.load(f)
            stats['details'] = len(details)
            # Store raw data first
            self._store_raw_data('aircraftexchange', 'detail', ingestion_date, details_file, details)
            for detail in details:
                result = self._upsert_aircraftexchange_detail(detail, ingestion_date)
                if result == 'inserted':
                    stats['inserted'] += 1
                elif result == 'updated':
                    stats['updated'] += 1
                else:
                    stats['skipped'] += 1

        return stats

    def load_internal_db_data(self) -> Dict[str, int]:
        """Load internal database CSV files.

        Returns:
            Dict with counts
        """
        internal_path = self.store_base / "internaldb"
        stats = {
            'aircraft': 0,
            'sales': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
        }

        # Load aircraft.csv
        aircraft_file = internal_path / "aircraft.csv"
        if aircraft_file.exists():
            logger.info(f"Loading aircraft data from {aircraft_file}")
            with open(aircraft_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    result = self._upsert_internal_aircraft(row)
                    if result == 'inserted':
                        stats['inserted'] += 1
                    elif result == 'updated':
                        stats['updated'] += 1
                    else:
                        stats['skipped'] += 1
                    stats['aircraft'] += 1

        # Load recent_sales.csv
        sales_file = internal_path / "recent_sales.csv"
        if sales_file.exists():
            logger.info(f"Loading sales data from {sales_file}")
            with open(sales_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    result = self._upsert_internal_sale(row)
                    if result == 'inserted':
                        stats['inserted'] += 1
                    else:
                        stats['skipped'] += 1
                    stats['sales'] += 1

        return stats

    def _parse_price(self, price_str: Optional[str]) -> Optional[Decimal]:
        """Parse price string to Decimal.

        Args:
            price_str: Price string (e.g., "USD $1,234,567" or "Call for price")

        Returns:
            Decimal price or None
        """
        if not price_str:
            return None

        # Remove currency symbols, commas, spaces
        cleaned = re.sub(r'[^\d.]', '', str(price_str))
        if not cleaned:
            return None

        try:
            return Decimal(cleaned)
        except (InvalidOperation, ValueError):
            return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object.

        Args:
            date_str: Date string in various formats

        Returns:
            date object or None
        """
        if not date_str:
            return None

        # Try common date formats
        formats = ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError:
                continue

        return None

    def _parse_int(self, value: Optional[str]) -> Optional[int]:
        """Parse string to integer.

        Args:
            value: String value

        Returns:
            Integer or None
        """
        if not value:
            return None
        try:
            return int(float(str(value).replace(',', '')))
        except (ValueError, TypeError):
            return None

    def _parse_float(self, value: Optional[str]) -> Optional[Decimal]:
        """Parse string to Decimal.

        Args:
            value: String value

        Returns:
            Decimal or None
        """
        if not value:
            return None
        try:
            return Decimal(str(value).replace(',', ''))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _get_or_create_aircraft(
        self, serial_number: Optional[str], registration: Optional[str],
        manufacturer: Optional[str], model: Optional[str]
    ) -> Optional[str]:
        """Get or create aircraft record, return aircraft_id UUID.

        Args:
            serial_number: Aircraft serial number
            registration: Registration number
            manufacturer: Manufacturer name
            model: Model name

        Returns:
            aircraft_id UUID string or None
        """
        if not serial_number and not registration:
            return None

        # Try to find existing aircraft
        query = """
            SELECT id FROM aircraft
            WHERE (serial_number = %s AND serial_number IS NOT NULL)
               OR (registration_number = %s AND registration_number IS NOT NULL)
            LIMIT 1
        """
        result = self.db.execute_query(query, (serial_number, registration))
        if result:
            return str(result[0]['id'])

        # Create new aircraft
        insert_query = """
            INSERT INTO aircraft (
                serial_number, registration_number, manufacturer, model
            ) VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        result = self.db.execute_query(
            insert_query,
            (serial_number, registration, manufacturer, model)
        )
        if result:
            return str(result[0]['id'])
        return None

    def _upsert_controller_listing(
        self, listing: Dict, ingestion_date: date
    ) -> str:
        """Upsert Controller.com listing.

        Args:
            listing: Listing data dict
            ingestion_date: Date when data was scraped

        Returns:
            'inserted', 'updated', or 'skipped'
        """
        listing_url = listing.get('listing_url')
        if not listing_url:
            return 'skipped'

        # Check if listing exists (by listing_url, get latest record)
        # We want to update the latest record if it exists, or create new one
        check_query = """
            SELECT id, ask_price, listing_status, updated_at, ingestion_date
            FROM aircraft_listings
            WHERE listing_url = %s
            ORDER BY ingestion_date DESC, created_at DESC
            LIMIT 1
        """
        existing = self.db.execute_query(check_query, (listing_url,))

        # Extract data
        listing_id = listing.get('listing_id')
        ask_price = self._parse_price(listing.get('listing_price'))
        location = listing.get('listing_location')
        seller = listing.get('seller_name')

        # Parse aircraft info
        aircraft_model = listing.get('aircraft_model', '')
        year = self._parse_int(listing.get('year'))
        manufacturer = None
        model = None
        if aircraft_model:
            # Try to extract manufacturer/model from "YEAR MANUFACTURER MODEL"
            parts = aircraft_model.split()
            if parts:
                manufacturer = parts[1] if len(parts) > 1 else None
                model = ' '.join(parts[2:]) if len(parts) > 2 else None

        serial_number = None  # Not in index data
        registration = None  # Not in index data

        aircraft_id = self._get_or_create_aircraft(
            serial_number, registration, manufacturer, model
        )

        if existing:
            existing_record = existing[0]
            listing_db_id = existing_record['id']
            existing_ingestion_date = existing_record['ingestion_date']
            
            # Only update if this is a newer ingestion_date or same date (re-run)
            if existing_ingestion_date < ingestion_date or existing_ingestion_date == ingestion_date:
                old_price = existing_record['ask_price']
                old_status = existing_record['listing_status']

                # Track changes
                changes = []
                if old_price != ask_price:
                    changes.append(('ask_price', str(old_price) if old_price else None, str(ask_price) if ask_price else None))
                if old_status != 'for_sale':
                    changes.append(('listing_status', old_status, 'for_sale'))

                # Update with new ingestion_date if newer
                if existing_ingestion_date < ingestion_date:
                    update_query = """
                        UPDATE aircraft_listings
                        SET ask_price = %s, location = %s, seller = %s,
                            aircraft_id = %s, ingestion_date = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (ask_price, location, seller, aircraft_id, ingestion_date, listing_db_id)
                    )
                else:
                    # Same date, just update fields
                    update_query = """
                        UPDATE aircraft_listings
                        SET ask_price = %s, location = %s, seller = %s,
                            aircraft_id = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (ask_price, location, seller, aircraft_id, listing_db_id)
                    )

                # Record changes in history
                for field, old_val, new_val in changes:
                    history_query = """
                        INSERT INTO aircraft_listing_history
                        (listing_id, field_name, old_value, new_value, ingestion_date)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    self.db.execute_update(
                        history_query,
                        (listing_db_id, field, old_val, new_val, ingestion_date)
                    )

                return 'updated'
            else:
                # Older data, skip
                return 'skipped'
        else:
            # Insert new
            insert_query = """
                INSERT INTO aircraft_listings (
                    aircraft_id, listing_url, source_platform, source_listing_id,
                    listing_status, ask_price, location, seller,
                    ingestion_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.db.execute_update(
                insert_query,
                (
                    aircraft_id, listing_url, 'controller', listing_id,
                    'for_sale', ask_price, location, seller,
                    ingestion_date, json.dumps(listing)
                )
            )
            return 'inserted'

    def _upsert_controller_detail(
        self, detail: Dict, ingestion_date: date
    ) -> str:
        """Upsert Controller.com detail page data.

        Args:
            detail: Detail data dict
            ingestion_date: Date when data was scraped

        Returns:
            'inserted', 'updated', or 'skipped'
        """
        listing_url = detail.get('listing_url')
        if not listing_url:
            return 'skipped'

        # Check if listing exists (by listing_url, get latest record)
        check_query = """
            SELECT id, ask_price, listing_status, location, description,
                   airframe_total_time, seller, updated_at, ingestion_date
            FROM aircraft_listings
            WHERE listing_url = %s
            ORDER BY ingestion_date DESC, created_at DESC
            LIMIT 1
        """
        existing = self.db.execute_query(check_query, (listing_url,))

        # Extract detailed data
        serial_number = detail.get('serial_number')
        registration = detail.get('registration')
        manufacturer = detail.get('manufacturer')
        model = detail.get('model')
        year = self._parse_int(detail.get('year'))
        ask_price = self._parse_price(detail.get('asking_price'))
        location = detail.get('location')
        description = detail.get('description')
        seller = detail.get('seller_broker_name')
        airframe_time = self._parse_float(detail.get('total_time_hours'))

        aircraft_id = self._get_or_create_aircraft(
            serial_number, registration, manufacturer, model
        )

        # Extract features and inspections
        features = None  # Controller doesn't have structured features
        next_inspections = None  # Controller doesn't have structured inspections

        if existing:
            existing_record = existing[0]
            listing_db_id = existing_record['id']
            existing_ingestion_date = existing_record['ingestion_date']
            
            # Only update if this is a newer ingestion_date or same date (re-run)
            if existing_ingestion_date < ingestion_date or existing_ingestion_date == ingestion_date:
                old_price = existing_record['ask_price']
                old_status = existing_record['listing_status']
                old_location = existing_record['location']
                old_description = existing_record['description']
                old_time = existing_record['airframe_total_time']

                # Track changes
                changes = []
                if old_price != ask_price:
                    changes.append(('ask_price', str(old_price) if old_price else None, str(ask_price) if ask_price else None))
                if old_location != location:
                    changes.append(('location', old_location, location))
                if old_description != description:
                    changes.append(('description', old_description[:100] if old_description else None, description[:100] if description else None))
                if old_time != airframe_time:
                    changes.append(('airframe_total_time', str(old_time) if old_time else None, str(airframe_time) if airframe_time else None))

                # Update with new ingestion_date if newer
                if existing_ingestion_date < ingestion_date:
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, seller = %s, airframe_total_time = %s,
                            features = %s, next_inspections = %s,
                            ingestion_date = %s, raw_data = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description, seller,
                            airframe_time, features, next_inspections,
                            ingestion_date, json.dumps(detail), listing_db_id
                        )
                    )
                else:
                    # Same date, just update fields
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, seller = %s, airframe_total_time = %s,
                            features = %s, next_inspections = %s,
                            raw_data = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description, seller,
                            airframe_time, features, next_inspections,
                            json.dumps(detail), listing_db_id
                        )
                    )

                # Record changes
                for field, old_val, new_val in changes:
                    history_query = """
                        INSERT INTO aircraft_listing_history
                        (listing_id, field_name, old_value, new_value, ingestion_date)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    self.db.execute_update(
                        history_query,
                        (listing_db_id, field, old_val, new_val, ingestion_date)
                    )

                return 'updated'
            else:
                # Older data, skip
                return 'skipped'
        else:
            # Insert new
            insert_query = """
                INSERT INTO aircraft_listings (
                    aircraft_id, listing_url, source_platform, source_listing_id,
                    listing_status, ask_price, location, description, seller,
                    airframe_total_time, features, next_inspections,
                    ingestion_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            listing_id = detail.get('listing_id') or listing_url.split('/')[-2] if '/' in listing_url else None
            self.db.execute_update(
                insert_query,
                (
                    aircraft_id, listing_url, 'controller', listing_id,
                    'for_sale', ask_price, location, description, seller,
                    airframe_time, features, next_inspections,
                    ingestion_date, json.dumps(detail)
                )
            )
            return 'inserted'

    def _upsert_aircraftexchange_listing(
        self, listing: Dict, ingestion_date: date
    ) -> str:
        """Upsert AircraftExchange.com listing.

        Args:
            listing: Listing data dict
            ingestion_date: Date when data was scraped

        Returns:
            'inserted', 'updated', or 'skipped'
        """
        listing_url = listing.get('listing_url')
        if not listing_url:
            return 'skipped'

        # Check if listing exists (by listing_url, get latest record)
        check_query = """
            SELECT id, ingestion_date FROM aircraft_listings
            WHERE listing_url = %s
            ORDER BY ingestion_date DESC, created_at DESC
            LIMIT 1
        """
        existing = self.db.execute_query(check_query, (listing_url,))

        ask_price = self._parse_price(listing.get('asking_price'))
        location = listing.get('location')
        dealer = listing.get('dealer_name')

        if existing:
            existing_record = existing[0]
            existing_ingestion_date = existing_record['ingestion_date']
            
            # Only update if this is a newer ingestion_date or same date (re-run)
            if existing_ingestion_date < ingestion_date or existing_ingestion_date == ingestion_date:
                if existing_ingestion_date < ingestion_date:
                    update_query = """
                        UPDATE aircraft_listings
                        SET ask_price = %s, location = %s, seller = %s,
                            ingestion_date = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (ask_price, location, dealer, ingestion_date, existing_record['id'])
                    )
                else:
                    update_query = """
                        UPDATE aircraft_listings
                        SET ask_price = %s, location = %s, seller = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (ask_price, location, dealer, existing_record['id'])
                    )
                return 'updated'
            else:
                return 'skipped'
        else:
            insert_query = """
                INSERT INTO aircraft_listings (
                    listing_url, source_platform, listing_status,
                    ask_price, location, seller, ingestion_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.db.execute_update(
                insert_query,
                (
                    listing_url, 'aircraftexchange', 'for_sale',
                    ask_price, location, dealer, ingestion_date,
                    json.dumps(listing)
                )
            )
            return 'inserted'

    def _upsert_aircraftexchange_detail(
        self, detail: Dict, ingestion_date: date
    ) -> str:
        """Upsert AircraftExchange.com detail page data.

        Args:
            detail: Detail data dict
            ingestion_date: Date when data was scraped

        Returns:
            'inserted', 'updated', or 'skipped'
        """
        listing_url = detail.get('listing_url')
        if not listing_url:
            return 'skipped'

        # Check if listing exists (by listing_url, get latest record)
        check_query = """
            SELECT id, ingestion_date FROM aircraft_listings
            WHERE listing_url = %s
            ORDER BY ingestion_date DESC, created_at DESC
            LIMIT 1
        """
        existing = self.db.execute_query(check_query, (listing_url,))

        serial_number = detail.get('serial_number')
        registration = detail.get('registration')
        manufacturer = detail.get('manufacturer')
        model = detail.get('model')
        ask_price = self._parse_price(detail.get('asking_price'))
        location = detail.get('location')
        description = detail.get('description')

        aircraft_id = self._get_or_create_aircraft(
            serial_number, registration, manufacturer, model
        )

        if existing:
            existing_record = existing[0]
            existing_ingestion_date = existing_record['ingestion_date']
            
            # Only update if this is a newer ingestion_date or same date (re-run)
            if existing_ingestion_date < ingestion_date or existing_ingestion_date == ingestion_date:
                if existing_ingestion_date < ingestion_date:
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, ingestion_date = %s,
                            raw_data = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description,
                            ingestion_date, json.dumps(detail), existing_record['id']
                        )
                    )
                else:
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, raw_data = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description,
                            json.dumps(detail), existing_record['id']
                        )
                    )
                return 'updated'
            else:
                return 'skipped'
        else:
            insert_query = """
                INSERT INTO aircraft_listings (
                    aircraft_id, listing_url, source_platform, listing_status,
                    ask_price, location, description, ingestion_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.db.execute_update(
                insert_query,
                (
                    aircraft_id, listing_url, 'aircraftexchange', 'for_sale',
                    ask_price, location, description, ingestion_date,
                    json.dumps(detail)
                )
            )
            return 'inserted'

    def _upsert_internal_aircraft(self, row: Dict) -> str:
        """Upsert internal aircraft CSV row.

        Args:
            row: CSV row as dict

        Returns:
            'inserted', 'updated', or 'skipped'
        """
        serial_number = row.get('Serial Number', '').strip()
        registration = row.get('Registration Number', '').strip()

        if not serial_number and not registration:
            return 'skipped'

        # Get or create aircraft
        aircraft_id = self._get_or_create_aircraft(
            serial_number if serial_number else None,
            registration if registration else None,
            row.get('Make'),
            row.get('Model')
        )

        # This is master data, update aircraft table
        update_query = """
            UPDATE aircraft
            SET manufacturer = COALESCE(%s, manufacturer),
                model = COALESCE(%s, model),
                manufacturer_year = COALESCE(%s, manufacturer_year),
                delivery_year = COALESCE(%s, delivery_year),
                category = COALESCE(%s, category),
                aircraft_status = COALESCE(%s, aircraft_status),
                registration_number = COALESCE(%s, registration_number),
                number_of_passengers = COALESCE(%s, number_of_passengers),
                registration_country = COALESCE(%s, registration_country),
                based_country = COALESCE(%s, based_country),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """
        self.db.execute_update(
            update_query,
            (
                row.get('Make'), row.get('Model'),
                self._parse_int(row.get('Manufacturer Year')),
                self._parse_int(row.get('Delivery Year')),
                row.get('Category'),
                row.get('Aircraft Status'),
                registration if registration else None,
                self._parse_int(row.get('Number of Passengers')),
                row.get('Registration Country'),
                row.get('Based Country'),
                aircraft_id
            )
        )

        return 'updated' if aircraft_id else 'inserted'

    def _upsert_internal_sale(self, row: Dict) -> str:
        """Insert internal sales CSV row (append-only).

        Args:
            row: CSV row as dict

        Returns:
            'inserted' or 'skipped'
        """
        serial_number = row.get('Serial Number', '').strip()
        date_sold_str = row.get('Date Sold', '').strip()

        if not serial_number or not date_sold_str:
            return 'skipped'

        date_sold = self._parse_date(date_sold_str)
        if not date_sold:
            return 'skipped'

        # Check if sale already exists
        check_query = """
            SELECT id FROM aircraft_sales
            WHERE serial_number = %s AND date_sold = %s
            LIMIT 1
        """
        existing = self.db.execute_query(check_query, (serial_number, date_sold))
        if existing:
            return 'skipped'

        # Get aircraft_id
        aircraft_id = None
        if serial_number:
            aircraft_query = "SELECT id FROM aircraft WHERE serial_number = %s LIMIT 1"
            result = self.db.execute_query(aircraft_query, (serial_number,))
            if result:
                aircraft_id = result[0]['id']

        # Insert sale
        insert_query = """
            INSERT INTO aircraft_sales (
                aircraft_id, serial_number, registration_number,
                manufacturer, model, manufacturer_year, delivery_year, category,
                transaction_status, sold_price, ask_price, take_price,
                date_sold, days_on_market, airframe_total_time,
                apu_total_time, prop_total_time, engine_program,
                engine_program_deferment, apu_program, apu_program_deferment,
                airframe_program, registration_country, based_country,
                number_of_passengers, interior_year, exterior_year,
                seller, buyer, seller_broker, buyer_broker, has_damage,
                percent_of_book, features, source_platform, source_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """
        self.db.execute_update(
            insert_query,
            (
                aircraft_id,
                serial_number,
                row.get('Registration Number'),
                row.get('Make'),
                row.get('Model'),
                self._parse_int(row.get('Manufacturer Year')),
                self._parse_int(row.get('Delivery Year')),
                row.get('Category'),
                row.get('Transaction Status'),
                self._parse_price(row.get('Sold Price')),
                self._parse_price(row.get('Ask Price')),
                self._parse_price(row.get('Take Price')),
                date_sold,
                self._parse_int(row.get('Days on market')),
                self._parse_float(row.get('Airframe Total Time')),
                self._parse_float(row.get('APU Total Time')),
                self._parse_float(row.get('Prop Total Time')),
                row.get('Engine Program'),
                self._parse_price(row.get('Engine Program Deferment Amount')),
                row.get('APU Program'),
                self._parse_price(row.get('APU Program Deferment Amount')),
                row.get('Airframe Program'),
                row.get('Registration Country'),
                row.get('Based Country'),
                self._parse_int(row.get('Number of Passengers')),
                self._parse_int(row.get('Interior Year')),
                self._parse_int(row.get('Exterior Year')),
                row.get('Seller'),
                row.get('Buyer'),
                row.get('Seller Broker'),
                row.get('Buyer Broker'),
                row.get('Has Damage', '').lower() == 'true' if row.get('Has Damage') else False,
                self._parse_float(row.get('% of Book')),
                json.dumps([f.strip() for f in row.get('Features', '').split(',') if f.strip()]) if row.get('Features') else None,
                'internal',
                json.dumps(row)
            )
        )
        return 'inserted'

    def load_all_latest(self) -> Dict[str, Any]:
        """Load latest date data from all sources.

        Returns:
            Dict with summary statistics
        """
        summary = {
            'controller': None,
            'aircraftexchange': None,
            'internaldb': None,
            'total_inserted': 0,
            'total_updated': 0,
            'total_skipped': 0,
        }

        # Load Controller
        controller_date = self.find_latest_date('controller')
        if controller_date:
            stats = self.load_controller_data(controller_date)
            summary['controller'] = {'date': controller_date.isoformat(), **stats}
            summary['total_inserted'] += stats['inserted']
            summary['total_updated'] += stats['updated']
            summary['total_skipped'] += stats['skipped']

        # Load AircraftExchange
        aircraftexchange_date = self.find_latest_date('aircraftexchange')
        if aircraftexchange_date:
            stats = self.load_aircraftexchange_data(aircraftexchange_date)
            summary['aircraftexchange'] = {'date': aircraftexchange_date.isoformat(), **stats}
            summary['total_inserted'] += stats['inserted']
            summary['total_updated'] += stats['updated']
            summary['total_skipped'] += stats['skipped']

        # Load Internal DB
        stats = self.load_internal_db_data()
        summary['internaldb'] = stats
        summary['total_inserted'] += stats['inserted']
        summary['total_updated'] += stats['updated']
        summary['total_skipped'] += stats['skipped']

        return summary

    def _store_raw_data(
        self, source_platform: str, source_type: str,
        ingestion_date: date, file_path: Path, data: List[Dict]
    ) -> None:
        """Store raw data in raw_data_store table (append-only).

        Args:
            source_platform: Source name (controller, aircraftexchange, etc.)
            source_type: Type (index, detail, csv, etc.)
            ingestion_date: Date when data was scraped
            file_path: Path to source file
            data: Data to store (list of dicts or single dict)
        """
        try:
            # Store each record separately for granular access
            if isinstance(data, list):
                for record in data:
                    listing_url = record.get('listing_url') if isinstance(record, dict) else None
                    # Check if already exists
                    check_query = """
                        SELECT id FROM raw_data_store
                        WHERE source_platform = %s AND source_type = %s
                          AND ingestion_date = %s AND listing_url = %s
                          AND file_path = %s
                        LIMIT 1
                    """
                    exists = self.db.execute_query(
                        check_query,
                        (source_platform, source_type, ingestion_date, listing_url, str(file_path))
                    )
                    if exists:
                        continue  # Skip if already stored
                    
                    insert_query = """
                        INSERT INTO raw_data_store (
                            source_platform, source_type, ingestion_date,
                            file_path, listing_url, raw_data
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    self.db.execute_update(
                        insert_query,
                        (
                            source_platform, source_type, ingestion_date,
                            str(file_path), listing_url, json.dumps(record)
                        )
                    )
            else:
                # Single record
                listing_url = data.get('listing_url') if isinstance(data, dict) else None
                # Check if already exists
                check_query = """
                    SELECT id FROM raw_data_store
                    WHERE source_platform = %s AND source_type = %s
                      AND ingestion_date = %s AND listing_url = %s
                      AND file_path = %s
                    LIMIT 1
                """
                exists = self.db.execute_query(
                    check_query,
                    (source_platform, source_type, ingestion_date, listing_url, str(file_path))
                )
                if exists:
                    return  # Skip if already stored
                
                insert_query = """
                    INSERT INTO raw_data_store (
                        source_platform, source_type, ingestion_date,
                        file_path, listing_url, raw_data
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        source_platform, source_type, ingestion_date,
                        str(file_path), listing_url, json.dumps(data)
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to store raw data: {e}", exc_info=True)
            # Don't fail the whole process if raw storage fails
