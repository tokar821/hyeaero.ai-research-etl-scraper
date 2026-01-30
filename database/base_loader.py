"""Base loader class with shared utilities for all data loaders."""

import re
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Optional, Any
from decimal import Decimal, InvalidOperation
import logging

from .postgres_client import PostgresClient

logger = logging.getLogger(__name__)


class BaseLoader:
    """Base class for all data loaders with shared utilities."""

    def __init__(self, db_client: PostgresClient, store_base_path: Optional[Path] = None):
        """Initialize base loader.

        Args:
            db_client: PostgreSQL client instance
            store_base_path: Base path to store/ directory. Defaults to ./store/raw
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

    def _clean_registration(self, registration: Optional[str]) -> Optional[str]:
        """Clean registration number to extract just the registration code.
        
        Examples:
            "N6EU (Registration Retained by Seller)" -> "N6EU"
            "N123AB" -> "N123AB"
            "HB-LML" -> "HB-LML"
        
        Args:
            registration: Raw registration string
            
        Returns:
            Cleaned registration string (max 50 chars) or None
        """
        if not registration:
            return None
        
        # Remove extra text in parentheses or after certain patterns
        # Extract just the registration code (usually first part before space/parentheses)
        cleaned = registration.strip()
        
        # Remove text in parentheses
        cleaned = re.sub(r'\([^)]*\)', '', cleaned).strip()
        
        # Take first part before any space or special markers
        cleaned = cleaned.split()[0] if cleaned.split() else cleaned
        
        # Limit to 50 characters (database constraint)
        cleaned = cleaned[:50] if len(cleaned) > 50 else cleaned
        
        return cleaned if cleaned else None

    @staticmethod
    def _truncate(s: Optional[str], max_len: int) -> Optional[str]:
        """Truncate string to max_len for VARCHAR columns. Returns None if s is None."""
        if s is None:
            return None
        s = str(s).strip()
        return s[:max_len] if len(s) > max_len else (s or None)

    def _get_or_create_aircraft(
        self, serial_number: Optional[str], registration: Optional[str],
        manufacturer: Optional[str], model: Optional[str],
        condition: Optional[str] = None, based_at: Optional[str] = None,
        year: Optional[int] = None, category: Optional[str] = None
    ) -> Optional[str]:
        """Get or create aircraft record, return aircraft_id UUID.

        Args:
            serial_number: Aircraft serial number (can be None)
            registration: Registration number (can be None)
            manufacturer: Manufacturer name
            model: Model name
            condition: Aircraft condition (Used, New, etc.)
            based_at: Airport code/location
            year: Manufacturer year
            category: Aircraft category

        Returns:
            aircraft_id UUID string or None if both serial_number and registration are None
        """
        # Truncate to schema limits: serial_number/manufacturer/model/based_at 100, registration 50, condition/category 50
        serial_number = self._truncate(serial_number, 100)
        registration = self._truncate(registration, 50)
        manufacturer = self._truncate(manufacturer, 100)
        model = self._truncate(model, 100)
        based_at = self._truncate(based_at, 100)
        condition = self._truncate(condition, 50)
        category = self._truncate(category, 50)

        # At least one identifier must exist (serial_number OR registration_number)
        # OR we must have manufacturer+model to create a placeholder
        if not serial_number and not registration:
            # If we have manufacturer+model, we can still create (for listings without identifiers)
            if manufacturer and model:
                # Create with NULL identifiers (will be updated later if identifiers found)
                insert_query = """
                    INSERT INTO aircraft (
                        serial_number, registration_number, manufacturer, model,
                        manufacturer_year, category, condition, based_at
                    ) VALUES (NULL, NULL, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                try:
                    result = self.db.execute_query(insert_query, (manufacturer, model, year, category, condition, based_at))
                    if result:
                        return str(result[0]['id'])
                except Exception as e:
                    logger.warning(f"Failed to create aircraft placeholder (mfr={manufacturer}, model={model}): {e}")
            return None

        # Clean registration (also enforces 50-char limit)
        registration = self._clean_registration(registration) if registration else None

        # Try to find existing aircraft by serial_number or registration_number
        query = """
            SELECT id FROM aircraft
            WHERE (serial_number = %s AND serial_number IS NOT NULL)
               OR (registration_number = %s AND registration_number IS NOT NULL)
            LIMIT 1
        """
        result = self.db.execute_query(query, (serial_number, registration))
        if result:
            aircraft_id = str(result[0]['id'])
            # Update aircraft with new info if provided
            if condition or based_at or year or category:
                update_query = """
                    UPDATE aircraft SET
                        condition = COALESCE(%s, condition),
                        based_at = COALESCE(%s, based_at),
                        manufacturer_year = COALESCE(%s, manufacturer_year),
                        category = COALESCE(%s, category),
                        manufacturer = COALESCE(%s, manufacturer),
                        model = COALESCE(%s, model),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                self.db.execute_update(
                    update_query,
                    (condition, based_at, year, category, manufacturer, model, aircraft_id)
                )
            return aircraft_id

        # Create new aircraft (serial_number can be NULL)
        insert_query = """
            INSERT INTO aircraft (
                serial_number, registration_number, manufacturer, model,
                manufacturer_year, category, condition, based_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        try:
            result = self.db.execute_query(
                insert_query,
                (serial_number, registration, manufacturer, model, year, category, condition, based_at)
            )
            if result:
                return str(result[0]['id'])
        except Exception as e:
            logger.warning(f"Failed to create aircraft (serial={serial_number}, reg={registration}): {e}")
            # If unique constraint violation, try to find again (race condition)
            result = self.db.execute_query(query, (serial_number, registration))
            if result:
                return str(result[0]['id'])
        return None

    def _store_engine(
        self, aircraft_id: Optional[str], listing_id: Optional[str],
        engine_position: int, make_model: Optional[str], serial_number: Optional[str],
        hours_since_new: Optional[float], hours_since_overhaul: Optional[float],
        hours_since_hot_section: Optional[float], cycles: Optional[int],
        tbo_hours: Optional[int], tbo_years: Optional[int], notes: Optional[str],
        source_platform: str, ingestion_date: date
    ) -> None:
        """Store engine data in aircraft_engines table.
        
        Args:
            aircraft_id: Aircraft UUID
            listing_id: Listing UUID (if from listing)
            engine_position: 1 or 2
            make_model: Engine make/model
            serial_number: Engine serial number
            hours_since_new: Hours since new
            hours_since_overhaul: Hours since overhaul
            hours_since_hot_section: Hours since hot section
            cycles: Engine cycles
            tbo_hours: TBO hours
            tbo_years: TBO years
            notes: Engine notes
            source_platform: Source platform
            ingestion_date: Ingestion date
        """
        if not make_model and not serial_number:
            return  # Skip if no engine data
        
        # Truncate make_model if too long (safety measure, though schema uses TEXT now)
        if make_model and len(make_model) > 1000:
            make_model = make_model[:1000] + "..."
        
        try:
            # Check if engine already exists
            check_query = """
                SELECT id FROM aircraft_engines
                WHERE aircraft_id = %s AND listing_id = %s AND engine_position = %s
                  AND ingestion_date = %s
                LIMIT 1
            """
            exists = self.db.execute_query(
                check_query,
                (aircraft_id, listing_id, engine_position, ingestion_date)
            )
            
            if exists:
                # Update existing
                update_query = """
                    UPDATE aircraft_engines SET
                        make_model = %s, serial_number = %s,
                        hours_since_new = %s, hours_since_overhaul = %s,
                        hours_since_hot_section = %s, cycles = %s,
                        tbo_hours = %s, tbo_years = %s, notes = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                self.db.execute_update(
                    update_query,
                    (
                        make_model, serial_number, hours_since_new, hours_since_overhaul,
                        hours_since_hot_section, cycles, tbo_hours, tbo_years, notes,
                        exists[0]['id']
                    )
                )
            else:
                # Insert new
                insert_query = """
                    INSERT INTO aircraft_engines (
                        aircraft_id, listing_id, engine_position, make_model, serial_number,
                        hours_since_new, hours_since_overhaul, hours_since_hot_section,
                        cycles, tbo_hours, tbo_years, notes,
                        source_platform, ingestion_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        aircraft_id, listing_id, engine_position, make_model, serial_number,
                        hours_since_new, hours_since_overhaul, hours_since_hot_section,
                        cycles, tbo_hours, tbo_years, notes,
                        source_platform, ingestion_date
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to store engine data: {e}")

    def _store_raw_data(
        self, source_platform: str, source_type: str,
        ingestion_date: date, file_path: Path, data: Any
    ) -> None:
        """Store raw data in raw_data_store table (append-only).

        Args:
            source_platform: Source name (controller, aircraftexchange, etc.)
            source_type: Type (index, detail, csv, etc.)
            ingestion_date: Date when data was scraped
            file_path: Path to source file
            data: Data to store (list of dicts or single dict)
        """
        import json
        try:
            # Store each record separately for granular access
            if isinstance(data, list):
                total = len(data)
                logger.info(f"Storing raw_data_store: {source_platform}/{source_type} - {total} records")
                stored = 0
                progress_every = max(1, min(50, total // 20))  # every 50 records, or ~20 steps for small totals
                for idx, record in enumerate(data):
                    n = idx + 1
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
                        # Progress every N records (whether we insert or skip)
                        if n % progress_every == 0 or n == total:
                            logger.info(f"raw_data_store progress: {n}/{total} processed, {stored} inserted")
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
                    stored += 1
                    # Progress every N records and at end
                    if n % progress_every == 0 or n == total:
                        logger.info(f"raw_data_store progress: {n}/{total} processed, {stored} inserted")
                logger.info(f"raw_data_store done: {source_platform}/{source_type} - {stored} inserted, {total - stored} skipped (already present)")
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

    def _store_apu(
        self, aircraft_id: Optional[str], listing_id: Optional[str],
        make_model: Optional[str], hours_since_new: Optional[float],
        hours_since_overhaul: Optional[float], maintenance_program: Optional[str],
        notes: Optional[str], source_platform: str, ingestion_date: date
    ) -> None:
        """Store APU data in aircraft_apus table.
        
        Args:
            aircraft_id: Aircraft UUID
            listing_id: Listing UUID (if from listing)
            make_model: APU make/model
            hours_since_new: Hours since new
            hours_since_overhaul: Hours since overhaul
            maintenance_program: APU maintenance program
            notes: APU notes
            source_platform: Source platform
            ingestion_date: Ingestion date
        """
        if not make_model:
            return  # Skip if no APU data
        
        # Truncate make_model if too long (safety measure, though schema uses TEXT now)
        if make_model and len(make_model) > 1000:
            make_model = make_model[:1000] + "..."
        
        try:
            # Check if APU already exists
            check_query = """
                SELECT id FROM aircraft_apus
                WHERE aircraft_id = %s AND listing_id = %s AND ingestion_date = %s
                LIMIT 1
            """
            exists = self.db.execute_query(
                check_query,
                (aircraft_id, listing_id, ingestion_date)
            )
            
            if exists:
                # Update existing
                update_query = """
                    UPDATE aircraft_apus SET
                        make_model = %s, hours_since_new = %s,
                        hours_since_overhaul = %s, maintenance_program = %s,
                        notes = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                self.db.execute_update(
                    update_query,
                    (make_model, hours_since_new, hours_since_overhaul, maintenance_program, notes, exists[0]['id'])
                )
            else:
                # Insert new
                insert_query = """
                    INSERT INTO aircraft_apus (
                        aircraft_id, listing_id, make_model,
                        hours_since_new, hours_since_overhaul, maintenance_program, notes,
                        source_platform, ingestion_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        aircraft_id, listing_id, make_model,
                        hours_since_new, hours_since_overhaul, maintenance_program, notes,
                        source_platform, ingestion_date
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to store APU data: {e}")
