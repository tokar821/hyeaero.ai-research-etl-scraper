"""Internal database loader.

Loads internal CSV files (aircraft.csv, recent_sales.csv) into PostgreSQL database.
"""

import json
import csv
from pathlib import Path
from typing import Dict, Optional
import logging

from ..base_loader import BaseLoader

logger = logging.getLogger(__name__)


class InternalLoader(BaseLoader):
    """Loader for internal database CSV files."""

    def load_internal_db_data(
        self,
        limit: Optional[int] = None,
        aircraft_only: bool = False,
        sales_only: bool = False,
    ) -> Dict[str, int]:
        """Load internal database CSV files.

        Args:
            limit: Optional limit on number of records to process
            aircraft_only: If True, load only aircraft.csv (skip recent_sales.csv)
            sales_only: If True, load only recent_sales.csv (skip aircraft.csv)

        Returns:
            Dict with counts: {'aircraft': X, 'sales': Y, 'inserted': Z, 'updated': W, 'skipped': N}
        """
        internal_path = self.store_base / "internaldb"
        stats = {
            'aircraft': 0,
            'sales': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
        }

        # Load aircraft.csv (unless sales_only)
        aircraft_file = internal_path / "aircraft.csv"
        if not sales_only and aircraft_file.exists():
            logger.info(f"Loading aircraft data from {aircraft_file}")
            try:
                with open(aircraft_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in aircraft.csv (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached Internal DB aircraft limit ({limit}), stopping")
                            break
                        
                        # Log progress for every record
                        logger.info(f"Processing Internal DB aircraft {i + 1}/{total_to_process}: Serial={row.get('Serial Number', 'N/A')}, Reg={row.get('Registration Number', 'N/A')}")
                        
                        logger.debug(f"Processing aircraft: Serial={row.get('Serial Number')}, Reg={row.get('Registration Number')}, "
                                   f"Make={row.get('Make')}, Model={row.get('Model')}, Year={row.get('Year')}")
                        
                        result = self._upsert_internal_aircraft(row)
                        if result == 'inserted':
                            stats['inserted'] += 1
                            logger.info(f"[OK] [{i + 1}/{total_to_process}] Inserted Internal DB aircraft: Serial={row.get('Serial Number')}")
                        elif result == 'updated':
                            stats['updated'] += 1
                            logger.info(f"[OK] [{i + 1}/{total_to_process}] Updated Internal DB aircraft: Serial={row.get('Serial Number')}")
                        else:
                            stats['skipped'] += 1
                            logger.info(f"[SKIP] [{i + 1}/{total_to_process}] Skipped Internal DB aircraft: Serial={row.get('Serial Number')}")
                        stats['aircraft'] += 1
                    
                    logger.info(f"Internal DB aircraft processing complete: {stats['inserted']} inserted, {stats['updated']} updated, {stats['skipped']} skipped")
            except Exception as e:
                logger.error(f"Error loading aircraft.csv: {e}", exc_info=True)

        # Load recent_sales.csv (unless aircraft_only)
        sales_file = internal_path / "recent_sales.csv"
        if not aircraft_only and sales_file.exists():
            logger.info(f"Loading sales data from {sales_file}")
            try:
                with open(sales_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in recent_sales.csv (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached Internal DB sales limit ({limit}), stopping")
                            break
                        
                        # Log progress for every record
                        logger.info(f"Processing Internal DB sale {i + 1}/{total_to_process}: Serial={row.get('Serial Number', 'N/A')}, Price={row.get('Sold Price', 'N/A')}")
                        
                        logger.debug(f"Processing sale: Serial={row.get('Serial Number')}, Make={row.get('Make')}, "
                                   f"Model={row.get('Model')}, Sold Price={row.get('Sold Price')}, Date={row.get('Date Sold')}")
                        
                        result = self._upsert_internal_sale(row)
                        if result == 'inserted':
                            stats['inserted'] += 1
                            logger.info(f"[OK] [{i + 1}/{total_to_process}] Inserted Internal DB sale: Serial={row.get('Serial Number')}, Price={row.get('Sold Price')}")
                        else:
                            stats['skipped'] += 1
                            logger.info(f"[SKIP] [{i + 1}/{total_to_process}] Skipped Internal DB sale: Serial={row.get('Serial Number')}")
                        stats['sales'] += 1
                    
                    logger.info(f"Internal DB sales processing complete: {stats['inserted']} inserted, {stats['skipped']} skipped")
            except Exception as e:
                logger.error(f"Error loading recent_sales.csv: {e}", exc_info=True)

        return stats

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

        if not aircraft_id:
            return 'skipped'

        # Avoid unique violation: only set registration_number if not already used by another aircraft
        registration_to_set = registration
        if registration:
            conflict = self.db.execute_query(
                "SELECT id FROM aircraft WHERE registration_number = %s AND id != %s LIMIT 1",
                (registration, aircraft_id)
            )
            if conflict:
                registration_to_set = None  # keep current; another aircraft already has this reg
                logger.debug(
                    f"Registration {registration} already used by another aircraft; not updating for aircraft_id={aircraft_id}"
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
                registration_to_set,
                self._parse_int(row.get('Number of Passengers')),
                row.get('Registration Country'),
                row.get('Based Country'),
                aircraft_id
            )
        )

        return 'updated'

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

        # Get aircraft_id (by serial_number or registration_number)
        aircraft_id = None
        registration = row.get('Registration Number', '').strip()
        if serial_number:
            aircraft_query = "SELECT id FROM aircraft WHERE serial_number = %s LIMIT 1"
            result = self.db.execute_query(aircraft_query, (serial_number,))
            if result:
                aircraft_id = result[0]['id']
        elif registration:
            aircraft_query = "SELECT id FROM aircraft WHERE registration_number = %s LIMIT 1"
            result = self.db.execute_query(aircraft_query, (registration,))
            if result:
                aircraft_id = result[0]['id']

        # Parse deferment amounts
        engine_program_deferment_amount = self._parse_price(row.get('Engine Program Deferment Amount'))
        apu_program_deferment_amount = self._parse_price(row.get('APU Program Deferment Amount'))
        
        # Convert amounts to booleans (True if amount exists and > 0)
        engine_program_deferment = engine_program_deferment_amount is not None and engine_program_deferment_amount > 0
        apu_program_deferment = apu_program_deferment_amount is not None and apu_program_deferment_amount > 0
        
        # Parse features
        features_str = row.get('Features', '')
        features = None
        if features_str:
            feature_list = [f.strip() for f in features_str.split(',') if f.strip()]
            features = json.dumps(feature_list) if feature_list else None

        # Insert sale
        insert_query = """
            INSERT INTO aircraft_sales (
                aircraft_id, serial_number,
                manufacturer, model, manufacturer_year, delivery_year, category,
                transaction_status, sold_price, ask_price, take_price,
                date_sold, days_on_market, airframe_total_time,
                apu_total_time, prop_total_time, engine_program,
                engine_program_deferment, engine_program_deferment_amount,
                apu_program, apu_program_deferment, apu_program_deferment_amount,
                airframe_program, registration_country, based_country,
                number_of_passengers, interior_year, exterior_year,
                seller, buyer, seller_broker, buyer_broker, has_damage,
                percent_of_book, features, source_platform, source_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        self.db.execute_update(
            insert_query,
            (
                aircraft_id,
                serial_number,
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
                engine_program_deferment,
                engine_program_deferment_amount,
                row.get('APU Program'),
                apu_program_deferment,
                apu_program_deferment_amount,
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
                features,
                'internal',
                json.dumps(row)
            )
        )
        return 'inserted'
