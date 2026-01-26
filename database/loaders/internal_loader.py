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

    def load_internal_db_data(self, limit: Optional[int] = None) -> Dict[str, int]:
        """Load internal database CSV files.

        Args:
            limit: Optional limit on number of records to process

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

        # Load aircraft.csv
        aircraft_file = internal_path / "aircraft.csv"
        if aircraft_file.exists():
            logger.info(f"Loading aircraft data from {aircraft_file}")
            with open(aircraft_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if limit and i >= limit:
                        logger.info(f"Reached Internal DB aircraft limit ({limit}), stopping")
                        break
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
                for i, row in enumerate(reader):
                    if limit and i >= limit:
                        logger.info(f"Reached Internal DB sales limit ({limit}), stopping")
                        break
                    result = self._upsert_internal_sale(row)
                    if result == 'inserted':
                        stats['inserted'] += 1
                    else:
                        stats['skipped'] += 1
                    stats['sales'] += 1

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
