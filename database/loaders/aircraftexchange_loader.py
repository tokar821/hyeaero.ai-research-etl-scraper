"""AircraftExchange.com data loader.

Loads AircraftExchange.com aircraft listings and details into PostgreSQL database.
"""

import json
from datetime import date
from pathlib import Path
from typing import Dict, Optional
import logging

from ..base_loader import BaseLoader

logger = logging.getLogger(__name__)


class AircraftExchangeLoader(BaseLoader):
    """Loader for AircraftExchange.com data."""

    def load_aircraftexchange_data(self, ingestion_date: date, limit: Optional[int] = None) -> Dict[str, int]:
        """Load AircraftExchange.com data for a specific date.
        
        Loads data from:
        - index/listings_metadata.json (main index)
        - details/details_metadata.json (main details)
        - manufacturers/*/manufacturer_listings_metadata.json (manufacturer listings)
        - manufacturers/*/details/details_metadata.json (manufacturer details)

        Args:
            ingestion_date: Date to load
            limit: Optional limit on number of records to process

        Returns:
            Dict with counts
        """
        date_str = ingestion_date.strftime("%Y-%m-%d")
        base_path = self.store_base / "aircraftexchange" / date_str

        stats = {
            'listings': 0,
            'details': 0,
            'manufacturer_listings': 0,
            'manufacturer_details': 0,
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
            # Store raw data first (limited if needed)
            listings_to_store = listings[:limit] if limit else listings
            self._store_raw_data('aircraftexchange', 'index', ingestion_date, index_file, listings_to_store)
            for i, listing in enumerate(listings):
                if limit and i >= limit:
                    logger.info(f"Reached AircraftExchange index limit ({limit}), stopping")
                    break
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
            # Store raw data first (limited if needed)
            details_to_store = details[:limit] if limit else details
            self._store_raw_data('aircraftexchange', 'detail', ingestion_date, details_file, details_to_store)
            for i, detail in enumerate(details):
                if limit and i >= limit:
                    logger.info(f"Reached AircraftExchange detail limit ({limit}), stopping")
                    break
                result = self._upsert_aircraftexchange_detail(detail, ingestion_date)
                if result == 'inserted':
                    stats['inserted'] += 1
                elif result == 'updated':
                    stats['updated'] += 1
                else:
                    stats['skipped'] += 1

        # Load manufacturer data
        manufacturers_path = base_path / "manufacturers"
        if manufacturers_path.exists():
            logger.info(f"Loading AircraftExchange manufacturer data from {manufacturers_path}")
            
            # Find all manufacturer folders (e.g., 1_cessna, 2_gulfstream)
            manufacturer_folders = [d for d in manufacturers_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
            logger.info(f"Found {len(manufacturer_folders)} manufacturer folders")
            
            for manufacturer_folder in manufacturer_folders:
                manufacturer_name = manufacturer_folder.name
                
                # Load manufacturer listings
                manufacturer_listings_file = manufacturer_folder / "manufacturer_listings_metadata.json"
                if manufacturer_listings_file.exists():
                    logger.info(f"Loading manufacturer listings: {manufacturer_name}")
                    with open(manufacturer_listings_file, 'r', encoding='utf-8') as f:
                        manufacturer_listings = json.load(f)
                    stats['manufacturer_listings'] += len(manufacturer_listings)
                    
                    # Store raw data (limited if needed)
                    listings_to_store = manufacturer_listings[:limit] if limit else manufacturer_listings
                    self._store_raw_data(
                        'aircraftexchange', 
                        'manufacturer_listings', 
                        ingestion_date, 
                        manufacturer_listings_file, 
                        listings_to_store
                    )
                    
                    # Process listings (may overlap with index, but deduplication handled by listing_url)
                    for i, listing in enumerate(manufacturer_listings):
                        if limit and i >= limit:
                            break  # Stop processing but continue to next manufacturer
                        result = self._upsert_aircraftexchange_listing(listing, ingestion_date)
                        if result == 'inserted':
                            stats['inserted'] += 1
                        elif result == 'updated':
                            stats['updated'] += 1
                        else:
                            stats['skipped'] += 1
                
                # Load manufacturer details
                manufacturer_details_file = manufacturer_folder / "details" / "details_metadata.json"
                if manufacturer_details_file.exists():
                    logger.info(f"Loading manufacturer details: {manufacturer_name}")
                    with open(manufacturer_details_file, 'r', encoding='utf-8') as f:
                        manufacturer_details = json.load(f)
                    stats['manufacturer_details'] += len(manufacturer_details)
                    
                    # Store raw data (limited if needed)
                    details_to_store = manufacturer_details[:limit] if limit else manufacturer_details
                    self._store_raw_data(
                        'aircraftexchange', 
                        'manufacturer_details', 
                        ingestion_date, 
                        manufacturer_details_file, 
                        details_to_store
                    )
                    
                    # Process details (may overlap with main details, but deduplication handled by listing_url)
                    for i, detail in enumerate(manufacturer_details):
                        if limit and i >= limit:
                            break  # Stop processing but continue to next manufacturer
                        result = self._upsert_aircraftexchange_detail(detail, ingestion_date)
                        if result == 'inserted':
                            stats['inserted'] += 1
                        elif result == 'updated':
                            stats['updated'] += 1
                        else:
                            stats['skipped'] += 1

        return stats

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
        registration = self._clean_registration(detail.get('registration'))
        manufacturer = detail.get('manufacturer')
        model = detail.get('model')
        year = self._parse_int(detail.get('year'))
        ask_price = self._parse_price(detail.get('asking_price'))
        location = detail.get('location')
        description = detail.get('description')
        dealer_name = detail.get('dealer_name')
        condition = detail.get('condition')
        aircraft_type = detail.get('aircraft_type')
        
        # Extract additional fields
        total_time = self._parse_float(detail.get('total_time'))
        total_cycles = self._parse_int(detail.get('total_cycles'))
        number_of_seats = self._parse_int(detail.get('number_of_seats'))
        avionics_description = detail.get('avionics_description')
        additional_equipment = detail.get('additional_equipment')
        inspection_status = detail.get('inspection_status')
        
        # Engine data
        engine_1_make_model = detail.get('engine_1_make_model')
        engine_1_serial_number = detail.get('engine_1_serial_number')
        engine_1_hours_since_new = self._parse_float(detail.get('engine_1_hours_since_new'))
        engine_1_hours_since_overhaul = self._parse_float(detail.get('engine_1_hours_since_overhaul'))
        engine_1_hours_since_hot_section = self._parse_float(detail.get('engine_1_hours_since_hot_section'))
        engine_2_make_model = detail.get('engine_2_make_model')
        engine_2_serial_number = detail.get('engine_2_serial_number')
        engine_2_hours_since_new = self._parse_float(detail.get('engine_2_hours_since_new'))
        engine_2_hours_since_overhaul = self._parse_float(detail.get('engine_2_hours_since_overhaul'))
        engine_2_hours_since_hot_section = self._parse_float(detail.get('engine_2_hours_since_hot_section'))
        
        # Seller contact info
        seller_contact_name = detail.get('seller_contact_name')
        seller_phone = detail.get('seller_phone')
        seller_email = detail.get('seller_email')
        
        # Only create aircraft if we have at least serial_number or registration
        aircraft_id = None
        if serial_number or registration:
            aircraft_id = self._get_or_create_aircraft(
                serial_number, registration, manufacturer, model,
                condition=condition, year=year, category=aircraft_type
            )

        if existing:
            existing_record = existing[0]
            existing_ingestion_date = existing_record['ingestion_date']
            
            # Only update if this is a newer ingestion_date or same date (re-run)
            if existing_ingestion_date < ingestion_date or existing_ingestion_date == ingestion_date:
                # Build features and inspections
                feature_list = []
                if avionics_description:
                    feature_list.append(f"Avionics: {avionics_description[:200]}")
                if additional_equipment:
                    feature_list.append(f"Equipment: {additional_equipment[:200]}")
                features = json.dumps(feature_list) if feature_list else None
                next_inspections = json.dumps([inspection_status]) if inspection_status else None
                
                if existing_ingestion_date < ingestion_date:
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, seller = %s,
                            airframe_total_time = %s, airframe_total_cycles = %s,
                            features = %s, next_inspections = %s,
                            number_of_passengers = %s, additional_equipment = %s,
                            avionics_description = %s, inspection_status = %s,
                            seller_contact_name = %s, seller_phone = %s, seller_email = %s,
                            ingestion_date = %s, raw_data = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description, dealer_name,
                            total_time, total_cycles, features, next_inspections,
                            number_of_seats, additional_equipment,
                            avionics_description, inspection_status,
                            seller_contact_name, seller_phone, seller_email,
                            ingestion_date, json.dumps(detail), existing_record['id']
                        )
                    )
                else:
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, seller = %s,
                            airframe_total_time = %s, airframe_total_cycles = %s,
                            features = %s, next_inspections = %s,
                            number_of_passengers = %s, additional_equipment = %s,
                            avionics_description = %s, inspection_status = %s,
                            seller_contact_name = %s, seller_phone = %s, seller_email = %s,
                            raw_data = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description, dealer_name,
                            total_time, total_cycles, features, next_inspections,
                            number_of_seats, additional_equipment,
                            avionics_description, inspection_status,
                            seller_contact_name, seller_phone, seller_email,
                            json.dumps(detail), existing_record['id']
                        )
                    )
                
                # Store engines
                if engine_1_make_model or engine_1_hours_since_new:
                    self._store_engine(
                        aircraft_id, existing_record['id'], 1,
                        engine_1_make_model, engine_1_serial_number,
                        engine_1_hours_since_new, engine_1_hours_since_overhaul,
                        engine_1_hours_since_hot_section, None, None, None,
                        None, 'aircraftexchange', ingestion_date
                    )
                if engine_2_make_model or engine_2_hours_since_new:
                    self._store_engine(
                        aircraft_id, existing_record['id'], 2,
                        engine_2_make_model, engine_2_serial_number,
                        engine_2_hours_since_new, engine_2_hours_since_overhaul,
                        engine_2_hours_since_hot_section, None, None, None,
                        None, 'aircraftexchange', ingestion_date
                    )
                return 'updated'
            else:
                return 'skipped'
        else:
            # Build features and inspections
            feature_list = []
            if avionics_description:
                feature_list.append(f"Avionics: {avionics_description[:200]}")
            if additional_equipment:
                feature_list.append(f"Equipment: {additional_equipment[:200]}")
            features = json.dumps(feature_list) if feature_list else None
            next_inspections = json.dumps([inspection_status]) if inspection_status else None
            
            insert_query = """
                INSERT INTO aircraft_listings (
                    aircraft_id, listing_url, source_platform, listing_status,
                    ask_price, location, description, seller,
                    airframe_total_time, airframe_total_cycles,
                    features, next_inspections, number_of_passengers,
                    additional_equipment, avionics_description, inspection_status,
                    seller_contact_name, seller_phone, seller_email,
                    ingestion_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            result = self.db.execute_query(
                insert_query,
                (
                    aircraft_id, listing_url, 'aircraftexchange', 'for_sale',
                    ask_price, location, description, dealer_name,
                    total_time, total_cycles,
                    features, next_inspections, number_of_seats,
                    additional_equipment, avionics_description, inspection_status,
                    seller_contact_name, seller_phone, seller_email,
                    ingestion_date, json.dumps(detail)
                )
            )
            new_listing_id = result[0]['id'] if result else None
            
            # Store engines
            if engine_1_make_model or engine_1_hours_since_new:
                self._store_engine(
                    aircraft_id, new_listing_id, 1,
                    engine_1_make_model, engine_1_serial_number,
                    engine_1_hours_since_new, engine_1_hours_since_overhaul,
                    engine_1_hours_since_hot_section, None, None, None,
                    None, 'aircraftexchange', ingestion_date
                )
            if engine_2_make_model or engine_2_hours_since_new:
                self._store_engine(
                    aircraft_id, new_listing_id, 2,
                    engine_2_make_model, engine_2_serial_number,
                    engine_2_hours_since_new, engine_2_hours_since_overhaul,
                    engine_2_hours_since_hot_section, None, None, None,
                    None, 'aircraftexchange', ingestion_date
                )
            
            return 'inserted'
