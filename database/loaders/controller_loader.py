"""Controller.com data loader.

Loads Controller.com aircraft listings and details into PostgreSQL database.
"""

import json
import re
from datetime import date
from pathlib import Path
from typing import Dict, Optional
import logging

from ..base_loader import BaseLoader

logger = logging.getLogger(__name__)


class ControllerLoader(BaseLoader):
    """Loader for Controller.com data."""

    def load_controller_data(
        self, ingestion_date: date, limit: Optional[int] = None, skip_index: bool = False
    ) -> Dict[str, int]:
        """Load Controller.com data for a specific date.

        Args:
            ingestion_date: Date to load
            limit: Optional limit on number of records to process
            skip_index: If True, skip index (listings) and load only detail.

        Returns:
            Dict with counts: {'listings': X, 'details': Y, 'inserted': Z, 'updated': W, 'skipped': N}
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

        # Load index listings (unless skip_index)
        if skip_index:
            logger.info("Skipping Controller index (--skip-controller-index); loading detail only")
        index_file = base_path / "index" / "listings_metadata.json"
        if not skip_index and index_file.exists():
            logger.info(f"Loading Controller index data from {index_file}")
            try:
                with open(index_file, 'r', encoding='utf-8') as f:
                    listings = json.load(f)
                logger.info(f"Successfully loaded {len(listings)} Controller index listings from JSON")
                stats['listings'] = len(listings)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Controller index JSON: {e}")
                return stats
            except Exception as e:
                logger.error(f"Error reading Controller index file: {e}", exc_info=True)
                return stats
            
            # Store raw data first (limited if needed)
            listings_to_store = listings[:limit] if limit else listings
            logger.debug(f"Storing {len(listings_to_store)} Controller index records to raw_data_store")
            self._store_raw_data('controller', 'index', ingestion_date, index_file, listings_to_store)
            
            # Process listings (basic info, will be enriched by details)
            logger.info(f"Processing {len(listings_to_store)} Controller index listings...")
            for i, listing in enumerate(listings):
                if limit and i >= limit:
                    logger.info(f"Reached Controller index limit ({limit}), stopping")
                    break
                
                # Log progress for every record
                logger.info(f"Processing Controller index listing {i + 1}/{len(listings_to_store)}: {listing.get('listing_url', 'N/A')}")
                
                logger.debug(f"Processing listing: URL={listing.get('listing_url')}, ID={listing.get('listing_id')}, "
                           f"Price={listing.get('listing_price')}, Model={listing.get('aircraft_model')}")
                
                result = self._upsert_controller_listing(listing, ingestion_date)
                if result == 'inserted':
                    stats['inserted'] += 1
                    logger.info(f"[OK] [{i + 1}/{len(listings_to_store)}] Inserted Controller listing: {listing.get('listing_url')}")
                elif result == 'updated':
                    stats['updated'] += 1
                    logger.info(f"[OK] [{i + 1}/{len(listings_to_store)}] Updated Controller listing: {listing.get('listing_url')}")
                else:
                    stats['skipped'] += 1
                    logger.info(f"[SKIP] [{i + 1}/{len(listings_to_store)}] Skipped Controller listing: {listing.get('listing_url')}")
            
            logger.info(f"Controller index processing complete: {stats['inserted']} inserted, {stats['updated']} updated, {stats['skipped']} skipped")

        # Load detail listings
        details_file = base_path / "details" / "details_metadata.json"
        if details_file.exists():
            logger.info(f"Loading Controller detail data from {details_file}")
            try:
                with open(details_file, 'r', encoding='utf-8') as f:
                    details = json.load(f)
                logger.info(f"Successfully loaded {len(details)} Controller detail records from JSON")
                stats['details'] = len(details)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Controller detail JSON: {e}")
                return stats
            except Exception as e:
                logger.error(f"Error reading Controller detail file: {e}", exc_info=True)
                return stats
            
            # Store raw data first (limited if needed)
            details_to_store = details[:limit] if limit else details
            logger.debug(f"Storing {len(details_to_store)} Controller detail records to raw_data_store")
            self._store_raw_data('controller', 'detail', ingestion_date, details_file, details_to_store)
            
            logger.info(f"Processing {len(details_to_store)} Controller detail records...")
            for i, detail in enumerate(details):
                if limit and i >= limit:
                    logger.info(f"Reached Controller detail limit ({limit}), stopping")
                    break
                
                # Log progress for every record
                logger.info(f"Processing Controller detail {i + 1}/{len(details_to_store)}: {detail.get('listing_url', 'N/A')}")
                
                # Log key extracted fields
                logger.debug(f"Processing detail: URL={detail.get('listing_url')}, "
                           f"Serial={detail.get('serial_number')}, Reg={detail.get('registration_number')}, "
                           f"Make={detail.get('manufacturer')}, Model={detail.get('model')}, "
                           f"Year={detail.get('manufacturer_year')}, Price={detail.get('ask_price')}")
                
                result = self._upsert_controller_detail(detail, ingestion_date)
                if result == 'inserted':
                    stats['inserted'] += 1
                    logger.info(f"[OK] [{i + 1}/{len(details_to_store)}] Inserted Controller detail: {detail.get('listing_url')}")
                elif result == 'updated':
                    stats['updated'] += 1
                    logger.info(f"[OK] [{i + 1}/{len(details_to_store)}] Updated Controller detail: {detail.get('listing_url')}")
                else:
                    stats['skipped'] += 1
                    logger.info(f"[SKIP] [{i + 1}/{len(details_to_store)}] Skipped Controller detail: {detail.get('listing_url')}")
            
            logger.info(f"Controller detail processing complete: {stats['inserted']} inserted, {stats['updated']} updated, {stats['skipped']} skipped")

        return stats

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
            logger.warning(f"Controller listing missing listing_url, skipping. Available keys: {list(listing.keys())}")
            return 'skipped'

        # Check if listing exists (by listing_url, get latest record)
        check_query = """
            SELECT id, ask_price, listing_status, updated_at, ingestion_date
            FROM aircraft_listings
            WHERE listing_url = %s
            ORDER BY ingestion_date DESC, created_at DESC
            LIMIT 1
        """
        existing = self.db.execute_query(check_query, (listing_url,))

        # Extract data
        listing_id = (listing.get('listing_id') or '')[:100] if listing.get('listing_id') else None  # source_listing_id VARCHAR(100)
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

        # Only create aircraft if we have at least serial_number or registration
        aircraft_id = None
        if serial_number or registration:
            aircraft_id = self._get_or_create_aircraft(
                serial_number, registration, manufacturer, model
            )

        if existing:
            existing_record = existing[0]
            listing_db_id = existing_record['id']
            existing_ingestion_date = existing_record['ingestion_date']
            
            logger.debug(f"Found existing Controller listing (ID: {listing_db_id}, existing_date: {existing_ingestion_date}, new_date: {ingestion_date})")
            
            # Only update if this is a newer ingestion_date or same date (re-run)
            if existing_ingestion_date < ingestion_date or existing_ingestion_date == ingestion_date:
                old_price = existing_record['ask_price']
                old_status = existing_record['listing_status']

                # Track changes
                changes = []
                if old_price != ask_price:
                    changes.append(('ask_price', str(old_price) if old_price else None, str(ask_price) if ask_price else None))
                    logger.debug(f"Price change detected: {old_price} -> {ask_price}")
                if old_status != 'for_sale':
                    changes.append(('listing_status', old_status, 'for_sale'))
                    logger.debug(f"Status change detected: {old_status} -> for_sale")

                # Update with new ingestion_date if newer
                if existing_ingestion_date < ingestion_date:
                    logger.debug(f"Updating listing with newer ingestion_date")
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
                    logger.debug(f"Updating listing with same ingestion_date")
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
                    logger.debug(f"Recorded history change: {field} = {old_val} -> {new_val}")

                return 'updated'
            else:
                # Older data, skip
                logger.debug(f"Skipping older data (existing: {existing_ingestion_date}, new: {ingestion_date})")
                return 'skipped'
        else:
            # Insert new
            logger.debug(f"Inserting new Controller listing: URL={listing_url}, Price={ask_price}, Location={location}, "
                        f"Manufacturer={manufacturer}, Model={model}, Year={year}")
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
            logger.debug(f"Successfully inserted new Controller listing (URL: {listing_url})")
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
            logger.warning(f"Controller detail missing listing_url, skipping. Available keys: {list(detail.keys())}")
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
        registration = self._clean_registration(detail.get('registration'))
        manufacturer = detail.get('manufacturer')
        model = detail.get('model')
        year = self._parse_int(detail.get('year'))
        ask_price = self._parse_price(detail.get('asking_price'))
        location = detail.get('location')
        description = detail.get('description')
        seller = detail.get('seller_broker_name')
        airframe_time = self._parse_float(detail.get('total_time_hours'))
        airframe_landings = self._parse_int(detail.get('total_landings'))
        condition = detail.get('condition')
        based_at = (detail.get('based_at') or '')[:100] if detail.get('based_at') else None  # VARCHAR(100)
        aircraft_type = detail.get('aircraft_type')
        
        # Extract additional fields
        number_of_seats = self._parse_int(detail.get('number_of_seats'))
        year_painted = self._parse_int(detail.get('year_painted'))
        exterior_notes = detail.get('exterior_notes')
        interior_notes = detail.get('interior_notes')
        props_notes = detail.get('props_notes')
        additional_equipment = detail.get('additional_equipment')
        avionics_description = detail.get('avionics_description')
        avionics_list = detail.get('avionics_list')
        modifications = detail.get('modifications')
        inspection_status = detail.get('inspection_status')
        airframe_notes = detail.get('airframe_notes')
        complete_logs = detail.get('complete_logs')
        maintenance_tracking = detail.get('maintenance_tracking')
        
        # Engine data
        engine_1_make_model = detail.get('engine_1_make_model')
        engine_1_time = self._parse_float(detail.get('engine_1_time'))
        engine_1_cycles = self._parse_int(detail.get('engine_1_cycles'))
        engine_1_tbo = detail.get('engine_1_tbo')
        engine_1_notes = detail.get('engine_1_notes')
        engine_2_make_model = detail.get('engine_2_make_model')
        engine_2_time = self._parse_float(detail.get('engine_2_time'))
        engine_2_cycles = self._parse_int(detail.get('engine_2_cycles'))
        engine_2_tbo = detail.get('engine_2_tbo')
        engine_2_notes = detail.get('engine_2_notes')
        
        # Parse TBO (format: "2000 hrs" or "2000 hrs / 6 Years")
        engine_1_tbo_hours = None
        engine_1_tbo_years = None
        if engine_1_tbo:
            tbo_match = re.search(r'(\d+)\s*hrs', str(engine_1_tbo), re.IGNORECASE)
            if tbo_match:
                engine_1_tbo_hours = int(tbo_match.group(1))
            year_match = re.search(r'(\d+)\s*years?', str(engine_1_tbo), re.IGNORECASE)
            if year_match:
                engine_1_tbo_years = int(year_match.group(1))
        
        engine_2_tbo_hours = None
        engine_2_tbo_years = None
        if engine_2_tbo:
            tbo_match = re.search(r'(\d+)\s*hrs', str(engine_2_tbo), re.IGNORECASE)
            if tbo_match:
                engine_2_tbo_hours = int(tbo_match.group(1))
            year_match = re.search(r'(\d+)\s*years?', str(engine_2_tbo), re.IGNORECASE)
            if year_match:
                engine_2_tbo_years = int(year_match.group(1))
        
        # APU data
        apu = detail.get('apu')
        apu_maintenance_program = detail.get('apu_maintenance_program')
        apu_notes = detail.get('apu_notes')
        
        # Seller contact info (VARCHAR 255/50 in schema)
        seller_contact_name = self._truncate(detail.get('seller_contact_name'), 255)
        seller_phone = self._truncate(detail.get('seller_phone'), 50)
        seller_email = self._truncate(detail.get('seller_email'), 255)
        seller_location = self._truncate(detail.get('seller_location'), 255)
        # Truncate other VARCHAR(255) listing fields
        seller = self._truncate(seller, 255) if seller else None
        maintenance_tracking = self._truncate(maintenance_tracking, 255) if maintenance_tracking else None

        # Only create aircraft if we have at least serial_number or registration
        aircraft_id = None
        if serial_number or registration:
            aircraft_id = self._get_or_create_aircraft(
                serial_number, registration, manufacturer, model,
                condition=condition, based_at=based_at, year=year, category=aircraft_type
            )

        # Build features list from available data
        feature_list = []
        if avionics_description:
            feature_list.append(f"Avionics: {avionics_description[:200]}")
        if additional_equipment:
            feature_list.append(f"Equipment: {additional_equipment[:200]}")
        if props_notes:
            feature_list.append(f"Props: {props_notes[:200]}")
        features = json.dumps(feature_list) if feature_list else None

        logger.debug(f"Extracted Controller detail fields: Serial={serial_number}, Reg={registration}, "
                    f"Make={manufacturer}, Model={model}, Year={year}, Price={ask_price}, "
                    f"Time={airframe_time}, Location={location}")
        
        if existing:
            existing_record = existing[0]
            listing_db_id = existing_record['id']
            existing_ingestion_date = existing_record['ingestion_date']
            
            logger.debug(f"Found existing Controller detail listing (ID: {listing_db_id}, existing_date: {existing_ingestion_date}, new_date: {ingestion_date})")
            
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
                    logger.debug(f"Price change: {old_price} -> {ask_price}")
                if old_location != location:
                    changes.append(('location', old_location, location))
                    logger.debug(f"Location change: {old_location} -> {location}")
                if old_description != description:
                    changes.append(('description', old_description[:100] if old_description else None, description[:100] if description else None))
                    logger.debug(f"Description changed (length: {len(old_description) if old_description else 0} -> {len(description) if description else 0})")
                if old_time != airframe_time:
                    changes.append(('airframe_total_time', str(old_time) if old_time else None, str(airframe_time) if airframe_time else None))
                    logger.debug(f"Airframe time change: {old_time} -> {airframe_time}")

                # Update with new ingestion_date if newer
                if existing_ingestion_date < ingestion_date:
                    logger.debug(f"Updating Controller detail with newer ingestion_date (ID: {listing_db_id})")
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, seller = %s, airframe_total_time = %s,
                            airframe_total_landings = %s, airframe_notes = %s,
                            complete_logs = %s, maintenance_tracking_program = %s,
                            features = %s, next_inspections = %s,
                            number_of_passengers = %s, exterior_year = %s, year_painted = %s,
                            props_notes = %s, additional_equipment = %s,
                            modifications = %s, avionics_description = %s, avionics_list = %s,
                            exterior_notes = %s, interior_notes = %s,
                            inspection_status = %s, based_at = %s,
                            seller_contact_name = %s, seller_phone = %s,
                            seller_email = %s, seller_location = %s,
                            ingestion_date = %s, raw_data = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description, seller,
                            airframe_time, airframe_landings, airframe_notes,
                            complete_logs, maintenance_tracking,
                            features, None,  # next_inspections
                            number_of_seats, year_painted, year_painted,
                            props_notes, additional_equipment,
                            modifications, avionics_description, avionics_list,
                            exterior_notes, interior_notes,
                            inspection_status, based_at,
                            seller_contact_name, seller_phone,
                            seller_email, seller_location,
                            ingestion_date, json.dumps(detail), listing_db_id
                        )
                    )
                    logger.debug(f"Successfully updated Controller detail listing (ID: {listing_db_id})")
                else:
                    # Same date, just update fields
                    logger.debug(f"Updating Controller detail with same ingestion_date (ID: {listing_db_id})")
                    update_query = """
                        UPDATE aircraft_listings
                        SET aircraft_id = %s, ask_price = %s, location = %s,
                            description = %s, seller = %s, airframe_total_time = %s,
                            airframe_total_landings = %s, airframe_notes = %s,
                            complete_logs = %s, maintenance_tracking_program = %s,
                            features = %s, next_inspections = %s,
                            number_of_passengers = %s, exterior_year = %s, year_painted = %s,
                            props_notes = %s, additional_equipment = %s,
                            modifications = %s, avionics_description = %s, avionics_list = %s,
                            exterior_notes = %s, interior_notes = %s,
                            inspection_status = %s, based_at = %s,
                            seller_contact_name = %s, seller_phone = %s,
                            seller_email = %s, seller_location = %s,
                            raw_data = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    self.db.execute_update(
                        update_query,
                        (
                            aircraft_id, ask_price, location, description, seller,
                            airframe_time, airframe_landings, airframe_notes,
                            complete_logs, maintenance_tracking,
                            features, None,  # next_inspections
                            number_of_seats, year_painted, year_painted,
                            props_notes, additional_equipment,
                            modifications, avionics_description, avionics_list,
                            exterior_notes, interior_notes,
                            inspection_status, based_at,
                            seller_contact_name, seller_phone,
                            seller_email, seller_location,
                            json.dumps(detail), listing_db_id
                        )
                    )
                    logger.debug(f"Successfully updated Controller detail listing (ID: {listing_db_id})")
                
                # Store engines and APU
                if engine_1_make_model or engine_1_time:
                    logger.debug(f"Storing engine 1: {engine_1_make_model}, Time={engine_1_time}, Cycles={engine_1_cycles}")
                    self._store_engine(
                        aircraft_id, listing_db_id, 1,
                        engine_1_make_model, None, engine_1_time, None, None,
                        engine_1_cycles, engine_1_tbo_hours, engine_1_tbo_years,
                        engine_1_notes, 'controller', ingestion_date
                    )
                if engine_2_make_model or engine_2_time:
                    logger.debug(f"Storing engine 2: {engine_2_make_model}, Time={engine_2_time}, Cycles={engine_2_cycles}")
                    self._store_engine(
                        aircraft_id, listing_db_id, 2,
                        engine_2_make_model, None, engine_2_time, None, None,
                        engine_2_cycles, engine_2_tbo_hours, engine_2_tbo_years,
                        engine_2_notes, 'controller', ingestion_date
                    )
                if apu:
                    logger.debug(f"Storing APU: {apu}, Program={apu_maintenance_program}")
                    self._store_apu(
                        aircraft_id, listing_db_id, apu, None, None,
                        apu_maintenance_program, apu_notes, 'controller', ingestion_date
                    )

                # Record changes
                for field, old_val, new_val in changes:
                    logger.debug(f"Recording history change: {field} = {old_val} -> {new_val}")
                    history_query = """
                        INSERT INTO aircraft_listing_history
                        (listing_id, field_name, old_value, new_value, ingestion_date)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    self.db.execute_update(
                        history_query,
                        (listing_db_id, field, old_val, new_val, ingestion_date)
                    )

                logger.debug(f"Successfully updated Controller detail (URL: {listing_url}, {len(changes)} changes)")
                return 'updated'
            else:
                # Older data, skip
                logger.debug(f"Skipping older Controller detail data (existing: {existing_ingestion_date}, new: {ingestion_date})")
                return 'skipped'
        else:
            # Insert new
            insert_query = """
                INSERT INTO aircraft_listings (
                    aircraft_id, listing_url, source_platform, source_listing_id,
                    listing_status, ask_price, location, description, seller,
                    airframe_total_time, airframe_total_landings, airframe_notes,
                    complete_logs, maintenance_tracking_program,
                    features, next_inspections,
                    number_of_passengers, exterior_year, year_painted,
                    props_notes, additional_equipment,
                    modifications, avionics_description, avionics_list,
                    exterior_notes, interior_notes,
                    inspection_status, based_at,
                    seller_contact_name, seller_phone,
                    seller_email, seller_location,
                    ingestion_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            listing_id = detail.get('listing_id') or (listing_url.split('/')[-2] if '/' in listing_url else None)
            listing_id = (listing_id or '')[:100] if listing_id else None  # source_listing_id VARCHAR(100)
            result = self.db.execute_query(
                insert_query,
                (
                    aircraft_id, listing_url, 'controller', listing_id,
                    'for_sale', ask_price, location, description, seller,
                    airframe_time, airframe_landings, airframe_notes,
                    complete_logs, maintenance_tracking,
                    features, None,  # next_inspections
                    number_of_seats, year_painted, year_painted,
                    props_notes, additional_equipment,
                    modifications, avionics_description, avionics_list,
                    exterior_notes, interior_notes,
                    inspection_status, based_at,
                    seller_contact_name, seller_phone,
                    seller_email, seller_location,
                    ingestion_date, json.dumps(detail)
                )
            )
            new_listing_id = result[0]['id'] if result else None
            logger.debug(f"Inserted new Controller detail listing (ID: {new_listing_id}, URL: {listing_url})")
            
            # Store engines and APU
            if engine_1_make_model or engine_1_time:
                logger.debug(f"Storing engine 1: {engine_1_make_model}, Time={engine_1_time}, Cycles={engine_1_cycles}")
                self._store_engine(
                    aircraft_id, new_listing_id, 1,
                    engine_1_make_model, None, engine_1_time, None, None,
                    engine_1_cycles, engine_1_tbo_hours, engine_1_tbo_years,
                    engine_1_notes, 'controller', ingestion_date
                )
            if engine_2_make_model or engine_2_time:
                logger.debug(f"Storing engine 2: {engine_2_make_model}, Time={engine_2_time}, Cycles={engine_2_cycles}")
                self._store_engine(
                    aircraft_id, new_listing_id, 2,
                    engine_2_make_model, None, engine_2_time, None, None,
                    engine_2_cycles, engine_2_tbo_hours, engine_2_tbo_years,
                    engine_2_notes, 'controller', ingestion_date
                )
            if apu:
                logger.debug(f"Storing APU: {apu}, Program={apu_maintenance_program}")
                self._store_apu(
                    aircraft_id, new_listing_id, apu, None, None,
                    apu_maintenance_program, apu_notes, 'controller', ingestion_date
                )
            
            logger.debug(f"Successfully inserted new Controller detail (URL: {listing_url}, ID: {new_listing_id})")
            return 'inserted'
