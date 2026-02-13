"""FAA Aircraft Registration Database loader.

Loads FAA data files (MASTER, ACFTREF, DEALER, DEREG, ENGINE, DOCINDEX, RESERVED, PDFs)
into PostgreSQL database.
"""

import json
import csv
import hashlib
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional
import logging

from ..base_loader import BaseLoader

logger = logging.getLogger(__name__)

# Try to import PyPDF2 for PDF processing
try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logger.warning("PyPDF2 not available. PDF extraction will be skipped. Install with: pip install PyPDF2")


class FAALoader(BaseLoader):
    """Loader for FAA Aircraft Registration Database data."""

    def load_faa_data(
        self,
        ingestion_date: date,
        limit: Optional[int] = None,
        master_offset: int = 0,
    ) -> Dict[str, int]:
        """Load FAA Aircraft Registration Database data for a specific date.
        
        Loads ALL FAA files:
        - MASTER.txt (aircraft registrations)
        - ACFTREF.txt (manufacturer/model reference)
        - DEALER.txt (dealer information)
        - DEREG.txt (deregistered aircraft)
        - ENGINE.txt (engine information)
        - DOCINDEX.txt (document index)
        - RESERVED.txt (reserved registrations)
        - PDF files (ardata.pdf, documentation)

        Args:
            ingestion_date: Date to load
            limit: Optional limit on number of records to process
            master_offset: Skip first N rows of MASTER.txt (0 = process from start; use to continue after partial save)

        Returns:
            Dict with counts for all files
        """
        date_str = ingestion_date.strftime("%Y-%m-%d")
        base_path = self.store_base / "faa" / date_str
        extracted_path = base_path / "extracted"

        stats = {
            'master': {'records': 0, 'inserted': 0, 'updated': 0, 'skipped': 0},
            'acftref': {'records': 0, 'inserted': 0, 'skipped': 0},
            'dealer': {'records': 0, 'inserted': 0, 'skipped': 0},
            'dereg': {'records': 0, 'inserted': 0, 'skipped': 0},
            'engine': {'records': 0, 'inserted': 0, 'skipped': 0},
            'docindex': {'records': 0, 'inserted': 0, 'skipped': 0},
            'reserved': {'records': 0, 'inserted': 0, 'skipped': 0},
            'pdfs': {'files': 0, 'inserted': 0, 'skipped': 0},
            'total_inserted': 0,
            'total_updated': 0,
            'total_skipped': 0,
        }

        # Load ACFTREF.txt first (needed for decoding manufacturer/model codes)
        acftref_file = extracted_path / "ACFTREF.txt"
        acftref_lookup = {}
        if acftref_file.exists():
            logger.info(f"Loading FAA ACFTREF data from {acftref_file}")
            try:
                # Read file directly - CSV reader can handle trailing comma (creates empty field)
                # Use utf-8-sig to automatically handle BOM, but we still need to normalize field names
                with open(acftref_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    # Log field names to debug parsing issues
                    fieldnames = reader.fieldnames
                    # Normalize field names: strip BOM and whitespace
                    fieldname_map = {}
                    if fieldnames:
                        normalized_fieldnames = [fn.strip('\ufeff').strip() if fn else fn for fn in fieldnames]
                        # Create a mapping from original to normalized
                        fieldname_map = {orig: norm for orig, norm in zip(fieldnames, normalized_fieldnames) if orig != norm}
                        if fieldname_map:
                            logger.info(f"Normalized field names (removed BOM): {list(fieldname_map.items())[:3]}")
                    logger.info(f"ACFTREF field names detected: {fieldnames[:5]}...")
                    
                    # If there's an empty field name from trailing comma, note it
                    if fieldnames and fieldnames[-1] == '':
                        logger.info("Detected trailing comma in header (empty field at end)")
                    
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in ACFTREF.txt (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        # Normalize row keys (remove BOM from field names)
                        if fieldname_map:
                            normalized_row = {}
                            for key, value in row.items():
                                normalized_key = fieldname_map.get(key, key.strip('\ufeff').strip())
                                normalized_row[normalized_key] = value
                            row = normalized_row
                        
                        # Always load full ACFTREF for lookup (needed for MASTER decoding)
                        # But limit storage if requested
                        # Handle BOM character in CODE field name
                        code = row.get('CODE', '').strip()
                        if not code:
                            # Try with BOM character (some files have \ufeff prefix)
                            code = row.get('\ufeffCODE', '').strip()
                        if not code:
                            # Try alternative key names (case variations)
                            code = row.get('code', '').strip() or row.get('Code', '').strip()
                        
                        # Debug first few rows to see what we're getting
                        if i < 3:
                            logger.info(f"Row {i}: CODE field='{code}', all keys: {list(row.keys())[:3]}")
                            # Show first few values
                            first_values = [str(v)[:20] for v in list(row.values())[:3]]
                            logger.info(f"Row {i} first values: {first_values}")
                        
                        if code:
                            acftref_lookup[code] = {
                                'manufacturer': row.get('MFR', row.get('mfr', '')).strip(),
                                'model': row.get('MODEL', row.get('model', '')).strip(),
                                'type_aircraft': row.get('TYPE-ACFT', row.get('type-acft', '')).strip(),
                                'type_engine': row.get('TYPE-ENG', row.get('type-eng', '')).strip(),
                                'no_engines': row.get('NO-ENG', row.get('no-eng', '')).strip(),
                                'no_seats': row.get('NO-SEATS', row.get('no-seats', '')).strip(),
                            }
                            stats['acftref']['records'] += 1
                        # Store in faa_aircraft_reference table (limited if needed)
                        if not limit or i < limit:
                            # Log progress for every record being stored
                            logger.info(f"Processing FAA ACFTREF row {i + 1}/{total_to_process}: Code={code}")
                            self._store_faa_aircraft_reference(row, ingestion_date)
                            stats['acftref']['inserted'] += 1
                            logger.info(f"[OK] [{i + 1}/{total_to_process}] Stored FAA ACFTREF: Code={code}")
                            # Also store raw data
                            self._store_faa_csv_row('faa', 'acftref', ingestion_date, acftref_file, row)
                
                logger.info(f"FAA ACFTREF processing complete: {stats['acftref']['inserted']} inserted, loaded {len(acftref_lookup)} codes for lookup")
                if len(acftref_lookup) == 0 and total_rows > 0:
                    logger.error("No ACFTREF codes loaded despite processing rows! Check CODE field extraction.")
                    logger.error(f"Field names were: {fieldnames}")
            except Exception as e:
                logger.error(f"Error loading ACFTREF.txt: {e}", exc_info=True)
        else:
            logger.warning(f"FAA ACFTREF.txt not found at {acftref_file}")

        # Load MASTER.txt (main aircraft registration file)
        master_file = extracted_path / "MASTER.txt"
        if master_file.exists():
            logger.info(f"Loading FAA MASTER data from {master_file}")
            try:
                with open(master_file, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    # Count total rows first for progress tracking
                    all_rows = list(reader)
                    total_rows = len(all_rows)
                    # Skip first master_offset rows (to continue from a previous partial run)
                    rows = all_rows[master_offset:] if master_offset > 0 else all_rows
                    if master_offset > 0:
                        logger.info(f"FAA MASTER: skipping first {master_offset} rows (already saved); processing from row {master_offset + 1} to {total_rows}")
                    logger.info(f"Found {total_rows} rows in MASTER.txt (processing {len(rows)} rows, up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, len(rows)) if limit else len(rows)
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached FAA MASTER limit ({limit}), stopping")
                            break
                        row_num = master_offset + i + 1  # 1-based display
                        # Log progress for every record
                        logger.info(f"Processing FAA MASTER row {row_num}/{total_rows}: N-Number={row.get('N-NUMBER', 'N/A')}")
                        
                        # Decode manufacturer/model using ACFTREF lookup
                        mfr_mdl_code = row.get('MFR MDL CODE', row.get('mfr mdl code', row.get('mfr_mdl_code', ''))).strip()
                        if mfr_mdl_code and mfr_mdl_code in acftref_lookup:
                            ref = acftref_lookup[mfr_mdl_code]
                            row['_decoded_manufacturer'] = ref['manufacturer']
                            row['_decoded_model'] = ref['model']
                            logger.debug(f"Decoded MFR/MDL code {mfr_mdl_code} -> {ref['manufacturer']} {ref['model']}")
                        else:
                            logger.debug(f"No ACFTREF lookup found for code: {mfr_mdl_code}")
                        
                        result = self._upsert_faa_aircraft(row, ingestion_date)
                        if result == 'inserted':
                            stats['master']['inserted'] += 1
                            stats['total_inserted'] += 1
                            logger.info(f"[OK] [{row_num}/{total_rows}] Inserted FAA MASTER: N-Number={row.get('N-NUMBER')}, Serial={row.get('SERIAL-NUMBER')}")
                        elif result == 'updated':
                            stats['master']['updated'] += 1
                            stats['total_updated'] += 1
                            logger.info(f"[OK] [{row_num}/{total_rows}] Updated FAA MASTER: N-Number={row.get('N-NUMBER')}")
                        else:
                            stats['master']['skipped'] += 1
                            stats['total_skipped'] += 1
                            logger.info(f"[SKIP] [{row_num}/{total_rows}] Skipped FAA MASTER: N-Number={row.get('N-NUMBER')}")
                        stats['master']['records'] += 1
                    
                    logger.info(f"FAA MASTER processing complete: {stats['master']['inserted']} inserted, {stats['master']['updated']} updated, {stats['master']['skipped']} skipped")
            except Exception as e:
                logger.error(f"Error loading MASTER.txt: {e}", exc_info=True)
        else:
            logger.warning(f"FAA MASTER.txt not found at {master_file}")

        # Load DEALER.txt
        dealer_file = extracted_path / "DEALER.txt"
        if dealer_file.exists():
            logger.info(f"Loading FAA DEALER data from {dealer_file}")
            try:
                with open(dealer_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        logger.info(f"DEALER field names detected: {list(reader.fieldnames)[:10]}")
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in DEALER.txt (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached FAA DEALER limit ({limit}), stopping")
                            break
                        
                        # Log progress for every record
                        logger.info(f"Processing FAA DEALER row {i + 1}/{total_to_process}: Name={row.get('NAME', 'N/A')[:30]}")
                        
                        self._store_faa_dealer(row, ingestion_date)
                        stats['dealer']['records'] += 1
                        stats['dealer']['inserted'] += 1
                        stats['total_inserted'] += 1
                        logger.info(f"[OK] [{i + 1}/{total_to_process}] Stored FAA DEALER: Name={row.get('NAME', 'N/A')[:50]}")
                        # Also store raw data
                        self._store_faa_csv_row('faa', 'dealer', ingestion_date, dealer_file, row)
                    
                    logger.info(f"FAA DEALER processing complete: {stats['dealer']['inserted']} inserted")
            except Exception as e:
                logger.error(f"Error loading DEALER.txt: {e}", exc_info=True)

        # Load DEREG.txt
        dereg_file = extracted_path / "DEREG.txt"
        if dereg_file.exists():
            logger.info(f"Loading FAA DEREG data from {dereg_file}")
            try:
                with open(dereg_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        logger.info(f"DEREG field names detected: {list(reader.fieldnames)[:10]}")
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in DEREG.txt (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached FAA DEREG limit ({limit}), stopping")
                            break
                        
                        # Log progress for every record
                        logger.info(f"Processing FAA DEREG row {i + 1}/{total_to_process}: Serial={row.get('SERIAL-NUMBER', 'N/A')}, N-Number={row.get('N-NUMBER', 'N/A')}")
                        
                        result = self._store_faa_deregistered(row, ingestion_date)
                        stats['dereg']['records'] += 1
                        if result:
                            stats['dereg']['inserted'] += 1
                            stats['total_inserted'] += 1
                            logger.info(f"[OK] [{i + 1}/{total_to_process}] Inserted FAA DEREG: Serial={row.get('SERIAL-NUMBER')}, N-Number={row.get('N-NUMBER')}")
                        else:
                            stats['dereg']['skipped'] += 1
                            stats['total_skipped'] += 1
                            logger.info(f"[SKIP] [{i + 1}/{total_to_process}] Skipped FAA DEREG: Serial={row.get('SERIAL-NUMBER')}, N-Number={row.get('N-NUMBER')}")
                        # Also store raw data
                        self._store_faa_csv_row('faa', 'dereg', ingestion_date, dereg_file, row)
                    
                    logger.info(f"FAA DEREG processing complete: {stats['dereg']['inserted']} inserted, {stats['dereg']['skipped']} skipped")
            except Exception as e:
                logger.error(f"Error loading DEREG.txt: {e}", exc_info=True)

        # Load ENGINE.txt
        engine_file = extracted_path / "ENGINE.txt"
        if engine_file.exists():
            logger.info(f"Loading FAA ENGINE data from {engine_file}")
            try:
                with open(engine_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        logger.info(f"ENGINE field names detected: {list(reader.fieldnames)[:10]}")
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in ENGINE.txt (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached FAA ENGINE limit ({limit}), stopping")
                            break
                        
                        # Log progress for every record
                        logger.info(f"Processing FAA ENGINE row {i + 1}/{total_to_process}: Code={row.get('CODE', 'N/A')}")
                        
                        self._store_faa_engine_reference(row, ingestion_date)
                        stats['engine']['records'] += 1
                        stats['engine']['inserted'] += 1
                        stats['total_inserted'] += 1
                        logger.info(f"[OK] [{i + 1}/{total_to_process}] Stored FAA ENGINE: Code={row.get('CODE', 'N/A')}")
                        # Also store raw data
                        self._store_faa_csv_row('faa', 'engine', ingestion_date, engine_file, row)
                    
                    logger.info(f"FAA ENGINE processing complete: {stats['engine']['inserted']} inserted")
            except Exception as e:
                logger.error(f"Error loading ENGINE.txt: {e}", exc_info=True)

        # Load DOCINDEX.txt
        docindex_file = extracted_path / "DOCINDEX.txt"
        if docindex_file.exists():
            logger.info(f"Loading FAA DOCINDEX data from {docindex_file}")
            try:
                with open(docindex_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        logger.info(f"DOCINDEX field names detected: {list(reader.fieldnames)[:10]}")
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in DOCINDEX.txt (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached FAA DOCINDEX limit ({limit}), stopping")
                            break
                        
                        # Log progress for every record
                        logger.info(f"Processing FAA DOCINDEX row {i + 1}/{total_to_process}: Serial-ID={row.get('SERIAL-ID', 'N/A')}, DOC-ID={row.get('DOC-ID', 'N/A')}")
                        
                        result = self._store_faa_document_index(row, ingestion_date)
                        stats['docindex']['records'] += 1
                        if result:
                            stats['docindex']['inserted'] += 1
                            stats['total_inserted'] += 1
                            logger.info(f"[OK] [{i + 1}/{total_to_process}] Inserted FAA DOCINDEX: Serial-ID={row.get('SERIAL-ID')}, DOC-ID={row.get('DOC-ID')}")
                        else:
                            stats['docindex']['skipped'] += 1
                            stats['total_skipped'] += 1
                            logger.info(f"[SKIP] [{i + 1}/{total_to_process}] Skipped FAA DOCINDEX: Serial-ID={row.get('SERIAL-ID')}, DOC-ID={row.get('DOC-ID')}")
                        # Also store raw data
                        self._store_faa_csv_row('faa', 'docindex', ingestion_date, docindex_file, row)
                    
                    logger.info(f"FAA DOCINDEX processing complete: {stats['docindex']['inserted']} inserted, {stats['docindex']['skipped']} skipped")
            except Exception as e:
                logger.error(f"Error loading DOCINDEX.txt: {e}", exc_info=True)

        # Load RESERVED.txt
        reserved_file = extracted_path / "RESERVED.txt"
        if reserved_file.exists():
            logger.info(f"Loading FAA RESERVED data from {reserved_file}")
            try:
                with open(reserved_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        logger.info(f"RESERVED field names detected: {list(reader.fieldnames)[:10]}")
                    rows = list(reader)
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows in RESERVED.txt (processing up to {limit if limit else 'all'})")
                    
                    total_to_process = min(limit, total_rows) if limit else total_rows
                    for i, row in enumerate(rows):
                        if limit and i >= limit:
                            logger.info(f"Reached FAA RESERVED limit ({limit}), stopping")
                            break
                        
                        # Log progress for every record
                        logger.info(f"Processing FAA RESERVED row {i + 1}/{total_to_process}: N-Number={row.get('N-NUMBER', 'N/A')}")
                        
                        self._store_faa_reserved(row, ingestion_date)
                        stats['reserved']['records'] += 1
                        stats['reserved']['inserted'] += 1
                        stats['total_inserted'] += 1
                        logger.info(f"[OK] [{i + 1}/{total_to_process}] Stored FAA RESERVED: N-Number={row.get('N-NUMBER', 'N/A')}")
                        # Also store raw data
                        self._store_faa_csv_row('faa', 'reserved', ingestion_date, reserved_file, row)
                    
                    logger.info(f"FAA RESERVED processing complete: {stats['reserved']['inserted']} inserted")
            except Exception as e:
                logger.error(f"Error loading RESERVED.txt: {e}", exc_info=True)

        # Load PDF files (ardata.pdf and documentation)
        # Note: PDFs are always loaded (not limited) as they're complete documents
        pdf_files = list(base_path.glob("*.pdf")) + list(extracted_path.glob("*.pdf"))
        total_pdfs = len(pdf_files)
        if total_pdfs > 0:
            logger.info(f"Found {total_pdfs} PDF files to process")
            for i, pdf_file in enumerate(pdf_files):
                logger.info(f"Processing FAA PDF {i + 1}/{total_pdfs}: {pdf_file.name}")
                result = self._store_faa_pdf('faa', ingestion_date, pdf_file)
                if result == 'inserted':
                    stats['pdfs']['inserted'] += 1
                    stats['total_inserted'] += 1
                    logger.info(f"[OK] [{i + 1}/{total_pdfs}] Stored FAA PDF: {pdf_file.name}")
                else:
                    stats['pdfs']['skipped'] += 1
                    stats['total_skipped'] += 1
                    logger.info(f"[SKIP] [{i + 1}/{total_pdfs}] Skipped FAA PDF: {pdf_file.name}")
                stats['pdfs']['files'] += 1
            
            logger.info(f"FAA PDF processing complete: {stats['pdfs']['inserted']} inserted, {stats['pdfs']['skipped']} skipped")

        return stats

    def _upsert_faa_aircraft(self, row: Dict, ingestion_date: date) -> str:
        """Upsert FAA aircraft registration data.

        Args:
            row: CSV row as dict from MASTER.txt
            ingestion_date: Date when data was scraped

        Returns:
            'inserted', 'updated', or 'skipped'
        """
        # Extract key fields (handle case variations)
        n_number = row.get('N-NUMBER', row.get('n-number', row.get('n_number', ''))).strip()
        registration = self._clean_registration(n_number)
        serial_number = row.get('SERIAL NUMBER', row.get('serial number', row.get('serial_number', ''))).strip()
        
        # Skip if no identifiers at all
        if not n_number and not serial_number:
            return 'skipped'

        # Parse manufacturer/model code (decoded from ACFTREF.txt if available)
        mfr_mdl_code = row.get('MFR MDL CODE', row.get('mfr mdl code', row.get('mfr_mdl_code', ''))).strip()
        manufacturer = row.get('_decoded_manufacturer')  # Set by load_faa_data if ACFTREF loaded
        model = row.get('_decoded_model')  # Set by load_faa_data if ACFTREF loaded
        
        year_mfr = self._parse_int(row.get('YEAR MFR', row.get('year mfr', row.get('year_mfr', ''))).strip())
        
        # Parse dates (handle case variations)
        cert_issue_date = self._parse_date(row.get('CERT ISSUE DATE', row.get('cert issue date', row.get('cert_issue_date', ''))).strip())
        last_action_date = self._parse_date(row.get('LAST ACTION DATE', row.get('last action date', row.get('last_action_date', ''))).strip())
        expiration_date = self._parse_date(row.get('EXPIRATION DATE', row.get('expiration date', row.get('expiration_date', ''))).strip())
        
        # Status
        status_code = row.get('STATUS CODE', row.get('status code', row.get('status_code', ''))).strip()
        aircraft_status = 'Active' if status_code == 'V' else 'Inactive'
        
        # Location (handle case variations)
        city = row.get('CITY', row.get('city', '')).strip()
        state = row.get('STATE', row.get('state', '')).strip()
        country = row.get('COUNTRY', row.get('country', '')).strip() or 'US'
        location = f"{city}, {state}" if city and state else city or state or None
        
        # Get or create aircraft (only if we have at least serial_number or registration)
        aircraft_id = None
        if serial_number or registration:
            aircraft_id = self._get_or_create_aircraft(
                serial_number if serial_number else None,
                registration if registration else None,
                manufacturer,
                model
            )
        
        # If we can't create aircraft and have no identifier, skip
        if not aircraft_id and not serial_number and not registration:
            return 'skipped'

        # Parse additional FAA fields (handle case variations)
        air_worth_date = self._parse_date(row.get('AIR WORTH DATE', row.get('air worth date', row.get('air_worth_date', ''))).strip())
        type_registrant = self._parse_int(row.get('TYPE REGISTRANT', row.get('type registrant', row.get('type_registrant', ''))).strip())
        mode_s_code = row.get('MODE S CODE', row.get('mode s code', row.get('mode_s_code', ''))).strip()
        mode_s_code_hex = row.get('MODE S CODE HEX', row.get('mode s code hex', row.get('mode_s_code_hex', ''))).strip()
        type_aircraft = row.get('TYPE AIRCRAFT', row.get('type aircraft', row.get('type_aircraft', ''))).strip()
        type_engine = row.get('TYPE ENGINE', row.get('type engine', row.get('type_engine', ''))).strip()
        certification = row.get('CERTIFICATION', row.get('certification', '')).strip()
        fract_owner = row.get('FRACT OWNER', row.get('fract owner', row.get('fract_owner', ''))).strip()
        unique_id = row.get('UNIQUE ID', row.get('unique id', row.get('unique_id', ''))).strip()
        kit_field = row.get('KIT MFR, KIT MODEL', row.get('kit mfr, kit model', row.get('kit_mfr_kit_model', ''))).strip()
        kit_mfr = kit_field.split(',')[0].strip() if kit_field else None
        kit_model = ','.join(kit_field.split(',')[1:]).strip() if kit_field and ',' in kit_field else None
        
        # Other names (1-5) - handle case variations
        other_names = []
        for i in range(1, 6):
            other_name = row.get(f'OTHER NAMES({i})', row.get(f'other names({i})', row.get(f'other_names_{i}', ''))).strip()
            if other_name:
                other_names.append(other_name)
        
        # Update aircraft record with FAA data
        if aircraft_id:
            update_query = """
                UPDATE aircraft
                SET registration_number = COALESCE(%s, registration_number),
                    manufacturer = COALESCE(%s, manufacturer),
                    model = COALESCE(%s, model),
                    manufacturer_year = COALESCE(%s, manufacturer_year),
                    aircraft_status = COALESCE(%s, aircraft_status),
                    registration_country = COALESCE(%s, registration_country),
                    based_country = COALESCE(%s, based_country),
                    airworthiness_date = COALESCE(%s, airworthiness_date),
                    certification = COALESCE(%s, certification),
                    type_aircraft = COALESCE(%s, type_aircraft),
                    type_engine = COALESCE(%s, type_engine),
                    mode_s_code = COALESCE(%s, mode_s_code),
                    mode_s_code_hex = COALESCE(%s, mode_s_code_hex),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """
            self.db.execute_update(
                update_query,
                (
                    registration if registration else None,
                    manufacturer, model, year_mfr, aircraft_status,
                    country, country, air_worth_date, certification,
                    type_aircraft, type_engine, mode_s_code, mode_s_code_hex,
                    aircraft_id
                )
            )
        
        # Store in faa_registrations table (only if we have n_number or serial_number)
        # Use original n_number (before cleaning) or serial_number as fallback
        effective_n_number = n_number if n_number else (serial_number if serial_number else None)
        if effective_n_number:
            self._store_faa_registration(
                row, aircraft_id, ingestion_date, effective_n_number, serial_number,
                mfr_mdl_code, manufacturer, model, year_mfr, type_registrant,
                cert_issue_date, last_action_date, expiration_date, air_worth_date,
                status_code, type_aircraft, type_engine, certification,
                mode_s_code, mode_s_code_hex, fract_owner, unique_id,
                kit_mfr, kit_model, other_names, city, state, country
            )

        # Store raw FAA data in raw_data_store
        raw_data = {
            'n_number': registration,
            'serial_number': serial_number,
            'mfr_mdl_code': mfr_mdl_code,
            'year_mfr': year_mfr,
            'status_code': status_code,
            'cert_issue_date': str(cert_issue_date) if cert_issue_date else None,
            'last_action_date': str(last_action_date) if last_action_date else None,
            'expiration_date': str(expiration_date) if expiration_date else None,
            'owner_name': row.get('NAME', '').strip(),
            'city': city,
            'state': state,
            'country': country,
            **row  # Include all original fields
        }
        
        # Store in raw_data_store
        try:
            # Check if already exists
            check_query = """
                SELECT id FROM raw_data_store
                WHERE source_platform = %s AND source_type = %s
                  AND ingestion_date = %s AND listing_url = %s
                LIMIT 1
            """
            exists = self.db.execute_query(
                check_query,
                ('faa', 'master', ingestion_date, registration or serial_number)
            )
            if not exists:
                insert_query = """
                    INSERT INTO raw_data_store (
                        source_platform, source_type, ingestion_date,
                        file_path, listing_url, raw_data
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        'faa', 'master', ingestion_date,
                        f"faa/{ingestion_date.strftime('%Y-%m-%d')}/extracted/MASTER.txt",
                        registration or serial_number,
                        json.dumps(raw_data)
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to store FAA raw data: {e}")

        return 'updated' if aircraft_id else 'inserted'

    def _store_faa_registration(
        self, row: Dict, aircraft_id: Optional[str], ingestion_date: date,
        n_number: Optional[str], serial_number: str, mfr_mdl_code: str,
        manufacturer: Optional[str], model: Optional[str], year_mfr: Optional[int],
        type_registrant: Optional[int], cert_issue_date: Optional[date],
        last_action_date: Optional[date], expiration_date: Optional[date],
        air_worth_date: Optional[date], status_code: str,
        type_aircraft: Optional[str], type_engine: Optional[str],
        certification: Optional[str], mode_s_code: Optional[str],
        mode_s_code_hex: Optional[str], fract_owner: Optional[str],
        unique_id: Optional[str], kit_mfr: Optional[str], kit_model: Optional[str],
        other_names: List[str], city: Optional[str], state: Optional[str], country: str
    ) -> None:
        """Store FAA registration in faa_registrations table."""
        try:
            # Skip if no n_number and no serial_number
            if not n_number and not serial_number:
                return
            
            # Use n_number if available, otherwise use serial_number (but n_number can be NULL in DB)
            # For lookup, use the provided n_number (which may be serial_number fallback from caller)
            effective_n_number = n_number if n_number else None  # Keep as None if not provided, schema allows NULL
            
            # Check if already exists (match by n_number OR by serial_number if n_number is NULL)
            if effective_n_number:
                check_query = """
                    SELECT id FROM faa_registrations
                    WHERE n_number = %s AND ingestion_date = %s
                    LIMIT 1
                """
                exists = self.db.execute_query(check_query, (effective_n_number, ingestion_date))
            elif serial_number:
                check_query = """
                    SELECT id FROM faa_registrations
                    WHERE n_number IS NULL AND serial_number = %s AND ingestion_date = %s
                    LIMIT 1
                """
                exists = self.db.execute_query(check_query, (serial_number, ingestion_date))
            else:
                return  # Can't store without at least one identifier
            
            # Extract registrant info (handle case variations)
            registrant_name = row.get('NAME', row.get('name', '')).strip()
            street = row.get('STREET', row.get('street', '')).strip()
            street2 = row.get('STREET2', row.get('street2', '')).strip()
            zip_code = row.get('ZIP CODE', row.get('zip code', row.get('zip_code', ''))).strip()
            region = row.get('REGION', row.get('region', '')).strip()
            county = row.get('COUNTY', row.get('county', '')).strip()
            eng_mfr_mdl = row.get('ENG MFR MDL', row.get('eng mfr mdl', row.get('eng_mfr_mdl', ''))).strip()
            
            if exists:
                # Update existing
                update_query = """
                    UPDATE faa_registrations SET
                        aircraft_id = %s, n_number = %s, serial_number = %s, mfr_mdl_code = %s,
                        eng_mfr_mdl = %s, year_mfr = %s, type_registrant = %s,
                        registrant_name = %s, street = %s, street2 = %s,
                        city = %s, state = %s, zip_code = %s, region = %s,
                        county = %s, country = %s, last_action_date = %s,
                        cert_issue_date = %s, certification = %s,
                        expiration_date = %s, air_worth_date = %s,
                        type_aircraft = %s, type_engine = %s, status_code = %s,
                        mode_s_code = %s, mode_s_code_hex = %s,
                        fract_owner = %s, unique_id = %s, kit_mfr = %s,
                        kit_model = %s, other_names = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                self.db.execute_update(
                    update_query,
                    (
                        aircraft_id, effective_n_number, serial_number, mfr_mdl_code, eng_mfr_mdl,
                        year_mfr, type_registrant, registrant_name, street, street2,
                        city, state, zip_code, region, county, country,
                        last_action_date, cert_issue_date, certification,
                        expiration_date, air_worth_date, type_aircraft, type_engine,
                        status_code, mode_s_code, mode_s_code_hex,
                        fract_owner, unique_id, kit_mfr, kit_model,
                        json.dumps(other_names) if other_names else None,
                        exists[0]['id']
                    )
                )
            else:
                # Insert new (use effective_n_number which can't be None)
                insert_query = """
                    INSERT INTO faa_registrations (
                        aircraft_id, n_number, serial_number, mfr_mdl_code,
                        eng_mfr_mdl, year_mfr, type_registrant, registrant_name,
                        street, street2, city, state, zip_code, region, county, country,
                        last_action_date, cert_issue_date, certification,
                        expiration_date, air_worth_date, type_aircraft, type_engine,
                        status_code, mode_s_code, mode_s_code_hex,
                        fract_owner, unique_id, kit_mfr, kit_model, other_names,
                        ingestion_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db.execute_update(
                    insert_query,
                    (
                        aircraft_id, n_number, serial_number, mfr_mdl_code, eng_mfr_mdl,
                        year_mfr, type_registrant, registrant_name, street, street2,
                        city, state, zip_code, region, county, country,
                        last_action_date, cert_issue_date, certification,
                        expiration_date, air_worth_date, type_aircraft, type_engine,
                        status_code, mode_s_code, mode_s_code_hex,
                        fract_owner, unique_id, kit_mfr, kit_model,
                        json.dumps(other_names) if other_names else None,
                        ingestion_date
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to store FAA registration: {e}")
    
    def _store_faa_aircraft_reference(self, row: Dict, ingestion_date: date) -> None:
        """Store ACFTREF row in faa_aircraft_reference table."""
        try:
            # Handle BOM character in CODE field name
            code = row.get('CODE', '').strip()
            if not code:
                code = row.get('\ufeffCODE', '').strip()  # Try with BOM character
            if not code:
                code = row.get('code', '').strip() or row.get('Code', '').strip()
            if not code:
                return
            
            # Check if exists
            check_query = "SELECT code FROM faa_aircraft_reference WHERE code = %s LIMIT 1"
            exists = self.db.execute_query(check_query, (code,))
            
            # Extract fields with case variations
            mfr = row.get('MFR', row.get('mfr', '')).strip()
            model = row.get('MODEL', row.get('model', '')).strip()
            type_acft = row.get('TYPE-ACFT', row.get('type-acft', row.get('TYPE_ACFT', ''))).strip()
            type_eng = row.get('TYPE-ENG', row.get('type-eng', row.get('TYPE_ENG', ''))).strip()
            ac_cat = row.get('AC-CAT', row.get('ac-cat', row.get('AC_CAT', ''))).strip()
            build_cert_ind = row.get('BUILD-CERT-IND', row.get('build-cert-ind', row.get('BUILD_CERT_IND', ''))).strip()
            no_eng = row.get('NO-ENG', row.get('no-eng', row.get('NO_ENG', '')))
            no_seats = row.get('NO-SEATS', row.get('no-seats', row.get('NO_SEATS', '')))
            ac_weight = row.get('AC-WEIGHT', row.get('ac-weight', row.get('AC_WEIGHT', ''))).strip()
            speed = row.get('SPEED', row.get('speed', '')).strip()
            tc_data_sheet = row.get('TC-DATA-SHEET', row.get('tc-data-sheet', row.get('TC_DATA_SHEET', ''))).strip()
            tc_data_holder = row.get('TC-DATA-HOLDER', row.get('tc-data-holder', row.get('TC_DATA_HOLDER', ''))).strip()
            
            if exists:
                # Update
                update_query = """
                    UPDATE faa_aircraft_reference SET
                        manufacturer = %s, model = %s, type_aircraft = %s,
                        type_engine = %s, ac_category = %s, build_cert_ind = %s,
                        number_of_engines = %s, number_of_seats = %s,
                        aircraft_weight = %s, speed = %s, tc_data_sheet = %s,
                        tc_data_holder = %s, ingestion_date = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE code = %s
                """
                self.db.execute_update(
                    update_query,
                    (
                        mfr, model, type_acft, type_eng, ac_cat, build_cert_ind,
                        self._parse_int(no_eng), self._parse_int(no_seats),
                        ac_weight, speed, tc_data_sheet, tc_data_holder,
                        ingestion_date, code
                    )
                )
            else:
                # Insert
                insert_query = """
                    INSERT INTO faa_aircraft_reference (
                        code, manufacturer, model, type_aircraft, type_engine,
                        ac_category, build_cert_ind, number_of_engines, number_of_seats,
                        aircraft_weight, speed, tc_data_sheet, tc_data_holder, ingestion_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        code, mfr, model, type_acft, type_eng, ac_cat, build_cert_ind,
                        self._parse_int(no_eng), self._parse_int(no_seats),
                        ac_weight, speed, tc_data_sheet, tc_data_holder, ingestion_date
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to store FAA aircraft reference: {e}")
    
    def _store_faa_engine_reference(self, row: Dict, ingestion_date: date) -> None:
        """Store ENGINE row in faa_engine_reference table."""
        try:
            # Try multiple field name variations (handle BOM, case variations)
            code = row.get('CODE', '').strip() or row.get('\ufeffCODE', '').strip() or row.get('code', '').strip()
            if not code:
                logger.debug(f"ENGINE row missing CODE field. Available keys: {list(row.keys())[:5]}")
                return
            
            # Check if exists
            check_query = "SELECT code FROM faa_engine_reference WHERE code = %s LIMIT 1"
            exists = self.db.execute_query(check_query, (code,))
            
            if exists:
                # Update
                update_query = """
                    UPDATE faa_engine_reference SET
                        manufacturer = %s, model = %s, type = %s,
                        horsepower = %s, thrust = %s, ingestion_date = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE code = %s
                """
                self.db.execute_update(
                    update_query,
                    (
                        row.get('MFR', '').strip(), row.get('MODEL', '').strip(),
                        row.get('TYPE', '').strip(), self._parse_int(row.get('HORSEPOWER')),
                        self._parse_int(row.get('THRUST')), ingestion_date, code
                    )
                )
            else:
                # Insert
                insert_query = """
                    INSERT INTO faa_engine_reference (
                        code, manufacturer, model, type, horsepower, thrust, ingestion_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        code, row.get('MFR', '').strip(), row.get('MODEL', '').strip(),
                        row.get('TYPE', '').strip(), self._parse_int(row.get('HORSEPOWER')),
                        self._parse_int(row.get('THRUST')), ingestion_date
                    )
                )
        except Exception as e:
            logger.error(f"Failed to store FAA engine reference (code={row.get('CODE', 'N/A')}): {e}", exc_info=True)
    
    def _store_faa_dealer(self, row: Dict, ingestion_date: date) -> None:
        """Store DEALER row in faa_dealers table."""
        try:
            certificate_number = row.get('CERTIFICATE-NUMBER', '').strip()
            if not certificate_number:
                return
            
            # Collect other names (1-25)
            other_names = []
            for i in range(1, 26):
                other_name = row.get(f'OTHER-NAMES-{i}', '').strip()
                if other_name:
                    other_names.append(other_name)
            
            # Check if exists
            check_query = """
                SELECT id FROM faa_dealers
                WHERE certificate_number = %s AND ingestion_date = %s
                LIMIT 1
            """
            exists = self.db.execute_query(check_query, (certificate_number, ingestion_date))
            
            if exists:
                # Update
                update_query = """
                    UPDATE faa_dealers SET
                        ownership = %s, certificate_date = %s, expiration_date = %s,
                        expiration_flag = %s, certificate_issue_count = %s,
                        name = %s, street = %s, street2 = %s, city = %s,
                        state = %s, zip_code = %s, other_names = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                self.db.execute_update(
                    update_query,
                    (
                        self._parse_int(row.get('OWNERSHIP')),
                        self._parse_date(row.get('CERTIFICATE-DATE')),
                        self._parse_date(row.get('EXPIRATION-DATE')),
                        row.get('EXPIRATION-FLAG', '').strip(),
                        self._parse_int(row.get('CERTIFICATE-ISSUE-COUNT')),
                        row.get('NAME', '').strip(), row.get('STREET', '').strip(),
                        row.get('STREET2', '').strip(), row.get('CITY', '').strip(),
                        row.get('STATE-ABBREV', '').strip(), row.get('ZIP-CODE', '').strip(),
                        json.dumps(other_names) if other_names else None,
                        exists[0]['id']
                    )
                )
            else:
                # Insert
                insert_query = """
                    INSERT INTO faa_dealers (
                        certificate_number, ownership, certificate_date, expiration_date,
                        expiration_flag, certificate_issue_count, name, street, street2,
                        city, state, zip_code, other_names, ingestion_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        certificate_number, self._parse_int(row.get('OWNERSHIP')),
                        self._parse_date(row.get('CERTIFICATE-DATE')),
                        self._parse_date(row.get('EXPIRATION-DATE')),
                        row.get('EXPIRATION-FLAG', '').strip(),
                        self._parse_int(row.get('CERTIFICATE-ISSUE-COUNT')),
                        row.get('NAME', '').strip(), row.get('STREET', '').strip(),
                        row.get('STREET2', '').strip(), row.get('CITY', '').strip(),
                        row.get('STATE-ABBREV', '').strip(), row.get('ZIP-CODE', '').strip(),
                        json.dumps(other_names) if other_names else None, ingestion_date
                    )
                )
        except Exception as e:
            logger.error(f"Failed to store FAA dealer (cert={certificate_number}): {e}", exc_info=True)
    
    def _store_faa_deregistered(self, row: Dict, ingestion_date: date) -> bool:
        """Store DEREG row in faa_deregistered table.
        
        Returns:
            True if successfully inserted/updated, False otherwise
        """
        try:
            # Try multiple field name variations
            n_number = (row.get('N-NUMBER', '').strip() or 
                       row.get('N_NUMBER', '').strip() or
                       row.get('n-number', '').strip())
            
            # SERIAL-NUMBER is the unique identifier (not N-NUMBER which can repeat)
            serial_number = row.get('SERIAL-NUMBER', '').strip()
            if not serial_number:
                logger.warning(f"DEREG row missing SERIAL-NUMBER field. Available keys: {list(row.keys())[:10]}")
                return False
            
            # Collect other names (1-5)
            other_names = []
            for i in range(1, 6):
                other_name = row.get(f'OTHER-NAMES({i})', '').strip()
                if other_name:
                    other_names.append(other_name)
            
            # Check if exists using serial_number (the unique identifier)
            check_query = """
                SELECT id FROM faa_deregistered
                WHERE serial_number = %s AND ingestion_date = %s
                LIMIT 1
            """
            exists = self.db.execute_query(check_query, (serial_number, ingestion_date))
            
            if not exists:
                # Parse kit mfr and model from combined field
                kit_field = row.get('KIT MFR, KIT MODEL', '').strip()
                kit_mfr = None
                kit_model = None
                if kit_field and ',' in kit_field:
                    parts = kit_field.split(',', 1)
                    kit_mfr = parts[0].strip() if parts[0] else None
                    kit_model = parts[1].strip() if len(parts) > 1 and parts[1] else None
                
                # Insert (deregistered records are append-only)
                insert_query = """
                    INSERT INTO faa_deregistered (
                        n_number, serial_number, mfr_mdl_code, status_code, name,
                        street_mail, street2_mail, city_mail, state_mail, zip_code_mail,
                        county_mail, country_mail, street_physical, street2_physical,
                        city_physical, state_physical, zip_code_physical, county_physical,
                        country_physical, eng_mfr_mdl, year_mfr, certification, region,
                        air_worth_date, cancel_date, mode_s_code, indicator_group,
                        exp_country, last_act_date, cert_issue_date, other_names,
                        kit_mfr, kit_model, mode_s_code_hex, ingestion_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db.execute_update(
                    insert_query,
                    (
                        n_number,
                        serial_number,  # Use extracted variable (unique identifier)
                        row.get('MFR-MDL-CODE', '').strip(),
                        row.get('STATUS-CODE', '').strip(),
                        row.get('NAME', '').strip(),
                        row.get('STREET-MAIL', '').strip(),
                        row.get('STREET2-MAIL', '').strip(),
                        row.get('CITY-MAIL', '').strip(),
                        row.get('STATE-ABBREV-MAIL', '').strip(),
                        row.get('ZIP-CODE-MAIL', '').strip(),
                        row.get('COUNTY-MAIL', '').strip(),
                        row.get('COUNTRY-MAIL', '').strip(),
                        row.get('STREET-PHYSICAL', '').strip(),
                        row.get('STREET2-PHYSICAL', '').strip(),
                        row.get('CITY-PHYSICAL', '').strip(),
                        row.get('STATE-ABBREV-PHYSICAL', '').strip(),
                        row.get('ZIP-CODE-PHYSICAL', '').strip(),
                        row.get('COUNTY-PHYSICAL', '').strip(),
                        row.get('COUNTRY-PHYSICAL', '').strip(),
                        row.get('ENG-MFR-MDL', '').strip(),
                        self._parse_int(row.get('YEAR-MFR')),
                        row.get('CERTIFICATION', '').strip(),
                        row.get('REGION', '').strip(),
                        self._parse_date(row.get('AIR-WORTH-DATE')),
                        self._parse_date(row.get('CANCEL-DATE')),
                        row.get('MODE-S-CODE', '').strip(),
                        row.get('INDICATOR-GROUP', '').strip(),
                        row.get('EXP-COUNTRY', '').strip(),
                        self._parse_date(row.get('LAST-ACT-DATE')),
                        self._parse_date(row.get('CERT-ISSUE-DATE')),
                        json.dumps(other_names) if other_names else None,
                        kit_mfr,
                        kit_model,
                        row.get('MODE S CODE HEX', '').strip(),
                        ingestion_date
                    )
                )
                logger.debug(f"Successfully inserted FAA deregistered: serial_number={serial_number}, n_number={n_number or 'N/A'}")
                return True
            else:
                logger.debug(f"FAA deregistered record already exists: serial_number={serial_number}, ingestion_date={ingestion_date}")
                return False
        except Exception as e:
            logger.error(f"Failed to store FAA deregistered (serial_number={serial_number if 'serial_number' in locals() else 'N/A'}, n_number={n_number if 'n_number' in locals() else 'N/A'}): {e}", exc_info=True)
            return False
    
    def _store_faa_document_index(self, row: Dict, ingestion_date: date) -> bool:
        """Store DOCINDEX row in faa_document_index table.
        
        Returns:
            True if successfully inserted, False otherwise
        """
        try:
            # Try multiple field name variations for DOC-ID
            doc_id = (row.get('DOC-ID', '').strip() or 
                     row.get('DOC_ID', '').strip() or
                     row.get('doc-id', '').strip())
            
            # SERIAL-ID is the unique identifier (DOC-ID is often empty)
            serial_id = row.get('SERIAL-ID', '').strip()
            if not serial_id:
                logger.warning(f"DOCINDEX row missing SERIAL-ID field. Available keys: {list(row.keys())[:10]}")
                return False
            
            # Check if exists using serial_id (the unique identifier)
            # Use combination of serial_id + doc_id + ingestion_date for uniqueness
            # (since same serial_id might appear with different doc_id on different dates)
            check_query = """
                SELECT id FROM faa_document_index
                WHERE serial_id = %s AND ingestion_date = %s
                LIMIT 1
            """
            exists = self.db.execute_query(check_query, (serial_id, ingestion_date))
            
            if exists:
                logger.debug(f"DOCINDEX record already exists: serial_id={serial_id}, ingestion_date={ingestion_date}")
                return False
            
            # Insert (record doesn't exist)
            insert_query = """
                INSERT INTO faa_document_index (
                    type_collateral, collateral, party, doc_id, drdate,
                    processing_date, corr_date, corr_id, serial_id, doc_type,
                    ingestion_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.db.execute_update(
                insert_query,
                    (
                        self._parse_int(row.get('TYPE-COLLATERAL')),
                        row.get('COLLATERAL', '').strip(), row.get('PARTY', '').strip(),
                        doc_id if doc_id else None,  # DOC-ID can be empty, store as NULL
                        self._parse_date(row.get('DRDATE')),
                        self._parse_date(row.get('PROCESSING-DATE')),
                        self._parse_date(row.get('CORR-DATE')), row.get('CORR-ID', '').strip(),
                        serial_id,  # Use extracted variable (unique identifier)
                        row.get('DOC-TYPE', '').strip(),
                        ingestion_date
                    )
            )
            logger.debug(f"Successfully inserted FAA document index: serial_id={serial_id}, doc_id={doc_id or 'N/A'}")
            return True
        except Exception as e:
            logger.error(f"Failed to store FAA document index (serial_id={serial_id if 'serial_id' in locals() else 'N/A'}, doc_id={doc_id if 'doc_id' in locals() else 'N/A'}): {e}", exc_info=True)
            return False
    
    def _store_faa_reserved(self, row: Dict, ingestion_date: date) -> None:
        """Store RESERVED row in faa_reserved table."""
        try:
            # Try multiple field name variations
            n_number = (row.get('N-NUMBER', '').strip() or 
                       row.get('N_NUMBER', '').strip() or
                       row.get('n-number', '').strip())
            if not n_number:
                logger.debug(f"RESERVED row missing N-NUMBER field. Available keys: {list(row.keys())[:5]}")
                return
            
            # Check if exists
            check_query = """
                SELECT id FROM faa_reserved
                WHERE n_number = %s AND ingestion_date = %s
                LIMIT 1
            """
            exists = self.db.execute_query(check_query, (n_number, ingestion_date))
            
            if not exists:
                # Insert
                insert_query = """
                    INSERT INTO faa_reserved (
                        n_number, registrant, street, street2, city, state, zip_code,
                        rsv_date, tr, exp_date, n_num_chg, purge_date, ingestion_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.db.execute_update(
                    insert_query,
                    (
                        n_number, row.get('REGISTRANT', '').strip(),
                        row.get('STREET', '').strip(), row.get('STREET2', '').strip(),
                        row.get('CITY', '').strip(), row.get('STATE', '').strip(),
                        row.get('ZIP CODE', '').strip(),
                        self._parse_date(row.get('RSV DATE')), row.get('TR', '').strip(),
                        self._parse_date(row.get('EXP DATE')), row.get('N-NUM-CHG', '').strip(),
                        self._parse_date(row.get('PURGE DATE')), ingestion_date
                    )
                )
        except Exception as e:
            logger.error(f"Failed to store FAA reserved (n_number={n_number}): {e}", exc_info=True)

    def _store_faa_csv_row(
        self, source_platform: str, source_type: str,
        ingestion_date: date, file_path: Path, row: Dict
    ) -> None:
        """Store a single FAA CSV row in raw_data_store table.
        
        Args:
            source_platform: Source name (always 'faa')
            source_type: Type (acftref, dealer, dereg, engine, docindex, reserved)
            ingestion_date: Date when data was scraped
            file_path: Path to source file
            row: CSV row as dict
        """
        try:
            # Check if already exists
            check_query = """
                SELECT id FROM raw_data_store
                WHERE source_platform = %s AND source_type = %s
                  AND ingestion_date = %s AND file_path = %s
                  AND raw_data = %s
                LIMIT 1
            """
            exists = self.db.execute_query(
                check_query,
                (source_platform, source_type, ingestion_date, str(file_path), json.dumps(row))
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
                    str(file_path), None, json.dumps(row)
                )
            )
        except Exception as e:
            logger.warning(f"Failed to store FAA CSV row: {e}", exc_info=True)
            # Don't fail the whole process if raw storage fails
    
    def _store_faa_pdf(
        self, source_platform: str, ingestion_date: date, pdf_file: Path
    ) -> str:
        """Store FAA PDF file in documents table.
        
        Args:
            source_platform: Source name (always 'faa')
            ingestion_date: Date when data was scraped
            pdf_file: Path to PDF file
            
        Returns:
            'inserted' or 'skipped'
        """
        if not PDF_AVAILABLE:
            logger.warning(f"PyPDF2 not available, skipping PDF: {pdf_file.name}")
            return 'skipped'
        
        try:
            # Calculate file hash for deduplication
            file_hash = hashlib.sha256(pdf_file.read_bytes()).hexdigest()
            file_size = pdf_file.stat().st_size
            
            # Check if already exists
            check_query = """
                SELECT id FROM documents
                WHERE source_platform = %s AND file_hash = %s
                LIMIT 1
            """
            exists = self.db.execute_query(check_query, (source_platform, file_hash))
            if exists:
                logger.debug(f"PDF already stored: {pdf_file.name}")
                return 'skipped'
            
            # Extract text from PDF
            extracted_text = ""
            metadata = {"pages": 0, "errors": []}
            
            try:
                with open(pdf_file, 'rb') as f:
                    pdf_reader = PyPDF2.PdfReader(f)
                    metadata["pages"] = len(pdf_reader.pages)
                    
                    for page_num, page in enumerate(pdf_reader.pages, 1):
                        try:
                            page_text = page.extract_text()
                            if page_text:
                                extracted_text += f"\n--- Page {page_num} ---\n{page_text}\n"
                        except Exception as e:
                            error_msg = f"Error extracting page {page_num}: {str(e)}"
                            metadata["errors"].append(error_msg)
                            logger.warning(f"{error_msg} in {pdf_file.name}")
            except Exception as e:
                error_msg = f"Error reading PDF: {str(e)}"
                metadata["errors"].append(error_msg)
                logger.warning(f"{error_msg} for {pdf_file.name}")
                # Continue with empty text if extraction fails
            
            # Insert into documents table
            insert_query = """
                INSERT INTO documents (
                    source_platform, document_type, file_path, file_name,
                    file_size, file_hash, extracted_text, metadata,
                    ingestion_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.db.execute_update(
                insert_query,
                (
                    source_platform,
                    'pdf',
                    str(pdf_file),
                    pdf_file.name,
                    file_size,
                    file_hash,
                    extracted_text,
                    json.dumps(metadata),
                    ingestion_date
                )
            )
            logger.info(f"Stored PDF: {pdf_file.name} ({metadata['pages']} pages, {len(extracted_text)} chars)")
            return 'inserted'
            
        except Exception as e:
            logger.error(f"Failed to store PDF {pdf_file.name}: {e}", exc_info=True)
            return 'skipped'
