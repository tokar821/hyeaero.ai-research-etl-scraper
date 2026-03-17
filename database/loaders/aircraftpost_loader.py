"""AircraftPost fleet extracted loader.

Loads store/raw/aircraftpost/<date>/fleet_extracted.json into PostgreSQL:
  aircraftpost_fleet_aircraft
and optionally raw_data_store (append-only).
"""

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from ..base_loader import BaseLoader

logger = logging.getLogger(__name__)


class AircraftPostLoader(BaseLoader):
    """Loader for AircraftPost fleet extracted aircraft records."""

    def load_aircraftpost_data(
        self,
        ingestion_date: date,
        limit: Optional[int] = None,
        store_raw: bool = True,
    ) -> Dict[str, int]:
        date_str = ingestion_date.strftime("%Y-%m-%d")
        base_path = self.store_base / "aircraftpost" / date_str
        extracted_file = base_path / "fleet_extracted.json"

        if not extracted_file.exists():
            logger.warning("AircraftPost extracted file not found: %s", extracted_file)
            return {"inserted": 0, "updated": 0, "skipped": 0, "raw_stored": 0}

        payload = json.loads(extracted_file.read_text(encoding="utf-8"))
        rows = payload.get("aircraft") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            logger.warning("AircraftPost extracted JSON invalid (no aircraft list)")
            return {"inserted": 0, "updated": 0, "skipped": 0, "raw_stored": 0}

        if limit:
            rows = rows[:limit]

        stats = {"inserted": 0, "updated": 0, "skipped": 0, "raw_stored": 0}

        for rec in rows:
            try:
                row = self._map_record(rec, ingestion_date, extracted_file)
                if not row:
                    stats["skipped"] += 1
                    continue

                existing_id = self._get_existing_id(row["aircraft_entity_id"], ingestion_date)
                if existing_id:
                    self._update_record(existing_id, row)
                    stats["updated"] += 1
                else:
                    self._insert_record(row)
                    stats["inserted"] += 1

                if store_raw:
                    self._store_raw(rec, ingestion_date, extracted_file)
                    stats["raw_stored"] += 1
            except Exception as e:
                logger.warning("AircraftPost record skipped (aircraft_entity_id=%r): %s", rec.get("aircraft_entity_id"), e)
                stats["skipped"] += 1

        logger.info(
            "AircraftPost load result: inserted=%s updated=%s skipped=%s raw_stored=%s",
            stats["inserted"], stats["updated"], stats["skipped"], stats.get("raw_stored", 0),
        )
        return stats

    def _map_record(self, rec: Dict[str, Any], ingestion_date: date, source_file: Path) -> Optional[Dict[str, Any]]:
        # Required source identifier
        ent = rec.get("aircraft_entity_id")
        if ent is None:
            return None
        try:
            aircraft_entity_id = int(ent)
        except Exception:
            return None

        fields = rec.get("fields") or {}
        sections = rec.get("sections") or {}

        def _val(key: str) -> Any:
            return fields.get(key)

        def _text(v: Any) -> Optional[str]:
            if v is None:
                return None
            if isinstance(v, dict):
                t = v.get("text")
                return str(t).strip() if t else None
            s = str(v).strip()
            return s or None

        def _href(v: Any) -> Optional[str]:
            if isinstance(v, dict):
                h = v.get("href")
                return str(h).strip() if h else None
            return None

        make_model_id = rec.get("make_model_id")
        try:
            make_model_id = int(make_model_id) if make_model_id is not None else None
        except Exception:
            make_model_id = None

        serial_number = self._truncate(str(rec.get("serial_number")).strip(), 100) if rec.get("serial_number") else None
        registration_number = self._clean_registration(_text(_val("Registration")))

        mfr_year = self._parse_int(_text(_val("MFR Year")))
        eis_date = self._truncate(_text(_val("EIS Date")), 20)
        country_code = self._truncate(_text(_val("Country")), 10)

        base_code = None
        base_v = _val("Base")
        if isinstance(base_v, dict):
            base_code = base_v.get("text")
        else:
            base_code = _text(base_v)
        base_code = self._truncate(base_code, 20) if base_code else None

        owner_url = _href(_val("Owner"))

        airframe_hours = self._parse_int(_text(_val("Airframe Hours")))
        total_landings = self._parse_int(_text(_val("Total Landings")))
        prior_owners = self._parse_int(_text(_val("Prior Owners")))
        passengers = self._parse_int(_text(_val("Passengers")))

        for_sale_v = _val("For Sale")
        for_sale = True if for_sale_v is True else False if for_sale_v is None else None

        engine_program_type = self._truncate(_text(_val("Engine Program Type")), 100)
        apu_program = self._truncate(_text(_val("APU Program")), 100)

        return {
            "make_model_id": make_model_id,
            "make_model_name": rec.get("make_model_name"),
            "aircraft_entity_id": aircraft_entity_id,
            "serial_number": serial_number,
            "registration_number": registration_number,
            "mfr_year": mfr_year,
            "eis_date": eis_date,
            "country_code": country_code,
            "base_code": base_code,
            "owner_url": owner_url,
            "airframe_hours": airframe_hours,
            "total_landings": total_landings,
            "prior_owners": prior_owners,
            "for_sale": for_sale,
            "passengers": passengers,
            "engine_program_type": engine_program_type,
            "apu_program": apu_program,
            "fields": json.dumps(fields, ensure_ascii=False),
            "sections": json.dumps(sections, ensure_ascii=False),
            "source_file_path": str(source_file),
            "ingestion_date": ingestion_date,
        }

    def _get_existing_id(self, aircraft_entity_id: int, ingestion_date: date) -> Optional[str]:
        q = """
            SELECT id FROM aircraftpost_fleet_aircraft
            WHERE aircraft_entity_id = %s AND ingestion_date = %s
            LIMIT 1
        """
        rows = self.db.execute_query(q, (aircraft_entity_id, ingestion_date))
        return rows[0]["id"] if rows else None

    def _insert_record(self, row: Dict[str, Any]) -> None:
        q = """
            INSERT INTO aircraftpost_fleet_aircraft (
                make_model_id, make_model_name, aircraft_entity_id,
                serial_number, registration_number,
                mfr_year, eis_date, country_code, base_code, owner_url,
                airframe_hours, total_landings, prior_owners, for_sale, passengers,
                engine_program_type, apu_program,
                fields, sections, source_file_path, ingestion_date
            ) VALUES (
                %(make_model_id)s, %(make_model_name)s, %(aircraft_entity_id)s,
                %(serial_number)s, %(registration_number)s,
                %(mfr_year)s, %(eis_date)s, %(country_code)s, %(base_code)s, %(owner_url)s,
                %(airframe_hours)s, %(total_landings)s, %(prior_owners)s, %(for_sale)s, %(passengers)s,
                %(engine_program_type)s, %(apu_program)s,
                %(fields)s::jsonb, %(sections)s::jsonb, %(source_file_path)s, %(ingestion_date)s
            )
        """
        self.db.execute_update(q, row)

    def _update_record(self, id_: str, row: Dict[str, Any]) -> None:
        q = """
            UPDATE aircraftpost_fleet_aircraft SET
                make_model_id = %(make_model_id)s,
                make_model_name = %(make_model_name)s,
                serial_number = %(serial_number)s,
                registration_number = %(registration_number)s,
                mfr_year = %(mfr_year)s,
                eis_date = %(eis_date)s,
                country_code = %(country_code)s,
                base_code = %(base_code)s,
                owner_url = %(owner_url)s,
                airframe_hours = %(airframe_hours)s,
                total_landings = %(total_landings)s,
                prior_owners = %(prior_owners)s,
                for_sale = %(for_sale)s,
                passengers = %(passengers)s,
                engine_program_type = %(engine_program_type)s,
                apu_program = %(apu_program)s,
                fields = %(fields)s::jsonb,
                sections = %(sections)s::jsonb,
                source_file_path = %(source_file_path)s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %(id)s
        """
        self.db.execute_update(q, {**row, "id": id_})

    def _store_raw(self, rec: Dict[str, Any], ingestion_date: date, file_path: Path) -> None:
        q = """
            INSERT INTO raw_data_store (source_platform, source_type, ingestion_date, file_path, raw_data)
            VALUES ('aircraftpost', 'fleet_aircraft', %s, %s, %s)
        """
        self.db.execute_update(q, (ingestion_date, str(file_path), json.dumps(rec, ensure_ascii=False)))

