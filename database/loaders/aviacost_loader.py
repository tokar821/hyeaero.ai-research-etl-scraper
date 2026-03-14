"""Aviacost aircraft details loader.

Loads Aviacost GetAircraftDetails JSON (store/raw/aviacost/<date>/aircraft_details.json)
into PostgreSQL aviacost_aircraft_details and optionally raw_data_store.
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..base_loader import BaseLoader

logger = logging.getLogger(__name__)


class AviacostLoader(BaseLoader):
    """Loader for Aviacost aircraft detail data."""

    def load_aviacost_data(
        self,
        ingestion_date: date,
        limit: Optional[int] = None,
        store_raw: bool = True,
    ) -> Dict[str, int]:
        """Load Aviacost aircraft details for a given date.

        Args:
            ingestion_date: Date of the scrape (folder name YYYY-MM-DD).
            limit: Optional max number of records to load (for testing).
            store_raw: If True, append each record to raw_data_store.

        Returns:
            Dict with inserted, updated, skipped, and (if store_raw) raw_stored.
        """
        date_str = ingestion_date.strftime("%Y-%m-%d")
        base_path = self.store_base / "aviacost" / date_str
        details_file = base_path / "aircraft_details.json"

        if not details_file.exists():
            logger.warning("Aviacost file not found: %s", details_file)
            return {"inserted": 0, "updated": 0, "skipped": 0, "raw_stored": 0}

        with open(details_file, "r", encoding="utf-8") as f:
            payload = json.load(f)

        data_list = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(data_list, list):
            logger.warning("Aviacost JSON has no 'data' list; got %s", type(payload))
            return {"inserted": 0, "updated": 0, "skipped": 0, "raw_stored": 0}

        if limit:
            data_list = data_list[:limit]

        stats = {"inserted": 0, "updated": 0, "skipped": 0, "raw_stored": 0}

        for item in data_list:
            try:
                row = self._map_record(item, ingestion_date)
                if not row:
                    stats["skipped"] += 1
                    continue

                existing = self._get_existing(row["aircraft_detail_id"])
                if existing:
                    self._update_record(existing["id"], row)
                    stats["updated"] += 1
                else:
                    self._insert_record(row)
                    stats["inserted"] += 1

                if store_raw:
                    self._store_raw(item, ingestion_date, details_file)
                    stats["raw_stored"] += 1
            except Exception as e:
                logger.warning("Aviacost record error (aircraft_detail_id=%s): %s", item.get("aircraftDetailId"), e)
                stats["skipped"] += 1

        logger.info("Aviacost load result: inserted=%s updated=%s skipped=%s raw_stored=%s",
                    stats["inserted"], stats["updated"], stats["skipped"], stats.get("raw_stored", 0))
        return stats

    def _map_record(self, item: Dict[str, Any], ingestion_date: date) -> Optional[Dict[str, Any]]:
        """Map API item to aviacost_aircraft_details columns."""
        detail_id = item.get("aircraftDetailId")
        if detail_id is None:
            return None

        mfr = item.get("aircraftManufacturer") or {}
        cat = item.get("aircraftCategory") or {}

        def dec(v):
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def int_(v):
            if v is None:
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        cost = dec(item.get("totalVariableCostPerHrsValue")) or dec(item.get("calc_TotalVariableCostPerHour"))

        last_updated = item.get("lastUpdatedOn")
        if isinstance(last_updated, str):
            try:
                last_updated = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
            except Exception:
                last_updated = None

        raw_data_val = item  # Keep as dict; will serialize for DB
        return {
            "aircraft_detail_id": int_(detail_id),
            "name": (item.get("name") or "").strip() or None,
            "description": (item.get("description") or "").strip() or None,
            "manufacturer_id": int_(mfr.get("aircraftManufacturerId")),
            "manufacturer_name": (mfr.get("name") or "").strip() or None,
            "category_id": int_(cat.get("aircraftCategoryId")),
            "category_name": (cat.get("category") or "").strip() or None,
            "avionics": (item.get("avionics") or "").strip() or None,
            "years_in_production": (item.get("yearsInProduction") or "").strip() or None,
            "average_pre_owned_price": dec(item.get("averagePreOwnedPrice")),
            "variable_cost_per_hour": cost,
            "fuel_gallons_per_hour": dec(item.get("fuelGallons")),
            "normal_cruise_speed_kts": dec(item.get("normalCruiseSpeedKts")),
            "seats_full_range_nm": dec(item.get("seatsFullRangeNm")),
            "typical_passenger_capacity_max": int_(item.get("typicalPassengerCapacityMax")),
            "max_takeoff_weight": int_(item.get("maxTakeoffWeight")),
            "powerplant": (item.get("powerplant") or "").strip() or None,
            "engine_model": (item.get("model") or "").strip() or None,
            "last_updated_on": last_updated,
            "raw_data": json.dumps(raw_data_val, ensure_ascii=False) if isinstance(raw_data_val, dict) else raw_data_val,
            "ingestion_date": ingestion_date,
        }
    def _get_existing(self, aircraft_detail_id: int) -> Optional[Dict[str, Any]]:
        """Return existing row if present."""
        q = """
            SELECT id FROM aviacost_aircraft_details
            WHERE aircraft_detail_id = %s
            LIMIT 1
        """
        rows = self.db.execute_query(q, (aircraft_detail_id,))
        return rows[0] if rows else None

    def _insert_record(self, row: Dict[str, Any]) -> None:
        """Insert one row into aviacost_aircraft_details."""
        q = """
            INSERT INTO aviacost_aircraft_details (
                aircraft_detail_id, name, description,
                manufacturer_id, manufacturer_name, category_id, category_name,
                avionics, years_in_production, average_pre_owned_price,
                variable_cost_per_hour, fuel_gallons_per_hour, normal_cruise_speed_kts,
                seats_full_range_nm, typical_passenger_capacity_max, max_takeoff_weight,
                powerplant, engine_model, last_updated_on, raw_data, ingestion_date
            ) VALUES (
                %(aircraft_detail_id)s, %(name)s, %(description)s,
                %(manufacturer_id)s, %(manufacturer_name)s, %(category_id)s, %(category_name)s,
                %(avionics)s, %(years_in_production)s, %(average_pre_owned_price)s,
                %(variable_cost_per_hour)s, %(fuel_gallons_per_hour)s, %(normal_cruise_speed_kts)s,
                %(seats_full_range_nm)s, %(typical_passenger_capacity_max)s, %(max_takeoff_weight)s,
                %(powerplant)s, %(engine_model)s, %(last_updated_on)s, %(raw_data)s::jsonb, %(ingestion_date)s
            )
        """
        self.db.execute_update(q, row)

    def _update_record(self, id_: str, row: Dict[str, Any]) -> None:
        """Update existing row by id."""
        q = """
            UPDATE aviacost_aircraft_details SET
                name = %(name)s, description = %(description)s,
                manufacturer_id = %(manufacturer_id)s, manufacturer_name = %(manufacturer_name)s,
                category_id = %(category_id)s, category_name = %(category_name)s,
                avionics = %(avionics)s, years_in_production = %(years_in_production)s,
                average_pre_owned_price = %(average_pre_owned_price)s,
                variable_cost_per_hour = %(variable_cost_per_hour)s,
                fuel_gallons_per_hour = %(fuel_gallons_per_hour)s,
                normal_cruise_speed_kts = %(normal_cruise_speed_kts)s,
                seats_full_range_nm = %(seats_full_range_nm)s,
                typical_passenger_capacity_max = %(typical_passenger_capacity_max)s,
                max_takeoff_weight = %(max_takeoff_weight)s,
                powerplant = %(powerplant)s, engine_model = %(engine_model)s,
                last_updated_on = %(last_updated_on)s, raw_data = %(raw_data)s::jsonb,
                ingestion_date = %(ingestion_date)s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %(id)s
        """
        self.db.execute_update(q, {**row, "id": id_})

    def _store_raw(self, item: Dict[str, Any], ingestion_date: date, file_path: Path) -> None:
        """Append one record to raw_data_store."""
        q = """
            INSERT INTO raw_data_store (source_platform, source_type, ingestion_date, file_path, raw_data)
            VALUES ('aviacost', 'aircraft_detail', %s, %s, %s)
        """
        self.db.execute_update(q, (
            ingestion_date,
            str(file_path),
            json.dumps(item, ensure_ascii=False),
        ))
