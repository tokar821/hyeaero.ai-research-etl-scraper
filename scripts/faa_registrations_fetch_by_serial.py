from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def main() -> None:
    # Load ETL DB credentials
    repo_root = Path(__file__).resolve().parents[1]  # etl-pipeline/
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("POSTGRES_CONNECTION_STRING not found (expected in etl-pipeline/.env)")

    # Serial numbers are stored as strings in faa_registrations.serial_number (VARCHAR)
    serial_numbers = ["10000", "5334"]

    # Set to a string like "2026-01-23" to filter by that ingestion snapshot.
    ingestion_date: str | None = "2026-01-23"

    where = "serial_number = ANY(%s)"
    params: list[object] = [serial_numbers]
    if ingestion_date:
        where += " AND ingestion_date = %s"
        params.append(ingestion_date)

    sql = f"""
    SELECT
      id,
      aircraft_id,
      n_number,
      serial_number,
      mfr_mdl_code,
      eng_mfr_mdl,
      year_mfr,
      type_registrant,
      registrant_name,
      street,
      street2,
      city,
      state,
      zip_code,
      region,
      county,
      country,
      last_action_date,
      cert_issue_date,
      certification,
      expiration_date,
      air_worth_date,
      type_aircraft,
      type_engine,
      status_code,
      mode_s_code,
      mode_s_code_hex,
      fract_owner,
      unique_id,
      kit_mfr,
      kit_model,
      other_names
    FROM faa_registrations
    WHERE {where}
    ORDER BY ingestion_date DESC
    """

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            colnames = [d[0] for d in cur.description]
            rows = cur.fetchall()

    result: list[dict[str, object]] = []
    for row in rows:
        d = dict(zip(colnames, row))
        result.append(d)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

