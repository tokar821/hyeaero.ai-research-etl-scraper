"""Extract structured aircraft data from saved AircraftPost Fleet Detail HTML pages.

Input: store/raw/aircraftpost/<date>/html/make_model_<id>.html
Output: store/raw/aircraftpost/<date>/fleet_extracted.json

The Fleet Detail pages are "wide tables" where:
- Columns are individual aircraft (serial numbers)
- Rows are fields (e.g., MFR Year, Registration, Airframe Hours)

This extractor pivots the table into one JSON object per aircraft column.
404 pages are skipped.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from utils.logger import get_logger

logger = get_logger(__name__)


_WS_RE = re.compile(r"\s+")


def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return _WS_RE.sub(" ", s).strip()


def _is_404_html(html_text: str) -> bool:
    t = html_text.lower()
    return ("doesn't exist (404)" in t) or ("the page you were looking for doesn't exist" in t)


def _cell_value(td) -> Any:
    """Extract a best-effort value from a <td> cell."""
    if td is None:
        return None

    # Checkmark icon -> True
    if td.find("i", class_=lambda c: c and "fa" in c):
        # Specifically treat ✔ row cells as boolean
        if "✔" in td.get_text(" ", strip=True):
            return True

    a = td.find("a")
    if a and a.get("href"):
        href = a.get("href")
        label = _clean_text(a.get_text(" ", strip=True))
        return {"text": label or None, "href": href}

    text = _clean_text(td.get_text(" ", strip=True))
    return text if text != "" else None


def _parse_headers(table) -> Tuple[List[Dict[str, Any]], int]:
    """Return (aircraft_columns, aircraft_col_count)."""
    thead = table.find("thead")
    if not thead:
        return [], 0
    ths = thead.find_all("th", recursive=True)
    # First th is row label header ("Serial Number"), remaining are aircraft columns.
    cols: List[Dict[str, Any]] = []
    for th in ths[1:]:
        serial = _clean_text(th.get_text(" ", strip=True)).split(" ")[0]
        inp = th.find("input", {"class": "select_aircraft"})
        aircraft_entity_id = inp.get("value") if inp else None
        if serial:
            cols.append({"serial_number": serial, "aircraft_entity_id": aircraft_entity_id})
    return cols, len(cols)


def _extract_make_model_id(soup: BeautifulSoup, fallback_make_model_id: Optional[int] = None) -> Optional[int]:
    hid = soup.find("input", {"name": "make_model", "id": "make_model", "type": "hidden"})
    if hid and hid.get("value"):
        try:
            return int(str(hid.get("value")).strip())
        except Exception:
            pass
    return fallback_make_model_id


def _extract_make_model_name(soup: BeautifulSoup) -> Optional[str]:
    h3 = soup.find("h3", {"class": lambda c: c and "inline" in c})
    if h3:
        t = _clean_text(h3.get_text(" ", strip=True))
        return t or None
    # fallback: select2 rendered title
    sel = soup.find(id="select2-make_model-container")
    if sel:
        t = _clean_text(sel.get_text(" ", strip=True))
        return t or None
    return None


@dataclass
class ExtractedModelResult:
    make_model_id: int
    make_model_name: Optional[str]
    aircraft: List[Dict[str, Any]]


def extract_from_html_file(html_path: Path) -> Optional[ExtractedModelResult]:
    html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    if _is_404_html(html_text):
        return None

    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table", {"class": lambda c: c and "detail-table" in c})
    if not table:
        return None

    make_model_name = _extract_make_model_name(soup)
    make_model_id = None
    m = re.search(r"make_model_(\d+)\\.html$", str(html_path).replace("/", "\\"))
    if m:
        try:
            make_model_id = int(m.group(1))
        except Exception:
            make_model_id = None
    make_model_id = _extract_make_model_id(soup, make_model_id)
    if make_model_id is None:
        return None

    headers, n = _parse_headers(table)
    if n <= 0:
        return None

    # Initialize one dict per aircraft column
    aircraft: List[Dict[str, Any]] = []
    for col in headers:
        aircraft.append(
            {
                "make_model_id": make_model_id,
                "make_model_name": make_model_name,
                "serial_number": col.get("serial_number"),
                "aircraft_entity_id": col.get("aircraft_entity_id"),
                "fields": {},
                "sections": {},
            }
        )

    tbody = table.find("tbody")
    if not tbody:
        return ExtractedModelResult(make_model_id=make_model_id, make_model_name=make_model_name, aircraft=aircraft)

    current_section: Optional[str] = None
    current_subsection: Optional[str] = None

    for tr in tbody.find_all("tr", recursive=False):
        th = tr.find("th")
        if not th:
            continue

        th_class = " ".join(th.get("class", [])).strip()
        label = _clean_text(th.get_text(" ", strip=True))
        if not label:
            continue

        # Section headers (e.g., "Standard Equipment", "Optional Equipment")
        if "th-title" in th_class:
            current_section = label
            current_subsection = None
            continue

        # Subsection headers inside a section (e.g., "Avionics", "Interior", "Other")
        if "td-sub-title" in th_class:
            current_subsection = label
            continue

        tds = tr.find_all("td", recursive=False)
        if not tds:
            continue

        values = [_cell_value(td) for td in tds]
        # Some rows may have fewer cells; pad
        if len(values) < n:
            values += [None] * (n - len(values))
        values = values[:n]

        for i in range(n):
            rec = aircraft[i]
            if current_section:
                sec = rec["sections"].setdefault(current_section, {})
                keyspace = sec
                if current_subsection:
                    keyspace = sec.setdefault(current_subsection, {})
                keyspace[label] = values[i]
            else:
                rec["fields"][label] = values[i]

    return ExtractedModelResult(make_model_id=make_model_id, make_model_name=make_model_name, aircraft=aircraft)


def extract_directory(html_dir: Path) -> Dict[str, Any]:
    """Extract all models in a directory containing make_model_*.html."""
    html_dir = Path(html_dir)
    files = sorted(html_dir.glob("make_model_*.html"), key=lambda p: p.name)

    all_aircraft: List[Dict[str, Any]] = []
    skipped_404: List[str] = []
    skipped_no_table: List[str] = []

    for p in files:
        try:
            res = extract_from_html_file(p)
            if res is None:
                # determine reason quickly
                txt = p.read_text(encoding="utf-8", errors="ignore")
                if _is_404_html(txt):
                    skipped_404.append(p.name)
                else:
                    skipped_no_table.append(p.name)
                continue
            all_aircraft.extend(res.aircraft)
        except Exception as e:
            skipped_no_table.append(f"{p.name} (error: {e})")

    return {
        "aircraft": all_aircraft,
        "counts": {
            "html_files": len(files),
            "aircraft_records": len(all_aircraft),
            "skipped_404": len(skipped_404),
            "skipped_no_table": len(skipped_no_table),
        },
        "skipped": {
            "404": skipped_404,
            "no_table": skipped_no_table,
        },
    }


def write_extracted_json(html_dir: Path, output_json_path: Path) -> Dict[str, Any]:
    payload = extract_directory(html_dir)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

