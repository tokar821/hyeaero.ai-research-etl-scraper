"""Aviacost.com Aircraft Data Scraper.

Fetches all aircraft data from the public API in a single GET request and
saves the response to store/raw/aviacost/<date>/ as JSON.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except ImportError:
    from requests.packages.urllib3.util.retry import Retry

from utils.logger import get_logger

logger = get_logger(__name__)

API_URL = "https://aviacost.com/api/home/GetAircraftDetails"


class AviacostScraperError(Exception):
    """Base exception for Aviacost scraper."""
    pass


class AviacostScraper:
    """Scraper for Aviacost aircraft data via GetAircraftDetails API."""

    def __init__(self, storage_base_path: Optional[Path] = None):
        """Initialize Aviacost scraper.

        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
        """
        if storage_base_path is None:
            storage_base_path = Path(__file__).parent.parent / "store"

        self.storage_base_path = Path(storage_base_path)
        self.raw_aviacost_path = self.storage_base_path / "raw" / "aviacost"
        self.raw_aviacost_path.mkdir(parents=True, exist_ok=True)

        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        })

        return session

    def fetch_aircraft_details(self) -> Any:
        """GET GetAircraftDetails API and return parsed JSON.

        Returns:
            Parsed JSON response (list or dict). Caller should handle structure.

        Raises:
            AviacostScraperError: On request or parse failure.
        """
        logger.info("Fetching %s", API_URL)
        response = self.session.get(API_URL, timeout=120)
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError as e:
            raise AviacostScraperError(f"Invalid JSON from API: {e}") from e

        return data

    def scrape(self, date: Optional[datetime] = None) -> Dict[str, Any]:
        """Fetch all aircraft data and save to store/raw/aviacost/<date>/ as JSON.

        Args:
            date: Date for storage path. If None, uses current date.

        Returns:
            Dict with:
                - date: Date string (YYYY-MM-DD)
                - output_dir: Path to output directory
                - aircraft_details_file: Path to aircraft_details.json
                - metadata_file: Path to scrape_metadata.json
                - aircraft_count: Number of aircraft if response is a list, else None
                - raw_keys: Top-level keys if response is dict (for inspection)
                - scrape_duration_seconds: Time taken
        """
        if date is None:
            date = datetime.now()

        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_aviacost_path / date_str
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("=" * 60)
        logger.info("Aviacost Aircraft Data Scraper")
        logger.info("Date: %s", date_str)
        logger.info("Output directory: %s", output_dir)
        logger.info("=" * 60)

        start = datetime.now()
        result = {
            "date": date_str,
            "output_dir": str(output_dir),
            "aircraft_details_file": None,
            "metadata_file": None,
            "aircraft_count": None,
            "raw_keys": None,
            "scrape_duration_seconds": 0.0,
        }

        try:
            data = self.fetch_aircraft_details()

            # Save full API response
            details_path = output_dir / "aircraft_details.json"
            with open(details_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            result["aircraft_details_file"] = str(details_path)

            # Infer count and structure for metadata
            if isinstance(data, list):
                count = len(data)
                result["aircraft_count"] = count
                logger.info("Response is a list: %s aircraft", count)
            elif isinstance(data, dict):
                result["raw_keys"] = list(data.keys())
                logger.info("Response is an object with keys: %s", result["raw_keys"])
                # If there's a list inside (e.g. data.aircraft or data.items), count it
                for key in ("aircraft", "items", "data", "results", "list"):
                    if isinstance(data.get(key), list):
                        result["aircraft_count"] = len(data[key])
                        logger.info("List '%s' has %s items", key, result["aircraft_count"])
                        break
            else:
                logger.warning("Unexpected response type: %s", type(data))

            # Scrape metadata
            metadata = {
                "source": "aviacost",
                "api_url": API_URL,
                "scrape_date": date_str,
                "scrape_timestamp": datetime.now().isoformat(),
                "aircraft_count": result["aircraft_count"],
                "raw_keys": result["raw_keys"],
            }
            metadata_path = output_dir / "scrape_metadata.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

            result["metadata_file"] = str(metadata_path)
            result["scrape_duration_seconds"] = (datetime.now() - start).total_seconds()

            logger.info("Saved aircraft_details.json and scrape_metadata.json to %s", output_dir)
            logger.info("Scrape duration: %.2f seconds", result["scrape_duration_seconds"])

            return result

        except requests.RequestException as e:
            logger.error("Aviacost API request failed: %s", e)
            raise AviacostScraperError(f"Request failed: {e}") from e
        except (AviacostScraperError, OSError) as e:
            raise
