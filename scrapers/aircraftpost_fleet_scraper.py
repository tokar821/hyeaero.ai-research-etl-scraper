"""AircraftPost.com Fleet detail HTML scraper.

Logs into AircraftPost using credentials from environment variables and then
downloads fleet detail pages for make_model IDs 1..92 (inclusive by default).

Saves raw HTML to:
  etl-pipeline/store/raw/aircraftpost/<YYYY-MM-DD>/html/make_model_<id>.html
and writes scrape metadata to scrape_metadata.json.
"""

import os
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from utils.logger import get_logger

logger = get_logger(__name__)

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False

try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except Exception:
    STEALTH_AVAILABLE = False


class AircraftPostScraperError(Exception):
    pass


@dataclass
class AircraftPostScrapeResult:
    date: str
    output_dir: str
    html_dir: str
    pages_saved: int
    requested_models: List[int]
    succeeded_models: List[int]
    failed_models: List[Dict[str, Any]]
    scrape_duration_seconds: float


class AircraftPostFleetScraper:
    BASE_URL = "https://aircraftpost.com"
    # Site's nav points to /sign_in (likely renders actual form)
    LOGIN_URL = "https://aircraftpost.com/sign_in"
    FLEET_DETAIL_URL = "https://aircraftpost.com/fleet/detail?make_model={id}&commit=Submit"

    def __init__(self, storage_base_path: Optional[Path] = None):
        if not PLAYWRIGHT_AVAILABLE:
            raise AircraftPostScraperError(
                "Playwright is not available. Install requirements and run `playwright install`."
            )

        if storage_base_path is None:
            storage_base_path = Path(__file__).parent.parent / "store"
        self.storage_base_path = Path(storage_base_path)
        self.raw_path = self.storage_base_path / "raw" / "aircraftpost"
        self.raw_path.mkdir(parents=True, exist_ok=True)

        self.email = (os.getenv("AIRCRAFTPOST_EMAIL") or "").strip()
        self.password = (os.getenv("AIRCRAFTPOST_PASSWORD") or "").strip()
        if not self.email or not self.password:
            raise AircraftPostScraperError(
                "Missing AIRCRAFTPOST_EMAIL or AIRCRAFTPOST_PASSWORD in environment."
            )

    def _human_pause(self, short: Tuple[float, float] = (2.5, 6.5), long_every: int = 9) -> None:
        """Randomized delays to mimic human browsing."""
        time.sleep(random.uniform(*short))
        if random.randint(1, long_every) == 1:
            time.sleep(random.uniform(8.0, 18.0))

    def _login(self, page) -> None:
        logger.info("Navigating to login page: %s", self.LOGIN_URL)
        page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=90_000)
        self._human_pause((1.0, 2.5), long_every=99)

        # Try a few resilient selectors (site appears to be Rails/Devise)
        email_locators = [
            "input[name='user[email]']",
            "input[type='email']",
            "input[name*='email' i]",
        ]
        password_locators = [
            "input[name='user[password]']",
            "input[type='password']",
            "input[name*='password' i]",
        ]

        def _first_visible_selector(selectors: List[str]) -> Optional[str]:
            for sel in selectors:
                try:
                    if page.locator(sel).first.is_visible(timeout=2_000):
                        return sel
                except Exception:
                    continue
            return None

        email_sel = _first_visible_selector(email_locators)
        pass_sel = _first_visible_selector(password_locators)
        if not email_sel or not pass_sel:
            # Debug snapshot for selector tuning
            try:
                debug_dir = self.raw_path / "_debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                (debug_dir / "login_page.html").write_text(page.content(), encoding="utf-8")
                inputs = page.evaluate(
                    """() => Array.from(document.querySelectorAll('input')).map(i => ({
                      type: i.getAttribute('type'),
                      name: i.getAttribute('name'),
                      id: i.getAttribute('id'),
                      placeholder: i.getAttribute('placeholder'),
                      autocomplete: i.getAttribute('autocomplete')
                    }))"""
                )
                (debug_dir / "login_inputs.json").write_text(json.dumps(inputs, indent=2), encoding="utf-8")
                logger.warning("Login debug saved to %s", debug_dir)
            except Exception as e:
                logger.warning("Failed to write login debug snapshot: %s", e)
            raise AircraftPostScraperError("Could not find login form fields on AircraftPost.")

        page.locator(email_sel).first.click()
        page.locator(email_sel).first.fill(self.email, timeout=10_000)
        self._human_pause((0.3, 0.9), long_every=99)
        page.locator(pass_sel).first.click()
        page.locator(pass_sel).first.fill(self.password, timeout=10_000)
        self._human_pause((0.4, 1.2), long_every=99)

        # Submit
        submit_selectors = [
            "input[type='submit']",
            "button[type='submit']",
            "text=/sign in|log in/i",
        ]
        clicked = False
        for sel in submit_selectors:
            try:
                page.locator(sel).first.click(timeout=5_000)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            raise AircraftPostScraperError("Could not submit login form.")

        try:
            page.wait_for_load_state("networkidle", timeout=60_000)
        except Exception:
            # Some sites never go fully idle; acceptable.
            pass

        # Basic success heuristic: leave sign_in page
        if "sign_in" in (page.url or ""):
            # Try to extract a flash error message if present
            msg = None
            for sel in ("div.alert", ".flash", ".alert", "text=/invalid|error/i"):
                try:
                    t = page.locator(sel).first.inner_text(timeout=2_000).strip()
                    if t:
                        msg = t
                        break
                except Exception:
                    continue
            raise AircraftPostScraperError(f"Login appears to have failed. {msg or ''}".strip())

        logger.info("Login successful (current URL: %s)", page.url)

    def scrape(
        self,
        start_model_id: int = 1,
        end_model_id: int = 92,
        date: Optional[datetime] = None,
        headless: bool = True,
    ) -> Dict[str, Any]:
        if date is None:
            date = datetime.now()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_path / date_str
        html_dir = output_dir / "html"
        html_dir.mkdir(parents=True, exist_ok=True)

        requested = list(range(int(start_model_id), int(end_model_id) + 1))
        succeeded: List[int] = []
        failed: List[Dict[str, Any]] = []
        start_ts = datetime.now()

        logger.info("=" * 60)
        logger.info("AircraftPost Fleet HTML Scraper")
        logger.info("Date: %s", date_str)
        logger.info("Output directory: %s", output_dir)
        logger.info("Models: %s..%s (%s pages)", start_model_id, end_model_id, len(requested))
        logger.info("Headless: %s", headless)
        logger.info("=" * 60)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                locale="en-US",
                timezone_id="America/New_York",
            )
            page = context.new_page()

            if STEALTH_AVAILABLE:
                try:
                    stealth_sync(page)
                except Exception as e:
                    logger.warning("Failed to apply stealth plugin: %s", e)

            # Light anti-detection init
            page.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )

            try:
                self._login(page)
            except Exception:
                context.close()
                browser.close()
                raise

            for mid in requested:
                url = self.FLEET_DETAIL_URL.format(id=mid)
                out_path = html_dir / f"make_model_{mid}.html"
                logger.info("Fetching make_model=%s", mid)
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    # Some pages might load dynamic content; wait a bit
                    try:
                        page.wait_for_load_state("networkidle", timeout=20_000)
                    except Exception:
                        pass

                    # Human-ish scroll
                    try:
                        page.mouse.wheel(0, random.randint(600, 1400))
                        self._human_pause((0.5, 1.6), long_every=99)
                        page.mouse.wheel(0, random.randint(400, 1200))
                    except Exception:
                        pass

                    html = page.content()
                    out_path.write_text(html, encoding="utf-8")
                    succeeded.append(mid)
                except PlaywrightTimeoutError as e:
                    failed.append({"make_model": mid, "url": url, "error": f"timeout: {e}"})
                except Exception as e:
                    failed.append({"make_model": mid, "url": url, "error": str(e)})

                self._human_pause()

            context.close()
            browser.close()

        duration = (datetime.now() - start_ts).total_seconds()

        metadata = {
            "source": "aircraftpost",
            "login_url": self.LOGIN_URL,
            "fleet_url_format": self.FLEET_DETAIL_URL,
            "scrape_date": date_str,
            "scrape_timestamp": datetime.now().isoformat(),
            "start_model_id": start_model_id,
            "end_model_id": end_model_id,
            "requested_models": requested,
            "succeeded_models": succeeded,
            "failed_models": failed,
            "pages_saved": len(succeeded),
            "html_dir": str(html_dir),
            "duration_seconds": duration,
        }
        meta_path = output_dir / "scrape_metadata.json"
        meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        return {
            "date": date_str,
            "output_dir": str(output_dir),
            "html_dir": str(html_dir),
            "pages_saved": len(succeeded),
            "requested_models": requested,
            "succeeded_models": succeeded,
            "failed_models": failed,
            "scrape_duration_seconds": duration,
            "metadata_file": str(meta_path),
        }

