"""Controller.com Aircraft Listing Detail Scraper using undetected-chromedriver.

Alternative implementation using undetected-chromedriver (Selenium-based)
for better bot detection evasion. Use this if Playwright version is blocked.

Install: pip install undetected-chromedriver selenium beautifulsoup4
"""

import hashlib
import json
import os
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from selenium.webdriver.common.action_chains import ActionChains
    UNDETECTED_AVAILABLE = True
except ImportError:
    UNDETECTED_AVAILABLE = False

from bs4 import BeautifulSoup
from utils.logger import get_logger
from utils.chrome_utils import get_chrome_version, safe_driver_quit

logger = get_logger(__name__)


class ControllerDetailScraperUndetectedError(Exception):
    """Base exception for undetected detail scraper."""
    pass


class ControllerDetailScraperUndetected:
    """Scraper for Controller.com aircraft listing detail pages using undetected-chromedriver.
    
    This is an alternative to Playwright scraper. undetected-chromedriver
    is specifically designed to bypass bot detection systems.
    
    **Human-like Behavior Features:**
    - Variable delays (6-12 seconds between pages) to mimic reading time
    - Natural mouse movements and gradual scrolling
    - Random pauses and "thinking" time
    - Realistic browser window sizes and positions
    - Gradual content loading waits
    
    **Priority: Bot Detection Bypass > Speed**
    - Slower is better if it means avoiding detection
    - All actions include human-like randomization
    
    Install: pip install undetected-chromedriver selenium beautifulsoup4
    """
    
    BASE_URL = "https://www.controller.com"
    
    DEFAULT_TIMEZONES = ["America/New_York", "Europe/London", "Asia/Tokyo"]

    def _cleanup_chrome_locks(self, profile_dir: Path):
        """Clean up Chrome lock files from profile directory to allow new instance.
        Chrome creates SingletonLock, SingletonSocket, lockfile when using --user-data-dir.
        If previous instance didn't close cleanly, these prevent new instances.
        """
        lock_files = [
            profile_dir / "SingletonLock",
            profile_dir / "SingletonSocket",
            profile_dir / "lockfile",
            profile_dir / "Default" / "SingletonLock",
            profile_dir / "Default" / "SingletonSocket",
            profile_dir / "Default" / "lockfile",
        ]
        cleaned = 0
        for lock_file in lock_files:
            try:
                if lock_file.exists():
                    lock_file.unlink()
                    cleaned += 1
            except (OSError, PermissionError) as e:
                logger.debug("Could not remove lock file %s: %s", lock_file.name, e)
        if cleaned:
            logger.debug("Cleaned %d Chrome lock file(s) from %s", cleaned, profile_dir.name)
        # Small delay to ensure filesystem sync
        time.sleep(0.5)

    def __init__(
        self,
        storage_base_path: Optional[Path] = None,
        rate_limit: float = 6.0,
        headless: bool = False,
        profiles_dir: Optional[Path] = None,
        num_profiles: int = 0,
        proxy: Optional[str] = None,
        timezones: Optional[List[str]] = None,
    ):
        """Initialize undetected-chromedriver detail scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
            rate_limit: Base seconds to wait between requests (will be randomized).
            headless: Run browser in headless mode. Default: False.
            profiles_dir: Dir for Chrome user-data-dir profiles (multi-profile rotation). If None, no profiles.
            num_profiles: Number of profiles to rotate (e.g. 3 = different browser ID each cooldown). 0 = disable.
            proxy: Optional proxy 'host:port' (different IP). No auth; use IP whitelist or proxy provider.
            timezones: IANA timezone IDs per profile (e.g. America/New_York). Default: NY, London, Tokyo.
        """
        if not UNDETECTED_AVAILABLE:
            raise ControllerDetailScraperUndetectedError(
                "undetected-chromedriver not installed. "
                "Install with: pip install undetected-chromedriver selenium beautifulsoup4"
            )
        
        if storage_base_path is None:
            storage_base_path = Path(__file__).parent.parent / "store"
        
        self.storage_base_path = Path(storage_base_path)
        self.raw_controller_path = self.storage_base_path / "raw" / "controller"
        self.raw_controller_path.mkdir(parents=True, exist_ok=True)
        
        self.rate_limit = rate_limit
        self.headless = headless
        self.visited_urls = set()
        self.profiles_dir = Path(profiles_dir) if profiles_dir else None
        self.num_profiles = max(0, int(num_profiles))
        self.proxy = (proxy or "").strip() or None
        self.timezones = timezones if timezones else list(self.DEFAULT_TIMEZONES)
    
    def _setup_driver(self, profile_index: int = 0):
        """Setup undetected Chrome driver with human-like settings.
        Explicitly enables cookies and JavaScript. Optional: multi-profile (browser ID),
        proxy (IP), timezone override per profile to reduce CAPTCHA triggers.
        """
        version_main = get_chrome_version()
        if version_main:
            logger.info("Detected Chrome version: %s", version_main)
        
        # Multi-profile: use a *fresh* session dir per launch to avoid "chrome not reachable".
        # Reusing the same user-data-dir often causes connection failures (locks, stale state).
        # We still rotate profile_0/1/2 for cooldown; each launch uses profile_X/session_<ts>.
        profile_dir = None
        if self.profiles_dir and self.num_profiles > 0:
            base = self.profiles_dir / f"profile_{profile_index % self.num_profiles}"
            base.mkdir(parents=True, exist_ok=True)
            session_name = f"session_{int(time.time())}_{random.randint(1000, 9999)}"
            profile_dir = base / session_name
            profile_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Using Chrome profile: %s / %s (browser ID %d)", base.name, session_name, profile_index % self.num_profiles)
        
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            options = uc.ChromeOptions()
            
            if self.headless:
                options.add_argument('--headless=new')
            
            if profile_dir:
                options.add_argument(f'--user-data-dir={profile_dir.resolve()}')
            
            # Proxy: different IP (e.g. rotating residential). Format: host:port (no auth).
            if self.proxy:
                options.add_argument(f'--proxy-server={self.proxy}')
                logger.info("Using proxy: %s", self.proxy)
            
            # Human-like browser window size (not maximized, more natural)
            window_width = random.randint(1366, 1920)
            window_height = random.randint(768, 1080)
            options.add_argument(f'--window-size={window_width},{window_height}')
            
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            
            options.add_argument('--lang=en-US')
            options.add_argument('--disable-infobars')
            
            # Explicitly allow cookies and JavaScript (hCaptcha cites these as bot triggers)
            options.add_experimental_option('prefs', {
                'profile.default_content_setting_values.cookies': 1,
                'profile.default_content_setting_values.javascript': 1,
                'profile.block_third_party_cookies': 0,
            })
            
            try:
                driver = uc.Chrome(options=options, version_main=version_main)
                break
            except Exception as e:
                if ("cannot connect to chrome" in str(e).lower() or 
                    "session not created" in str(e).lower() or
                    "chrome not reachable" in str(e).lower()):
                    if attempt < max_retries and profile_dir:
                        logger.warning("Chrome connection failed (attempt %d/%d), cleaning locks and retrying...", attempt, max_retries)
                        self._cleanup_chrome_locks(profile_dir)
                        time.sleep(2.0 * attempt)  # Exponential backoff
                        continue
                raise
        
        # Timezone override per profile (different “location”)
        tz_list = self.timezones or self.DEFAULT_TIMEZONES
        if tz_list:
            tz_id = tz_list[profile_index % len(tz_list)]
            try:
                driver.execute_cdp_cmd('Emulation.setTimezoneOverride', {'timezoneId': tz_id})
                logger.info("Timezone override: %s", tz_id)
            except Exception as e:
                logger.debug("Timezone override failed (continuing): %s", e)
        
        # Set window size (don't maximize - humans don't always maximize)
        driver.set_window_size(window_width, window_height)
        
        # Human-like: Set window position (not always top-left)
        if not self.headless:
            try:
                x_offset = random.randint(0, 100)
                y_offset = random.randint(0, 100)
                driver.set_window_position(x_offset, y_offset)
            except Exception:
                pass
        
        return driver
    
    WARMUP_URL = "https://www.controller.com/listings/search"
    
    def _warmup_visit(self, driver):
        """Visit main site first to establish session/cookies. Reduces 'super-human' bot signal."""
        try:
            logger.info("Warm-up: visiting %s to establish session (cookies, JS)", self.WARMUP_URL)
            time.sleep(random.uniform(1.0, 2.5))
            driver.get(self.WARMUP_URL)
            try:
                wait = WebDriverWait(driver, 30)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except TimeoutException:
                pass
            initial = random.uniform(3, 6)
            time.sleep(initial)
            self._simulate_human_behavior(driver)
            rest = random.uniform(5, 15)
            logger.info("Warm-up: resting %.1f s (human-like)", rest)
            time.sleep(rest)
            logger.info("Warm-up complete")
        except Exception as e:
            logger.warning("Warm-up visit failed (continuing anyway): %s", e)
    
    def _wait_for_rate_limit(self):
        """Wait for rate limit delay with human-like randomization.
        Priority: avoid bot detection over speed. Slower = more human-like.
        Increased vs earlier to reduce 'super-human speed' hCaptcha trigger.
        """
        base_delay = self.rate_limit
        reading_time = random.uniform(6.0, 12.0)  # 6-12 s "reading" per listing
        jitter = random.uniform(1.0, 1.3)
        delay = base_delay + reading_time * jitter
        logger.debug("Human-like delay: %.2f seconds (mimicking reading time)", delay)
        time.sleep(delay)
    
    def _simulate_human_behavior(self, driver):
        """Simulate realistic human-like behavior on the page."""
        try:
            # Get viewport size
            viewport_width = driver.execute_script("return window.innerWidth")
            viewport_height = driver.execute_script("return window.innerHeight")
            
            # Create action chain for mouse movements
            actions = ActionChains(driver)
            
            # Random mouse movement (humans move mouse while reading)
            if random.random() > 0.3:  # 70% chance
                mouse_x = random.randint(100, max(200, viewport_width - 100))
                mouse_y = random.randint(100, max(200, viewport_height - 100))
                try:
                    current_x = viewport_width // 2
                    current_y = viewport_height // 2
                    steps = random.randint(3, 8)
                    
                    for i in range(steps):
                        target_x = current_x + (mouse_x - current_x) * (i + 1) / steps
                        target_y = current_y + (mouse_y - current_y) * (i + 1) / steps
                        offset_x = target_x - current_x
                        offset_y = target_y - current_y
                        
                        if abs(offset_x) > 1 or abs(offset_y) > 1:
                            actions.move_by_offset(int(offset_x), int(offset_y)).perform()
                            time.sleep(random.uniform(0.05, 0.15))
                    
                    time.sleep(random.uniform(0.3, 0.8))
                except Exception:
                    pass
            
            # Gradual scrolling (humans scroll in steps)
            scroll_steps = random.randint(4, 8)
            total_scroll = random.randint(400, 700)
            step_size = total_scroll // scroll_steps
            
            for step in range(scroll_steps):
                driver.execute_script(f"window.scrollBy(0, {step_size});")
                pause_time = random.uniform(0.6, 2.0)
                time.sleep(pause_time)
            
            # Sometimes scroll back up a bit
            if random.random() > 0.5:
                scroll_back = random.randint(50, 200)
                driver.execute_script(f"window.scrollBy(0, -{scroll_back});")
                time.sleep(random.uniform(0.4, 1.0))
            
            # Random pause (humans pause to "read" content)
            reading_pause = random.uniform(1.5, 4.0)
            logger.debug(f"Human reading pause: {reading_pause:.2f} seconds")
            time.sleep(reading_pause)
            
        except Exception as e:
            logger.debug(f"Error simulating human behavior: {e}")
            # Fallback: simple scroll
            try:
                scroll_amount = random.randint(300, 600)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(2.0, 4.0))
            except Exception:
                pass
    
    def _is_captcha_page(self, html_content: str) -> bool:
        """Check if the page is a CAPTCHA/bot detection page.
        
        Only returns True if we're CERTAIN it's a CAPTCHA page.
        If the page has actual listing content, return False even if CAPTCHA elements exist.
        """
        html_lower = html_content.lower()
        
        # Strong indicators of CAPTCHA page (must have these)
        strong_indicators = [
            "pardon our interruption",
            "distil_referrer",
        ]
        
        # Check if we have actual listing content (strong indicator it's NOT a CAPTCHA page)
        content_indicators = [
            "detail__title",
            "aircraft location",
            "seller information",
            "total time",
            "airframe",
            "engine",
            "avionics",
        ]
        
        has_strong_captcha = any(indicator in html_lower for indicator in strong_indicators)
        has_listing_content = any(indicator in html_lower for indicator in content_indicators)
        
        # If we have listing content, it's NOT a CAPTCHA page (even if CAPTCHA elements exist)
        if has_listing_content:
            return False
        
        # Only return True if we have strong CAPTCHA indicators AND no listing content
        return has_strong_captcha
    
    def _fetch_page(self, driver, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a detail page and return raw HTML with retry logic."""
        full_url = urljoin(self.BASE_URL, url) if not url.startswith('http') else url
        
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Navigating to: {full_url} (attempt {attempt}/{retries})")
                
                # Human-like: brief pause before navigation (reduce super-human speed)
                time.sleep(random.uniform(1.5, 3.5))
                
                driver.get(full_url)
                
                # Wait for page to load - wait for detail content with longer timeout
                try:
                    wait = WebDriverWait(driver, 45)  # Longer timeout
                    # Try multiple selectors that indicate the page has loaded
                    try:
                        # Wait for detail title (best indicator)
                        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "detail__title")))
                        logger.debug("Detail title found - page loaded")
                    except TimeoutException:
                        try:
                            # Fallback: wait for any listing content
                            wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Aircraft Location') or contains(text(), 'Total Time') or contains(text(), 'Seller Information')]")))
                            logger.debug("Listing content found - page loaded")
                        except TimeoutException:
                            # Last resort: wait for body
                            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                            logger.debug("Page body loaded")
                except TimeoutException:
                    logger.warning("Timeout waiting for page content, continuing anyway")
                
                # Human-like: wait for page to fully render (reduce super-human speed)
                initial_wait = random.uniform(5, 10)
                logger.debug("Initial page load wait: %.2f seconds (human-like)", initial_wait)
                time.sleep(initial_wait)
                
                self._simulate_human_behavior(driver)
                
                time.sleep(random.uniform(4, 8))
                
                # Get page content
                html_content = driver.page_source
                
                # Check if we got a CAPTCHA page (only if we're CERTAIN)
                if self._is_captcha_page(html_content):
                    if attempt < retries:
                        wait_time = attempt * 10  # Longer wait for CAPTCHA (10s, 20s, 30s)
                        logger.warning(f"CAPTCHA page detected for {full_url} - waiting {wait_time}s before retry (attempt {attempt}/{retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"CAPTCHA page detected after {retries} attempts for {full_url}")
                        return None
                
                content_length = len(html_content)
                logger.info(f"Retrieved {content_length:,} bytes from {full_url}")
                
                return html_content
                
            except WebDriverException as e:
                if attempt < retries:
                    wait_time = attempt * 5
                    logger.warning(f"Error fetching {url} (attempt {attempt}/{retries}): {e} - retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error fetching {url} after {retries} attempts: {e}")
                    return None
            except Exception as e:
                if attempt < retries:
                    wait_time = attempt * 5
                    logger.warning(f"Unexpected error fetching {url} (attempt {attempt}/{retries}): {e} - retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Unexpected error fetching {url} after {retries} attempts: {e}")
                    return None
        
        return None
    
    def _save_html_page(self, html_content: str, listing_id: str, output_dir: Path) -> Path:
        """Save HTML page to disk."""
        if listing_id:
            filename = f"listing_{listing_id}.html"
        else:
            url_hash = hashlib.md5(html_content.encode('utf-8')).hexdigest()[:8]
            filename = f"listing_{url_hash}.html"
        
        filepath = output_dir / filename
        
        html_bytes = html_content.encode('utf-8')
        with open(filepath, 'wb') as f:
            f.write(html_bytes)
        
        file_hash = hashlib.md5(html_bytes).hexdigest()
        file_size = filepath.stat().st_size
        
        logger.info(f"Saved detail page: {filename} ({file_size:,} bytes, MD5: {file_hash})")
        
        return filepath
    
    def _extract_listing_id(self, url: str) -> Optional[str]:
        """Extract listing ID from URL."""
        try:
            parsed_url = urlparse(url)
            path_parts = [p for p in parsed_url.path.split('/') if p]
            
            if 'listing' in path_parts:
                listing_idx = path_parts.index('listing')
                if listing_idx + 2 < len(path_parts):
                    potential_id = path_parts[listing_idx + 2]
                    if potential_id.isdigit():
                        return potential_id
            
            return None
        except Exception:
            return None

    def _backfill_details_from_html(
        self, output_dir: Path, listing_urls: List[str], count: int
    ) -> List[Dict]:
        """Build detail_data from existing listing_*.html files (no fetch).
        Iterates first `count` URLs in order, reads matching HTML, extracts fields.
        """
        detail_data: List[Dict] = []
        for i in range(min(count, len(listing_urls))):
            url = listing_urls[i]
            lid = self._extract_listing_id(url)
            if not lid:
                logger.warning(f"Backfill: no listing ID for URL {i+1}, skipping")
                continue
            path = output_dir / f"listing_{lid}.html"
            if not path.exists():
                logger.warning(f"Backfill: missing {path.name} for URL {i+1}, skipping")
                continue
            try:
                with open(path, 'r', encoding='utf-8', errors='replace') as f:
                    html = f.read()
            except Exception as e:
                logger.warning(f"Backfill: failed to read {path.name}: {e}, skipping")
                continue
            d = self._extract_detail_fields(html, url)
            detail_data.append(d)
            if (i + 1) % 50 == 0:
                logger.info(f"Backfill: extracted {i + 1}/{count} from HTML")
        logger.info(f"Backfill: extracted {len(detail_data)} detail records from HTML")
        return detail_data

    def _save_details_json(self, output_dir: Path, detail_data: List[Dict]) -> None:
        """Write details_metadata.json with current detail_data."""
        path = output_dir / "details_metadata.json"
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(detail_data, f, indent=2, ensure_ascii=False)

    def _extract_json_data(self, html_content: str) -> Optional[Dict]:
        """Extract embedded JSON data from HTML.
        
        The JSON data is embedded in the HTML, typically in a structure like:
        "DetailViewComponent": { "Props": { "SpecsExceptDescription": [...], ... } }
        """
        import re
        
        try:
            # Try to find JSON data embedded in HTML
            # Look for the pattern that contains SpecsExceptDescription
            # The JSON might be in a script tag or embedded directly
            
            # Method 1: Look for script tags with JSON
            soup = BeautifulSoup(html_content, 'html.parser')
            scripts = soup.find_all('script', type=lambda x: x and ('json' in str(x).lower() or x is None))
            
            for script in scripts:
                if script.string and 'SpecsExceptDescription' in script.string:
                    try:
                        # Try to extract JSON from script content
                        # The JSON might be part of a larger JavaScript object
                        json_match = re.search(r'\{[^{]*"SpecsExceptDescription"[^}]*\}', script.string, re.DOTALL)
                        if json_match:
                            # Try to find the full JSON structure
                            # Look for window.__INITIAL_STATE__ or similar
                            state_match = re.search(r'window\.__[A-Z_]+__\s*=\s*(\{.*?"SpecsExceptDescription".*?\})', script.string, re.DOTALL)
                            if state_match:
                                return json.loads(state_match.group(1))
                    except Exception:
                        continue
            
            # Method 2: Extract from HTML text directly (JSON might be embedded)
            # Look for the JSON structure in the raw HTML
            json_pattern = r'"DetailViewComponent"\s*:\s*\{[^}]*"Props"\s*:\s*\{[^}]*"SpecsExceptDescription"'
            match = re.search(json_pattern, html_content, re.DOTALL)
            if match:
                # Try to extract the full JSON object
                # Find the start of the object and extract until balanced braces
                start_pos = html_content.find('"DetailViewComponent"')
                if start_pos != -1:
                    # Find the opening brace
                    brace_start = html_content.find('{', start_pos)
                    if brace_start != -1:
                        # Count braces to find the end
                        brace_count = 0
                        in_string = False
                        escape_next = False
                        for i in range(brace_start, len(html_content)):
                            char = html_content[i]
                            if escape_next:
                                escape_next = False
                                continue
                            if char == '\\':
                                escape_next = True
                                continue
                            if char == '"':
                                in_string = not in_string
                                continue
                            if not in_string:
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        json_str = html_content[brace_start:i+1]
                                        try:
                                            return json.loads(json_str)
                                        except Exception:
                                            break
            
            # Method 3: Use BeautifulSoup to find text containing the JSON structure
            # and extract it using regex
            page_text = soup.get_text()
            if 'SpecsExceptDescription' in page_text:
                # The JSON is likely in the HTML but not in a script tag
                # Look for it in the raw HTML content
                json_start = html_content.find('"SpecsExceptDescription"')
                if json_start != -1:
                    # Go backwards to find the start of the JSON object
                    # and forwards to find the end
                    # This is a simplified approach - may need refinement
                    pass
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting JSON data: {e}")
            return None
    
    def _extract_detail_fields(self, html_content: str, listing_url: str) -> Dict:
        """Extract comprehensive detail fields from HTML.
        
        Primary method: Extract from embedded JSON data (most reliable).
        Fallback: Extract from HTML structure using BeautifulSoup and regex.
        """
        import re
        
        detail_data = {
            'listing_url': listing_url,
            'scrape_timestamp': datetime.now().isoformat(),
            
            # General Information
            'aircraft_model': None,
            'aircraft_type': None,
            'year': None,
            'manufacturer': None,
            'model': None,
            'serial_number': None,
            'registration': None,
            'condition': None,
            'based_at': None,
            'description': None,
            
            # Pricing (clean - no currency symbols or buttons)
            'asking_price': None,
            
            # Location (precise - only location, not other data)
            'location': None,
            
            # Airframe
            'total_time_hours': None,
            'total_landings': None,
            'maintenance_tracking': None,
            'airframe_notes': None,
            'complete_logs': None,
            
            # Engine 1
            'engine_1_make_model': None,
            'engine_1_time': None,
            'engine_1_cycles': None,
            'engine_1_tbo': None,
            'engine_1_notes': None,
            
            # Engine 2
            'engine_2_make_model': None,
            'engine_2_time': None,
            'engine_2_cycles': None,
            'engine_2_tbo': None,
            'engine_2_notes': None,
            
            # APU
            'apu': None,
            'apu_maintenance_program': None,
            'apu_notes': None,
            
            # Avionics
            'avionics_description': None,
            'avionics_list': None,
            
            # Exterior
            'year_painted': None,
            'exterior_notes': None,
            
            # Interior
            'number_of_seats': None,
            'galley': None,
            'galley_configuration': None,
            'interior_notes': None,
            
            # Seller Information
            'seller_broker_name': None,
            'seller_contact_name': None,
            'seller_location': None,
            'seller_phone': None,
            'seller_email': None,
            
            # Additional
            'props_notes': None,
            'modifications': None,
            'additional_equipment': None,
            'inspection_status': None,
        }
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            page_text = soup.get_text()
            
            # Method 1: Extract from HTML structure using CSS classes (most reliable)
            # The data is rendered in HTML using detail__specs-label and detail__specs-value classes
            specs_dict = {}
            
            # Find all spec wrappers (each section has its own wrapper)
            spec_wrappers = soup.find_all('div', class_='detail__specs-wrapper')
            
            # Extract from each wrapper to ensure proper pairing
            for wrapper in spec_wrappers:
                # Find all label elements
                labels = wrapper.find_all('div', class_='detail__specs-label')
                
                # For each label, find the next sibling value element
                for label_elem in labels:
                    label = label_elem.get_text(strip=True)
                    if not label:
                        continue
                    
                    # Find the next sibling that is a value element
                    next_elem = label_elem.find_next_sibling('div', class_='detail__specs-value')
                    if next_elem:
                        value = next_elem.get_text(strip=True)
                        if value:
                            # Only update if not already set (first occurrence wins)
                            if label not in specs_dict:
                                specs_dict[label] = value
            
            # Extract from HTML structure (Method 1 - most reliable)
            if specs_dict:
                # Map label names to fields
                if 'Year' in specs_dict:
                    detail_data['year'] = specs_dict['Year']
                if 'Manufacturer' in specs_dict:
                    detail_data['manufacturer'] = specs_dict['Manufacturer']
                if 'Model' in specs_dict:
                    detail_data['model'] = specs_dict['Model']
                if 'Serial Number' in specs_dict:
                    detail_data['serial_number'] = specs_dict['Serial Number']
                if 'Registration #' in specs_dict:
                    detail_data['registration'] = specs_dict['Registration #']
                if 'Condition' in specs_dict:
                    detail_data['condition'] = specs_dict['Condition']
                if 'Description' in specs_dict:
                    detail_data['description'] = specs_dict['Description']
                if 'Total Time' in specs_dict:
                    detail_data['total_time_hours'] = specs_dict['Total Time'].replace(',', '')
                if 'Complete Logs' in specs_dict:
                    detail_data['complete_logs'] = specs_dict['Complete Logs']
                if 'Airframe Notes' in specs_dict:
                    detail_data['airframe_notes'] = specs_dict['Airframe Notes']
                if 'Engine 1 Time' in specs_dict:
                    # Clean up SNEW, SMOH, etc.
                    clean_value = specs_dict['Engine 1 Time'].replace(',', '').replace('SNEW', '').replace('SMOH', '').replace('SINCE', '').strip()
                    detail_data['engine_1_time'] = clean_value
                if 'Engine TBO' in specs_dict:
                    detail_data['engine_1_tbo'] = specs_dict['Engine TBO'].replace(',', '')
                if 'Engine Notes' in specs_dict:
                    detail_data['engine_1_notes'] = specs_dict['Engine Notes']
                if 'Prop Notes' in specs_dict:
                    detail_data['props_notes'] = specs_dict['Prop Notes']
                if 'Avionics/Radios' in specs_dict:
                    detail_data['avionics_list'] = specs_dict['Avionics/Radios']
                    detail_data['avionics_description'] = specs_dict['Avionics/Radios']
                if 'Additional Equipment' in specs_dict:
                    detail_data['additional_equipment'] = specs_dict['Additional Equipment']
                if 'Exterior Notes' in specs_dict:
                    detail_data['exterior_notes'] = specs_dict['Exterior Notes']
                if 'Interior Notes' in specs_dict:
                    detail_data['interior_notes'] = specs_dict['Interior Notes']
                if 'Inspection Status' in specs_dict:
                    detail_data['inspection_status'] = specs_dict['Inspection Status']
                if 'Total Landings' in specs_dict:
                    detail_data['total_landings'] = specs_dict['Total Landings'].replace(',', '')
                if 'Maintenance Tracking' in specs_dict:
                    detail_data['maintenance_tracking'] = specs_dict['Maintenance Tracking']
                if 'Engine 1 Make/Model' in specs_dict:
                    detail_data['engine_1_make_model'] = specs_dict['Engine 1 Make/Model']
                if 'Engine 1 Cycles' in specs_dict:
                    detail_data['engine_1_cycles'] = specs_dict['Engine 1 Cycles'].replace(',', '')
                if 'Engine 2 Make/Model' in specs_dict:
                    detail_data['engine_2_make_model'] = specs_dict['Engine 2 Make/Model']
                if 'Engine 2 Time' in specs_dict:
                    clean_value = specs_dict['Engine 2 Time'].replace(',', '').replace('SNEW', '').replace('SMOH', '').replace('SINCE', '').strip()
                    detail_data['engine_2_time'] = clean_value
                if 'Engine 2 Cycles' in specs_dict:
                    detail_data['engine_2_cycles'] = specs_dict['Engine 2 Cycles'].replace(',', '')
                if 'Engine 2 TBO' in specs_dict:
                    detail_data['engine_2_tbo'] = specs_dict['Engine 2 TBO'].replace(',', '')
                if 'Engine 2 Notes' in specs_dict:
                    detail_data['engine_2_notes'] = specs_dict['Engine 2 Notes']
                if 'Year Painted' in specs_dict:
                    detail_data['year_painted'] = specs_dict['Year Painted']
                if 'Number of Seats' in specs_dict:
                    detail_data['number_of_seats'] = specs_dict['Number of Seats']
                if 'Galley' in specs_dict:
                    detail_data['galley'] = specs_dict['Galley']
                if 'Galley Configuration' in specs_dict:
                    detail_data['galley_configuration'] = specs_dict['Galley Configuration']
                if 'APU' in specs_dict:
                    detail_data['apu'] = specs_dict['APU']
                if 'APU Maintenance Program' in specs_dict:
                    detail_data['apu_maintenance_program'] = specs_dict['APU Maintenance Program']
                if 'APU Notes' in specs_dict:
                    detail_data['apu_notes'] = specs_dict['APU Notes']
                if 'Based at' in specs_dict:
                    detail_data['based_at'] = specs_dict['Based at']
            
            # Method 2: Try to extract from embedded JSON as fallback/supplement
            props = None
            try:
                detail_view_start = html_content.find('"DetailViewComponent"')
                if detail_view_start != -1:
                    props_key_pos = html_content.find('"Props"', detail_view_start, detail_view_start + 50000)
                    if props_key_pos != -1:
                        brace_start = html_content.find('{', props_key_pos)
                        if brace_start != -1:
                            brace_count = 0
                            in_string = False
                            escape_next = False
                            json_str = ""
                            
                            for i in range(brace_start, min(brace_start + 200000, len(html_content))):
                                char = html_content[i]
                                json_str += char
                                
                                if escape_next:
                                    escape_next = False
                                    continue
                                
                                if char == '\\':
                                    escape_next = True
                                    continue
                                
                                if char == '"':
                                    in_string = not in_string
                                    continue
                                
                                if not in_string:
                                    if char == '{':
                                        brace_count += 1
                                    elif char == '}':
                                        brace_count -= 1
                                        if brace_count == 0:
                                            try:
                                                props = json.loads(json_str)
                                                logger.debug(f"Successfully extracted JSON Props object ({len(json_str)} chars)")
                                                break
                                            except json.JSONDecodeError:
                                                break
            except Exception as e:
                logger.debug(f"Error extracting JSON Props: {e}")
            
            # Extract from JSON Props as fallback/supplement
            if props and isinstance(props, dict):
                # Only fill in fields that weren't found in HTML
                if not detail_data['description']:
                    desc_spec = props.get('DescriptionSpec', {})
                    if desc_spec and isinstance(desc_spec, dict):
                        detail_data['description'] = desc_spec.get('Value', '').strip()
                
                if not detail_data['aircraft_type']:
                    category_info = props.get('CategoryInformation', {})
                    if category_info and isinstance(category_info, dict):
                        detail_data['aircraft_type'] = category_info.get('CategoryName', '')
                
                if not detail_data['asking_price']:
                    price = props.get('Price', '')
                    if price:
                        price_clean = re.sub(r'^USD\s*', '', price, flags=re.IGNORECASE).strip()
                        detail_data['asking_price'] = price_clean
                
                if not detail_data['location']:
                    dealer_location = props.get('DealerLocation', '')
                    if dealer_location:
                        detail_data['location'] = dealer_location
                
                if not detail_data['seller_broker_name']:
                    branch_name = props.get('BranchName', '')
                    if branch_name:
                        detail_data['seller_broker_name'] = branch_name
                
                if not detail_data['seller_contact_name']:
                    dealer_contact = props.get('DealerContact', '')
                    if dealer_contact:
                        detail_data['seller_contact_name'] = dealer_contact
                
                if not detail_data['seller_location']:
                    dealer_location = props.get('DealerLocation', '')
                    if dealer_location:
                        detail_data['seller_location'] = dealer_location
                
                if not detail_data['seller_phone']:
                    dealer_phone = props.get('DealerPhone', '')
                    if dealer_phone:
                        detail_data['seller_phone'] = dealer_phone
            
            # Extract Aircraft Type from category link or detail__category
            category_div = soup.find('div', class_='detail__category')
            if category_div:
                category_link = category_div.find('a')
                if category_link:
                    detail_data['aircraft_type'] = category_link.get_text(strip=True)
            
            # Fallback: Extract from category link in breadcrumbs
            if not detail_data['aircraft_type']:
                category_link = soup.find('a', href=re.compile(r'/listings/for-sale/[^/]+/(?:3|6|8|9)'))
                if category_link:
                    detail_data['aircraft_type'] = category_link.get_text(strip=True)
            
            # Extract Aircraft Model & Year from title (fallback or supplement)
            h1_title = soup.find('h1', class_='detail__title')
            if h1_title:
                title_text = h1_title.get_text(strip=True)
                if not detail_data['aircraft_model']:
                    detail_data['aircraft_model'] = title_text
                
                # Extract year if not already found
                if not detail_data['year']:
                    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', title_text)
                    if year_match:
                        detail_data['year'] = year_match.group(1)
                
                # Extract manufacturer and model if not found
                if not detail_data['manufacturer'] or not detail_data['model']:
                    parts = title_text.split()
                    if len(parts) >= 3:
                        start_idx = 1 if parts[0].isdigit() else 0
                        if start_idx < len(parts) and not detail_data['manufacturer']:
                            detail_data['manufacturer'] = parts[start_idx]
                        if start_idx + 1 < len(parts) and not detail_data['model']:
                            detail_data['model'] = ' '.join(parts[start_idx + 1:])
            
            # Extract Asking Price (clean - no currency symbols or button text)
            # Only extract if not already found from JSON
            if not detail_data['asking_price']:
                # Look for price in HTML structure (more precise)
                price_elements = soup.find_all(['div', 'span', 'p'], class_=lambda x: x and 'price' in str(x).lower())
                for elem in price_elements:
                    price_text = elem.get_text(strip=True)
                    # Exclude button text and currency selector
                    if any(exclude in price_text.lower() for exclude in ['apply for financing', 'operating costs', 'currency', 'selector']):
                        continue
                    # Look for actual price (numbers with $ or "Call for price")
                    price_match = re.search(r'(\$[\d,]+(?:\.\d{2})?|Call\s+for\s+price|POA)', price_text, re.IGNORECASE)
                    if price_match:
                        price_val = price_match.group(1).strip()
                        # Remove "USD" prefix if present, keep only the number and $ or text
                        price_val = re.sub(r'^USD\s*', '', price_val, flags=re.IGNORECASE).strip()
                        detail_data['asking_price'] = price_val
                        break
                
                # Fallback: search in page text but exclude button areas
                if not detail_data['asking_price']:
                    # Remove button text areas first
                    clean_text = re.sub(r'Apply\s+for\s+Financing|Operating\s+Costs|Currency\s+Selector', '', page_text, flags=re.IGNORECASE)
                    price_patterns = [
                        r'(\$[\d,]+(?:\.\d{2})?)',  # Just the $ and number
                        r'Call\s+for\s+price',
                        r'POA',
                    ]
                    for pattern in price_patterns:
                        match = re.search(pattern, clean_text, re.IGNORECASE)
                        if match:
                            detail_data['asking_price'] = match.group(1) if match.groups() else match.group(0).strip()
                            break
            
            # Extract Location (precise - only location, no other data)
            # Only extract if not already found from JSON
            if not detail_data['location']:
                # Look for "Aircraft Location:" in HTML structure (more precise than page text)
                location_elem = soup.find(string=re.compile(r'^Aircraft\s+Location$', re.I))
                if location_elem:
                    # Get the parent and find the value
                    location_parent = location_elem.find_parent(['div', 'section', 'p'])
                    if location_parent:
                        # Get text from next sibling or within the same element
                        location_text = location_parent.get_text()
                        # Extract only the location part (stop at buttons or next section)
                        loc_match = re.search(
                            r'Aircraft\s+Location[:\s]+([^\n]+?)(?=\s*(?:Seller\s+Information|View\s+Seller|Video\s+Chat|Email\s+Seller|Operating\s+Costs|Apply\s+for\s+Financing|General|$))',
                            location_text,
                            re.IGNORECASE
                        )
                        if loc_match:
                            detail_data['location'] = loc_match.group(1).strip()
                
                # Fallback: Extract from page text (more careful pattern)
                if not detail_data['location']:
                    location_match = re.search(
                        r'Aircraft\s+Location[:\s]+([^,\n]+(?:,\s*[^,\n]+)?)',
                        page_text,
                        re.IGNORECASE
                    )
                    if location_match:
                        location_text = location_match.group(1).strip()
                        # Remove any button text that might have been captured
                        location_text = re.sub(r'\s*(Video\s+Chat|Email\s+Seller|Operating\s+Costs|Apply\s+for\s+Financing|Seller\s+Information).*$', '', location_text, flags=re.IGNORECASE)
                        detail_data['location'] = location_text.strip()
                
                # Final fallback: Extract from title
                if not detail_data['location']:
                    title_tag = soup.find('title')
                    if title_tag:
                        title_text = title_tag.get_text()
                        loc_match = re.search(r'in\s+([^|]+)', title_text, re.IGNORECASE)
                        if loc_match:
                            detail_data['location'] = loc_match.group(1).strip()
            
            # Extract Seller Information from HTML structure (precise)
            # Look for dealer-contact classes
            dealer_branch = soup.find('div', class_='dealer-contact__branch-name')
            if dealer_branch:
                branch_text = dealer_branch.get_text(strip=True)
                # Remove "strong" tags text if present
                strong_tag = dealer_branch.find('strong')
                if strong_tag:
                    detail_data['seller_broker_name'] = strong_tag.get_text(strip=True)
                else:
                    detail_data['seller_broker_name'] = branch_text
            
            dealer_contact_name = soup.find('div', class_='dealer-contact__name')
            if dealer_contact_name:
                contact_text = dealer_contact_name.get_text(strip=True)
                # Extract name after "Contact:"
                contact_match = re.search(r'Contact[:\s]+([^\n]+)', contact_text, re.IGNORECASE)
                if contact_match:
                    detail_data['seller_contact_name'] = contact_match.group(1).strip()
            
            dealer_location_elem = soup.find('div', class_='dealer-contact__location')
            if dealer_location_elem:
                detail_data['seller_location'] = dealer_location_elem.get_text(strip=True)
            
            dealer_phone_elem = soup.find('div', class_='dealer-contact__phone')
            if dealer_phone_elem:
                phone_link = dealer_phone_elem.find('a', href=re.compile(r'^tel:'))
                if phone_link:
                    phone_text = phone_link.get_text(strip=True)
                    # Extract phone number (remove tel: prefix if present)
                    phone_match = re.search(r'([+\d\s()-]+)', phone_text)
                    if phone_match:
                        detail_data['seller_phone'] = phone_match.group(1).strip()
            
            # Fallback: Extract from seller section text
            if not detail_data['seller_broker_name'] or not detail_data['seller_contact_name']:
                seller_section = soup.find(string=re.compile(r'^Seller\s+Information$', re.I))
                if seller_section:
                    seller_parent = seller_section.find_parent(['div', 'section'])
                    if seller_parent:
                        seller_text = seller_parent.get_text()
                        
                        if not detail_data['seller_broker_name']:
                            company_match = re.search(r'Seller\s+Information\s+([^\n]+?)(?=\s*Contact|View\s+Seller|$)', seller_text, re.IGNORECASE)
                            if company_match:
                                detail_data['seller_broker_name'] = company_match.group(1).strip()
                        
                        if not detail_data['seller_contact_name']:
                            contact_match = re.search(r'Contact[:\s]+([^\n]+)', seller_text, re.IGNORECASE)
                            if contact_match:
                                detail_data['seller_contact_name'] = contact_match.group(1).strip()
                        
                        if not detail_data['seller_location']:
                            seller_loc_match = re.search(r'Contact[^\n]+\n([A-Z][^\n]+(?:,\s*[A-Z][^\n]+)?)', seller_text, re.IGNORECASE)
                            if seller_loc_match:
                                loc_text = seller_loc_match.group(1).strip()
                                if not re.match(r'^[\d\s()+-]+$', loc_text):
                                    detail_data['seller_location'] = loc_text
                        
                        if not detail_data['seller_phone']:
                            phone_match = re.search(r'tel[:\s+]+([+\d\s()-]+)|Phone[:\s]+([+\d\s()-]+)', seller_text, re.IGNORECASE)
                            if phone_match:
                                detail_data['seller_phone'] = (phone_match.group(1) or phone_match.group(2)).strip()
            
            # Extract additional fields from HTML if not found in JSON
            # These use CSS classes for more reliable extraction
            
            # Props Notes (if not from JSON)
            if not detail_data['props_notes']:
                props_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Props$', re.I))
                if props_heading:
                    props_parent = props_heading.find_parent(['div', 'section'])
                    if props_parent:
                        props_text = props_parent.get_text()
                        notes_match = re.search(r'Prop\s+Notes\s+([^\n]+(?:\n[^\n]+)*)', props_text, re.IGNORECASE)
                        if notes_match:
                            detail_data['props_notes'] = notes_match.group(1).strip()
            
            # Additional Equipment (if not from JSON)
            if not detail_data['additional_equipment']:
                equip_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Additional\s+Equipment$', re.I))
                if equip_heading:
                    equip_parent = equip_heading.find_parent(['div', 'section'])
                    if equip_parent:
                        equip_text = equip_parent.get_text()
                        equip_match = re.search(r'Additional\s+Equipment\s+([^\n]+(?:\n[^\n]+)*)', equip_text, re.IGNORECASE)
                        if equip_match:
                            detail_data['additional_equipment'] = equip_match.group(1).strip()
            
            # Exterior Notes (if not from JSON)
            if not detail_data['exterior_notes']:
                exterior_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Exterior$', re.I))
                if exterior_heading:
                    exterior_parent = exterior_heading.find_parent(['div', 'section'])
                    if exterior_parent:
                        exterior_text = exterior_parent.get_text()
                        notes_match = re.search(r'Exterior\s+Notes\s+([^\n]+(?:\n[^\n]+)*)', exterior_text, re.IGNORECASE)
                        if notes_match:
                            detail_data['exterior_notes'] = notes_match.group(1).strip()
                        
                        # Year Painted
                        year_match = re.search(r'Year\s+Painted\s+(\d{4})', exterior_text, re.IGNORECASE)
                        if year_match:
                            detail_data['year_painted'] = year_match.group(1)
            
            # Interior Notes (if not from JSON)
            if not detail_data['interior_notes']:
                interior_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Interior$', re.I))
                if interior_heading:
                    interior_parent = interior_heading.find_parent(['div', 'section'])
                    if interior_parent:
                        interior_text = interior_parent.get_text()
                        notes_match = re.search(r'Interior\s+Notes\s+([^\n]+(?:\n[^\n]+)*)', interior_text, re.IGNORECASE)
                        if notes_match:
                            detail_data['interior_notes'] = notes_match.group(1).strip()
                        
                        # Number of Seats
                        seats_match = re.search(r'Number\s+of\s+Seats\s+(\d+)', interior_text, re.IGNORECASE)
                        if seats_match:
                            detail_data['number_of_seats'] = seats_match.group(1)
                        
                        # Galley
                        galley_match = re.search(r'Galley\s+(Yes|No)', interior_text, re.IGNORECASE)
                        if galley_match:
                            detail_data['galley'] = galley_match.group(1)
                        
                        # Galley Configuration
                        galley_config_match = re.search(r'Galley\s+Configuration\s+([^\n]+)', interior_text, re.IGNORECASE)
                        if galley_config_match:
                            detail_data['galley_configuration'] = galley_config_match.group(1).strip()
            
            # Inspection Status (if not from JSON)
            if not detail_data['inspection_status']:
                inspection_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Inspection\s+Status$', re.I))
                if inspection_heading:
                    inspection_parent = inspection_heading.find_parent(['div', 'section'])
                    if inspection_parent:
                        inspection_text = inspection_parent.get_text()
                        status_match = re.search(r'Inspection\s+Status\s+([^\n]+(?:\n[^\n]+)*)', inspection_text, re.IGNORECASE)
                        if status_match:
                            detail_data['inspection_status'] = status_match.group(1).strip()
            
            # Extract Engine 2 details (if present)
            # Look for "Engine 2" section
            engine2_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Engine\s+2$', re.I))
            if engine2_heading:
                engine2_parent = engine2_heading.find_parent(['div', 'section'])
                if engine2_parent:
                    engine2_text = engine2_parent.get_text()
                    
                    # Engine 2 Make/Model
                    make_match = re.search(r'Engine\s+2\s+Make/Model\s+([^\n]+)', engine2_text, re.IGNORECASE)
                    if make_match:
                        detail_data['engine_2_make_model'] = make_match.group(1).strip()
                    
                    # Engine 2 Time
                    time_match = re.search(r'Engine\s+2\s+Time\s+([\d,]+\.?\d*)\s*(?:SNEW|SMOH|SINCE)?', engine2_text, re.IGNORECASE)
                    if time_match:
                        detail_data['engine_2_time'] = time_match.group(1).replace(',', '').strip()
                    
                    # Engine 2 Cycles
                    cycles_match = re.search(r'Engine\s+2\s+Cycles\s+([\d,]+)', engine2_text, re.IGNORECASE)
                    if cycles_match:
                        detail_data['engine_2_cycles'] = cycles_match.group(1).replace(',', '')
                    
                    # Engine 2 TBO
                    tbo_match = re.search(r'Engine\s+2\s+TBO\s+([\d,]+)', engine2_text, re.IGNORECASE)
                    if tbo_match:
                        detail_data['engine_2_tbo'] = tbo_match.group(1).replace(',', '')
                    
                    # Engine 2 Notes
                    notes_match = re.search(r'Engine\s+2\s+Notes\s+([^\n]+(?:\n[^\n]+)*)', engine2_text, re.IGNORECASE)
                    if notes_match:
                        detail_data['engine_2_notes'] = notes_match.group(1).strip()
            
            # Extract APU details (only if not already found from specs_dict)
            if not detail_data['apu'] or not detail_data['apu_maintenance_program'] or not detail_data['apu_notes']:
                apu_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Auxiliary\s+Power\s+Unit$|^APU$', re.I))
                if apu_heading:
                    apu_parent = apu_heading.find_parent(['div', 'section'])
                    if apu_parent:
                        apu_text = apu_parent.get_text()
                        
                        if not detail_data['apu']:
                            # More precise pattern: look for "APU" label followed by value
                            apu_match = re.search(r'APU\s+(Yes|No)(?:\s|$)', apu_text, re.IGNORECASE)
                            if apu_match:
                                detail_data['apu'] = apu_match.group(1)
                        
                        if not detail_data['apu_maintenance_program']:
                            maint_match = re.search(r'APU\s+Maintenance\s+Program\s+([^\n]+)', apu_text, re.IGNORECASE)
                            if maint_match:
                                detail_data['apu_maintenance_program'] = maint_match.group(1).strip()
                        
                        if not detail_data['apu_notes']:
                            notes_match = re.search(r'APU\s+Notes\s+([^\n]+(?:\n[^\n]+)*)', apu_text, re.IGNORECASE)
                            if notes_match:
                                detail_data['apu_notes'] = notes_match.group(1).strip()
            
            # Extract Total Landings and Maintenance Tracking from Airframe section
            airframe_heading = soup.find(['h3', 'h4'], string=re.compile(r'^Airframe$', re.I))
            if airframe_heading:
                airframe_parent = airframe_heading.find_parent(['div', 'section'])
                if airframe_parent:
                    airframe_text = airframe_parent.get_text()
                    
                    # Total Landings
                    if not detail_data['total_landings']:
                        landings_match = re.search(r'Total\s+Landings\s+([\d,]+)', airframe_text, re.IGNORECASE)
                        if landings_match:
                            detail_data['total_landings'] = landings_match.group(1).replace(',', '')
                    
                    # Maintenance Tracking
                    if not detail_data['maintenance_tracking']:
                        maint_match = re.search(r'Maintenance\s+Tracking\s+([^\n]+)', airframe_text, re.IGNORECASE)
                        if maint_match:
                            detail_data['maintenance_tracking'] = maint_match.group(1).strip()
            
            # Extract Based At from General section
            general_heading = soup.find(['h3', 'h4'], string=re.compile(r'^General$', re.I))
            if general_heading:
                general_parent = general_heading.find_parent(['div', 'section'])
                if general_parent:
                    general_text = general_parent.get_text()
                    
                    if not detail_data['based_at']:
                        based_match = re.search(r'Based\s+at\s+([^\n]+)', general_text, re.IGNORECASE)
                        if based_match:
                            detail_data['based_at'] = based_match.group(1).strip()
            
        except Exception as e:
            logger.warning(f"Error extracting detail fields from {listing_url}: {e}", exc_info=True)
        
        return detail_data
    
    def load_listing_urls(self, index_metadata_path: Path) -> List[str]:
        """Load listing URLs from index scraper metadata file."""
        try:
            with open(index_metadata_path, 'r', encoding='utf-8') as f:
                listings = json.load(f)
            
            urls = set()
            for listing in listings:
                url = listing.get('listing_url')
                if url:
                    if 'controller.com/listing' in url.lower() and 'analyticstracking' not in url.lower():
                        urls.add(url)
            
            unique_urls = sorted(list(urls))
            logger.info(f"Loaded {len(unique_urls)} unique listing URLs from {index_metadata_path}")
            
            return unique_urls
            
        except Exception as e:
            logger.error(f"Error loading listing URLs from {index_metadata_path}: {e}")
            raise ControllerDetailScraperUndetectedError(f"Failed to load listing URLs: {e}") from e
    
    def scrape_details(
        self,
        listing_urls: Optional[List[str]] = None,
        index_metadata_path: Optional[Path] = None,
        date: Optional[datetime] = None,
        max_listings: Optional[int] = None,
        start_from: int = 1,
        cooldown_every: int = 0,
        cooldown_min_minutes: float = 10.0,
        cooldown_max_minutes: float = 30.0,
        cooldown_restart_browser: bool = True,
    ) -> Dict:
        """Scrape detail pages for listing URLs.

        Skip-if-exists: Always loads existing details_metadata.json. Skips fetching for any
        listing whose listing_url already has a detail record. Re-running is safe; no
        duplicates, no need to track --start-from.

        start_from: Unused when skip-if-exists. Kept for API compat.
        max_listings: Max *new* detail pages to scrape this run. None = no limit.

        Cooldown (CAPTCHA mitigation): after every cooldown_every successfully scraped
        listings, pause cooldown_min_minutes--cooldown_max_minutes (random), then
        optionally restart browser (new session). Set cooldown_every=0 to disable.
        """
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_controller_path / date_str / "details"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info("Controller.com Aircraft Listing Detail Scraper (Undetected Chrome)")
        logger.info("Date: %s", date_str)
        logger.info("Output directory: %s", output_dir)
        logger.info("Skip-if-exists: skip already-scraped listings; re-runs are safe.")
        logger.info("=" * 60)
        
        if listing_urls is None:
            if index_metadata_path is None:
                index_dir = self.raw_controller_path / date_str / "index"
                index_metadata_path = index_dir / "listings_metadata.json"
            if not index_metadata_path.exists():
                raise ControllerDetailScraperUndetectedError(
                    f"Index metadata file not found: {index_metadata_path}"
                )
            listing_urls = self.load_listing_urls(index_metadata_path)
        
        total_listings = len(listing_urls)
        detail_data: List[Dict] = []
        details_file = output_dir / "details_metadata.json"
        
        if details_file.exists():
            try:
                with open(details_file, 'r', encoding='utf-8') as f:
                    detail_data = json.load(f)
                logger.info(
                    "Loaded %d existing detail records from %s",
                    len(detail_data),
                    details_file.name,
                )
            except Exception as e:
                logger.warning("Could not load existing details JSON: %s", e)
                detail_data = []
        
        done_urls = {r.get("listing_url") for r in detail_data if r.get("listing_url")}
        
        # Backfill from HTML: we have listing_*.html but no JSON record (e.g. crash before save)
        backfilled = 0
        for listing_url in listing_urls:
            if listing_url in done_urls:
                continue
            lid = self._extract_listing_id(listing_url)
            if not lid:
                continue
            path = output_dir / f"listing_{lid}.html"
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    html = f.read()
            except Exception as e:
                logger.warning("Backfill: could not read %s: %s", path.name, e)
                continue
            d = self._extract_detail_fields(html, listing_url)
            detail_data.append(d)
            done_urls.add(listing_url)
            backfilled += 1
        if backfilled:
            self._save_details_json(output_dir, detail_data)
            logger.info("Backfill: added %d detail records from HTML -> JSON", backfilled)
        
        if done_urls:
            logger.info("Skipping %d already-scraped listings", len(done_urls))
        if max_listings is not None:
            logger.info("Limiting to %d new scrapes this run", max_listings)
        if cooldown_every and cooldown_every > 0:
            logger.info(
                "Cooldown: every %d listings, rest %.1f-%.1f mins, restart browser=%s",
                cooldown_every,
                cooldown_min_minutes,
                cooldown_max_minutes,
                cooldown_restart_browser,
            )
        
        result = {
            "date": date_str,
            "total_urls": total_listings,
            "listings_scraped": 0,
            "listings_skipped": 0,
            "listings_failed": 0,
            "html_files": [],
            "detail_data": detail_data,
            "scrape_duration": 0,
            "errors": [],
        }
        
        if len(done_urls) >= total_listings:
            logger.info("All %d listings already scraped; nothing to do.", total_listings)
            result["scrape_duration"] = time.time() - start_time
            return result
        
        try:
            profile_index = 0
            driver = self._setup_driver(profile_index)
            self._warmup_visit(driver)
            scraped_since_cooldown = 0
            scraped_this_run = 0
            driver_restart_failed = False
            try:
                for i, listing_url in enumerate(listing_urls):
                    # Check if driver restart failed - stop processing new listings
                    if driver_restart_failed:
                        logger.warning("Stopping: driver restart failed during cooldown. Remaining listings will be skipped.")
                        break
                    idx = i + 1
                    try:
                        if listing_url in done_urls:
                            result["listings_skipped"] += 1
                            logger.debug(
                                "Skipping listing %d/%d (already scraped): %s",
                                idx, total_listings, listing_url,
                            )
                            continue
                        
                        if max_listings is not None and scraped_this_run >= max_listings:
                            logger.info("Reached max_listings=%d new scrapes; stopping.", max_listings)
                            break
                        
                        if scraped_this_run > 0:
                            pre_pause = random.uniform(2.0, 5.0)
                            logger.debug("Pre-listing pause: %.2f seconds", pre_pause)
                            time.sleep(pre_pause)
                        
                        logger.info(
                            "Processing listing %d/%d: %s",
                            idx,
                            total_listings,
                            listing_url,
                        )
                        self._wait_for_rate_limit()
                        
                        # Safety check: ensure driver is valid before fetching
                        if driver is None:
                            logger.error("Driver is None. Stopping scraper.")
                            result["errors"].append("Driver became None during scraping")
                            break
                        
                        html_content = self._fetch_page(driver, listing_url)
                        if html_content is None:
                            logger.warning("Failed to fetch %s, skipping", listing_url)
                            result["errors"].append(f"Listing {idx}: Failed to fetch (None)")
                            result["listings_failed"] += 1
                            continue
                        
                        listing_id = self._extract_listing_id(listing_url)
                        html_file = self._save_html_page(
                            html_content, listing_id or str(idx), output_dir
                        )
                        result["html_files"].append(str(html_file))
                        
                        d = self._extract_detail_fields(html_content, listing_url)
                        result["detail_data"].append(d)
                        result["listings_scraped"] += 1
                        scraped_this_run += 1
                        scraped_since_cooldown += 1
                        done_urls.add(listing_url)
                        
                        self._save_details_json(output_dir, result["detail_data"])
                        logger.info(
                            "[OK] Scraped detail for listing %d/%d (saved JSON)",
                            idx,
                            total_listings,
                        )
                        
                        # Cooldown: every N listings, rest 10-30 mins and optionally restart browser
                        if (
                            cooldown_every
                            and cooldown_every > 0
                            and scraped_since_cooldown >= cooldown_every
                        ):
                            rest_sec = random.uniform(
                                cooldown_min_minutes * 60.0,
                                cooldown_max_minutes * 60.0,
                            )
                            rest_mins = rest_sec / 60.0
                            logger.info(
                                "Cooldown: %d listings done this batch. Resting %.1f mins (CAPTCHA mitigation).",
                                scraped_since_cooldown,
                                rest_mins,
                            )
                            time.sleep(rest_sec)
                            scraped_since_cooldown = 0
                            if cooldown_restart_browser:
                                if self.num_profiles > 0:
                                    profile_index = (profile_index + 1) % self.num_profiles
                                    logger.info(
                                        "Cooldown: restarting browser (profile %d/%d, new IP/tz/browser ID).",
                                        profile_index,
                                        self.num_profiles,
                                    )
                                else:
                                    logger.info("Cooldown: restarting browser (new session).")
                                safe_driver_quit(driver)
                                driver = None
                                # Retry driver setup with exponential backoff (critical for cooldown restart)
                                max_restart_retries = 3
                                for restart_attempt in range(1, max_restart_retries + 1):
                                    try:
                                        driver = self._setup_driver(profile_index)
                                        self._warmup_visit(driver)
                                        logger.info("Cooldown: browser restarted successfully")
                                        break
                                    except Exception as restart_err:
                                        if restart_attempt < max_restart_retries:
                                            wait_sec = 5.0 * restart_attempt
                                            logger.warning(
                                                "Cooldown: browser restart failed (attempt %d/%d): %s. Retrying in %.1fs...",
                                                restart_attempt,
                                                max_restart_retries,
                                                restart_err,
                                                wait_sec,
                                            )
                                            time.sleep(wait_sec)
                                        else:
                                            logger.error(
                                                "Cooldown: browser restart failed after %d attempts. Stopping scraper.",
                                                max_restart_retries,
                                                exc_info=True,
                                            )
                                            result["errors"].append(f"Cooldown restart failed after {max_restart_retries} attempts: {str(restart_err)}")
                                            # Mark flag to stop processing - cannot continue without a valid driver
                                            logger.error("Stopping scraper: cannot continue without valid browser session")
                                            driver_restart_failed = True
                                            driver = None
                                            break  # Break from retry loop
                    except Exception as e:
                        logger.error(
                            "Error processing listing %d (%s): %s",
                            idx,
                            listing_url,
                            e,
                            exc_info=True,
                        )
                        result["errors"].append(f"Listing {idx}: {str(e)}")
                        result["listings_failed"] += 1
                        # If driver is None or invalid, we can't continue
                        if driver is None:
                            logger.error("Driver is None after error. Stopping scraper.")
                            break
                        continue
            finally:
                safe_driver_quit(driver)
            
            result["scrape_duration"] = time.time() - start_time
            logger.info("=" * 60)
            logger.info("Detail Scrape Summary")
            logger.info("Total listings (index): %d", result["total_urls"])
            logger.info("Detail records in JSON: %d", len(result["detail_data"]))
            logger.info("Listings scraped this run: %d", result["listings_scraped"])
            logger.info("Listings skipped (already scraped): %d", result.get("listings_skipped", 0))
            logger.info("Listings failed this run: %d", result["listings_failed"])
            logger.info("HTML files saved this run: %d", len(result["html_files"]))
            logger.info("Scrape duration: %.2f seconds", result["scrape_duration"])
            if result["errors"]:
                logger.warning("Errors encountered: %d", len(result["errors"]))
            logger.info("=" * 60)
            return result
            
        except Exception as e:
            logger.error("Controller detail scraper failed: %s", e, exc_info=True)
            result["scrape_duration"] = time.time() - start_time
            raise


def main():
    """Main entry point for Controller detail scraper (undetected)."""
    from utils.logger import setup_logging
    
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        scraper = ControllerDetailScraperUndetected(rate_limit=6.0, headless=False)
        result = scraper.scrape_details(max_listings=None)  # Scrape all listings
        
        logger.info("Controller detail scraper (undetected) completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Controller detail scraper (undetected) failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
