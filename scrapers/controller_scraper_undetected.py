"""Controller.com Aircraft Listings Scraper using undetected-chromedriver.

Alternative implementation using undetected-chromedriver (Selenium-based)
for better bot detection evasion. Use this if Playwright version is blocked.

Install: pip install undetected-chromedriver selenium beautifulsoup4
"""

import hashlib
import json
import os
import random
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

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

logger = get_logger(__name__)


def _safe_driver_quit(driver):
    """Safely quit Chrome driver, suppressing cleanup errors.
    
    This prevents 'Exception ignored' messages in terminal during garbage collection.
    """
    if driver:
        try:
            driver.quit()
        except Exception:
            # Silently handle cleanup errors (common with undetected-chromedriver)
            # These are harmless and happen during garbage collection
            pass


def _get_chrome_version():
    """Detect installed Chrome version to match ChromeDriver.
    
    Returns:
        int: Chrome major version number (e.g., 143) or None if detection fails.
    """
    try:
        # Try Windows registry method first
        import winreg
        try:
            key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Google\Chrome\BLBeacon"
            )
            version = winreg.QueryValueEx(key, "version")[0]
            winreg.CloseKey(key)
            match = re.search(r'(\d+)\.', version)
            if match:
                return int(match.group(1))
        except (WindowsError, OSError):
            pass
        
        # Try common Chrome installation paths
        chrome_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe",
        ]
        
        for chrome_path in chrome_paths:
            try:
                # Expand environment variables
                expanded_path = os.path.expandvars(chrome_path)
                if os.path.exists(expanded_path):
                    result = subprocess.run(
                        [expanded_path, "--version"],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        match = re.search(r'(\d+)\.', result.stdout)
                        if match:
                            return int(match.group(1))
            except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                continue
        
        logger.warning("Could not detect Chrome version automatically, using auto-detection")
        return None
    except Exception as e:
        logger.warning(f"Chrome version detection failed: {e}, using auto-detection")
        return None


class ControllerScraperUndetectedError(Exception):
    """Base exception for undetected-chromedriver scraper."""
    pass


class ControllerScraperUndetected:
    """Scraper for Controller.com using undetected-chromedriver (Selenium).
    
    This is an alternative to Playwright scraper. undetected-chromedriver
    is specifically designed to bypass bot detection systems.
    
    **Human-like Behavior Features:**
    - Variable delays (4-8 seconds between pages) to mimic reading time
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
    START_URL = "/listings/search?page=1"
    
    def __init__(self, storage_base_path: Optional[Path] = None, rate_limit: float = 6.0, headless: bool = False):
        """Initialize undetected-chromedriver scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
            rate_limit: Base seconds to wait between requests (will be randomized). Default: 6.0 seconds.
                        Actual delays will be 6-12 seconds to mimic human reading/thinking time.
            headless: Run browser in headless mode. Default: False (non-headless for better evasion).
        """
        if not UNDETECTED_AVAILABLE:
            raise ControllerScraperUndetectedError(
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
    
    def _setup_driver(self):
        """Setup undetected Chrome driver with human-like settings."""
        options = uc.ChromeOptions()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Human-like browser window size (not maximized, more natural)
        window_width = random.randint(1366, 1920)
        window_height = random.randint(768, 1080)
        options.add_argument(f'--window-size={window_width},{window_height}')
        
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        
        # Additional human-like browser arguments
        options.add_argument('--lang=en-US')
        options.add_argument('--disable-infobars')
        
        # Detect Chrome version to match ChromeDriver
        chrome_version = _get_chrome_version()
        if chrome_version:
            logger.info(f"Detected Chrome version: {chrome_version}")
        else:
            logger.info("Using auto-detection for Chrome version")
        
        # Create undetected driver with detected version
        driver = uc.Chrome(options=options, version_main=chrome_version)
        
        # Set window size (don't maximize - humans don't always maximize)
        driver.set_window_size(window_width, window_height)
        
        # Human-like: Set window position (not always top-left)
        if not self.headless:
            try:
                x_offset = random.randint(0, 100)
                y_offset = random.randint(0, 100)
                driver.set_window_position(x_offset, y_offset)
            except Exception:
                pass  # Continue if position setting fails
        
        return driver
    
    def _wait_for_rate_limit(self):
        """Wait for rate limit delay with human-like randomization.
        
        Uses longer, more variable delays to mimic human reading/thinking time.
        """
        # Human-like delay: base time + random variation (6-12 seconds typical)
        base_delay = self.rate_limit
        # Add random "reading time" - humans don't click instantly
        # Longer delays to mimic human reading/processing time
        reading_time = random.uniform(3.0, 6.0)  # 3-6 seconds of "reading"
        jitter = random.uniform(0.9, 1.3)  # Variation
        delay = base_delay + reading_time * jitter
        logger.debug(f"Human-like delay: {delay:.2f} seconds (mimicking reading time)")
        time.sleep(delay)
    
    def _simulate_human_behavior(self, driver):
        """Simulate realistic human-like behavior on the page.
        
        Includes:
        - Natural mouse movements with gradual transitions
        - Gradual scrolling in multiple steps (not instant)
        - Reading pauses between actions
        - Random small movements and corrections
        - Variable timing to mimic human inconsistency
        """
        try:
            # Get viewport size
            viewport_width = driver.execute_script("return window.innerWidth")
            viewport_height = driver.execute_script("return window.innerHeight")
            
            # Create action chain for mouse movements
            actions = ActionChains(driver)
            
            # 1. Random mouse movement (humans move mouse while reading)
            if random.random() > 0.3:  # 70% chance
                mouse_x = random.randint(100, max(200, viewport_width - 100))
                mouse_y = random.randint(100, max(200, viewport_height - 100))
                try:
                    # Move mouse gradually in steps (human-like)
                    current_x = viewport_width // 2
                    current_y = viewport_height // 2
                    steps = random.randint(3, 8)  # Multiple steps for smooth movement
                    
                    for i in range(steps):
                        target_x = current_x + (mouse_x - current_x) * (i + 1) / steps
                        target_y = current_y + (mouse_y - current_y) * (i + 1) / steps
                        offset_x = target_x - current_x
                        offset_y = target_y - current_y
                        
                        if abs(offset_x) > 1 or abs(offset_y) > 1:  # Only move if significant
                            actions.move_by_offset(int(offset_x), int(offset_y)).perform()
                            time.sleep(random.uniform(0.05, 0.15))  # Small pause between movements
                    
                    time.sleep(random.uniform(0.3, 0.8))
                except Exception:
                    pass  # Continue if mouse movement fails
            
            # 2. Gradual scrolling (humans scroll in steps, not all at once)
            scroll_steps = random.randint(4, 8)  # 4-8 scroll steps (more gradual)
            total_scroll = random.randint(400, 700)  # Total pixels to scroll
            step_size = total_scroll // scroll_steps
            
            for step in range(scroll_steps):
                # Scroll gradually
                driver.execute_script(f"window.scrollBy(0, {step_size});")
                # Variable pause between scrolls (humans read at different speeds)
                pause_time = random.uniform(0.6, 2.0)  # Longer pauses
                time.sleep(pause_time)
            
            # 3. Sometimes scroll back up a bit (humans don't scroll perfectly)
            if random.random() > 0.5:  # 50% chance (more realistic)
                scroll_back = random.randint(50, 200)
                driver.execute_script(f"window.scrollBy(0, -{scroll_back});")
                time.sleep(random.uniform(0.4, 1.0))
            
            # 4. Random pause (humans pause to "read" content)
            reading_pause = random.uniform(1.5, 4.0)  # Longer reading pause
            logger.debug(f"Human reading pause: {reading_pause:.2f} seconds")
            time.sleep(reading_pause)
            
        except Exception as e:
            logger.debug(f"Error simulating human behavior: {e}")
            # Fallback: simple scroll if advanced features fail
            try:
                scroll_amount = random.randint(300, 600)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(2.0, 4.0))  # Longer fallback pause
            except Exception:
                pass
    
    def _fetch_page(self, driver, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a page using Selenium and wait for content to load."""
        full_url = urljoin(self.BASE_URL, url) if not url.startswith('http') else url
        
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Navigating to: {full_url} (attempt {attempt}/{retries})")
                
                # Human-like: brief pause before navigation (like thinking)
                time.sleep(random.uniform(0.5, 1.5))
                
                driver.get(full_url)
                
                # Wait for page to load - wait for listContainer or listing cards to appear
                try:
                    wait = WebDriverWait(driver, 30)  # Longer timeout for human-like patience
                    # Try to wait for listContainer first
                    try:
                        wait.until(EC.presence_of_element_located((By.ID, "listContainer")))
                        logger.debug("ListContainer found")
                    except TimeoutException:
                        # Fallback: try to find listing cards
                        try:
                            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "list-listing-card-wrapper")))
                            logger.debug("Listing cards found")
                        except TimeoutException:
                            logger.warning("ListContainer and listing cards not found, continuing anyway")
                except Exception as e:
                    logger.debug(f"Wait for elements failed: {e}, continuing")
                
                # Human-like: Wait for page to fully render (humans don't read instantly)
                # Longer wait to mimic human reading/processing time
                initial_wait = random.uniform(4, 7)  # 4-7 seconds initial wait
                logger.debug(f"Initial page load wait: {initial_wait:.2f} seconds (human-like)")
                time.sleep(initial_wait)
                
                # Simulate human behavior (natural scrolling, mouse movements)
                self._simulate_human_behavior(driver)
                
                # Additional wait for any lazy-loaded content (humans pause after scrolling)
                post_scroll_wait = random.uniform(2, 4)  # 2-4 seconds after scrolling
                logger.debug(f"Post-scroll wait: {post_scroll_wait:.2f} seconds (human-like)")
                time.sleep(post_scroll_wait)
                
                # Get page source
                html_content = driver.page_source
                content_length = len(html_content)
                logger.info(f"Retrieved {content_length:,} bytes from {full_url}")
                
                # Check for bot detection (but be less aggressive - only if very small or specific text)
                # If we got substantial content (>50KB), assume it's good even if bot detection text appears
                if content_length < 50000:  # Very small page likely an error
                    if 'Pardon Our Interruption' in html_content or 'distil_referrer' in html_content:
                        logger.warning(f"Bot detection page detected (small page) for {full_url}")
                        if attempt < retries:
                            wait_time = attempt * 10
                            logger.warning(f"Waiting {wait_time}s before retry (attempt {attempt}/{retries})")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Bot detection page persisted after {retries} attempts")
                            return None
                else:
                    # Large page - check if we actually have listing content
                    if 'list-listing-card-wrapper' in html_content or 'listContainer' in html_content:
                        logger.info("Page loaded successfully with listing content")
                    elif 'Pardon Our Interruption' in html_content:
                        logger.warning("Bot detection text found but page is large - might be false positive")
                
                self.visited_urls.add(url)
                return html_content
                
            except Exception as e:
                if attempt < retries:
                    wait_time = attempt * 5
                    logger.warning(f"Error fetching {url} (attempt {attempt}/{retries}): {e} - retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error fetching {url} after {retries} attempts: {e}")
                    return None
        
        return None
    
    def scrape_listings(self, date: Optional[datetime] = None, max_pages: Optional[int] = None, start_page: int = 1) -> Dict:
        """Scrape Controller.com listings using undetected-chromedriver.
        
        Args:
            date: Optional date for output directory. Defaults to today.
            max_pages: Optional maximum pages to scrape. None = all pages.
            start_page: Page number to start from (for resuming). Default: 1.
        """
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_controller_path / date_str / "index"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info("Controller.com Scraper (undetected-chromedriver)")
        logger.info(f"Date: {date_str}")
        logger.info(f"Output directory: {output_dir}")
        if start_page > 1:
            logger.info(f"Resuming from page: {start_page}")
        logger.info("=" * 60)
        
        # Load existing listings if resuming
        all_listings = []
        listings_file = output_dir / "listings_metadata.json"
        if start_page > 1 and listings_file.exists():
            try:
                with open(listings_file, 'r', encoding='utf-8') as f:
                    all_listings = json.load(f)
                logger.info(f"Loaded {len(all_listings):,} existing listings from previous scrape")
            except Exception as e:
                logger.warning(f"Could not load existing listings: {e}, starting fresh")
                all_listings = []
        
        result = {
            "date": date_str,
            "pages_scraped": 0,
            "total_listings": 0,
            "html_files": [],
            "listings_data": [],
            "scrape_duration": 0,
            "errors": []
        }
        
        driver = None
        page_num = start_page - 1  # Initialize before try block to avoid UnboundLocalError
        
        try:
            driver = self._setup_driver()
            
            # Construct URL for starting page
            if start_page > 1:
                current_url = f"/listings/search?page={start_page}"
            else:
                current_url = self.START_URL
            page_num = start_page
            
            logger.info("=" * 60)
            if start_page > 1:
                logger.info(f"Resuming scrape from page {start_page}...")
            else:
                logger.info("Starting to scrape ALL pages...")
            logger.info("Stopping condition: Y = Z (current_end >= total_listings)")
            logger.info("Example: '5,093 - 5,122 of 5,122 Listings' means last page")
            logger.info("=" * 60)
            
            while current_url:
                # Check max_pages limit only if specified (for testing)
                if max_pages and page_num > max_pages:
                    logger.info(f"Reached max pages limit ({max_pages})")
                    break
                
                logger.info(f"Processing page {page_num}...")
                
                # Human-like: Pause before starting to process page (like thinking)
                if page_num > start_page:
                    pre_page_pause = random.uniform(1.0, 3.0)
                    logger.debug(f"Pre-page pause: {pre_page_pause:.2f} seconds (human-like)")
                    time.sleep(pre_page_pause)
                
                self._wait_for_rate_limit()
                
                html_content = self._fetch_page(driver, current_url)
                
                if html_content is None:
                    logger.warning(f"Failed to fetch page {page_num}, skipping")
                    result["errors"].append(f"Page {page_num}: Failed to fetch")
                    break
                
                # Save HTML
                html_file = self._save_html_page(html_content, page_num, output_dir)
                result["html_files"].append(str(html_file))
                
                # Extract pagination info
                pagination_info = self._extract_pagination_info(html_content)
                
                # On first page (or start_page): Get total count and calculate expected pages
                if page_num == start_page and pagination_info:
                    result["total_listings_count"] = pagination_info['total_listings']
                    total_listings = pagination_info['total_listings']
                    listings_per_page = pagination_info['current_end'] - pagination_info['current_start'] + 1
                    expected_pages = (total_listings + listings_per_page - 1) // listings_per_page
                    result["expected_pages"] = expected_pages
                    logger.info("=" * 60)
                    logger.info("Pagination Information (from page 1):")
                    logger.info(f"  Total listings on site: {total_listings:,}")
                    logger.info(f"  Listings per page: ~{listings_per_page}")
                    logger.info(f"  Expected pages: ~{expected_pages}")
                    logger.info("  Stopping when: current_end >= total_listings (Y = Z)")
                    logger.info("=" * 60)
                
                # Extract listings
                full_url = urljoin(self.BASE_URL, current_url) if not current_url.startswith('http') else current_url
                listings = self._extract_listings(html_content, full_url)
                all_listings.extend(listings)
                
                logger.info(f"Page {page_num}: Extracted {len(listings)} listings (Total so far: {len(all_listings):,})")
                
                # Save JSON incrementally after each page (so progress is saved)
                listings_file = output_dir / "listings_metadata.json"
                with open(listings_file, 'w', encoding='utf-8') as f:
                    json.dump(all_listings, f, indent=2, ensure_ascii=False)
                logger.debug(f"Saved {len(all_listings):,} listings to JSON (incremental save)")
                
                # Check if last page: Y = Z (current_end >= total_listings)
                if pagination_info and pagination_info.get('is_last_page', False):
                    logger.info("=" * 60)
                    logger.info(f"Reached last page (page {page_num})")
                    logger.info(f"Condition met: current_end ({pagination_info['current_end']:,}) >= total_listings ({pagination_info['total_listings']:,})")
                    logger.info("Pagination complete - all pages scraped!")
                    logger.info("=" * 60)
                    break
                
                # Find next page URL
                next_url = self._find_next_page_url(html_content, current_url)
                if next_url:
                    if next_url.startswith(self.BASE_URL):
                        current_url = next_url[len(self.BASE_URL):]
                    else:
                        current_url = next_url
                    page_num += 1
                else:
                    # No next page found - check if we're close to total (fallback safety check)
                    if result.get("total_listings_count"):
                        scraped_ratio = len(all_listings) / result["total_listings_count"]
                        if scraped_ratio >= 0.99:
                            logger.info(f"No next page found and scraped {len(all_listings):,} listings ({scraped_ratio*100:.1f}% of {result['total_listings_count']:,}) - assuming complete")
                        else:
                            logger.warning(f"No next page found but only scraped {len(all_listings):,} listings ({scraped_ratio*100:.1f}% of {result['total_listings_count']:,}) - may be incomplete")
                    logger.info("No next page URL found - pagination complete")
                    break
                
        except Exception as e:
            logger.error(f"Scraper failed: {e}", exc_info=True)
            result["errors"].append(str(e))
        finally:
            _safe_driver_quit(driver)
        
        result["pages_scraped"] = page_num
        result["total_listings"] = len(all_listings)
        result["listings_data"] = all_listings
        result["scrape_duration"] = time.time() - start_time
        
        # Final JSON save (already saved incrementally, but ensure final save)
        if all_listings:
            with open(listings_file, 'w', encoding='utf-8') as f:
                json.dump(all_listings, f, indent=2, ensure_ascii=False)
            logger.info(f"Final save: {len(all_listings):,} total listings saved to JSON")
        
        logger.info("=" * 60)
        logger.info("Scrape Summary")
        logger.info(f"Pages scraped: {result['pages_scraped']}")
        if result.get("expected_pages"):
            logger.info(f"Expected pages: ~{result['expected_pages']}")
        logger.info(f"Total listings: {result['total_listings']:,}")
        if result.get("total_listings_count"):
            logger.info(f"Total listings on site: {result['total_listings_count']:,}")
        logger.info(f"HTML files saved: {len(result['html_files'])}")
        logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
        if result["errors"]:
            logger.warning(f"Errors encountered: {len(result['errors'])}")
        logger.info("=" * 60)
        
        return result
    
    def _save_html_page(self, html_content: str, page_num: int, output_dir: Path) -> Path:
        """Save HTML page to disk."""
        filename = f"page_{page_num:04d}.html"
        filepath = output_dir / filename
        html_bytes = html_content.encode('utf-8')
        with open(filepath, 'wb') as f:
            f.write(html_bytes)
        file_hash = hashlib.md5(html_bytes).hexdigest()
        file_size = filepath.stat().st_size
        logger.info(f"Saved page {page_num}: {filename} ({file_size:,} bytes, MD5: {file_hash})")
        return filepath
    
    def _extract_listings(self, html_content: str, page_url: str) -> List[Dict]:
        """Extract listings from HTML (same logic as Playwright version)."""
        listings = []
        scrape_timestamp = datetime.now().isoformat()
        seen_urls = set()
        
        try:
            import re
            soup = BeautifulSoup(html_content, 'html.parser')
            
            list_container = soup.find('div', id='listContainer', class_=lambda x: x and 'list-container' in str(x).lower())
            if not list_container:
                list_container = soup.find('div', class_=lambda x: x and 'list-container' in str(x).lower())
            
            if not list_container:
                listing_cards = soup.find_all('div', class_='list-listing-card-wrapper')
            else:
                listing_cards = list_container.find_all('div', class_='list-listing-card-wrapper')
            
            logger.info(f"Found {len(listing_cards)} listing card wrappers")
            
            for card_idx, card_wrapper in enumerate(listing_cards, 1):
                try:
                    listing_data = {
                        'listing_url': None,
                        'listing_id': None,
                        'aircraft_model': None,
                        'aircraft_type': None,  # e.g., "Jet Aircraft", "Piston Single Aircraft"
                        'year': None,  # Extracted from model
                        'listing_location': None,
                        'listing_price': None,
                        'total_time_hours': None,  # Total Time
                        'seller_name': None,
                        'seller_phone': None,
                        'seller_email': None,
                        'is_premium_listing': False,
                        'payment_estimate': None,  # "Payments as low as..."
                        'scrape_timestamp': scrape_timestamp,
                        'page_url': page_url,
                        'position': card_idx
                    }
                    
                    card_div = card_wrapper.find('div', class_='listing-card-grid', attrs={'data-listing-id': True})
                    if not card_div:
                        card_div = card_wrapper.find('div', id=re.compile(r'^\d+$'))
                        if not card_div:
                            card_div = card_wrapper.find('div', class_='listing-card-grid')
                    
                    if card_div:
                        listing_id = card_div.get('data-listing-id')
                        if not listing_id:
                            parent_div = card_wrapper.find('div', id=re.compile(r'^\d+$'))
                            if parent_div:
                                listing_id = parent_div.get('id')
                        if listing_id:
                            listing_data['listing_id'] = str(listing_id)
                    
                    listing_url = None
                    title_link = card_wrapper.find('a', class_='list-listing-title-link', href=re.compile(r'/listing/for-sale/'))
                    if title_link:
                        href = title_link.get('href', '')
                        if href and '/listing/for-sale/' in href:
                            listing_url = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                    
                    if not listing_url:
                        details_link = card_wrapper.find('a', class_='view-listing-details-link', href=re.compile(r'/listing/for-sale/'))
                        if details_link:
                            href = details_link.get('href', '')
                            if href and '/listing/for-sale/' in href:
                                listing_url = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                    
                    if not listing_url:
                        continue
                    
                    if listing_url in seen_urls:
                        continue
                    seen_urls.add(listing_url)
                    listing_data['listing_url'] = listing_url
                    
                    if not listing_data['listing_id']:
                        parsed_url = urlparse(listing_url)
                        path_parts = [p for p in parsed_url.path.split('/') if p]
                        if 'listing' in path_parts and 'for-sale' in path_parts:
                            listing_idx = path_parts.index('for-sale')
                            if listing_idx + 1 < len(path_parts):
                                listing_data['listing_id'] = path_parts[listing_idx + 1]
                    
                    title_h2 = card_wrapper.find('h2', class_='listing-portion-title')
                    if title_h2:
                        title_link = title_h2.find('a', class_='list-listing-title-link')
                        if title_link:
                            listing_data['aircraft_model'] = title_link.get_text(strip=True)
                        else:
                            listing_data['aircraft_model'] = title_h2.get_text(strip=True)
                    
                    price_container = card_wrapper.find('div', class_='retail-price-container')
                    if price_container:
                        price_span = price_container.find('span', class_='price')
                        if price_span:
                            listing_data['listing_price'] = price_span.get_text(strip=True)
                    
                    location_div = card_wrapper.find('div', class_='machine-location')
                    if location_div:
                        location_text = location_div.get_text(strip=True)
                        location_text = re.sub(r'^Location:\s*', '', location_text, flags=re.IGNORECASE).strip()
                        if location_text:
                            listing_data['listing_location'] = location_text
                    
                    # Extract Year from aircraft model (e.g., "1997 GULFSTREAM GV")
                    if listing_data['aircraft_model']:
                        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', listing_data['aircraft_model'])
                        if year_match:
                            listing_data['year'] = year_match.group(1)
                    
                    # Extract Aircraft Type (e.g., "Jet Aircraft", "Piston Single Aircraft")
                    aircraft_type_div = card_wrapper.find('div', class_=lambda x: x and 'aircraft-type' in str(x).lower())
                    if not aircraft_type_div:
                        # Try finding text that contains "Aircraft" after the model
                        type_text = card_wrapper.get_text()
                        type_match = re.search(r'(Jet Aircraft|Piston Single Aircraft|Piston Twin Aircraft|Turboprop Aircraft|Helicopter|Other)', type_text, re.IGNORECASE)
                        if type_match:
                            listing_data['aircraft_type'] = type_match.group(1)
                    
                    # Extract Total Time Hours
                    total_time_div = card_wrapper.find('div', class_=lambda x: x and 'total-time' in str(x).lower())
                    if not total_time_div:
                        # Look for "Total Time:" text pattern
                        card_text = card_wrapper.get_text()
                        tt_match = re.search(r'Total\s+Time[:\s]+([\d,]+\.?\d*)', card_text, re.IGNORECASE)
                        if tt_match:
                            listing_data['total_time_hours'] = tt_match.group(1).replace(',', '')
                    
                    # Extract Seller Name
                    seller_div = card_wrapper.find('div', class_=lambda x: x and 'seller' in str(x).lower())
                    if seller_div:
                        seller_text = seller_div.get_text(strip=True)
                        seller_match = re.search(r'Seller[:\s]+(.+)', seller_text, re.IGNORECASE)
                        if seller_match:
                            listing_data['seller_name'] = seller_match.group(1).strip()
                    
                    # Extract Seller Phone
                    phone_link = card_wrapper.find('a', href=re.compile(r'^tel:'))
                    if phone_link:
                        phone_href = phone_link.get('href', '')
                        phone_match = re.search(r'tel:([\d\-\(\)\s]+)', phone_href)
                        if phone_match:
                            listing_data['seller_phone'] = phone_match.group(1).strip()
                    else:
                        # Try finding phone in text
                        card_text = card_wrapper.get_text()
                        phone_match = re.search(r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', card_text)
                        if phone_match:
                            listing_data['seller_phone'] = phone_match.group(1).strip()
                    
                    # Extract Seller Email (check for email seller link)
                    email_link = card_wrapper.find('a', href=re.compile(r'^mailto:'))
                    if email_link:
                        email_href = email_link.get('href', '')
                        email_match = re.search(r'mailto:([^\s]+)', email_href)
                        if email_match:
                            listing_data['seller_email'] = email_match.group(1)
                    
                    # Check if Premium Listing
                    premium_badge = card_wrapper.find('span', class_=lambda x: x and 'premium' in str(x).lower())
                    if premium_badge or 'Premium Listing' in card_wrapper.get_text():
                        listing_data['is_premium_listing'] = True
                    
                    # Extract Payment Estimate (e.g., "Payments as low as USD $3,847.10*")
                    payment_text = card_wrapper.get_text()
                    payment_match = re.search(r'Payments\s+as\s+low\s+as\s+([^\\*]+)', payment_text, re.IGNORECASE)
                    if payment_match:
                        listing_data['payment_estimate'] = payment_match.group(1).strip()
                    
                    listings.append(listing_data)
                    
                except Exception as e:
                    logger.warning(f"Error extracting listing from card {card_idx}: {e}")
                    continue
            
            logger.info(f"Extracted {len(listings)} listings from page")
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}", exc_info=True)
        
        return listings
    
    def _extract_pagination_info(self, html_content: str) -> Optional[Dict]:
        """Extract pagination information from HTML."""
        try:
            import re
            soup = BeautifulSoup(html_content, 'html.parser')
            page_text = soup.get_text()
            pattern = r'(\d{1,4}(?:,\d{3})*)\s*-\s*(\d{1,4}(?:,\d{3})*)\s+of\s+(\d{1,4}(?:,\d{3})*)\s+Listings'
            match = re.search(pattern, page_text, re.IGNORECASE)
            
            if match:
                current_start = int(match.group(1).replace(',', ''))
                current_end = int(match.group(2).replace(',', ''))
                total_listings = int(match.group(3).replace(',', ''))
                is_last_page = (current_end >= total_listings)
                logger.info(f"Pagination: {current_start} - {current_end} of {total_listings:,} (Last: {is_last_page})")
                return {
                    'current_start': current_start,
                    'current_end': current_end,
                    'total_listings': total_listings,
                    'is_last_page': is_last_page
                }
            return None
        except Exception as e:
            logger.warning(f"Error extracting pagination info: {e}")
            return None
    
    def _find_next_page_url(self, html_content: str, current_url: str) -> Optional[str]:
        """Find next page URL."""
        current_page_num = self._extract_page_number(current_url)
        
        if not html_content and current_page_num:
            try:
                if not current_url.startswith('http'):
                    full_current_url = urljoin(self.BASE_URL, current_url)
                else:
                    full_current_url = current_url
                parsed = urlparse(full_current_url)
                query_params = parse_qs(parsed.query)
                if 'page' in query_params:
                    target_page = current_page_num + 1
                    query_params['page'] = [str(target_page)]
                    new_query = urlencode(query_params, doseq=True)
                    next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
                    logger.info(f"Constructed next page URL: {next_url}")
                    return next_url
            except Exception as e:
                logger.warning(f"Failed to construct next page URL: {e}")
                return None
        
        try:
            if not html_content:
                return None
            soup = BeautifulSoup(html_content, 'html.parser')
            if current_page_num:
                target_page = current_page_num + 1
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href', '')
                    link_page_num = self._extract_page_number(href)
                    if link_page_num == target_page:
                        if not href.startswith('http'):
                            href = urljoin(self.BASE_URL, href)
                        return href
                # Manual construction
                if not current_url.startswith('http'):
                    current_url = urljoin(self.BASE_URL, current_url)
                parsed = urlparse(current_url)
                query_params = parse_qs(parsed.query)
                if 'page' in query_params:
                    query_params['page'] = [str(target_page)]
                    new_query = urlencode(query_params, doseq=True)
                    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
            return None
        except Exception as e:
            logger.warning(f"Error finding next page: {e}")
            return None
    
    def _extract_page_number(self, url: str) -> Optional[int]:
        """Extract page number from URL."""
        try:
            import re
            patterns = [r'[?&]page=(\d+)', r'/page/(\d+)']
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    return int(match.group(1))
            return None
        except Exception:
            return None