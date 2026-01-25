"""AircraftExchange.com Listings Index Scraper using undetected-chromedriver.

Scrapes the main listings page: https://aircraftexchange.com/aircraft-for-sale/all

Extracts:
- Aircraft model
- Year
- Total time
- Asking price
- Dealer name
- Location
- Listing URL
- (and more available fields)

Install: pip install undetected-chromedriver selenium beautifulsoup4
"""

import hashlib
import json
import random
import re
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
from utils.chrome_utils import get_chrome_version, safe_driver_quit

logger = get_logger(__name__)


class AircraftExchangeScraperError(Exception):
    """Base exception for AircraftExchange scraper."""
    pass


class AircraftExchangeIndexScraperUndetected:
    """Scraper for AircraftExchange.com listings page using undetected-chromedriver.
    
    Scrapes: https://aircraftexchange.com/aircraft-for-sale/all
    
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
    
    BASE_URL = "https://aircraftexchange.com"
    START_URL = "/aircraft-for-sale/all"
    
    def __init__(self, storage_base_path: Optional[Path] = None, rate_limit: float = 6.0, headless: bool = False):
        """Initialize undetected-chromedriver scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
            rate_limit: Base seconds to wait between requests (will be randomized). Default: 6.0 seconds.
                        Actual delays will be 6-12 seconds to mimic human reading/thinking time.
            headless: Run browser in headless mode. Default: False (non-headless for better evasion).
        """
        if not UNDETECTED_AVAILABLE:
            raise AircraftExchangeScraperError(
                "undetected-chromedriver not installed. "
                "Install with: pip install undetected-chromedriver selenium beautifulsoup4"
            )
        
        if storage_base_path is None:
            storage_base_path = Path(__file__).parent.parent / "store"
        
        self.storage_base_path = Path(storage_base_path)
        self.raw_aircraftexchange_path = self.storage_base_path / "raw" / "aircraftexchange"
        self.raw_aircraftexchange_path.mkdir(parents=True, exist_ok=True)
        
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
        
        version_main = get_chrome_version()
        if version_main:
            logger.info(f"Detected Chrome version: {version_main}")
        driver = uc.Chrome(options=options, version_main=version_main)
        
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
        
        logger.info("Undetected Chrome driver initialized successfully")
        return driver
    
    def _simulate_human_behavior(self, driver):
        """Simulate human-like browsing behavior."""
        try:
            # 1. Random mouse movements (humans move mouse while reading)
            actions = ActionChains(driver)
            for _ in range(random.randint(2, 4)):
                x_offset = random.randint(-100, 100)
                y_offset = random.randint(-50, 50)
                actions.move_by_offset(x_offset, y_offset)
            actions.perform()
            time.sleep(random.uniform(0.3, 0.8))
            
            # 2. Gradual scrolling (humans scroll gradually, not all at once)
            scroll_steps = random.randint(3, 6)
            total_scroll = random.randint(500, 1200)
            scroll_per_step = total_scroll // scroll_steps
            
            for step in range(scroll_steps):
                scroll_amount = scroll_per_step + random.randint(-50, 50)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.5, 1.2))  # Pause between scrolls
            
            # 3. Scroll back up a bit (humans sometimes scroll back to re-read)
            if random.random() < 0.3:  # 30% chance
                scroll_back = random.randint(100, 300)
                driver.execute_script(f"window.scrollBy(0, -{scroll_back});")
                time.sleep(random.uniform(0.5, 1.0))
            
            # 4. Random pause (humans pause to "read" content)
            reading_pause = random.uniform(1.5, 4.0)
            logger.debug(f"Human reading pause: {reading_pause:.2f} seconds")
            time.sleep(reading_pause)
            
        except Exception as e:
            logger.debug(f"Error simulating human behavior: {e}")
            # Fallback: simple scroll if advanced features fail
            try:
                scroll_amount = random.randint(300, 600)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(2.0, 4.0))
            except Exception:
                pass
    
    def _wait_for_rate_limit(self):
        """Wait for rate limit delay with human-like randomization.
        Priority: avoid bot detection over speed. Slower = more human-like.
        Matches Controller scraper behavior.
        """
        base_delay = self.rate_limit
        reading_time = random.uniform(3.0, 6.0)  # 3-6 seconds "reading" per page
        jitter = random.uniform(0.9, 1.3)
        delay = base_delay + reading_time * jitter
        logger.debug(f"Human-like delay: {delay:.2f} seconds (mimicking reading time)")
        time.sleep(delay)
    
    def _fetch_page(self, driver, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a page using Selenium and wait for content to load."""
        full_url = urljoin(self.BASE_URL, url) if not url.startswith('http') else url
        
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Navigating to: {full_url} (attempt {attempt}/{retries})")
                
                # Human-like: brief pause before navigation (matches Controller)
                pre_page_pause = random.uniform(1.0, 2.5)
                logger.debug(f"Pre-navigation pause: {pre_page_pause:.2f} seconds")
                time.sleep(pre_page_pause)
                
                driver.get(full_url)
                
                # Wait for page to load - wait for listings to appear
                try:
                    wait = WebDriverWait(driver, 30)
                    # Wait for listings container or individual listing cards
                    try:
                        # Try to find listings container
                        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "border-6")))
                        logger.debug("Listings container found")
                    except TimeoutException:
                        # Fallback: try to find individual listing divs
                        try:
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.w-full.sm\\:w-1\\/2.lg\\:w-1\\/4.mb-4")))
                            logger.debug("Listing cards found")
                        except TimeoutException:
                            logger.warning("Listings not found, continuing anyway")
                except Exception as e:
                    logger.debug(f"Wait for elements failed: {e}, continuing")
                
                # Human-like: Wait for page to fully render (matches Controller)
                initial_wait = random.uniform(4, 8)
                logger.debug(f"Initial page load wait: {initial_wait:.2f} seconds (human-like)")
                time.sleep(initial_wait)
                
                # Simulate human behavior
                self._simulate_human_behavior(driver)
                
                # Additional wait for any lazy-loaded content (matches Controller)
                post_scroll_wait = random.uniform(3, 6)
                logger.debug(f"Post-scroll wait: {post_scroll_wait:.2f} seconds (human-like)")
                time.sleep(post_scroll_wait)
                
                # Get page source
                html_content = driver.page_source
                content_length = len(html_content)
                logger.info(f"Retrieved {content_length:,} bytes from {full_url}")
                
                # Check for bot detection
                if content_length < 50000:
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
                    # Large page - check if we have listing content
                    if 'aircraft-for-sale' in html_content or 'View Details' in html_content:
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
    
    def _page_html_path(self, page_num: int, output_dir: Path) -> Path:
        return output_dir / f"page_{page_num:04d}.html"

    def _discover_done_pages(self, output_dir: Path) -> List[int]:
        pages = []
        for f in output_dir.glob("page_*.html"):
            m = re.match(r"page_(\d+)\.html", f.name)
            if m:
                pages.append(int(m.group(1)))
        return sorted(pages)

    def _backfill_listings_from_html(
        self, output_dir: Path, all_listings: List[Dict], done_pages: List[int], listings_file: Path
    ) -> List[Dict]:
        done_urls = {l.get("listing_url") for l in all_listings if l.get("listing_url")}
        added = 0
        for p in done_pages:
            path = self._page_html_path(p, output_dir)
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    html = f.read()
            except Exception as e:
                logger.warning("Backfill: could not read %s: %s", path.name, e)
                continue
            page_url = self.START_URL if p == 1 else f"{self.START_URL}?page={p}"
            full_url = urljoin(self.BASE_URL, page_url)
            listings = self._extract_listings(html, full_url)
            for li in listings:
                u = li.get("listing_url")
                if u and u not in done_urls:
                    done_urls.add(u)
                    all_listings.append(li)
                    added += 1
        if added:
            with open(listings_file, "w", encoding="utf-8") as f:
                json.dump(all_listings, f, indent=2, ensure_ascii=False)
            logger.info("Backfill: added %d listings from %d HTML pages -> JSON", added, len(done_pages))
        return all_listings

    def scrape_listings(self, date: Optional[datetime] = None, max_pages: Optional[int] = None) -> Dict:
        """Scrape AircraftExchange.com listings. Skip-if-exists + backfill (same date).
        Re-runs skip pages with HTML; append only new. If HTML exists but JSON not saved,
        backfill from HTML -> JSON. No overwrite/delete.
        """
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_aircraftexchange_path / date_str / "index"
        output_dir.mkdir(parents=True, exist_ok=True)
        listings_file = output_dir / "listings_metadata.json"
        
        logger.info("=" * 60)
        logger.info("AircraftExchange.com Index Scraper (undetected-chromedriver)")
        logger.info(f"Date: {date_str}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("Skip-if-exists + backfill: same date re-runs safe; no overwrite/delete.")
        logger.info("=" * 60)
        
        all_listings: List[Dict] = []
        if listings_file.exists():
            try:
                with open(listings_file, "r", encoding="utf-8") as f:
                    all_listings = json.load(f)
                logger.info("Loaded %d existing listings from JSON", len(all_listings))
            except Exception as e:
                logger.warning("Could not load JSON: %s, starting fresh", e)
                all_listings = []
        
        done_pages = self._discover_done_pages(output_dir)
        if done_pages:
            logger.info("Found %d pages with HTML (skip-if-exists)", len(done_pages))
        all_listings = self._backfill_listings_from_html(
            output_dir, all_listings, done_pages, listings_file
        )
        done_pages_set = set(done_pages)
        done_urls = {l.get("listing_url") for l in all_listings if l.get("listing_url")}
        
        result = {
            "date": date_str,
            "pages_scraped": 0,
            "pages_skipped": 0,
            "total_listings": 0,
            "html_files": [],
            "listings_data": [],
            "scrape_duration": 0,
            "errors": [],
        }
        
        # Early exit: if we have page 1 HTML and no pagination, we're done
        if done_pages and 1 in done_pages_set:
            page1_path = self._page_html_path(1, output_dir)
            try:
                with open(page1_path, "r", encoding="utf-8", errors="replace") as f:
                    html_content = f.read()
                next_url = self._find_next_page_url(html_content, urljoin(self.BASE_URL, self.START_URL))
                if not next_url:
                    # No pagination, all done
                    logger.info("All pages already scraped (page 1 exists, no pagination); nothing to do.")
                    result["pages_skipped"] = len(done_pages)
                    result["total_listings"] = len(all_listings)
                    result["scrape_duration"] = time.time() - start_time
                    if all_listings:
                        with open(listings_file, "w", encoding="utf-8") as f:
                            json.dump(all_listings, f, indent=2, ensure_ascii=False)
                    logger.info("=" * 60)
                    logger.info("Scrape Summary")
                    logger.info("Pages scraped this run: %d", result["pages_scraped"])
                    logger.info("Pages skipped (existing HTML): %d", result.get("pages_skipped", 0))
                    logger.info("Total listings: %d", result["total_listings"])
                    logger.info("HTML files saved: %d", len(result["html_files"]))
                    logger.info("Scrape duration: %.2f seconds", result["scrape_duration"])
                    logger.info("=" * 60)
                    return result
            except Exception as e:
                logger.debug("Could not check page 1 for early exit: %s", e)
        
        driver = None
        page_num = 1
        current_url = self.START_URL
        last_fetched_page = 0
        
        logger.info("=" * 60)
        logger.info("Starting pages (skip existing, append only)...")
        logger.info("=" * 60)
        
        try:
            driver = self._setup_driver()
            while current_url:
                if max_pages and page_num > max_pages:
                    logger.info("Reached max pages limit (%d)", max_pages)
                    break
                
                page_url = self.START_URL if page_num == 1 else f"{self.START_URL}?page={page_num}"
                full_page_url = urljoin(self.BASE_URL, page_url)
                
                if page_num in done_pages_set:
                    result["pages_skipped"] += 1
                    path = self._page_html_path(page_num, output_dir)
                    try:
                        with open(path, "r", encoding="utf-8", errors="replace") as f:
                            html_content = f.read()
                    except Exception as e:
                        logger.warning("Could not read %s: %s, will re-fetch", path.name, e)
                        done_pages_set.discard(page_num)
                        result["pages_skipped"] -= 1
                        continue
                    next_url = self._find_next_page_url(html_content, full_page_url)
                    if next_url:
                        current_url = next_url[len(self.BASE_URL):] if next_url.startswith(self.BASE_URL) else next_url
                        page_num += 1
                        continue
                    break
                
                logger.info("Processing page %d...", page_num)
                if last_fetched_page and page_num > last_fetched_page:
                    time.sleep(random.uniform(1.0, 3.0))
                if page_num > 1:
                    self._wait_for_rate_limit()
                html_content = self._fetch_page(driver, current_url)
                if not html_content:
                    result["errors"].append(f"Page {page_num}: Failed to fetch")
                    break
                self._save_html_page(html_content, page_num, output_dir)
                result["html_files"].append(str(self._page_html_path(page_num, output_dir)))
                result["pages_scraped"] += 1
                done_pages_set.add(page_num)
                last_fetched_page = page_num
                
                listings = self._extract_listings(html_content, full_page_url)
                new_count = 0
                for li in listings:
                    u = li.get("listing_url")
                    if u and u not in done_urls:
                        done_urls.add(u)
                        all_listings.append(li)
                        new_count += 1
                logger.info("Page %d: Extracted %d listings (%d new, total: %d)", page_num, len(listings), new_count, len(all_listings))
                
                with open(listings_file, "w", encoding="utf-8") as f:
                    json.dump(all_listings, f, indent=2, ensure_ascii=False)
                
                next_url = self._find_next_page_url(html_content, full_page_url)
                if next_url:
                    current_url = next_url[len(self.BASE_URL):] if next_url.startswith(self.BASE_URL) else next_url
                    page_num += 1
                else:
                    logger.info("No next page URL found - pagination complete")
                    break
                
        except Exception as e:
            logger.error("Scraper failed: %s", e, exc_info=True)
            result["errors"].append(str(e))
        finally:
            safe_driver_quit(driver)
        
        result["total_listings"] = len(all_listings)
        result["listings_data"] = all_listings
        result["scrape_duration"] = time.time() - start_time
        
        if all_listings:
            with open(listings_file, "w", encoding="utf-8") as f:
                json.dump(all_listings, f, indent=2, ensure_ascii=False)
            logger.info("Final save: %d total listings saved to JSON", len(all_listings))
        
        logger.info("=" * 60)
        logger.info("Scrape Summary")
        logger.info("Pages scraped this run: %d", result["pages_scraped"])
        logger.info("Pages skipped (existing HTML): %d", result.get("pages_skipped", 0))
        logger.info("Total listings: %d", result["total_listings"])
        logger.info("HTML files saved: %d", len(result["html_files"]))
        logger.info("Scrape duration: %.2f seconds", result["scrape_duration"])
        if result["errors"]:
            logger.warning("Errors encountered: %d", len(result["errors"]))
        logger.info("=" * 60)
        return result
    
    def _save_html_page(self, html_content: str, page_num: int, output_dir: Path) -> Path:
        """Save HTML page to disk."""
        path = self._page_html_path(page_num, output_dir)
        html_bytes = html_content.encode("utf-8")
        with open(path, "wb") as f:
            f.write(html_bytes)
        file_size = path.stat().st_size
        logger.info("Saved page %d: %s (%d bytes)", page_num, path.name, file_size)
        return path
    
    def _extract_listings(self, html_content: str, page_url: str) -> List[Dict]:
        """Extract listings from HTML.
        
        Based on the HTML structure from AircraftExchange:
        <div class="w-full sm:w-1/2 lg:w-1/4 mb-4">
            <h5 class="text-xs">1967 Bell 205</h5>
            <p class="text-xs">Offered by: Wetzel Aviation, Inc.</p>
            <p class="text-xs"><a href="...">View Details</a></p>
        </div>
        """
        listings = []
        scrape_timestamp = datetime.now().isoformat()
        seen_urls = set()
        
        try:
            import re
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all listing divs
            # Try multiple patterns to find listings
            listing_divs = soup.find_all('div', class_=lambda x: x and 'w-full' in str(x) and 'sm:w-1/2' in str(x) and 'lg:w-1/4' in str(x) and 'mb-4' in str(x))
            
            # Fallback: look for divs containing "View Details" links
            if not listing_divs:
                listing_divs = []
                for div in soup.find_all('div'):
                    link = div.find('a', string=re.compile(r'View Details', re.I))
                    if link and div.find('h5'):
                        listing_divs.append(div)
            
            logger.info(f"Found {len(listing_divs)} listing divs")
            
            for div_idx, listing_div in enumerate(listing_divs, 1):
                try:
                    listing_data = {
                        'listing_url': None,
                        'aircraft_model': None,
                        'year': None,
                        'total_time': None,
                        'asking_price': None,
                        'dealer_name': None,
                        'location': None,
                        'scrape_timestamp': scrape_timestamp,
                        'page_url': page_url,
                        'page_position': div_idx
                    }
                    
                    # Extract aircraft model (from h5)
                    h5 = listing_div.find('h5', class_='text-xs')
                    if h5:
                        model_text = h5.get_text(strip=True)
                        listing_data['aircraft_model'] = model_text
                        
                        # Try to extract year from model (e.g., "1967 Bell 205")
                        year_match = re.search(r'^(\d{4})\s+', model_text)
                        if year_match:
                            listing_data['year'] = year_match.group(1)
                    
                    # Extract dealer name (from "Offered by: ...")
                    paragraphs = listing_div.find_all('p', class_='text-xs')
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        if 'Offered by:' in text:
                            dealer_match = re.search(r'Offered by:\s*(.+)', text)
                            if dealer_match:
                                listing_data['dealer_name'] = dealer_match.group(1).strip()
                        
                        # Extract "View Details" link
                        link = p.find('a', href=True)
                        if link and 'View Details' in link.get_text():
                            href = link.get('href')
                            if href:
                                if href.startswith('http'):
                                    listing_data['listing_url'] = href
                                else:
                                    listing_data['listing_url'] = urljoin(self.BASE_URL, href)
                    
                    # Only add if we have at least a listing URL
                    if listing_data['listing_url']:
                        if listing_data['listing_url'] not in seen_urls:
                            seen_urls.add(listing_data['listing_url'])
                            listings.append(listing_data)
                        else:
                            logger.debug(f"Duplicate listing URL skipped: {listing_data['listing_url']}")
                    else:
                        logger.warning(f"Listing div {div_idx} missing listing URL")
                
                except Exception as e:
                    logger.warning(f"Error extracting listing {div_idx}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error extracting listings: {e}", exc_info=True)
        
        return listings
    
    def _find_next_page_url(self, html_content: str, current_url: str) -> Optional[str]:
        """Find next page URL from HTML.
        
        AircraftExchange may use different pagination patterns.
        Check for common patterns like "Next" links, page numbers, etc.
        """
        try:
            import re
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for "Next" link or pagination buttons
            # Common patterns: <a href="...">Next</a>, <a class="next">, etc.
            next_link = soup.find('a', string=re.compile(r'Next', re.I))
            if next_link and next_link.get('href'):
                href = next_link.get('href')
                if href.startswith('http'):
                    return href
                else:
                    return urljoin(self.BASE_URL, href)
            
            # Look for pagination with page numbers
            # Try to find current page number and increment
            current_page_num = self._extract_page_number(current_url)
            if current_page_num:
                next_page_num = current_page_num + 1
                # Try to construct next page URL
                if '?' in current_url:
                    base_url, query = current_url.split('?', 1)
                    params = parse_qs(query)
                    params['page'] = [str(next_page_num)]
                    next_url = f"{base_url}?{urlencode(params, doseq=True)}"
                else:
                    next_url = f"{current_url.rstrip('/')}?page={next_page_num}"
                
                logger.info(f"Constructed next page URL: {next_url}")
                return next_url
            
            # Check if there are more listings visible (heuristic)
            # If we found listings, assume there might be more pages
            # This is a fallback - better to have explicit pagination detection
            
        except Exception as e:
            logger.warning(f"Error finding next page: {e}")
            return None
        
        return None
    
    def _extract_page_number(self, url: str) -> Optional[int]:
        """Extract page number from URL."""
        try:
            import re
            patterns = [r'[?&]page=(\d+)', r'/page/(\d+)', r'/all\?page=(\d+)']
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    return int(match.group(1))
            return None
        except Exception:
            return None
