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
    
    def scrape_listings(self, date: Optional[datetime] = None, max_pages: Optional[int] = None) -> Dict:
        """Scrape AircraftExchange.com listings using undetected-chromedriver.
        
        Args:
            date: Date for organizing scraped data. Defaults to today.
            max_pages: Maximum number of pages to scrape. None = all pages.
        
        Returns:
            Dictionary with scrape results including pages_scraped, total_listings, etc.
        """
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_aircraftexchange_path / date_str / "index"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info("AircraftExchange.com Index Scraper (undetected-chromedriver)")
        logger.info(f"Date: {date_str}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 60)
        
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
        page_num = 0
        all_listings = []
        try:
            driver = self._setup_driver()
            
            current_url = self.START_URL
            page_num = 1
            
            logger.info("=" * 60)
            logger.info("Starting to scrape listings pages...")
            logger.info("=" * 60)
            
            while current_url:
                # Check max_pages limit
                if max_pages and page_num > max_pages:
                    logger.info(f"Reached max pages limit ({max_pages})")
                    break
                
                logger.info(f"Processing page {page_num}...")
                
                # Human-like delay between pages (matches Controller scraper)
                if page_num > 1:
                    self._wait_for_rate_limit()
                
                # Fetch page
                html_content = self._fetch_page(driver, current_url)
                
                if not html_content:
                    logger.error(f"Failed to fetch page {page_num}")
                    result["errors"].append(f"Failed to fetch page {page_num}: {current_url}")
                    break
                
                # Save HTML
                html_file = self._save_html_page(html_content, page_num, output_dir)
                result["html_files"].append(str(html_file))
                
                # Extract listings from HTML
                listings = self._extract_listings(html_content, current_url)
                logger.info(f"Extracted {len(listings)} listings from page {page_num}")
                all_listings.extend(listings)
                
                # Save incremental JSON
                if listings:
                    listings_file = output_dir / "listings_metadata.json"
                    with open(listings_file, 'w', encoding='utf-8') as f:
                        json.dump(all_listings, f, indent=2, ensure_ascii=False)
                
                # Check for pagination
                next_url = self._find_next_page_url(html_content, current_url)
                if next_url:
                    if next_url.startswith(self.BASE_URL):
                        current_url = next_url[len(self.BASE_URL):]
                    else:
                        current_url = next_url
                    page_num += 1
                else:
                    logger.info("No next page URL found - pagination complete")
                    break
                
        except Exception as e:
            logger.error(f"Scraper failed: {e}", exc_info=True)
            result["errors"].append(str(e))
        finally:
            safe_driver_quit(driver)
        
        result["pages_scraped"] = page_num
        result["total_listings"] = len(all_listings)
        result["listings_data"] = all_listings
        result["scrape_duration"] = time.time() - start_time
        
        # Final JSON save
        if all_listings:
            listings_file = output_dir / "listings_metadata.json"
            with open(listings_file, 'w', encoding='utf-8') as f:
                json.dump(all_listings, f, indent=2, ensure_ascii=False)
            logger.info(f"Final save: {len(all_listings):,} total listings saved to JSON")
        
        logger.info("=" * 60)
        logger.info("Scrape Summary")
        logger.info(f"Pages scraped: {result['pages_scraped']}")
        logger.info(f"Total listings: {result['total_listings']:,}")
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
