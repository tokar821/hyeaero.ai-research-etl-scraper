"""AircraftExchange.com Manufacturer-Based Detail Scraper using undetected-chromedriver.

This scraper:
1. Loads manufacturer URLs from manufacturers_metadata.json
2. For each manufacturer, visits their listings page
3. Extracts all listing URLs from the manufacturer's listings page
4. Scrapes each detail page

This is different from the regular detail scraper because it:
- Works with manufacturer-specific listing pages
- Handles pagination on manufacturer listing pages
- Then scrapes detail pages from those listings

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


class AircraftExchangeManufacturerDetailScraperError(Exception):
    """Base exception for AircraftExchange manufacturer detail scraper."""
    pass


class AircraftExchangeManufacturerDetailScraperUndetected:
    """Scraper for AircraftExchange.com manufacturer-based detail pages using undetected-chromedriver.
    
    Workflow:
    1. Load manufacturers_metadata.json (from manufacturer scraper)
    2. For each manufacturer, visit their listings page
    3. Extract all listing URLs from manufacturer's listings page (handle pagination)
    4. Scrape each detail page
    
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
    
    BASE_URL = "https://aircraftexchange.com"
    
    def __init__(self, storage_base_path: Optional[Path] = None, rate_limit: float = 6.0, headless: bool = False):
        """Initialize undetected-chromedriver manufacturer detail scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
            rate_limit: Base seconds to wait between requests (will be randomized). Default: 6.0 seconds.
                        Actual delays will be 6-12 seconds to mimic human reading/thinking time.
            headless: Run browser in headless mode. Default: False (non-headless for better evasion).
        """
        if not UNDETECTED_AVAILABLE:
            raise AircraftExchangeManufacturerDetailScraperError(
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
        
        # Human-like browser window size
        window_width = random.randint(1366, 1920)
        window_height = random.randint(768, 1080)
        options.add_argument(f'--window-size={window_width},{window_height}')
        
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--lang=en-US')
        options.add_argument('--disable-infobars')
        
        version_main = get_chrome_version()
        if version_main:
            logger.info(f"Detected Chrome version: {version_main}")
        driver = uc.Chrome(options=options, version_main=version_main)
        
        # Set window size
        driver.set_window_size(window_width, window_height)
        
        # Human-like: Set window position
        if not self.headless:
            try:
                x_offset = random.randint(0, 100)
                y_offset = random.randint(0, 100)
                driver.set_window_position(x_offset, y_offset)
            except Exception:
                pass
        
        logger.info("Undetected Chrome driver initialized successfully")
        return driver
    
    def _simulate_human_behavior(self, driver):
        """Simulate realistic human-like behavior on the page."""
        try:
            viewport_width = driver.execute_script("return window.innerWidth")
            viewport_height = driver.execute_script("return window.innerHeight")
            
            actions = ActionChains(driver)
            
            if random.random() > 0.3:
                mouse_x = random.randint(100, max(200, viewport_width - 100))
                mouse_y = random.randint(100, max(200, viewport_height - 100))
                try:
                    current_x = viewport_width // 2
                    current_y = viewport_height // 2
                    actions.move_by_offset(mouse_x - current_x, mouse_y - current_y)
                    actions.perform()
                    time.sleep(random.uniform(0.2, 0.5))
                except Exception:
                    pass
            
            # Gradual scrolling
            scroll_steps = random.randint(4, 8)
            total_scroll = random.randint(800, 2000)
            scroll_per_step = total_scroll // scroll_steps
            
            for step in range(scroll_steps):
                scroll_amount = scroll_per_step + random.randint(-50, 50)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.5, 1.5))
            
            if random.random() < 0.3:
                scroll_back = random.randint(200, 500)
                driver.execute_script(f"window.scrollBy(0, -{scroll_back});")
                time.sleep(random.uniform(0.5, 1.0))
            
            reading_pause = random.uniform(2.0, 5.0)
            logger.debug(f"Human reading pause: {reading_pause:.2f} seconds")
            time.sleep(reading_pause)
            
        except Exception as e:
            logger.debug(f"Error simulating human behavior: {e}")
            try:
                scroll_amount = random.randint(500, 1000)
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
        reading_time = random.uniform(4.0, 8.0)  # 4-8 seconds "reading" per page/listing
        jitter = random.uniform(1.0, 1.4)
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
                
                # Wait for page to load
                try:
                    wait = WebDriverWait(driver, 30)
                    try:
                        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        logger.debug("Page body found")
                    except TimeoutException:
                        logger.warning("Page body not found, continuing anyway")
                except Exception as e:
                    logger.debug(f"Wait for elements failed: {e}, continuing")
                
                # Human-like: Wait for page to fully render (matches Controller)
                initial_wait = random.uniform(4, 8)
                logger.debug(f"Initial page load wait: {initial_wait:.2f} seconds (human-like)")
                time.sleep(initial_wait)
                
                self._simulate_human_behavior(driver)
                
                # Additional wait for any lazy-loaded content (matches Controller)
                post_scroll_wait = random.uniform(3, 6)
                logger.debug(f"Post-scroll wait: {post_scroll_wait:.2f} seconds (human-like)")
                time.sleep(post_scroll_wait)
                
                html_content = driver.page_source
                content_length = len(html_content)
                logger.info(f"Retrieved {content_length:,} bytes from {full_url}")
                
                if content_length < 50000:
                    if 'Pardon Our Interruption' in html_content or 'distil_referrer' in html_content:
                        logger.warning(f"Bot detection page detected for {full_url}")
                        if attempt < retries:
                            wait_time = attempt * 10
                            logger.warning(f"Waiting {wait_time}s before retry (attempt {attempt}/{retries})")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Bot detection page persisted after {retries} attempts")
                            return None
                
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
    
    def scrape_manufacturer_listings(self, manufacturer: Dict, date: Optional[datetime] = None, max_pages: Optional[int] = None, max_models: Optional[int] = None) -> List[Dict]:
        """Scrape all listing URLs from a manufacturer's listings page.
        
        Workflow:
        1. Visit manufacturer page (shows model categories)
        2. Extract model category links
        3. Visit each model category page (shows actual listings)
        4. Extract listing URLs from model pages
        5. Handle pagination on model pages
        
        Args:
            manufacturer: Dictionary with 'name', 'url', and 'manufacturer_id' keys.
            date: Date for organizing scraped data.
            max_pages: Maximum number of pages to scrape per model. None = all pages.
            max_models: Maximum number of model categories to process. None = all models.
        
        Returns:
            List of listing dictionaries with listing_url and other basic info.
        """
        if date is None:
            date = datetime.now()
        
        date_str = date.strftime("%Y-%m-%d")
        manufacturer_id = manufacturer.get('manufacturer_id', 'unknown')
        manufacturer_name = manufacturer.get('name', 'unknown').lower().replace(' ', '_')
        
        output_dir = self.raw_aircraftexchange_path / date_str / "manufacturers" / f"{manufacturer_id}_{manufacturer_name}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info(f"Scraping listings for manufacturer: {manufacturer['name']}")
        logger.info(f"URL: {manufacturer['url']}")
        logger.info("=" * 60)
        
        all_listings = []
        driver = None
        
        try:
            driver = self._setup_driver()
            
            # Step 1: Visit manufacturer page to get model category links
            logger.info("Step 1: Fetching manufacturer page to find model categories...")
            manufacturer_html = self._fetch_page(driver, manufacturer['url'])
            
            if not manufacturer_html:
                logger.error(f"Failed to fetch manufacturer page for {manufacturer['name']}")
                return []
            
            # Save manufacturer page HTML
            mfg_html_file = output_dir / "manufacturer_page.html"
            with open(mfg_html_file, 'wb') as f:
                f.write(manufacturer_html.encode('utf-8'))
            logger.info(f"Saved manufacturer page HTML")
            
            # Extract model category links
            model_links = self._extract_model_category_links(manufacturer_html, manufacturer['url'])
            logger.info(f"Found {len(model_links)} model categories")
            
            if not model_links:
                # Fallback: Maybe this manufacturer page shows listings directly
                logger.info("No model categories found, checking if page shows listings directly...")
                direct_listings = self._extract_listings_from_manufacturer_page(manufacturer_html, manufacturer['url'], manufacturer['name'])
                if direct_listings:
                    logger.info(f"Found {len(direct_listings)} direct listings on manufacturer page")
                    all_listings.extend(direct_listings)
                    return all_listings
            
            if max_models:
                model_links = model_links[:max_models]
                logger.info(f"Limiting to {max_models} model categories")
            
            # Step 2: Visit each model category page and extract listings
            for model_idx, model_link in enumerate(model_links, 1):
                logger.info("=" * 60)
                logger.info(f"Model {model_idx}/{len(model_links)}: {model_link.get('model_name', 'Unknown')}")
                logger.info(f"URL: {model_link['url']}")
                logger.info("=" * 60)
                
                # Human-like delay between models
                if model_idx > 1:
                    model_delay = random.uniform(self.rate_limit, self.rate_limit * 2)
                    logger.info(f"Human-like delay before next model: {model_delay:.2f} seconds")
                    time.sleep(model_delay)
                
                # Scrape listings from this model category page
                model_listings = self._scrape_model_category_listings(
                    driver, 
                    model_link, 
                    manufacturer['name'],
                    output_dir,
                    date_str,
                    max_pages
                )
                logger.info(f"Found {len(model_listings)} listings for {model_link.get('model_name', 'Unknown')}")
                all_listings.extend(model_listings)
                
                # Save incremental JSON
                if all_listings:
                    listings_file = output_dir / "manufacturer_listings_metadata.json"
                    with open(listings_file, 'w', encoding='utf-8') as f:
                        json.dump(all_listings, f, indent=2, ensure_ascii=False)
        
        except Exception as e:
            logger.error(f"Error scraping manufacturer listings for {manufacturer['name']}: {e}", exc_info=True)
        finally:
            safe_driver_quit(driver)
        
        logger.info(f"Total listings found for {manufacturer['name']}: {len(all_listings)}")
        return all_listings
    
    def _extract_model_category_links(self, html_content: str, manufacturer_url: str) -> List[Dict]:
        """Extract model category links from manufacturer page.
        
        HTML structure:
        <a href="https://aircraftexchange.com/aircraft-for-sale/75/acj/297/acj320">ACJ320</a>
        <span class="bg-yellow-dark ..."><a ...>2</a></span>  (count badge)
        """
        model_links = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all links that point to model category pages
            # Pattern: /aircraft-for-sale/{manufacturer_id}/{manufacturer_slug}/{model_id}/{model_slug}
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href')
                if href and '/aircraft-for-sale/' in href:
                    # Check if it's a model category link (has manufacturer and model IDs)
                    # Pattern: /aircraft-for-sale/{mfg_id}/{mfg_slug}/{model_id}/{model_slug}
                    match = re.search(r'/aircraft-for-sale/(\d+)/([^/]+)/(\d+)/([^/]+)', href)
                    if match:
                        model_name = link.get_text(strip=True)
                        # Skip if it's just a number (count badge)
                        if not model_name.isdigit() and model_name:
                            full_url = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                            
                            # Check if we already have this model
                            if not any(m['url'] == full_url for m in model_links):
                                model_links.append({
                                    'model_name': model_name,
                                    'url': full_url,
                                    'model_id': match.group(3)
                                })
            
            logger.info(f"Extracted {len(model_links)} model category links")
            
        except Exception as e:
            logger.error(f"Error extracting model category links: {e}", exc_info=True)
        
        return model_links
    
    def _scrape_model_category_listings(self, driver, model_link: Dict, manufacturer_name: str, output_dir: Path, date_str: str, max_pages: Optional[int] = None) -> List[Dict]:
        """Scrape listings from a model category page (handles pagination).
        
        Args:
            driver: Selenium driver instance.
            model_link: Dictionary with 'model_name' and 'url'.
            manufacturer_name: Name of the manufacturer.
            output_dir: Base output directory for this manufacturer.
            date_str: Date string for organizing files.
            max_pages: Maximum pages to scrape for this model. None = all pages.
        
        Returns:
            List of listing dictionaries.
        """
        all_listings = []
        model_name = model_link.get('model_name', 'unknown').lower().replace(' ', '_')
        model_output_dir = output_dir / f"models" / model_name
        model_output_dir.mkdir(parents=True, exist_ok=True)
        
        current_url = model_link['url']
        page_num = 1
        
        while current_url:
            if max_pages and page_num > max_pages:
                logger.info(f"Reached max pages limit ({max_pages}) for model {model_link['model_name']}")
                break
            
            logger.info(f"Processing page {page_num} for model {model_link['model_name']}...")
            
            if page_num > 1:
                self._wait_for_rate_limit()
            
            html_content = self._fetch_page(driver, current_url)
            
            if not html_content:
                logger.error(f"Failed to fetch page {page_num} for model {model_link['model_name']}")
                break
            
            # Save HTML
            html_file = self._save_html_page(html_content, page_num, model_output_dir)
            
            # Extract listings from this page
            listings = self._extract_listings_from_manufacturer_page(html_content, current_url, manufacturer_name)
            logger.info(f"Extracted {len(listings)} listings from page {page_num}")
            all_listings.extend(listings)
            
            # Check for pagination
            next_url = self._find_next_page_url(html_content, current_url)
            if next_url:
                if next_url.startswith(self.BASE_URL):
                    current_url = next_url[len(self.BASE_URL):]
                else:
                    current_url = next_url
                page_num += 1
            else:
                logger.info(f"No next page URL found for model {model_link['model_name']} - pagination complete")
                break
        
        return all_listings
    
    def _extract_listings_from_manufacturer_page(self, html_content: str, page_url: str, manufacturer_name: str) -> List[Dict]:
        """Extract listing URLs from a manufacturer's listings page.
        
        Handles two structures:
        1. Index-style: divs with class "w-full sm:w-1/2 lg:w-1/4 mb-4"
        2. Model category page: divs with class "aircraft-box" containing "aircraft" divs
        """
        listings = []
        scrape_timestamp = datetime.now().isoformat()
        seen_urls = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Method 1: Try index-style structure (w-full sm:w-1/2 lg:w-1/4 mb-4)
            listing_divs = soup.find_all('div', class_=lambda x: x and 'w-full' in str(x) and 'sm:w-1/2' in str(x) and 'lg:w-1/4' in str(x) and 'mb-4' in str(x))
            
            # Method 2: Try model category page structure (aircraft-box)
            if not listing_divs:
                listing_divs = soup.find_all('div', class_='aircraft-box')
                logger.info(f"Found {len(listing_divs)} aircraft-box divs (model category page structure)")
            
            # Method 3: Fallback - look for divs containing "View Details" links
            if not listing_divs:
                listing_divs = []
                for div in soup.find_all('div'):
                    link = div.find('a', string=re.compile(r'View Details', re.I))
                    if link:
                        # Check if it has aircraft info (h2, h5, or aircraft model text)
                        if div.find(['h2', 'h5']) or 'aircraft' in div.get_text().lower():
                            listing_divs.append(div)
            
            logger.info(f"Found {len(listing_divs)} listing containers")
            
            for div_idx, listing_div in enumerate(listing_divs, 1):
                try:
                    listing_data = {
                        'listing_url': None,
                        'aircraft_model': None,
                        'year': None,
                        'dealer_name': None,
                        'manufacturer': manufacturer_name,
                        'manufacturer_page_url': page_url,
                        'scrape_timestamp': scrape_timestamp,
                        'page_position': div_idx
                    }
                    
                    # Extract listing URL - look for "View Details" link or detail page link
                    detail_links = listing_div.find_all('a', href=re.compile(r'/details/\d+/', re.I))
                    if detail_links:
                        href = detail_links[0].get('href')
                        if href:
                            if href.startswith('http'):
                                listing_data['listing_url'] = href
                            else:
                                listing_data['listing_url'] = urljoin(self.BASE_URL, href)
                    
                    # Also check for links in h2 or other headings
                    if not listing_data['listing_url']:
                        h2_link = listing_div.find('h2')
                        if h2_link:
                            parent_link = h2_link.find_parent('a', href=True)
                            if parent_link:
                                href = parent_link.get('href')
                                if '/details/' in href:
                                    listing_data['listing_url'] = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                    
                    # Extract aircraft model
                    h2 = listing_div.find('h2', class_='font-semibold')
                    if h2:
                        model_text = h2.get_text(strip=True)
                        listing_data['aircraft_model'] = model_text
                        # Extract year
                        year_match = re.search(r'^(\d{4})\s+', model_text)
                        if year_match:
                            listing_data['year'] = year_match.group(1)
                    else:
                        # Try h5 (index page style)
                        h5 = listing_div.find('h5', class_='text-xs')
                        if h5:
                            model_text = h5.get_text(strip=True)
                            listing_data['aircraft_model'] = model_text
                            year_match = re.search(r'^(\d{4})\s+', model_text)
                            if year_match:
                                listing_data['year'] = year_match.group(1)
                    
                    # Extract dealer name
                    # Look for "Offered by:" text
                    offered_by_elem = listing_div.find(string=re.compile(r'Offered by:', re.I))
                    if offered_by_elem:
                        parent = offered_by_elem.find_parent()
                        if parent:
                            # Get dealer name from link or text after "Offered by:"
                            dealer_link = parent.find('a', href=re.compile(r'/aircraft-by-broker/'))
                            if dealer_link:
                                listing_data['dealer_name'] = dealer_link.get_text(strip=True)
                            else:
                                # Extract from text
                                dealer_text = parent.get_text()
                                dealer_match = re.search(r'Offered by:\s*([^\n]+)', dealer_text, re.I)
                                if dealer_match:
                                    listing_data['dealer_name'] = dealer_match.group(1).strip()
                    
                    # Also try paragraphs (index page style)
                    if not listing_data['dealer_name']:
                        paragraphs = listing_div.find_all('p', class_='text-xs')
                        for p in paragraphs:
                            text = p.get_text(strip=True)
                            if 'Offered by:' in text:
                                dealer_match = re.search(r'Offered by:\s*(.+)', text)
                                if dealer_match:
                                    listing_data['dealer_name'] = dealer_match.group(1).strip()
                    
                    # Extract additional info from model category page structure
                    if listing_div.find('div', class_='aircraft'):
                        info_div = listing_div.find('div', class_='information')
                        if info_div:
                            # Extract serial number, hours, cycles from ul list
                            info_ul = info_div.find('ul', class_='list-reset')
                            if info_ul:
                                list_items = info_ul.find_all('li')
                                for li in list_items:
                                    label_span = li.find('span', class_='font-bold uppercase tracking-wide')
                                    if label_span:
                                        label = label_span.get_text(strip=True)
                                        value = li.get_text().replace(label, '', 1).strip()
                                        # Could store these in listing_data if needed
                    
                    if listing_data['listing_url']:
                        if listing_data['listing_url'] not in seen_urls:
                            seen_urls.add(listing_data['listing_url'])
                            listings.append(listing_data)
                        else:
                            logger.debug(f"Duplicate listing URL skipped: {listing_data['listing_url']}")
                    else:
                        logger.warning(f"Listing container {div_idx} missing listing URL")
                
                except Exception as e:
                    logger.warning(f"Error extracting listing {div_idx}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error extracting listings: {e}", exc_info=True)
        
        return listings
    
    def scrape_manufacturer_details(self, manufacturer: Dict, listings: List[Dict], date: Optional[datetime] = None, max_listings: Optional[int] = None) -> Dict:
        """Scrape detail pages for listings from a manufacturer.
        
        Args:
            manufacturer: Dictionary with manufacturer info.
            listings: List of listing dictionaries with listing_url.
            date: Date for organizing scraped data.
            max_listings: Maximum number of listings to scrape. None = all listings.
        
        Returns:
            Dictionary with scrape results.
        """
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        manufacturer_id = manufacturer.get('manufacturer_id', 'unknown')
        manufacturer_name = manufacturer.get('name', 'unknown').lower().replace(' ', '_')
        
        output_dir = self.raw_aircraftexchange_path / date_str / "manufacturers" / f"{manufacturer_id}_{manufacturer_name}" / "details"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info(f"Scraping detail pages for {manufacturer['name']}")
        logger.info(f"Total listings to scrape: {len(listings)}")
        logger.info("=" * 60)
        
        if max_listings:
            listings = listings[:max_listings]
            logger.info(f"Limiting to {max_listings} listings for testing")
        
        result = {
            "manufacturer": manufacturer['name'],
            "manufacturer_id": manufacturer_id,
            "date": date_str,
            "listings_scraped": 0,
            "html_files": [],
            "details_data": [],
            "scrape_duration": 0,
            "errors": []
        }
        
        driver = None
        try:
            driver = self._setup_driver()
            
            for idx, listing in enumerate(listings, 1):
                listing_url = listing.get('listing_url')
                if not listing_url:
                    logger.warning(f"Listing {idx} missing URL, skipping")
                    continue
                
                logger.info("=" * 60)
                logger.info(f"Scraping detail {idx}/{len(listings)}: {listing.get('aircraft_model', 'Unknown')}")
                logger.info(f"URL: {listing_url}")
                logger.info("=" * 60)
                
                # Human-like: "thinking" pause before each listing (except first) - matches Controller
                if idx > 1:
                    pre_pause = random.uniform(1.5, 4.0)
                    logger.debug(f"Pre-listing thinking pause: {pre_pause:.2f} seconds")
                    time.sleep(pre_pause)
                
                # Human-like delay between listings (matches Controller detail scraper)
                if idx > 1:
                    self._wait_for_rate_limit()
                
                # Fetch detail page
                html_content = self._fetch_page(driver, listing_url)
                
                if not html_content:
                    logger.error(f"Failed to fetch detail page for listing {idx}")
                    result["errors"].append(f"Failed to fetch: {listing_url}")
                    continue
                
                # Extract listing ID from URL
                listing_id = self._extract_listing_id(listing_url)
                
                # Save HTML
                html_filename = f"listing_{listing_id}.html" if listing_id else f"listing_{idx:06d}.html"
                html_filepath = output_dir / html_filename
                with open(html_filepath, 'wb') as f:
                    f.write(html_content.encode('utf-8'))
                result["html_files"].append(str(html_filepath))
                logger.info(f"Saved HTML: {html_filename}")
                
                # Extract detail fields (reuse extraction logic from regular detail scraper)
                detail_data = self._extract_detail_fields(html_content, listing_url, listing)
                
                # Save to results
                result["details_data"].append(detail_data)
                result["listings_scraped"] += 1
                
                # Save incremental JSON
                details_file = output_dir / "details_metadata.json"
                with open(details_file, 'w', encoding='utf-8') as f:
                    json.dump(result["details_data"], f, indent=2, ensure_ascii=False)
                logger.info(f"Incremental save: {len(result['details_data'])} details saved")
                
        except Exception as e:
            logger.error(f"Scraper failed: {e}", exc_info=True)
            result["errors"].append(str(e))
        finally:
            safe_driver_quit(driver)
        
        result["scrape_duration"] = time.time() - start_time
        
        # Final JSON save
        if result["details_data"]:
            details_file = output_dir / "details_metadata.json"
            with open(details_file, 'w', encoding='utf-8') as f:
                json.dump(result["details_data"], f, indent=2, ensure_ascii=False)
            logger.info(f"Final save: {len(result['details_data'])} details saved")
        
        logger.info("=" * 60)
        logger.info(f"Scrape Summary for {manufacturer['name']}")
        logger.info(f"Listings scraped: {result['listings_scraped']}/{len(listings)}")
        logger.info(f"HTML files saved: {len(result['html_files'])}")
        logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
        logger.info("=" * 60)
        
        return result
    
    def scrape_all_manufacturer_details(self, manufacturers_metadata_path: Optional[Path] = None, date: Optional[datetime] = None, max_manufacturers: Optional[int] = None, max_pages_per_manufacturer: Optional[int] = None, max_listings_per_manufacturer: Optional[int] = None) -> Dict:
        """Complete workflow: Load manufacturers, scrape their listings pages, then scrape detail pages.
        
        Args:
            manufacturers_metadata_path: Path to manufacturers_metadata.json. If None, looks for latest.
            date: Date for organizing scraped data. If None, uses today.
            max_manufacturers: Maximum number of manufacturers to process. None = all.
            max_pages_per_manufacturer: Maximum pages to scrape per manufacturer listing page. None = all pages.
            max_listings_per_manufacturer: Maximum detail pages to scrape per manufacturer. None = all listings.
        
        Returns:
            Dictionary with overall scrape results.
        """
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        
        # Load manufacturers metadata
        if manufacturers_metadata_path is None:
            mfg_dir = self.raw_aircraftexchange_path / date_str / "manufacturers"
            manufacturers_metadata_path = mfg_dir / "manufacturers_metadata.json"
        
        if not manufacturers_metadata_path.exists():
            raise AircraftExchangeManufacturerDetailScraperError(
                f"Manufacturers metadata file not found: {manufacturers_metadata_path}"
            )
        
        logger.info("=" * 60)
        logger.info("AircraftExchange Manufacturer Detail Scraper - All Manufacturers")
        logger.info(f"Loading manufacturers from: {manufacturers_metadata_path}")
        logger.info("=" * 60)
        
        with open(manufacturers_metadata_path, 'r', encoding='utf-8') as f:
            manufacturers = json.load(f)
        
        logger.info(f"Loaded {len(manufacturers)} manufacturers")
        
        if max_manufacturers:
            manufacturers = manufacturers[:max_manufacturers]
            logger.info(f"Limiting to {max_manufacturers} manufacturers")
        
        overall_result = {
            "date": date_str,
            "manufacturers_processed": 0,
            "total_listings_found": 0,
            "total_details_scraped": 0,
            "manufacturer_results": [],
            "errors": []
        }
        
        # Process each manufacturer
        for idx, manufacturer in enumerate(manufacturers, 1):
            logger.info("=" * 60)
            logger.info(f"Manufacturer {idx}/{len(manufacturers)}: {manufacturer['name']}")
            logger.info("=" * 60)
            
            # Human-like delay between manufacturers (longer pause when switching manufacturers)
            if idx > 1:
                # Longer pause when switching to a different manufacturer (more human-like)
                manufacturer_delay = random.uniform(8.0, 15.0)
                logger.info(f"Human-like delay before next manufacturer: {manufacturer_delay:.2f} seconds")
                time.sleep(manufacturer_delay)
            
            try:
                # Step 1: Scrape manufacturer's listings page(s) to get listing URLs
                logger.info("Step 1: Scraping manufacturer listings page...")
                listings = self.scrape_manufacturer_listings(manufacturer, date, max_pages_per_manufacturer)
                overall_result["total_listings_found"] += len(listings)
                
                if not listings:
                    logger.warning(f"No listings found for {manufacturer['name']}")
                    overall_result["manufacturer_results"].append({
                        "manufacturer": manufacturer['name'],
                        "listings_found": 0,
                        "details_scraped": 0,
                        "error": "No listings found"
                    })
                    continue
                
                # Step 2: Scrape detail pages for each listing
                logger.info(f"Step 2: Scraping {len(listings)} detail pages...")
                detail_result = self.scrape_manufacturer_details(
                    manufacturer, 
                    listings, 
                    date, 
                    max_listings_per_manufacturer
                )
                
                overall_result["manufacturer_results"].append({
                    "manufacturer": manufacturer['name'],
                    "manufacturer_id": manufacturer.get('manufacturer_id'),
                    "listings_found": len(listings),
                    "details_scraped": detail_result["listings_scraped"],
                    "html_files": len(detail_result["html_files"]),
                    "errors": detail_result["errors"]
                })
                overall_result["total_details_scraped"] += detail_result["listings_scraped"]
                overall_result["manufacturers_processed"] += 1
                
            except Exception as e:
                logger.error(f"Error processing manufacturer {manufacturer['name']}: {e}", exc_info=True)
                overall_result["errors"].append(f"{manufacturer['name']}: {str(e)}")
                overall_result["manufacturer_results"].append({
                    "manufacturer": manufacturer['name'],
                    "listings_found": 0,
                    "details_scraped": 0,
                    "error": str(e)
                })
        
        overall_result["scrape_duration"] = time.time() - start_time
        
        # Save overall summary
        summary_file = self.raw_aircraftexchange_path / date_str / "manufacturers" / "manufacturer_details_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(overall_result, f, indent=2, ensure_ascii=False)
        
        logger.info("=" * 60)
        logger.info("Overall Scrape Summary")
        logger.info(f"Manufacturers processed: {overall_result['manufacturers_processed']}/{len(manufacturers)}")
        logger.info(f"Total listings found: {overall_result['total_listings_found']:,}")
        logger.info(f"Total details scraped: {overall_result['total_details_scraped']:,}")
        logger.info(f"Total duration: {overall_result['scrape_duration']:.2f} seconds")
        logger.info("=" * 60)
        
        return overall_result
    
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
    
    def _find_next_page_url(self, html_content: str, current_url: str) -> Optional[str]:
        """Find next page URL from HTML."""
        try:
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for "Next" link
            next_link = soup.find('a', string=re.compile(r'Next', re.I))
            if next_link and next_link.get('href'):
                href = next_link.get('href')
                if href.startswith('http'):
                    return href
                else:
                    return urljoin(self.BASE_URL, href)
            
            # Try to construct next page URL
            current_page_num = self._extract_page_number(current_url)
            if current_page_num:
                next_page_num = current_page_num + 1
                if '?' in current_url:
                    base_url, query = current_url.split('?', 1)
                    params = parse_qs(query)
                    params['page'] = [str(next_page_num)]
                    next_url = f"{base_url}?{urlencode(params, doseq=True)}"
                else:
                    next_url = f"{current_url.rstrip('/')}?page={next_page_num}"
                
                logger.info(f"Constructed next page URL: {next_url}")
                return next_url
            
        except Exception as e:
            logger.warning(f"Error finding next page: {e}")
            return None
        
        return None
    
    def _extract_page_number(self, url: str) -> Optional[int]:
        """Extract page number from URL."""
        try:
            patterns = [r'[?&]page=(\d+)', r'/page/(\d+)']
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    return int(match.group(1))
            return None
        except Exception:
            return None
    
    def _extract_listing_id(self, url: str) -> Optional[str]:
        """Extract listing ID from URL.
        
        Example: /jet-aircraft-for-sale/details/7494/1967-bell-205 -> 7494
        """
        try:
            match = re.search(r'/details/(\d+)/', url)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None
    
    def _extract_detail_fields(self, html_content: str, listing_url: str, base_listing: Dict) -> Dict:
        """Extract all available fields from detail page HTML.
        
        Reuses the same extraction logic as the regular detail scraper.
        """
        detail_data = {
            "listing_url": listing_url,
            "scrape_timestamp": datetime.now().isoformat(),
            # Fields from base listing
            "aircraft_model": base_listing.get('aircraft_model'),
            "year": base_listing.get('year'),
            "dealer_name": base_listing.get('dealer_name'),
            "manufacturer": base_listing.get('manufacturer'),
            # Fields to extract from detail page
            "total_time": None,
            "total_cycles": None,
            "asking_price": None,
            "location": None,
            "description": None,
            "model": None,
            "serial_number": None,
            "registration": None,
            "tail_number": None,
            "condition": None,
            "aircraft_type": None,
            # Engine fields
            "engine_1_make_model": None,
            "engine_1_serial_number": None,
            "engine_1_hours_since_new": None,
            "engine_1_hours_since_overhaul": None,
            "engine_1_hours_since_hot_section": None,
            "engine_2_make_model": None,
            "engine_2_serial_number": None,
            "engine_2_hours_since_new": None,
            "engine_2_hours_since_overhaul": None,
            "engine_2_hours_since_hot_section": None,
            # Avionics
            "avionics_description": None,
            # Additional equipment
            "additional_equipment": None,
            # Inspections
            "inspection_status": None,
            # Seller contact
            "seller_contact_name": None,
            "seller_phone": None,
            "seller_email": None,
        }
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            page_text = soup.get_text()
            
            # Method 1: Extract from structured datum divs
            datum_divs = soup.find_all('div', class_='datum')
            for datum in datum_divs:
                label_elem = datum.find('span', class_='label')
                value_elem = datum.find('div', class_='value')
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    value = value_elem.get_text(strip=True)
                    
                    label_lower = label.lower()
                    if 'total hours' in label_lower:
                        detail_data['total_time'] = value.replace(',', '').strip()
                    elif 'total cycles' in label_lower:
                        detail_data['total_cycles'] = value.replace(',', '').strip()
                    elif 'serial number' in label_lower and not detail_data['serial_number']:
                        detail_data['serial_number'] = value.strip()
                    elif 'tail number' in label_lower:
                        detail_data['tail_number'] = value.strip()
                        detail_data['registration'] = value.strip()
                    elif 'hours since new' in label_lower and not detail_data['engine_1_hours_since_new']:
                        detail_data['engine_1_hours_since_new'] = value.replace(',', '').strip()
                    elif 'hours since overhaul' in label_lower and not detail_data['engine_1_hours_since_overhaul']:
                        detail_data['engine_1_hours_since_overhaul'] = value.replace(',', '').strip()
                    elif 'hours since hot section' in label_lower and not detail_data['engine_1_hours_since_hot_section']:
                        detail_data['engine_1_hours_since_hot_section'] = value.replace(',', '').strip()
            
            # Method 2: Extract from summary list
            summary_lists = soup.find_all('ul', class_='list-reset')
            for ul in summary_lists:
                list_items = ul.find_all('li')
                for li in list_items:
                    label_span = li.find('span', class_='font-bold uppercase tracking-wide')
                    if label_span:
                        label = label_span.get_text(strip=True)
                        value = li.get_text().replace(label, '', 1).strip()
                        
                        if 'Serial Number' in label:
                            detail_data['serial_number'] = value
                        elif 'Tail Number' in label:
                            detail_data['tail_number'] = value
                            detail_data['registration'] = value
                        elif 'Hours' in label and not detail_data['total_time']:
                            detail_data['total_time'] = value.replace(',', '').strip()
                        elif 'Cycles' in label:
                            detail_data['total_cycles'] = value.replace(',', '').strip()
            
            # Extract asking price
            price_elem = soup.find('p', class_=lambda x: x and 'mt-2' in str(x))
            if price_elem:
                price_text = price_elem.get_text()
                if 'Price:' in price_text:
                    price_value = price_text.split('Price:')[-1].strip()
                    if price_value:
                        detail_data['asking_price'] = price_value
            
            if not detail_data['asking_price']:
                price_patterns = [
                    r'Price[:]\s*([^\n]+)',
                    r'\$[\d,]+(?:\.[\d]{2})?',
                    r'Make\s+Offer',
                    r'Call\s+for\s+price',
                ]
                for pattern in price_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        price_val = match.group(1 if match.groups() else 0).strip()
                        if price_val and price_val != 'Price:':
                            detail_data['asking_price'] = price_val
                            break
            
            # Extract location from broker section
            broker_section = soup.find('div', class_='broker')
            if broker_section:
                broker_text = broker_section.get_text()
                location_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})', broker_text)
                if location_match:
                    detail_data['location'] = location_match.group(1).strip()
                
                contact_links = broker_section.find_all('a', href=re.compile(r'^tel:'))
                if contact_links:
                    phone_text = contact_links[0].get_text(strip=True)
                    detail_data['seller_phone'] = phone_text
                
                contact_link = broker_section.find('a', string=re.compile(r'Contact\s+', re.I))
                if contact_link:
                    contact_text = contact_link.get_text(strip=True)
                    name_match = re.search(r'Contact\s+(.+)', contact_text, re.I)
                    if name_match:
                        detail_data['seller_contact_name'] = name_match.group(1).strip()
            
            # Extract description
            desc_div = soup.find('div', class_='mt-4 leading-normal')
            if desc_div:
                desc_text = desc_div.get_text(strip=True)
                if desc_text:
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    detail_data['description'] = desc_text[:2000]
            
            # Extract from Airframe section
            airframe_section = soup.find('div', id='airframe-section')
            if airframe_section:
                airframe_data = airframe_section.find_all('div', class_='datum')
                for datum in airframe_data:
                    label = datum.find('span', class_='label')
                    value = datum.find('div', class_='value')
                    if label and value:
                        label_text = label.get_text(strip=True)
                        value_text = value.get_text(strip=True)
                        if 'Total Hours' in label_text and not detail_data['total_time']:
                            detail_data['total_time'] = value_text.replace(',', '').strip()
                        elif 'Total Cycles' in label_text and not detail_data['total_cycles']:
                            detail_data['total_cycles'] = value_text.replace(',', '').strip()
            
            # Extract from Engines section
            engines_section = soup.find('div', id='engines-section')
            if engines_section:
                engine_datum = engines_section.find('div', class_='datum')
                if engine_datum:
                    engine_value = engine_datum.find('div', class_='value')
                    if engine_value and not engine_value.find('span', class_='label'):
                        engine_text = engine_value.get_text(strip=True)
                        if engine_text:
                            detail_data['engine_1_make_model'] = engine_text
                
                engine_divs = engines_section.find_all('div', class_='engine')
                if len(engine_divs) > 1:
                    engine_2 = engine_divs[1]
                    engine_2_data = engine_2.find_all('div', class_='datum')
                    for datum in engine_2_data:
                        label = datum.find('span', class_='label')
                        value = datum.find('div', class_='value')
                        if label and value:
                            label_text = label.get_text(strip=True)
                            value_text = value.get_text(strip=True)
                            if 'Serial Number' in label_text:
                                detail_data['engine_2_serial_number'] = value_text.strip()
                            elif 'Hours Since New' in label_text:
                                detail_data['engine_2_hours_since_new'] = value_text.replace(',', '').strip()
                            elif 'Hours Since Overhaul' in label_text:
                                detail_data['engine_2_hours_since_overhaul'] = value_text.replace(',', '').strip()
                            elif 'Hours Since Hot Section' in label_text:
                                detail_data['engine_2_hours_since_hot_section'] = value_text.replace(',', '').strip()
            
            # Extract from Avionics section
            avionics_section = soup.find('div', id='avionics-section')
            if avionics_section:
                avionics_value = avionics_section.find('div', class_='value')
                if avionics_value:
                    avionics_text = avionics_value.get_text(strip=True)
                    if avionics_text:
                        detail_data['avionics_description'] = avionics_text
            
            # Extract from Additional Equipment section
            other_section = soup.find('div', id='other')
            if other_section:
                other_text = other_section.get_text(strip=True)
                if other_text:
                    detail_data['additional_equipment'] = other_text[:1000]
            
            # Extract from Inspections section
            inspections_section = soup.find('div', id='inspections-section')
            if inspections_section:
                inspection_text = inspections_section.get_text(strip=True)
                if inspection_text:
                    detail_data['inspection_status'] = inspection_text[:500]
            
            # Extract manufacturer and model from title if not set
            if not detail_data['manufacturer']:
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text()
                    model_match = re.search(r'(\d{4})?\s*([A-Z][A-Za-z0-9\s-]+?)\s+for\s+Sale', title_text)
                    if model_match:
                        if model_match.group(1):
                            detail_data['year'] = detail_data['year'] or model_match.group(1)
                        model_name = model_match.group(2).strip()
                        parts = model_name.split()
                        if len(parts) > 1:
                            detail_data['manufacturer'] = parts[0]
                            detail_data['model'] = ' '.join(parts[1:])
                        else:
                            detail_data['model'] = model_name
            
        except Exception as e:
            logger.warning(f"Error extracting detail fields: {e}", exc_info=True)
        
        return detail_data
