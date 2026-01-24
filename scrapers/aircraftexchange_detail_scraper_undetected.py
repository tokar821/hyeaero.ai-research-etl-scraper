"""AircraftExchange.com Aircraft Listing Detail Scraper using undetected-chromedriver.

Scrapes individual aircraft detail pages to extract full information.

Extracts:
- Aircraft model
- Year
- Total time
- Asking price
- Dealer name
- Location
- And all other available fields from detail pages

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


class AircraftExchangeDetailScraperError(Exception):
    """Base exception for AircraftExchange detail scraper."""
    pass


class AircraftExchangeDetailScraperUndetected:
    """Scraper for AircraftExchange.com aircraft listing detail pages using undetected-chromedriver.
    
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
        """Initialize undetected-chromedriver detail scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
            rate_limit: Base seconds to wait between requests (will be randomized). Default: 6.0 seconds.
                        Actual delays will be 6-12 seconds to mimic human reading/thinking time.
            headless: Run browser in headless mode. Default: False (non-headless for better evasion).
        """
        if not UNDETECTED_AVAILABLE:
            raise AircraftExchangeDetailScraperError(
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
            # Get viewport size
            viewport_width = driver.execute_script("return window.innerWidth")
            viewport_height = driver.execute_script("return window.innerHeight")
            
            # Create action chain for mouse movements
            actions = ActionChains(driver)
            
            # Random mouse movement
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
            
            # Sometimes scroll back
            if random.random() < 0.3:
                scroll_back = random.randint(200, 500)
                driver.execute_script(f"window.scrollBy(0, -{scroll_back});")
                time.sleep(random.uniform(0.5, 1.0))
            
            # Reading pause
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
        Matches Controller detail scraper behavior.
        """
        base_delay = self.rate_limit
        reading_time = random.uniform(4.0, 8.0)  # 4-8 seconds "reading" per listing
        jitter = random.uniform(1.0, 1.4)
        delay = base_delay + reading_time * jitter
        logger.debug(f"Human-like delay: {delay:.2f} seconds (mimicking reading time)")
        time.sleep(delay)
    
    def _fetch_page(self, driver, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a detail page using Selenium and wait for content to load."""
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
    
    def scrape_details(self, listings_metadata_path: Optional[Path] = None, date: Optional[datetime] = None, max_listings: Optional[int] = None) -> Dict:
        """Scrape detail pages for listings from metadata JSON.
        
        Args:
            listings_metadata_path: Path to listings_metadata.json file. If None, looks for latest.
            date: Date for organizing scraped data. If None, uses today.
            max_listings: Maximum number of listings to scrape. None = all listings.
        
        Returns:
            Dictionary with scrape results.
        """
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_aircraftexchange_path / date_str / "details"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load listings metadata
        if listings_metadata_path is None:
            # Try to find latest metadata file
            index_dir = self.raw_aircraftexchange_path / date_str / "index"
            listings_metadata_path = index_dir / "listings_metadata.json"
        
        if not listings_metadata_path.exists():
            raise AircraftExchangeDetailScraperError(f"Listings metadata file not found: {listings_metadata_path}")
        
        logger.info("=" * 60)
        logger.info("AircraftExchange Detail Scraper")
        logger.info(f"Loading listings from: {listings_metadata_path}")
        logger.info("=" * 60)
        
        with open(listings_metadata_path, 'r', encoding='utf-8') as f:
            listings = json.load(f)
        
        logger.info(f"Loaded {len(listings)} listings from metadata")
        
        if max_listings:
            listings = listings[:max_listings]
            logger.info(f"Limiting to {max_listings} listings for testing")
        
        result = {
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
                logger.info(f"Scraping listing {idx}/{len(listings)}: {listing.get('aircraft_model', 'Unknown')}")
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
                
                # Extract detail fields
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
        logger.info("Scrape Summary")
        logger.info(f"Listings scraped: {result['listings_scraped']}/{len(listings)}")
        logger.info(f"HTML files saved: {len(result['html_files'])}")
        logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
        if result["errors"]:
            logger.warning(f"Errors encountered: {len(result['errors'])}")
        logger.info("=" * 60)
        
        return result
    
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
        
        AircraftExchange detail pages use:
        - <div class="datum"> with <span class="label"> and <div class="value"> for structured data
        - <li> with <span class="font-bold uppercase tracking-wide"> for summary info
        - Sections: Airframe, Engines, Avionics, Inspections, etc.
        """
        detail_data = {
            "listing_url": listing_url,
            "scrape_timestamp": datetime.now().isoformat(),
            # Fields from base listing (index page)
            "aircraft_model": base_listing.get('aircraft_model'),
            "year": base_listing.get('year'),
            "dealer_name": base_listing.get('dealer_name'),
            # Fields to extract from detail page
            "total_time": None,
            "total_cycles": None,
            "asking_price": None,
            "location": None,
            "description": None,
            "manufacturer": None,
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
            
            # Method 1: Extract from structured datum divs (label-value pairs)
            datum_divs = soup.find_all('div', class_='datum')
            for datum in datum_divs:
                label_elem = datum.find('span', class_='label')
                value_elem = datum.find('div', class_='value')
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    value = value_elem.get_text(strip=True)
                    
                    # Map labels to fields
                    label_lower = label.lower()
                    if 'total hours' in label_lower:
                        detail_data['total_time'] = value.replace(',', '').strip()
                    elif 'total cycles' in label_lower:
                        detail_data['total_cycles'] = value.replace(',', '').strip()
                    elif 'serial number' in label_lower and not detail_data['serial_number']:
                        detail_data['serial_number'] = value.strip()
                    elif 'tail number' in label_lower:
                        detail_data['tail_number'] = value.strip()
                        detail_data['registration'] = value.strip()  # Same as tail number
                    elif 'hours since new' in label_lower and not detail_data['engine_1_hours_since_new']:
                        detail_data['engine_1_hours_since_new'] = value.replace(',', '').strip()
                    elif 'hours since overhaul' in label_lower and not detail_data['engine_1_hours_since_overhaul']:
                        detail_data['engine_1_hours_since_overhaul'] = value.replace(',', '').strip()
                    elif 'hours since hot section' in label_lower and not detail_data['engine_1_hours_since_hot_section']:
                        detail_data['engine_1_hours_since_hot_section'] = value.replace(',', '').strip()
            
            # Method 2: Extract from summary list (ul with li elements)
            summary_lists = soup.find_all('ul', class_='list-reset')
            for ul in summary_lists:
                list_items = ul.find_all('li')
                for li in list_items:
                    label_span = li.find('span', class_='font-bold uppercase tracking-wide')
                    if label_span:
                        label = label_span.get_text(strip=True)
                        # Get text after the label span
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
            
            # Also try regex patterns for price
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
                # Look for location text (usually after dealer name)
                broker_text = broker_section.get_text()
                # Pattern: "Offered by: ... Dealer Name ... City, State"
                location_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})', broker_text)
                if location_match:
                    detail_data['location'] = location_match.group(1).strip()
                
                # Extract seller contact info
                contact_links = broker_section.find_all('a', href=re.compile(r'^tel:'))
                if contact_links:
                    phone_text = contact_links[0].get_text(strip=True)
                    detail_data['seller_phone'] = phone_text
                
                # Extract contact name from "Contact ..." link
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
                    # Clean up description (remove excessive whitespace)
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    detail_data['description'] = desc_text[:2000]  # Limit length
            
            # Extract from Airframe section
            airframe_section = soup.find('div', id='airframe-section')
            if airframe_section:
                # Total Hours and Cycles already extracted above, but double-check
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
                # Engine make/model (usually first datum without label)
                engine_datum = engines_section.find('div', class_='datum')
                if engine_datum:
                    engine_value = engine_datum.find('div', class_='value')
                    if engine_value and not engine_value.find('span', class_='label'):
                        engine_text = engine_value.get_text(strip=True)
                        if engine_text:
                            detail_data['engine_1_make_model'] = engine_text
                
                # Engine details (already extracted above from datum divs)
                # Check for Engine 2
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
                    # Pattern: "MODEL for Sale | AircraftExchange" or "YEAR MODEL for Sale"
                    # Try to extract model name
                    model_match = re.search(r'(\d{4})?\s*([A-Z][A-Za-z0-9\s-]+?)\s+for\s+Sale', title_text)
                    if model_match:
                        if model_match.group(1):
                            detail_data['year'] = detail_data['year'] or model_match.group(1)
                        model_name = model_match.group(2).strip()
                        # Try to split manufacturer and model
                        parts = model_name.split()
                        if len(parts) > 1:
                            detail_data['manufacturer'] = parts[0]
                            detail_data['model'] = ' '.join(parts[1:])
                        else:
                            detail_data['model'] = model_name
            
        except Exception as e:
            logger.warning(f"Error extracting detail fields: {e}", exc_info=True)
        
        return detail_data
