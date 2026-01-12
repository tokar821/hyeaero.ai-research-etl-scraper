"""Controller.com Aircraft Listings Scraper.

Scrapes aircraft listings from Controller.com with pagination support.
Uses Playwright for JavaScript-rendered content and bot protection bypass.
Saves raw HTML responses and extracts basic listing information.
"""

import hashlib
import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, Page, Browser
from bs4 import BeautifulSoup

try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False

from utils.logger import get_logger

logger = get_logger(__name__)


class ControllerScraperError(Exception):
    """Base exception for Controller scraper."""
    pass


class ControllerDownloadError(ControllerScraperError):
    """Raised when download fails."""
    pass


class ControllerScraper:
    """Scraper for Controller.com aircraft listings using Playwright.
    
    Enhanced with strong anti-bot detection measures:
    - playwright-stealth plugin (if available)
    - Non-headless mode by default
    - Human-like mouse movements and scrolling
    - Realistic browser headers and fingerprinting
    - Enhanced fingerprinting protection (Canvas, WebGL, etc.)
    - Direct URL access (no homepage visit - no login needed)
    - Randomized rate limiting
    
    Note: If Playwright continues to be detected, consider using
    undetected-chromedriver (Selenium-based) as an alternative.
    """
    
    BASE_URL = "https://www.controller.com"
    START_URL = "/listings/search?page=1"
    
    # Rate limiting: seconds to wait between requests (with randomization)
    RATE_LIMIT_DELAY = 3.0  # 3 seconds base delay between requests (will be randomized 2.4-4.5s)
    
    def __init__(self, storage_base_path: Optional[Path] = None, rate_limit: float = 2.0, headless: bool = False):
        """Initialize Controller scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
            rate_limit: Seconds to wait between requests. Default: 2.0 seconds.
            headless: Run browser in headless mode. Default: False (non-headless for better bot detection evasion).
        """
        if storage_base_path is None:
            storage_base_path = Path(__file__).parent.parent / "store"
        
        self.storage_base_path = Path(storage_base_path)
        self.raw_controller_path = self.storage_base_path / "raw" / "controller"
        
        # Create directories if they don't exist
        self.raw_controller_path.mkdir(parents=True, exist_ok=True)
        
        self.rate_limit = rate_limit
        self.headless = headless
        
        # Track visited URLs to avoid duplicates
        self.visited_urls = set()
    
    def _setup_browser(self, playwright) -> Browser:
        """Setup and configure Playwright browser with anti-bot evasion.
        
        Args:
            playwright: Playwright instance.
            
        Returns:
            Configured browser instance.
        """
        # Launch browser (non-headless by default for better bot detection evasion)
        # Non-headless mode runs a real browser window which is harder for sites to detect
        browser = playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--window-size=1920,1080',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-site-isolation-trials',
                '--disable-infobars',
                '--disable-notifications',
                '--disable-popup-blocking',
            ]
        )
        
        return browser
    
    def _setup_page(self, browser: Browser) -> Page:
        """Setup and configure browser page.
        
        Args:
            browser: Browser instance.
            
        Returns:
            Configured page instance.
        """
        # Use persistent context to save cookies/session (helps avoid detection)
        # This makes the browser behave more like a real user session
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
            permissions=['geolocation'],
            geolocation={'latitude': 40.7128, 'longitude': -74.0060},  # NYC coordinates
            color_scheme='light',
            # Add more realistic browser properties
            device_scale_factor=1,
            has_touch=False,
            is_mobile=False,
            java_script_enabled=True,
        )
        
        # Add extra headers to mimic real browser
        context.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Cache-Control': 'max-age=0',
        })
        
        page = context.new_page()
        
        # Apply playwright-stealth plugin if available
        if STEALTH_AVAILABLE:
            try:
                stealth_sync(page)
                logger.debug("Applied playwright-stealth plugin")
            except Exception as e:
                logger.warning(f"Failed to apply stealth plugin: {e}")
        
        # Enhanced comprehensive anti-detection scripts
        page.add_init_script("""
            // Remove webdriver property completely
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override plugins to match real browser
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const plugins = [];
                    for (let i = 0; i < 5; i++) {
                        plugins.push({
                            name: `Plugin ${i}`,
                            description: `Plugin ${i} Description`,
                            filename: `plugin${i}.dll`
                        });
                    }
                    return plugins;
                }
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            
            // Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Chrome object with more properties
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // Override getBattery
            if (navigator.getBattery) {
                navigator.getBattery = () => Promise.resolve({
                    charging: true,
                    chargingTime: 0,
                    dischargingTime: Infinity,
                    level: 1
                });
            }
            
            // Override permissions
            const PermissionStatus = function() {};
            PermissionStatus.prototype.state = 'granted';
            window.PermissionStatus = PermissionStatus;
            
            // Canvas fingerprint protection - add noise
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function() {
                const context = this.getContext('2d');
                if (context) {
                    const imageData = context.getImageData(0, 0, this.width, this.height);
                    // Add minimal noise to prevent fingerprinting
                    for (let i = 0; i < imageData.data.length; i += 4) {
                        if (Math.random() < 0.001) {
                            imageData.data[i] = Math.min(255, imageData.data[i] + 1);
                        }
                    }
                    context.putImageData(imageData, 0, 0);
                }
                return originalToDataURL.apply(this, arguments);
            };
            
            // Override WebGL fingerprinting
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) { // UNMASKED_VENDOR_WEBGL
                    return 'Intel Inc.';
                }
                if (parameter === 37446) { // UNMASKED_RENDERER_WEBGL
                    return 'Intel Iris OpenGL Engine';
                }
                return getParameter.apply(this, arguments);
            };
            
            // Override Notification permission
            const OriginalNotification = window.Notification;
            window.Notification = function(title, options) {
                return new OriginalNotification(title, options);
            };
            Object.setPrototypeOf(window.Notification, OriginalNotification);
            Object.defineProperty(window.Notification, 'permission', {
                get: () => 'default'
            });
            
            // Override connection property
            Object.defineProperty(navigator, 'connection', {
                get: () => ({
                    effectiveType: '4g',
                    rtt: 50,
                    downlink: 10,
                    saveData: false
                })
            });
            
            // Remove automation indicators
            delete navigator.__proto__.webdriver;
        """)
        
        return page
    
    def _wait_for_rate_limit(self):
        """Wait for rate limit delay with randomization."""
        # Add random jitter (0.8 to 1.5x the rate limit) to avoid detection
        jitter = random.uniform(0.8, 1.5)
        delay = self.rate_limit * jitter
        time.sleep(delay)
    
    def _simulate_human_behavior(self, page: Page):
        """Simulate human-like behavior on the page (mouse movements, scrolling).
        
        Args:
            page: Playwright page instance.
        """
        try:
            # Random mouse movement
            viewport = page.viewport_size
            if viewport:
                mouse_x = random.randint(100, viewport['width'] - 100)
                mouse_y = random.randint(100, viewport['height'] - 100)
                page.mouse.move(mouse_x, mouse_y, steps=random.randint(5, 15))
                page.wait_for_timeout(random.uniform(200, 500))
            
            # Scroll down gradually (human-like scrolling)
            scroll_amount = random.randint(200, 500)
            scroll_steps = random.randint(3, 8)
            step_size = scroll_amount // scroll_steps
            
            for _ in range(scroll_steps):
                page.evaluate(f"window.scrollBy(0, {step_size})")
                page.wait_for_timeout(random.uniform(100, 300))
            
            # Small random scroll back up (humans don't scroll perfectly)
            if random.random() > 0.5:
                page.evaluate(f"window.scrollBy(0, -{random.randint(50, 150)})")
                page.wait_for_timeout(random.uniform(200, 400))
                
        except Exception as e:
            logger.debug(f"Error simulating human behavior: {e}")
            # Continue even if simulation fails
    
    def _fetch_page(self, page: Page, url: str, wait_selector: Optional[str] = None, retries: int = 3) -> Optional[str]:
        """Fetch a page and return raw HTML with retry logic.
        
        Args:
            page: Playwright page instance.
            url: URL to fetch.
            wait_selector: Optional CSS selector to wait for before getting HTML.
            retries: Number of retry attempts for failed requests. Default: 3.
            
        Returns:
            Raw HTML content as string, or None if failed after retries.
            
        Raises:
            ControllerDownloadError: If download fails after all retries.
        """
        if url in self.visited_urls:
            logger.debug(f"Already visited: {url}")
            return None
        
        full_url = urljoin(self.BASE_URL, url) if not url.startswith('http') else url
        
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Navigating to: {full_url} (attempt {attempt}/{retries})")
                
                # Navigate to page (use domcontentloaded for faster loading, then wait for content)
                response = page.goto(
                    full_url,
                    wait_until='domcontentloaded',
                    timeout=90000  # 90 seconds timeout
                )
                
                # Handle HTTP errors - retry on 4xx/5xx (except 404 which means page doesn't exist)
                if response and response.status >= 400:
                    if response.status == 404:
                        logger.warning(f"Page not found (404) for {full_url} - skipping")
                        return None
                    
                    # Retry on transient errors (405, 429, 500, 502, 503, 504)
                    if response.status in [405, 429, 500, 502, 503, 504] and attempt < retries:
                        wait_time = attempt * 5  # Exponential backoff: 5s, 10s, 15s
                        logger.warning(f"HTTP {response.status} error for {full_url} - retrying in {wait_time}s (attempt {attempt}/{retries})")
                        time.sleep(wait_time)
                        continue
                    
                    logger.error(f"HTTP {response.status} error for {full_url} after {attempt} attempts")
                    raise ControllerDownloadError(f"HTTP {response.status} error")
                
                # Wait for page to fully load
                # First wait for DOM content to be loaded
                try:
                    page.wait_for_load_state('domcontentloaded', timeout=30000)
                except Exception:
                    logger.warning("DOM content loaded timeout, continuing")
                
                # Wait for actual content selector (listContainer) to appear
                try:
                    page.wait_for_selector('#listContainer', timeout=20000, state='attached')
                    logger.debug("List container found, waiting for content")
                except Exception:
                    # Fallback: try alternative selector
                    try:
                        page.wait_for_selector('.list-container', timeout=10000, state='attached')
                        logger.debug("List container (class) found")
                    except Exception:
                        logger.warning("List container selector not found, continuing anyway")
                
                # Handle cookie consent banner if present (click X to dismiss)
                try:
                    # Look for cookie banner close button (X)
                    cookie_close = page.locator('button[aria-label*="close" i], button[aria-label*="dismiss" i], .cookie-banner button:has-text("X"), [class*="cookie"] button:has-text("X")')
                    if cookie_close.count() > 0:
                        cookie_close.first.click()
                        logger.debug("Dismissed cookie consent banner")
                        page.wait_for_timeout(500)  # Brief wait after clicking
                except Exception as e:
                    logger.debug(f"Cookie banner not found or already dismissed: {e}")
                
                # Wait for network to settle (reduced timeout to avoid being stuck)
                try:
                    page.wait_for_load_state('networkidle', timeout=10000)
                except Exception:
                    logger.debug("Network idle timeout, waiting fixed delay")
                    # Wait additional time for JavaScript to render
                    page.wait_for_timeout(random.uniform(2000, 4000))
                
                # Simulate human behavior (mouse movements, scrolling)
                self._simulate_human_behavior(page)
                
                # Get page content
                html_content = page.content()
                
                content_length = len(html_content)
                logger.info(f"Retrieved {content_length:,} bytes from {full_url}")
                
                # Check for bot detection (CAPTCHA page)
                if 'Pardon Our Interruption' in html_content or 'distil_referrer' in html_content:
                    logger.warning(f"Bot detection page detected for {full_url}")
                    if attempt < retries:
                        wait_time = attempt * 10  # Longer wait for bot detection
                        logger.warning(f"Waiting {wait_time}s before retry (attempt {attempt}/{retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Bot detection page persisted after {retries} attempts for {full_url}")
                        return None
                
                # Check if page content is too small (likely an error page)
                if content_length < 50000:  # Less than 50KB is suspicious for a listings page
                    logger.warning(f"Page content suspiciously small ({content_length} bytes) for {full_url}")
                    # Check if it's actually an error page
                    if 'error' in html_content.lower()[:500] or '404' in html_content[:500] or '403' in html_content[:500]:
                        logger.warning(f"Error page detected for {full_url}")
                        if attempt < retries:
                            wait_time = attempt * 5
                            time.sleep(wait_time)
                            continue
                        return None
                
                self.visited_urls.add(url)
                
                return html_content
                
            except Exception as e:
                if attempt < retries:
                    wait_time = attempt * 5  # Exponential backoff
                    logger.warning(f"Error fetching {url} (attempt {attempt}/{retries}): {e} - retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error fetching {url} after {retries} attempts: {e}")
                    raise ControllerDownloadError(f"Failed to fetch {url} after {retries} attempts: {e}") from e
        
        return None
    
    def _save_html_page(self, html_content: str, page_num: int, output_dir: Path) -> Path:
        """Save HTML page to disk.
        
        Args:
            html_content: HTML content as string.
            page_num: Page number.
            output_dir: Directory to save to.
            
        Returns:
            Path to saved file.
        """
        filename = f"page_{page_num:04d}.html"
        filepath = output_dir / filename
        
        # Convert to bytes for saving
        html_bytes = html_content.encode('utf-8')
        
        with open(filepath, 'wb') as f:
            f.write(html_bytes)
        
        file_hash = hashlib.md5(html_bytes).hexdigest()
        file_size = filepath.stat().st_size
        
        logger.info(f"Saved page {page_num}: {filename} ({file_size:,} bytes, MD5: {file_hash})")
        
        return filepath
    
    def _extract_total_listings_count(self, html_content: str) -> Optional[int]:
        """Extract total listings count from HTML.
        
        Looks for patterns like "309 - 336 of 5,121 Listings" or "253 - 280 of 5,121 Listings"
        
        Args:
            html_content: HTML content as string.
            
        Returns:
            Total listings count if found, None otherwise.
        """
        try:
            import re
            
            # Pattern: "X - Y of Z Listings" where Z is the total
            patterns = [
                r'(\d{1,3}(?:,\d{3})*)\s*-\s*\d{1,3}(?:,\d{3})*\s+of\s+(\d{1,3}(?:,\d{3})*)\s+Listings',
                r'of\s+(\d{1,3}(?:,\d{3})*)\s+Listings',
                r'total[:\s]+(\d{1,3}(?:,\d{3})*)\s+listings',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    # Get the last match (usually the total)
                    if isinstance(matches[0], tuple):
                        total_str = matches[0][-1]  # Get last group if multiple groups
                    else:
                        total_str = matches[-1]  # Get last match
                    
                    # Remove commas and convert to int
                    total_count = int(total_str.replace(',', ''))
                    logger.info(f"Found total listings count: {total_count:,}")
                    return total_count
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting total listings count: {e}")
            return None
    
    def _extract_listings(self, html_content: str, page_url: str) -> List[Dict]:
        """Extract basic listing information from HTML using structured card parsing.
        
        Finds the list-container div, then extracts data from each list-listing-card-wrapper.
        Only extracts URLs matching /listing/for-sale/ pattern.
        
        Args:
            html_content: HTML content as string.
            page_url: URL of the page.
            
        Returns:
            List of dictionaries with extracted listing data.
        """
        listings = []
        scrape_timestamp = datetime.now().isoformat()
        seen_urls = set()  # Track unique URLs to avoid duplicates
        
        try:
            import re
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the main list container
            list_container = soup.find('div', id='listContainer', class_=lambda x: x and 'list-container' in str(x).lower())
            if not list_container:
                list_container = soup.find('div', class_=lambda x: x and 'list-container' in str(x).lower())
            
            if not list_container:
                logger.warning("Could not find list-container div, trying alternative methods")
                # Fallback: try to find listing cards directly
                listing_cards = soup.find_all('div', class_='list-listing-card-wrapper')
            else:
                # Find all listing card wrappers within the container
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
                    
                    # Find the inner card div with data-listing-id (listing-card-grid)
                    card_div = card_wrapper.find('div', class_='listing-card-grid', attrs={'data-listing-id': True})
                    if not card_div:
                        # Alternative: find div with id attribute (listing ID) - the div inside card_wrapper
                        card_div = card_wrapper.find('div', id=re.compile(r'^\d+$'))
                        if not card_div:
                            # Try finding listing-card-grid without data-listing-id
                            card_div = card_wrapper.find('div', class_='listing-card-grid')
                    
                    # Extract Listing ID from data-listing-id attribute or id attribute
                    if card_div:
                        listing_id = card_div.get('data-listing-id')
                        if not listing_id:
                            # Try id attribute from parent div (the div with numeric id)
                            parent_div = card_wrapper.find('div', id=re.compile(r'^\d+$'))
                            if parent_div:
                                listing_id = parent_div.get('id')
                        if listing_id:
                            listing_data['listing_id'] = str(listing_id)
                    
                    # Extract Listing URL - ONLY from /listing/for-sale/ pattern
                    # Try multiple locations: title link, view details link
                    listing_url = None
                    
                    # Method 1: Find link in list-listing-title-link (title link)
                    title_link = card_wrapper.find('a', class_='list-listing-title-link', href=re.compile(r'/listing/for-sale/'))
                    if title_link:
                        href = title_link.get('href', '')
                        if href and '/listing/for-sale/' in href:
                            listing_url = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                    
                    # Method 2: Find link in view-listing-details-link (View Details button)
                    if not listing_url:
                        details_link = card_wrapper.find('a', class_='view-listing-details-link', href=re.compile(r'/listing/for-sale/'))
                        if details_link:
                            href = details_link.get('href', '')
                            if href and '/listing/for-sale/' in href:
                                listing_url = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                    
                    # Skip if no valid listing URL found
                    if not listing_url:
                        logger.debug(f"No valid listing URL found for card {card_idx}")
                        continue
                    
                    # Skip duplicates
                    if listing_url in seen_urls:
                        continue
                    seen_urls.add(listing_url)
                    listing_data['listing_url'] = listing_url
                    
                    # Extract listing ID from URL if not already found
                    if not listing_data['listing_id']:
                        parsed_url = urlparse(listing_url)
                        path_parts = [p for p in parsed_url.path.split('/') if p]
                        if 'listing' in path_parts and 'for-sale' in path_parts:
                            listing_idx = path_parts.index('for-sale')
                            if listing_idx + 1 < len(path_parts):
                                listing_data['listing_id'] = path_parts[listing_idx + 1]
                    
                    # Extract Aircraft Model from listing-portion-title -> list-listing-title-link
                    title_h2 = card_wrapper.find('h2', class_='listing-portion-title')
                    if title_h2:
                        title_link = title_h2.find('a', class_='list-listing-title-link')
                        if title_link:
                            listing_data['aircraft_model'] = title_link.get_text(strip=True)
                        else:
                            # Fallback: get text from h2 itself
                            listing_data['aircraft_model'] = title_h2.get_text(strip=True)
                    
                    # Extract Price from retail-price-container -> price span
                    price_container = card_wrapper.find('div', class_='retail-price-container')
                    if price_container:
                        price_span = price_container.find('span', class_='price')
                        if price_span:
                            listing_data['listing_price'] = price_span.get_text(strip=True)
                    
                    # Extract Location from machine-location div
                    location_div = card_wrapper.find('div', class_='machine-location')
                    if location_div:
                        location_text = location_div.get_text(strip=True)
                        # Remove "Location:" prefix if present
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
            
            logger.info(f"Extracted {len(listings)} listings from page (after deduplication)")
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}", exc_info=True)
        
        return listings
    
    def _extract_pagination_info(self, html_content: str) -> Optional[Dict]:
        """Extract pagination information from HTML (e.g., "309 - 336 of 5,121 Listings").
        
        Args:
            html_content: HTML content as string.
            
        Returns:
            Dictionary with pagination info: {
                'current_start': int,
                'current_end': int,
                'total_listings': int,
                'is_last_page': bool
            } or None if not found.
        """
        try:
            import re
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for pattern like "309 - 336 of 5,121 Listings" or "253 - 280 of 5,121 Listings"
            # Search in all text
            page_text = soup.get_text()
            
            # Pattern: numbers - numbers of numbers Listings
            pattern = r'(\d{1,4}(?:,\d{3})*)\s*-\s*(\d{1,4}(?:,\d{3})*)\s+of\s+(\d{1,4}(?:,\d{3})*)\s+Listings'
            match = re.search(pattern, page_text, re.IGNORECASE)
            
            if match:
                current_start = int(match.group(1).replace(',', ''))
                current_end = int(match.group(2).replace(',', ''))
                total_listings = int(match.group(3).replace(',', ''))
                
                # Stop scraping when Y = Z (current_end >= total_listings)
                # Example: "5,093 - 5,121 of 5,121 Listings" means we've reached the last page
                is_last_page = (current_end >= total_listings)
                
                logger.info(f"Pagination info: {current_start} - {current_end} of {total_listings:,} Listings (Last page: {is_last_page})")
                
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
    
    def _find_next_page_url(self, html_content: str, current_url: str, allow_manual_construction: bool = True) -> Optional[str]:
        """Find next page URL from HTML or construct manually.
        
        Args:
            html_content: HTML content as string (can be empty if page fetch failed).
            current_url: Current page URL.
            allow_manual_construction: If True, construct URL manually if HTML parsing fails.
            
        Returns:
            Next page URL if found, None otherwise.
        """
        # Extract current page number from URL
        current_page_num = self._extract_page_number(current_url)
        
        # If no HTML content, construct next page URL manually
        if not html_content and allow_manual_construction and current_page_num:
            try:
                from urllib.parse import urlparse, parse_qs, urlencode
                
                # Make current_url absolute if it's relative
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
                    logger.info(f"Constructed next page URL manually (no HTML, page {target_page}): {next_url}")
                    return next_url
            except Exception as e:
                logger.warning(f"Failed to construct next page URL manually: {e}")
                return None
        
        try:
            if not html_content:
                return None
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Pattern 1: Look for "next" button/link (common pagination)
            next_link = None
            next_link = soup.find('a', text=lambda x: x and x.strip().lower() == 'next')
            if not next_link:
                next_link = soup.find('a', {'aria-label': lambda x: x and 'next' in str(x).lower()})
            if not next_link:
                next_link = soup.find('a', class_=lambda x: x and 'next' in str(x).lower())
            if not next_link:
                next_link = soup.find('button', text=lambda x: x and 'next' in str(x).lower() if x else False)
            
            if next_link:
                href = next_link.get('href') or next_link.get('data-href') or next_link.get('onclick')
                if href and 'page=' in str(href):
                    # Make absolute URL if needed
                    if not href.startswith('http'):
                        href = urljoin(self.BASE_URL, href)
                    logger.info(f"Found next page URL (next button): {href}")
                    return href
            
            # Pattern 2: Look for page number links (e.g., page 2, 3, 4...)
            if current_page_num:
                # Find links with page number one higher than current
                target_page = current_page_num + 1
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    href = link.get('href', '')
                    link_text = link.get_text(strip=True)
                    
                    # Check if link points to next page
                    link_page_num = self._extract_page_number(href)
                    if link_page_num == target_page:
                        if not href.startswith('http'):
                            href = urljoin(self.BASE_URL, href)
                        logger.info(f"Found next page URL (page {target_page}): {href}")
                        return href
                    
                    # Also check if link text matches page number
                    if link_text == str(target_page) and 'page=' in href:
                        if not href.startswith('http'):
                            href = urljoin(self.BASE_URL, href)
                        logger.info(f"Found next page URL (page link {target_page}): {href}")
                        return href
                
                # Pattern 3: Construct next page URL manually if we can't find a link
                # This is a fallback - try to build the next page URL
                from urllib.parse import urlparse, parse_qs, urlencode
                
                # Make current_url absolute if it's relative
                if not current_url.startswith('http'):
                    current_url = urljoin(self.BASE_URL, current_url)
                
                parsed = urlparse(current_url)
                query_params = parse_qs(parsed.query)
                
                if 'page' in query_params:
                    query_params['page'] = [str(target_page)]
                    new_query = urlencode(query_params, doseq=True)
                    next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
                    logger.info(f"Constructed next page URL (page {target_page}): {next_url}")
                    return next_url
            
            # Pattern 4: Check if there's a "last" page link to determine max pages
            # For now, we'll try pattern 3 as fallback
            
            logger.info(f"No next page found - pagination complete (last page: {current_page_num})")
            return None
            
        except Exception as e:
            logger.warning(f"Error finding next page: {e}")
            return None
    
    def _extract_page_number(self, url: str) -> Optional[int]:
        """Extract page number from URL.
        
        Args:
            url: URL to extract page number from.
            
        Returns:
            Page number if found, None otherwise.
        """
        try:
            import re
            patterns = [
                r'[?&]page=(\d+)',
                r'/page/(\d+)',
                r'/p(\d+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url, re.IGNORECASE)
                if match:
                    return int(match.group(1))
            
            return None
        except Exception:
            return None
    
    def scrape_listings(
        self,
        date: Optional[datetime] = None,
        max_pages: Optional[int] = None
    ) -> Dict:
        """Scrape Controller.com aircraft listings with pagination.
        
        Args:
            date: Date for storage path. If None, uses current date.
            max_pages: Maximum number of pages to scrape. If None, scrapes all pages.
            
        Returns:
            Dictionary with scrape statistics:
            - date: Date string
            - pages_scraped: Number of pages scraped
            - total_listings: Total listings extracted
            - html_files: List of saved HTML files
            - listings_data: List of extracted listings
            - scrape_duration: Duration in seconds
        """
        from playwright.sync_api import sync_playwright
        
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_controller_path / date_str / "index"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info("Controller.com Aircraft Listings Scraper (Playwright)")
        logger.info(f"Date: {date_str}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Start URL: {self.BASE_URL}{self.START_URL}")
        logger.info("=" * 60)
        
        result = {
            "date": date_str,
            "pages_scraped": 0,
            "total_listings": 0,
            "total_listings_count": None,  # Total count from UI
            "expected_pages": None,  # Expected page count (calculated from page 1)
            "html_files": [],
            "listings_data": [],
            "scrape_duration": 0,
            "errors": []
        }
        
        try:
            with sync_playwright() as playwright:
                # Setup browser
                browser = self._setup_browser(playwright)
                page = self._setup_page(browser)
                
                try:
                    # Go directly to listings URL (no homepage visit needed - no login required)
                    current_url = self.START_URL
                    page_num = 1
                    all_listings = []
                    
                    while current_url:
                        try:
                            # Check max pages limit
                            if max_pages and page_num > max_pages:
                                logger.info(f"Reached max pages limit ({max_pages})")
                                break
                            
                            logger.info(f"Processing page {page_num}...")
                            
                            # Fetch page with rate limiting
                            self._wait_for_rate_limit()
                            
                            try:
                                html_content = self._fetch_page(
                                    page, 
                                    current_url, 
                                    wait_selector=None  # Will wait for networkidle by default
                                )
                            except ControllerDownloadError as e:
                                logger.error(f"Failed to fetch page {page_num} after retries: {e}")
                                result["errors"].append(f"Page {page_num}: {str(e)}")
                                
                                # Check if we've reached the last page based on total listings
                                # If we have scraped close to total, assume we're done
                                if result.get("total_listings_count") and len(all_listings) >= result["total_listings_count"] * 0.95:
                                    logger.info(f"Scraped {len(all_listings):,} listings, close to total ({result['total_listings_count']:,}) - assuming complete")
                                    break
                                
                                # Otherwise, try to continue with next page
                                # Construct next page URL manually
                                from urllib.parse import urlparse, parse_qs, urlencode
                                if not current_url.startswith('http'):
                                    full_current_url = urljoin(self.BASE_URL, current_url)
                                else:
                                    full_current_url = current_url
                                
                                parsed = urlparse(full_current_url)
                                query_params = parse_qs(parsed.query)
                                
                                if 'page' in query_params:
                                    next_page_num = int(query_params['page'][0]) + 1
                                    query_params['page'] = [str(next_page_num)]
                                    new_query = urlencode(query_params, doseq=True)
                                    next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
                                    next_url_relative = f"{parsed.path}?{new_query}"
                                    
                                    logger.info(f"Skipping failed page {page_num}, trying next page {next_page_num}")
                                    current_url = next_url_relative
                                    page_num = next_page_num
                                    continue
                                else:
                                    logger.warning(f"Cannot determine next page, stopping")
                                    break
                            
                            if html_content is None:
                                logger.warning(f"Failed to fetch page {page_num} (returned None), skipping")
                                result["errors"].append(f"Page {page_num}: Failed to fetch (None)")
                                
                                # Check if we've reached the last page
                                if result.get("total_listings_count") and len(all_listings) >= result["total_listings_count"] * 0.95:
                                    logger.info(f"Scraped {len(all_listings):,} listings, close to total ({result['total_listings_count']:,}) - assuming complete")
                                    break
                                
                                # Try next page
                                next_url = self._find_next_page_url("", current_url)  # Pass empty HTML since we don't have it
                                if next_url:
                                    if next_url.startswith(self.BASE_URL):
                                        next_url_relative = next_url[len(self.BASE_URL):]
                                    else:
                                        next_url_relative = next_url
                                    current_url = next_url_relative
                                    page_num += 1
                                    continue
                                else:
                                    logger.warning(f"Cannot find next page, stopping")
                                    break
                            
                            # Save raw HTML
                            html_file = self._save_html_page(html_content, page_num, output_dir)
                            result["html_files"].append(str(html_file))
                            
                            # Extract pagination info FIRST (especially on page 1 to get total count)
                            pagination_info = self._extract_pagination_info(html_content)
                            
                            # On page 1: Get total count and calculate expected pages
                            if page_num == 1 and pagination_info:
                                result["total_listings_count"] = pagination_info['total_listings']
                                total_listings = pagination_info['total_listings']
                                listings_per_page = pagination_info['current_end'] - pagination_info['current_start'] + 1
                                expected_pages = (total_listings + listings_per_page - 1) // listings_per_page  # Ceiling division
                                result["expected_pages"] = expected_pages
                                
                                logger.info("=" * 60)
                                logger.info("Pagination Information (from page 1):")
                                logger.info(f"  Total listings on site: {total_listings:,}")
                                logger.info(f"  Listings per page: ~{listings_per_page}")
                                logger.info(f"  Expected pages: ~{expected_pages}")
                                logger.info("=" * 60)
                                logger.info("Starting to scrape all pages...")
                                logger.info("JSON file will be updated after each page: listings_metadata.json")
                                logger.info("=" * 60)
                            
                            # Extract listings
                            full_url = urljoin(self.BASE_URL, current_url) if not current_url.startswith('http') else current_url
                            listings = self._extract_listings(html_content, full_url)
                            all_listings.extend(listings)
                            
                            logger.info(f"Page {page_num}: Extracted {len(listings)} listings (Total so far: {len(all_listings):,})")
                            
                            # Check if this is the last page (Primary condition: Y = Z)
                            if pagination_info and pagination_info.get('is_last_page', False):
                                logger.info(f"Reached last page (page {page_num}) - pagination complete (Y = Z)")
                                # Save JSON after last page
                                listings_file = output_dir / "listings_metadata.json"
                                with open(listings_file, 'w', encoding='utf-8') as f:
                                    json.dump(all_listings, f, indent=2, ensure_ascii=False)
                                logger.info(f"Saved {len(all_listings):,} listings to JSON (final page)")
                                break
                            
                            # Fallback condition: Stop if we've reached approximately expected pages
                            # This is a safety mechanism in case Y = Z detection fails
                            if result.get("expected_pages") and page_num >= result["expected_pages"]:
                                logger.info(f"Reached expected page count (page {page_num} >= {result['expected_pages']}) - stopping as fallback")
                                logger.info(f"Note: This is a fallback stop. If Y = Z was not detected, check pagination info.")
                                # Save JSON
                                listings_file = output_dir / "listings_metadata.json"
                                with open(listings_file, 'w', encoding='utf-8') as f:
                                    json.dump(all_listings, f, indent=2, ensure_ascii=False)
                                logger.info(f"Saved {len(all_listings):,} listings to JSON (fallback stop)")
                                break
                            
                            # Save JSON incrementally after each page
                            listings_file = output_dir / "listings_metadata.json"
                            with open(listings_file, 'w', encoding='utf-8') as f:
                                json.dump(all_listings, f, indent=2, ensure_ascii=False)
                            logger.info(f"[OK] Updated listings_metadata.json with {len(all_listings):,} listings (page {page_num})")
                            
                            # Find next page
                            next_url = self._find_next_page_url(html_content, current_url)
                            
                            if not next_url:
                                logger.info("No more pages to process")
                                break
                            
                            if next_url in self.visited_urls:
                                logger.info("Next page already visited, pagination complete")
                                break
                            
                            # Convert absolute URL to relative for tracking if needed
                            if next_url.startswith(self.BASE_URL):
                                next_url_relative = next_url[len(self.BASE_URL):]
                            else:
                                next_url_relative = next_url
                            
                            current_url = next_url_relative
                            page_num += 1
                            
                        except ControllerDownloadError as e:
                            # Already handled in try block above, but catch here as fallback
                            logger.error(f"Error processing page {page_num}: {e}")
                            result["errors"].append(f"Page {page_num}: {str(e)}")
                            
                            # Don't break immediately - check if we've reached last page
                            # Only break if we can't continue
                            if result.get("total_listings_count") and len(all_listings) >= result["total_listings_count"] * 0.95:
                                logger.info(f"Scraped {len(all_listings):,} listings, close to total ({result['total_listings_count']:,}) - stopping")
                                break
                            
                            # Try to continue to next page
                            try:
                                from urllib.parse import urlparse, parse_qs, urlencode
                                if not current_url.startswith('http'):
                                    full_current_url = urljoin(self.BASE_URL, current_url)
                                else:
                                    full_current_url = current_url
                                
                                parsed = urlparse(full_current_url)
                                query_params = parse_qs(parsed.query)
                                
                                if 'page' in query_params:
                                    next_page_num = int(query_params['page'][0]) + 1
                                    query_params['page'] = [str(next_page_num)]
                                    new_query = urlencode(query_params, doseq=True)
                                    next_url_relative = f"{parsed.path}?{new_query}"
                                    current_url = next_url_relative
                                    page_num = next_page_num
                                    logger.info(f"Continuing to next page {next_page_num} after error")
                                    continue
                            except Exception:
                                pass
                            
                            # If we can't continue, stop
                            logger.error(f"Cannot continue after error on page {page_num}, stopping")
                            break
                            
                        except Exception as e:
                            logger.error(f"Unexpected error on page {page_num}: {e}", exc_info=True)
                            result["errors"].append(f"Page {page_num}: {str(e)}")
                            
                            # Don't break immediately - check if we've reached last page
                            if result.get("total_listings_count") and len(all_listings) >= result["total_listings_count"] * 0.95:
                                logger.info(f"Scraped {len(all_listings):,} listings, close to total ({result['total_listings_count']:,}) - stopping")
                                break
                            
                            # Try to continue
                            try:
                                from urllib.parse import urlparse, parse_qs, urlencode
                                if not current_url.startswith('http'):
                                    full_current_url = urljoin(self.BASE_URL, current_url)
                                else:
                                    full_current_url = current_url
                                
                                parsed = urlparse(full_current_url)
                                query_params = parse_qs(parsed.query)
                                
                                if 'page' in query_params:
                                    next_page_num = int(query_params['page'][0]) + 1
                                    query_params['page'] = [str(next_page_num)]
                                    new_query = urlencode(query_params, doseq=True)
                                    next_url_relative = f"{parsed.path}?{new_query}"
                                    current_url = next_url_relative
                                    page_num = next_page_num
                                    logger.info(f"Continuing to next page {next_page_num} after unexpected error")
                                    continue
                            except Exception:
                                pass
                            
                            logger.error(f"Cannot continue after unexpected error on page {page_num}, stopping")
                            break
                    
                    # JSON is already saved incrementally after each page, but ensure final save
                    listings_file = output_dir / "listings_metadata.json"
                    with open(listings_file, 'w', encoding='utf-8') as f:
                        json.dump(all_listings, f, indent=2, ensure_ascii=False)
                    logger.info(f"Final save: {len(all_listings):,} total listings saved to JSON")
                    
                    result["pages_scraped"] = page_num - 1
                    result["total_listings"] = len(all_listings)
                    result["listings_data"] = all_listings
                    
                finally:
                    # Clean up
                    page.close()
                    browser.close()
            
            result["scrape_duration"] = time.time() - start_time
            
            # Summary
            logger.info("=" * 60)
            logger.info("Scrape Summary")
            logger.info(f"Pages scraped: {result['pages_scraped']}")
            if result.get("expected_pages"):
                logger.info(f"Expected pages (from page 1): ~{result['expected_pages']}")
            logger.info(f"Total listings scraped: {result['total_listings']:,}")
            if result.get("total_listings_count"):
                logger.info(f"Total listings on site: {result['total_listings_count']:,}")
            logger.info(f"HTML files saved: {len(result['html_files'])}")
            logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
            if result["errors"]:
                logger.warning(f"Errors encountered: {len(result['errors'])}")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"Controller scraper failed: {e}", exc_info=True)
            result["scrape_duration"] = time.time() - start_time
            raise


def main():
    """Main entry point for Controller scraper."""
    from utils.logger import setup_logging
    
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        scraper = ControllerScraper(rate_limit=2.0)
        result = scraper.scrape_listings(max_pages=None)  # Scrape all pages
        
        logger.info("Controller scraper completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Controller scraper failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
