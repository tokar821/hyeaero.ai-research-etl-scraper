"""Controller.com Aircraft Listings Scraper.

Scrapes aircraft listings from Controller.com with pagination support.
Uses Playwright for JavaScript-rendered content and bot protection bypass.
Saves raw HTML responses and extracts basic listing information.
"""

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, Page, Browser
from bs4 import BeautifulSoup

from utils.logger import get_logger

logger = get_logger(__name__)


class ControllerScraperError(Exception):
    """Base exception for Controller scraper."""
    pass


class ControllerDownloadError(ControllerScraperError):
    """Raised when download fails."""
    pass


class ControllerScraper:
    """Scraper for Controller.com aircraft listings using Playwright."""
    
    BASE_URL = "https://www.controller.com"
    START_URL = "/listings/search?page=1"
    
    # Rate limiting: seconds to wait between requests
    RATE_LIMIT_DELAY = 2.0  # 2 seconds between requests
    
    def __init__(self, storage_base_path: Optional[Path] = None, rate_limit: float = 2.0):
        """Initialize Controller scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
            rate_limit: Seconds to wait between requests. Default: 2.0 seconds.
        """
        if storage_base_path is None:
            storage_base_path = Path(__file__).parent.parent / "store"
        
        self.storage_base_path = Path(storage_base_path)
        self.raw_controller_path = self.storage_base_path / "raw" / "controller"
        
        # Create directories if they don't exist
        self.raw_controller_path.mkdir(parents=True, exist_ok=True)
        
        self.rate_limit = rate_limit
        
        # Track visited URLs to avoid duplicates
        self.visited_urls = set()
        
    def _setup_browser(self, playwright) -> Browser:
        """Setup and configure Playwright browser.
        
        Args:
            playwright: Playwright instance.
            
        Returns:
            Configured browser instance.
        """
        # Launch browser in headless mode (set to False for debugging)
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
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
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
        )
        
        # Add extra headers
        context.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        })
        
        page = context.new_page()
        
        # Remove webdriver property to avoid detection
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        return page
    
    def _wait_for_rate_limit(self):
        """Wait for rate limit delay."""
        time.sleep(self.rate_limit)
    
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
                
                # Wait for page to load (wait for content selector if provided)
                if wait_selector:
                    try:
                        page.wait_for_selector(wait_selector, timeout=10000)
                    except Exception:
                        logger.warning(f"Selector {wait_selector} not found, continuing anyway")
                else:
                    # Default: wait for dynamic content and network to settle
                    try:
                        # Wait for network idle (max 15 seconds)
                        page.wait_for_load_state('networkidle', timeout=15000)
                    except Exception:
                        # If networkidle times out, wait a bit anyway
                        logger.debug("Network idle timeout, waiting fixed delay")
                        page.wait_for_timeout(3000)
                
                # Get page content
                html_content = page.content()
                
                content_length = len(html_content)
                logger.info(f"Retrieved {content_length:,} bytes from {full_url}")
                
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
        """Extract basic listing information from HTML.
        
        Args:
            html_content: HTML content as string.
            page_url: URL of the page.
            
        Returns:
            List of dictionaries with extracted listing data.
        """
        listings = []
        scrape_timestamp = datetime.now().isoformat()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for listing containers - Controller.com specific patterns
            # Try multiple common patterns
            listing_containers = []
            
            # Pattern 1: Common listing card classes
            listing_containers = soup.find_all(['article', 'div'], class_=lambda x: x and (
                'listing' in str(x).lower() or 
                'result' in str(x).lower() or 
                'item' in str(x).lower() or
                'card' in str(x).lower() or
                'tile' in str(x).lower()
            ))
            
            # Pattern 2: If no results, try data attributes
            if not listing_containers:
                listing_containers = soup.find_all('div', attrs={'data-listing-id': True})
            
            # Pattern 3: Try any div with href link (listing cards often have links)
            if not listing_containers:
                potential_containers = soup.find_all('div')
                for div in potential_containers:
                    if div.find('a', href=lambda x: x and '/listings/' in str(x)):
                        listing_containers.append(div)
            
            logger.info(f"Found {len(listing_containers)} potential listing containers on page")
            
            for idx, container in enumerate(listing_containers, 1):
                try:
                    listing_data = {
                        'listing_url': None,
                        'listing_id': None,
                        'aircraft_model': None,
                        'listing_location': None,
                        'listing_price': None,
                        'scrape_timestamp': scrape_timestamp,
                        'page_url': page_url,
                        'position': idx
                    }
                    
                    # Extract listing URL
                    link = container.find('a', href=True)
                    if link:
                        href = link.get('href', '')
                        if href:
                            listing_data['listing_url'] = urljoin(self.BASE_URL, href) if not href.startswith('http') else href
                            
                            # Try to extract listing ID from URL
                            parsed_url = urlparse(listing_data['listing_url'])
                            path_parts = [p for p in parsed_url.path.split('/') if p]
                            if 'listings' in path_parts:
                                listing_idx = path_parts.index('listings')
                                if listing_idx + 1 < len(path_parts):
                                    listing_data['listing_id'] = path_parts[listing_idx + 1]
                    
                    # Try data attribute for listing ID
                    if not listing_data['listing_id']:
                        listing_id_attr = container.get('data-listing-id') or container.get('data-id')
                        if listing_id_attr:
                            listing_data['listing_id'] = str(listing_id_attr)
                    
                    # Extract aircraft model (raw text)
                    # Look for model in common locations
                    model_elements = [
                        container.find('h2'),
                        container.find('h3'),
                        container.find('h4'),
                        container.find('a', class_=lambda x: x and 'title' in str(x).lower()),
                        container.find('span', class_=lambda x: x and 'model' in str(x).lower()),
                        container.find('div', class_=lambda x: x and 'model' in str(x).lower()),
                    ]
                    
                    for element in model_elements:
                        if element:
                            text = element.get_text(strip=True)
                            if text and len(text) > 3:  # Minimum length filter
                                listing_data['aircraft_model'] = text
                                break
                    
                    # Extract location (raw text)
                    location_elements = [
                        container.find('span', class_=lambda x: x and 'location' in str(x).lower()),
                        container.find('div', class_=lambda x: x and 'location' in str(x).lower()),
                        container.find('p', class_=lambda x: x and 'location' in str(x).lower()),
                        container.find('span', class_=lambda x: x and ('city' in str(x).lower() or 'state' in str(x).lower())),
                    ]
                    
                    for element in location_elements:
                        if element:
                            listing_data['listing_location'] = element.get_text(strip=True)
                            break
                    
                    # Extract price (raw text)
                    price_elements = [
                        container.find('span', class_=lambda x: x and 'price' in str(x).lower()),
                        container.find('div', class_=lambda x: x and 'price' in str(x).lower()),
                        container.find('p', class_=lambda x: x and 'price' in str(x).lower()),
                    ]
                    
                    for element in price_elements:
                        if element:
                            text = element.get_text(strip=True)
                            if '$' in text or 'price' in text.lower():
                                listing_data['listing_price'] = text
                                break
                    
                    # Fallback: search for $ in text anywhere in container
                    if not listing_data['listing_price']:
                        container_text = container.get_text()
                        import re
                        price_match = re.search(r'\$[\d,]+', container_text)
                        if price_match:
                            listing_data['listing_price'] = price_match.group(0)
                    
                    # Only add if we have at least a URL
                    if listing_data['listing_url']:
                        listings.append(listing_data)
                    
                except Exception as e:
                    logger.warning(f"Error extracting listing {idx}: {e}")
                    continue
            
            logger.info(f"Extracted {len(listings)} listings from page")
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
        
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
