"""Controller.com Aircraft Listing Detail Scraper.

Scrapes detailed information from individual listing pages.
Reads listing URLs from index scraper output and extracts detailed fields.
Uses Playwright for JavaScript-rendered content and bot protection bypass.
Saves raw HTML responses and extracted detail information.
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


class ControllerDetailScraperError(Exception):
    """Base exception for Controller detail scraper."""
    pass


class ControllerDetailDownloadError(ControllerDetailScraperError):
    """Raised when download fails."""
    pass


class ControllerDetailScraper:
    """Scraper for Controller.com aircraft listing detail pages using Playwright."""
    
    BASE_URL = "https://www.controller.com"
    
    # Rate limiting: seconds to wait between requests
    RATE_LIMIT_DELAY = 2.0  # 2 seconds between requests
    
    def __init__(self, storage_base_path: Optional[Path] = None, rate_limit: float = 2.0):
        """Initialize Controller detail scraper.
        
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
    
    def _fetch_page(self, page: Page, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a detail page and return raw HTML with retry logic.
        
        Args:
            page: Playwright page instance.
            url: URL to fetch.
            retries: Number of retry attempts for failed requests. Default: 3.
            
        Returns:
            Raw HTML content as string, or None if failed after retries.
            
        Raises:
            ControllerDetailDownloadError: If download fails after all retries.
        """
        # Always re-scrape - don't skip even if URL was visited
        # Site data can change, so we always fetch fresh data
        
        full_url = urljoin(self.BASE_URL, url) if not url.startswith('http') else url
        
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Navigating to: {full_url} (attempt {attempt}/{retries})")
                
                # Navigate to page
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
                    raise ControllerDetailDownloadError(f"HTTP {response.status} error")
                
                # Wait for page to load
                try:
                    page.wait_for_load_state('networkidle', timeout=15000)
                except Exception:
                    logger.debug("Network idle timeout, waiting fixed delay")
                    page.wait_for_timeout(3000)
                
                # Get page content
                html_content = page.content()
                
                content_length = len(html_content)
                logger.info(f"Retrieved {content_length:,} bytes from {full_url}")
                
        # Note: We don't track visited_urls for detail pages
        # Always re-scrape because site data can change
                
                return html_content
                
            except Exception as e:
                if attempt < retries:
                    wait_time = attempt * 5  # Exponential backoff
                    logger.warning(f"Error fetching {url} (attempt {attempt}/{retries}): {e} - retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Error fetching {url} after {retries} attempts: {e}")
                    raise ControllerDetailDownloadError(f"Failed to fetch {url} after {retries} attempts: {e}") from e
        
        return None
    
    def _save_html_page(self, html_content: str, listing_id: str, output_dir: Path) -> Path:
        """Save HTML page to disk.
        
        Note: Always overwrites existing files. HTML pages are re-scraped on every run
        because the site can update with more or less data.
        
        Args:
            html_content: HTML content as string.
            listing_id: Listing ID (from URL or metadata).
            output_dir: Directory to save to.
            
        Returns:
            Path to saved file.
        """
        # Use listing ID if available, otherwise generate filename from hash
        if listing_id:
            filename = f"listing_{listing_id}.html"
        else:
            # Generate filename from URL hash
            url_hash = hashlib.md5(html_content.encode('utf-8')).hexdigest()[:8]
            filename = f"listing_{url_hash}.html"
        
        filepath = output_dir / filename
        
        # Convert to bytes for saving
        html_bytes = html_content.encode('utf-8')
        
        # Always overwrite - never skip pages (site data can change)
        with open(filepath, 'wb') as f:
            f.write(html_bytes)
        
        file_hash = hashlib.md5(html_bytes).hexdigest()
        file_size = filepath.stat().st_size
        
        logger.info(f"Saved detail page: {filename} ({file_size:,} bytes, MD5: {file_hash})")
        
        return filepath
    
    def _extract_listing_id(self, url: str) -> Optional[str]:
        """Extract listing ID from URL.
        
        Args:
            url: Listing URL.
            
        Returns:
            Listing ID if found, None otherwise.
        """
        try:
            parsed_url = urlparse(url)
            path_parts = [p for p in parsed_url.path.split('/') if p]
            
            # URL pattern: /listing/for-sale/{ID}/{slug}
            if 'listing' in path_parts:
                listing_idx = path_parts.index('listing')
                if listing_idx + 2 < len(path_parts):
                    # Next part after 'listing' should be ID (usually numeric)
                    potential_id = path_parts[listing_idx + 2]
                    if potential_id.isdigit():
                        return potential_id
            
            return None
        except Exception:
            return None
    
    def _extract_detail_fields(self, html_content: str, listing_url: str) -> Dict:
        """Extract detail fields from HTML (raw text, no normalization).
        
        First tries to extract from JSON-LD structured data (most reliable),
        then uses specific CSS classes, then falls back to HTML parsing.
        
        Args:
            html_content: HTML content as string.
            listing_url: URL of the listing.
            
        Returns:
            Dictionary with extracted fields (all raw text, no normalization).
        """
        detail_data = {
            'listing_url': listing_url,
            'aircraft_model': None,
            'year': None,
            'total_time_hours': None,
            'engine_hours': None,
            'avionics_description': None,
            'asking_price': None,
            'location': None,
            'seller_broker_name': None,
            'scrape_timestamp': datetime.now().isoformat(),
        }
        
        try:
            import re
            import json
            soup = BeautifulSoup(html_content, 'html.parser')
            page_text = soup.get_text()
            
            # Method 1: Extract from JSON-LD structured data (most reliable)
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_ld_scripts:
                if script.string:
                    try:
                        json_data = json.loads(script.string)
                        # Look for Product type (contains aircraft info)
                        if isinstance(json_data, dict) and json_data.get('@type') == 'Product':
                            if not detail_data['aircraft_model']:
                                detail_data['aircraft_model'] = json_data.get('name') or json_data.get('model')
                            if not detail_data['year']:
                                # Year might be in name (e.g., "2005 CESSNA CITATION XLS")
                                name = json_data.get('name', '')
                                year_match = re.search(r'\b(19\d{2}|20\d{2})\b', name)
                                if year_match:
                                    detail_data['year'] = year_match.group(1)
                    except json.JSONDecodeError:
                        continue
            
            # Method 2: Extract from specific CSS classes (Controller.com structure)
            # Aircraft Model - use detail__title class
            h1_title = soup.find('h1', class_='detail__title')
            if h1_title and not detail_data['aircraft_model']:
                detail_data['aircraft_model'] = h1_title.get_text(strip=True)
                # Extract year from title if not already found
                if not detail_data['year']:
                    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', detail_data['aircraft_model'])
                    if year_match:
                        detail_data['year'] = year_match.group(1)
            
            # Extract from detail__specs or similar sections
            specs_section = soup.find(['div', 'section'], class_=lambda x: x and 'spec' in str(x).lower())
            if specs_section:
                specs_text = specs_section.get_text()
                
                # Total Time
                if not detail_data['total_time_hours']:
                    tt_match = re.search(r'total\s+time[:\s]+([\d,]+)', specs_text, re.IGNORECASE)
                    if tt_match:
                        detail_data['total_time_hours'] = tt_match.group(1).replace(',', '')
                
                # Engine Hours
                if not detail_data['engine_hours']:
                    engine_match = re.search(r'engine\s+(?:1\s+)?time[:\s]+([\d,]+)', specs_text, re.IGNORECASE)
                    if engine_match:
                        detail_data['engine_hours'] = engine_match.group(1).replace(',', '')
            
            # Extract Asking Price
            # Look for price in specific classes or common patterns
            price_elements = [
                soup.find('div', class_=lambda x: x and 'price' in str(x).lower()),
                soup.find('span', class_=lambda x: x and 'price' in str(x).lower()),
                soup.find('p', class_=lambda x: x and 'price' in str(x).lower()),
            ]
            
            for elem in price_elements:
                if elem:
                    price_text = elem.get_text(strip=True)
                    if price_text and ('$' in price_text or 'price' in price_text.lower() or 'call' in price_text.lower()):
                        detail_data['asking_price'] = price_text
                        break
            
            # Fallback: search for price patterns in page text
            if not detail_data['asking_price']:
                price_patterns = [
                    r'\$[\d,]+(?:\.\d{2})?',
                    r'call\s+for\s+price',
                    r'poa',
                ]
                for pattern in price_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    if matches:
                        detail_data['asking_price'] = matches[0] if isinstance(matches[0], str) else str(matches[0])
                        break
            
            # Extract Location
            location_elements = [
                soup.find('div', class_=lambda x: x and 'location' in str(x).lower()),
                soup.find('span', class_=lambda x: x and 'location' in str(x).lower()),
                soup.find('p', class_=lambda x: x and 'location' in str(x).lower()),
            ]
            
            for elem in location_elements:
                if elem:
                    location_text = elem.get_text(strip=True)
                    if location_text:
                        detail_data['location'] = location_text
                        break
            
            # Extract from meta tags or title if location not found
            if not detail_data['location']:
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text()
                    # Title often contains location: "2005 CESSNA CITATION XLS For Sale in Austin, Texas"
                    location_match = re.search(r'in\s+([^|]+)', title_text, re.IGNORECASE)
                    if location_match:
                        detail_data['location'] = location_match.group(1).strip()
            
            # Extract Avionics Description
            # Look for sections with "avionics" in heading or content
            # Be more specific - look for avionics in headings first
            avionics_heading = soup.find(['h2', 'h3', 'h4'], string=re.compile(r'avionics', re.I))
            if avionics_heading:
                # Get the next sibling or parent section
                avionics_section = avionics_heading.find_next_sibling(['div', 'section', 'p'])
                if not avionics_section:
                    avionics_section = avionics_heading.find_parent(['div', 'section'])
                if avionics_section:
                    detail_data['avionics_description'] = avionics_section.get_text(strip=True)
            
            # Fallback: look for sections with "avionics" in class
            if not detail_data['avionics_description']:
                avionics_sections = soup.find_all(['div', 'section'], class_=lambda x: x and 'avionics' in str(x).lower())
                if avionics_sections:
                    detail_data['avionics_description'] = avionics_sections[0].get_text(strip=True)
            
            # Fallback: search for text containing "avionics" but limit length
            if not detail_data['avionics_description']:
                for element in soup.find_all(['div', 'section', 'p', 'li']):
                    text = element.get_text()
                    if 'avionics' in text.lower() and 50 < len(text) < 2000:  # Reasonable length
                        detail_data['avionics_description'] = text.strip()
                        break
            
            # Extract Seller/Broker Name
            # Look for "Seller Information" or "Dealer" patterns
            seller_patterns = [
                soup.find(string=re.compile(r'Seller Information|Dealer|Broker', re.I)),
                soup.find('div', class_=lambda x: x and ('seller' in str(x).lower() or 'broker' in str(x).lower() or 'dealer' in str(x).lower())),
                soup.find('span', class_=lambda x: x and ('seller' in str(x).lower() or 'broker' in str(x).lower() or 'dealer' in str(x).lower())),
            ]
            
            for pattern in seller_patterns:
                if pattern:
                    if hasattr(pattern, 'get_text'):
                        seller_text = pattern.get_text(strip=True)
                    else:
                        # It's a NavigableString, get parent
                        seller_text = pattern.parent.get_text(strip=True) if pattern.parent else str(pattern).strip()
                    
                    if seller_text and len(seller_text) > 2 and len(seller_text) < 200:  # Reasonable length
                        # Try to extract just the name, not all surrounding text
                        # Look for company names or contact names
                        name_match = re.search(r'([A-Z][a-zA-Z\s&,]+(?:LLC|Inc|Corp|Aircraft|Aviation)?)', seller_text)
                        if name_match:
                            detail_data['seller_broker_name'] = name_match.group(1).strip()
                        else:
                            detail_data['seller_broker_name'] = seller_text[:100]  # Limit length
                        break
            
            # Fallback: Extract Total Time and Engine Hours from page text if not found
            if not detail_data['total_time_hours']:
                tt_patterns = [
                    r'total\s+time[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
                    r'tt[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
                ]
                for pattern in tt_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        detail_data['total_time_hours'] = match.group(1).replace(',', '')
                        break
            
            if not detail_data['engine_hours']:
                engine_patterns = [
                    r'engine\s+(?:1\s+)?time[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
                    r'engine\s+hours?[:\s]+([\d,]+)',
                    r'eh[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
                ]
                for pattern in engine_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        detail_data['engine_hours'] = match.group(1).replace(',', '')
                        break
            
        except Exception as e:
            logger.warning(f"Error extracting detail fields from {listing_url}: {e}")
        
        return detail_data
    
    def load_listing_urls(self, index_metadata_path: Path) -> List[str]:
        """Load listing URLs from index scraper metadata file.
        
        Args:
            index_metadata_path: Path to listings_metadata.json from index scraper.
            
        Returns:
            List of unique listing URLs.
        """
        try:
            with open(index_metadata_path, 'r', encoding='utf-8') as f:
                listings = json.load(f)
            
            # Extract unique URLs - filter out financing/tracking URLs
            urls = set()
            for listing in listings:
                url = listing.get('listing_url')
                if url:
                    # Filter out financing/tracking URLs - only keep actual listing pages
                    if 'controller.com/listing' in url.lower() and 'analyticstracking' not in url.lower():
                        urls.add(url)
                    else:
                        logger.debug(f"Skipping non-listing URL: {url}")
            
            unique_urls = sorted(list(urls))
            logger.info(f"Loaded {len(unique_urls)} unique listing URLs from {index_metadata_path}")
            
            return unique_urls
            
        except Exception as e:
            logger.error(f"Error loading listing URLs from {index_metadata_path}: {e}")
            raise ControllerDetailScraperError(f"Failed to load listing URLs: {e}") from e
    
    def scrape_details(
        self,
        listing_urls: Optional[List[str]] = None,
        index_metadata_path: Optional[Path] = None,
        date: Optional[datetime] = None,
        max_listings: Optional[int] = None
    ) -> Dict:
        """Scrape detail pages for listing URLs.
        
        Args:
            listing_urls: List of listing URLs to scrape. If None, loads from index_metadata_path.
            index_metadata_path: Path to listings_metadata.json from index scraper.
            date: Date for storage path. If None, uses current date.
            max_listings: Maximum number of listings to scrape. If None, scrapes all.
            
        Returns:
            Dictionary with scrape statistics.
        """
        from playwright.sync_api import sync_playwright
        
        if date is None:
            date = datetime.now()
        
        start_time = time.time()
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_controller_path / date_str / "details"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info("Controller.com Aircraft Listing Detail Scraper (Playwright)")
        logger.info(f"Date: {date_str}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 60)
        
        # Load URLs if not provided
        if listing_urls is None:
            if index_metadata_path is None:
                # Default: look for listings_metadata.json in index directory
                index_dir = self.raw_controller_path / date_str / "index"
                index_metadata_path = index_dir / "listings_metadata.json"
            
            if not index_metadata_path.exists():
                raise ControllerDetailScraperError(f"Index metadata file not found: {index_metadata_path}")
            
            listing_urls = self.load_listing_urls(index_metadata_path)
        
        if max_listings:
            listing_urls = listing_urls[:max_listings]
            logger.info(f"Limiting to {max_listings} listings for testing")
        
        result = {
            "date": date_str,
            "total_urls": len(listing_urls),
            "listings_scraped": 0,
            "listings_failed": 0,
            "html_files": [],
            "detail_data": [],
            "scrape_duration": 0,
            "errors": []
        }
        
        try:
            with sync_playwright() as playwright:
                # Setup browser
                browser = self._setup_browser(playwright)
                page = self._setup_page(browser)
                
                try:
                    for idx, listing_url in enumerate(listing_urls, 1):
                        try:
                            logger.info(f"Processing listing {idx}/{len(listing_urls)}: {listing_url}")
                            
                            # Rate limiting
                            self._wait_for_rate_limit()
                            
                            # Fetch page
                            try:
                                html_content = self._fetch_page(page, listing_url)
                            except ControllerDetailDownloadError as e:
                                logger.error(f"Failed to fetch {listing_url}: {e}")
                                result["errors"].append(f"Listing {idx}: {str(e)}")
                                result["listings_failed"] += 1
                                continue
                            
                            if html_content is None:
                                logger.warning(f"Failed to fetch {listing_url} (returned None), skipping")
                                result["errors"].append(f"Listing {idx}: Failed to fetch (None)")
                                result["listings_failed"] += 1
                                continue
                            
                            # Extract listing ID
                            listing_id = self._extract_listing_id(listing_url)
                            
                            # Save HTML
                            html_file = self._save_html_page(html_content, listing_id or str(idx), output_dir)
                            result["html_files"].append(str(html_file))
                            
                            # Extract detail fields
                            detail_data = self._extract_detail_fields(html_content, listing_url)
                            result["detail_data"].append(detail_data)
                            result["listings_scraped"] += 1
                            
                            logger.info(f"[OK] Scraped detail for listing {idx}/{len(listing_urls)}")
                            
                        except Exception as e:
                            logger.error(f"Error processing listing {idx} ({listing_url}): {e}", exc_info=True)
                            result["errors"].append(f"Listing {idx}: {str(e)}")
                            result["listings_failed"] += 1
                            continue
                    
                    # Save detail data to JSON
                    details_file = output_dir / "details_metadata.json"
                    with open(details_file, 'w', encoding='utf-8') as f:
                        json.dump(result["detail_data"], f, indent=2, ensure_ascii=False)
                    logger.info(f"Saved {len(result['detail_data'])} detail records to {details_file}")
                    
                finally:
                    # Clean up
                    page.close()
                    browser.close()
            
            result["scrape_duration"] = time.time() - start_time
            
            # Summary
            logger.info("=" * 60)
            logger.info("Detail Scrape Summary")
            logger.info(f"Total URLs: {result['total_urls']}")
            logger.info(f"Listings scraped: {result['listings_scraped']}")
            logger.info(f"Listings failed: {result['listings_failed']}")
            logger.info(f"HTML files saved: {len(result['html_files'])}")
            logger.info(f"Scrape duration: {result['scrape_duration']:.2f} seconds")
            if result["errors"]:
                logger.warning(f"Errors encountered: {len(result['errors'])}")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"Controller detail scraper failed: {e}", exc_info=True)
            result["scrape_duration"] = time.time() - start_time
            raise


def main():
    """Main entry point for Controller detail scraper."""
    from utils.logger import setup_logging
    
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        scraper = ControllerDetailScraper(rate_limit=2.0)
        result = scraper.scrape_details()  # Will use default index_metadata_path
        
        logger.info("Controller detail scraper completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Controller detail scraper failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
