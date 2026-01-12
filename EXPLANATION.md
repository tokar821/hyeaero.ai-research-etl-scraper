# Controller.com Scraper - How It Works

## Two Scraping Methods

### 1. Playwright Method (`controller_scraper.py`)
**How it works:**
- Uses Playwright to control a real Chromium browser
- Applies stealth techniques to avoid bot detection:
  - Removes `navigator.webdriver` property
  - Modifies browser fingerprinting (Canvas, WebGL)
  - Uses realistic browser headers
  - Simulates human mouse movements and scrolling
- Goes directly to listings URL (no homepage visit needed)
- Handles cookie consent banner automatically
- Extracts data from HTML using BeautifulSoup

**Current Status:** May still face bot detection on some sites

### 2. Undetected-Chromedriver Method (`controller_scraper_undetected.py`)
**How it works:**
- Uses `undetected-chromedriver` library (Selenium-based)
- **undetected-chromedriver** is specifically designed to bypass bot detection:
  - Patches ChromeDriver executable to remove automation flags
  - Modifies Chrome's internal properties to hide automation
  - Uses real Chrome browser (not headless by default)
  - Automatically handles Chrome version matching
- More effective at bypassing bot detection systems
- Uses Selenium WebDriverWait to ensure content loads
- Same extraction logic as Playwright version

**Current Status:** ✅ Successfully bypasses bot detection and extracts data

## Data Extraction

### HTML Patterns Used (from listing cards):

1. **Listing Container:**
   - `div#listContainer` or `div.list-container`
   - Contains all listing cards

2. **Listing Cards:**
   - `div.list-listing-card-wrapper` - Each aircraft listing card

3. **Aircraft Model:**
   - `h2.listing-portion-title > a.list-listing-title-link`
   - Example: "1997 GULFSTREAM GV"

4. **Price:**
   - `div.retail-price-container > span.price`
   - Example: "USD $415,000" or "CALL FOR PRICE"

5. **Location:**
   - `div.machine-location`
   - Example: "Conroe, Texas"

6. **Listing URL:**
   - `a.list-listing-title-link[href*="/listing/for-sale/"]`
   - OR `a.view-listing-details-link[href*="/listing/for-sale/"]`

7. **Total Time:**
   - Pattern: "Total Time: 11,155.9" in card text

8. **Seller Info:**
   - Seller name: Pattern "Seller: FlyTru Aviation"
   - Phone: `a[href^="tel:"]` or regex pattern
   - Email: `a[href^="mailto:"]`

9. **Additional Fields:**
   - Aircraft Type: "Jet Aircraft", "Piston Single Aircraft", etc.
   - Year: Extracted from model name (regex)
   - Premium Listing: Badge or text check
   - Payment Estimate: "Payments as low as..." pattern

## How to Use

### Playwright Version:
```python
from scrapers.controller_scraper import ControllerScraper

scraper = ControllerScraper(rate_limit=3.0, headless=False)
result = scraper.scrape_listings(max_pages=None)  # All pages
```

### Undetected-Chromedriver Version (Recommended):
```python
from scrapers.controller_scraper_undetected import ControllerScraperUndetected

scraper = ControllerScraperUndetected(rate_limit=3.0, headless=False)
result = scraper.scrape_listings(max_pages=None)  # All pages
```

## Extracted Data Fields

Each listing includes:
- `listing_url` - Full URL to detail page
- `listing_id` - Unique listing ID
- `aircraft_model` - Full model name (e.g., "1997 GULFSTREAM GV")
- `aircraft_type` - Type (Jet, Piston Single, etc.)
- `year` - Year extracted from model
- `listing_location` - Location (city, state)
- `listing_price` - Price or "CALL FOR PRICE"
- `total_time_hours` - Total flight hours
- `seller_name` - Seller/dealer name
- `seller_phone` - Phone number
- `seller_email` - Email (if available)
- `is_premium_listing` - Boolean
- `payment_estimate` - Financing estimate
- `scrape_timestamp` - When data was scraped
- `page_url` - Source page URL
- `position` - Position on page

## Pagination

- Automatically detects pagination from text: "1 - 28 of 5,122 Listings"
- Stops when `current_end >= total_listings` (last page)
- Constructs next page URL automatically if needed
