# Controller Detail URL Parsing Pattern

This document shows the parsing pattern used to extract data from Controller.com aircraft listing detail pages.

## Overview

The `_extract_detail_fields()` method in `controller_detail_scraper.py` uses a **multi-layered fallback approach** to extract data:

1. **Method 1**: JSON-LD structured data (most reliable)
2. **Method 2**: Specific CSS classes (Controller.com structure)
3. **Method 3**: HTML parsing with regex patterns (fallback)

## Extracted Fields

The scraper extracts the following fields:

```python
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
```

## Parsing Methods by Field

### 1. Aircraft Model & Year

**Method 1: JSON-LD Structured Data**
```python
# Look for <script type="application/ld+json"> with @type="Product"
json_ld_scripts = soup.find_all('script', type='application/ld+json')
for script in json_ld_scripts:
    json_data = json.loads(script.string)
    if json_data.get('@type') == 'Product':
        detail_data['aircraft_model'] = json_data.get('name') or json_data.get('model')
        # Extract year from name (e.g., "2005 CESSNA CITATION XLS")
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', name)
        if year_match:
            detail_data['year'] = year_match.group(1)
```

**Method 2: CSS Class `detail__title`**
```python
# Look for <h1 class="detail__title">
h1_title = soup.find('h1', class_='detail__title')
if h1_title:
    detail_data['aircraft_model'] = h1_title.get_text(strip=True)
    # Extract year from title
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', detail_data['aircraft_model'])
    if year_match:
        detail_data['year'] = year_match.group(1)
```

**Example HTML:**
```html
<h1 class="detail__title">2005 CESSNA CITATION XLS</h1>
```

---

### 2. Total Time Hours

**Method 1: Specs Section**
```python
# Look for sections with "spec" in class name
specs_section = soup.find(['div', 'section'], class_=lambda x: x and 'spec' in str(x).lower())
if specs_section:
    specs_text = specs_section.get_text()
    # Pattern: "Total Time: 1,234" or "Total Time 1,234"
    tt_match = re.search(r'total\s+time[:\s]+([\d,]+)', specs_text, re.IGNORECASE)
    if tt_match:
        detail_data['total_time_hours'] = tt_match.group(1).replace(',', '')
```

**Method 2: Page Text Fallback**
```python
# Patterns:
# - "Total Time: 1,234 hours"
# - "TT: 1,234 hrs"
tt_patterns = [
    r'total\s+time[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
    r'tt[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
]
for pattern in tt_patterns:
    match = re.search(pattern, page_text, re.IGNORECASE)
    if match:
        detail_data['total_time_hours'] = match.group(1).replace(',', '')
        break
```

**Example HTML:**
```html
<div class="specs">
    <p>Total Time: 1,234 hours</p>
</div>
```

---

### 3. Engine Hours

**Method 1: Specs Section**
```python
# Pattern: "Engine Time: 1,234" or "Engine 1 Time: 1,234"
engine_match = re.search(r'engine\s+(?:1\s+)?time[:\s]+([\d,]+)', specs_text, re.IGNORECASE)
if engine_match:
    detail_data['engine_hours'] = engine_match.group(1).replace(',', '')
```

**Method 2: Page Text Fallback**
```python
# Patterns:
# - "Engine Time: 1,234 hours"
# - "Engine Hours: 1,234"
# - "EH: 1,234 hrs"
engine_patterns = [
    r'engine\s+(?:1\s+)?time[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
    r'engine\s+hours?[:\s]+([\d,]+)',
    r'eh[:\s]+([\d,]+)\s*(?:hours?|hrs?|h)?',
]
```

**Example HTML:**
```html
<div class="specs">
    <p>Engine Time: 987 hours</p>
</div>
```

---

### 4. Asking Price

**Method 1: Price CSS Classes**
```python
# Look for elements with "price" in class name
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
```

**Method 2: Regex Patterns in Page Text**
```python
# Patterns:
# - "$1,234,567.00"
# - "Call for Price"
# - "POA" (Price on Application)
price_patterns = [
    r'\$[\d,]+(?:\.\d{2})?',
    r'call\s+for\s+price',
    r'poa',
]
for pattern in price_patterns:
    matches = re.findall(pattern, page_text, re.IGNORECASE)
    if matches:
        detail_data['asking_price'] = matches[0]
        break
```

**Example HTML:**
```html
<div class="price">$2,500,000</div>
<!-- OR -->
<span class="price">Call for Price</span>
```

---

### 5. Location

**Method 1: Location CSS Classes**
```python
# Look for elements with "location" in class name
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
```

**Method 2: Title Tag Fallback**
```python
# Title often contains location: "2005 CESSNA CITATION XLS For Sale in Austin, Texas"
title_tag = soup.find('title')
if title_tag:
    title_text = title_tag.get_text()
    location_match = re.search(r'in\s+([^|]+)', title_text, re.IGNORECASE)
    if location_match:
        detail_data['location'] = location_match.group(1).strip()
```

**Example HTML:**
```html
<title>2005 CESSNA CITATION XLS For Sale in Austin, Texas | Controller.com</title>
<!-- OR -->
<div class="location">Austin, Texas</div>
```

---

### 6. Avionics Description

**Method 1: Avionics Heading**
```python
# Look for headings containing "avionics"
avionics_heading = soup.find(['h2', 'h3', 'h4'], string=re.compile(r'avionics', re.I))
if avionics_heading:
    # Get the next sibling or parent section
    avionics_section = avionics_heading.find_next_sibling(['div', 'section', 'p'])
    if not avionics_section:
        avionics_section = avionics_heading.find_parent(['div', 'section'])
    if avionics_section:
        detail_data['avionics_description'] = avionics_section.get_text(strip=True)
```

**Method 2: Avionics CSS Classes**
```python
# Look for sections with "avionics" in class
avionics_sections = soup.find_all(['div', 'section'], class_=lambda x: x and 'avionics' in str(x).lower())
if avionics_sections:
    detail_data['avionics_description'] = avionics_sections[0].get_text(strip=True)
```

**Method 3: Text Search Fallback**
```python
# Search for text containing "avionics" with reasonable length (50-2000 chars)
for element in soup.find_all(['div', 'section', 'p', 'li']):
    text = element.get_text()
    if 'avionics' in text.lower() and 50 < len(text) < 2000:
        detail_data['avionics_description'] = text.strip()
        break
```

**Example HTML:**
```html
<h3>Avionics</h3>
<div>
    <p>Garmin G1000 NXi, GFC 700 Autopilot, Garmin GTN 750, ...</p>
</div>
```

---

### 7. Seller/Broker Name

**Method 1: Seller Information Patterns**
```python
# Look for "Seller Information", "Dealer", or "Broker" text
seller_patterns = [
    soup.find(string=re.compile(r'Seller Information|Dealer|Broker', re.I)),
    soup.find('div', class_=lambda x: x and ('seller' in str(x).lower() or 'broker' in str(x).lower() or 'dealer' in str(x).lower())),
    soup.find('span', class_=lambda x: x and ('seller' in str(x).lower() or 'broker' in str(x).lower() or 'dealer' in str(x).lower())),
]

for pattern in seller_patterns:
    if pattern:
        seller_text = pattern.get_text(strip=True) if hasattr(pattern, 'get_text') else str(pattern).strip()
        # Extract company name (e.g., "ABC Aircraft LLC" or "XYZ Aviation Inc")
        name_match = re.search(r'([A-Z][a-zA-Z\s&,]+(?:LLC|Inc|Corp|Aircraft|Aviation)?)', seller_text)
        if name_match:
            detail_data['seller_broker_name'] = name_match.group(1).strip()
        break
```

**Example HTML:**
```html
<div class="seller-info">
    <h4>Seller Information</h4>
    <p>ABC Aircraft Sales LLC</p>
</div>
```

---

## Complete Parsing Flow

```python
def _extract_detail_fields(html_content: str, listing_url: str) -> Dict:
    """Extract detail fields using multi-layered fallback approach."""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    page_text = soup.get_text()
    
    # 1. Try JSON-LD structured data (most reliable)
    # 2. Try specific CSS classes (Controller.com structure)
    # 3. Fall back to regex patterns in page text
    
    # For each field:
    # - Try structured data first
    # - Then try CSS selectors
    # - Finally use regex on page text
    
    return detail_data
```

## Key CSS Classes to Look For

Based on the parsing pattern, these are the key CSS classes used by Controller.com:

- `detail__title` - Aircraft model and year
- `detail__specs` or any class containing `spec` - Specifications section
- Any class containing `price` - Price information
- Any class containing `location` - Location information
- Any class containing `avionics` - Avionics description
- Any class containing `seller`, `broker`, or `dealer` - Seller information

## Regex Patterns Summary

| Field | Regex Pattern |
|-------|--------------|
| Year | `r'\b(19\d{2}\|20\d{2})\b'` |
| Total Time | `r'total\s+time[:\s]+([\d,]+)'` or `r'tt[:\s]+([\d,]+)'` |
| Engine Hours | `r'engine\s+(?:1\s+)?time[:\s]+([\d,]+)'` or `r'eh[:\s]+([\d,]+)'` |
| Price | `r'\$[\d,]+(?:\.\d{2})?'` or `r'call\s+for\s+price'` or `r'poa'` |
| Location (from title) | `r'in\s+([^\|]+)'` |
| Seller Name | `r'([A-Z][a-zA-Z\s&,]+(?:LLC\|Inc\|Corp\|Aircraft\|Aviation)?)'` |

## Notes

1. **Always re-scrape**: The scraper always fetches fresh data, even if a URL was previously visited, because site data can change.

2. **Graceful degradation**: If one method fails, the scraper tries the next method in the fallback chain.

3. **Raw text extraction**: All extracted data is raw text with no normalization. Normalization happens in later ETL stages.

4. **Error handling**: If extraction fails for a field, it remains `None` rather than raising an exception, allowing partial data extraction.

5. **Rate limiting**: The scraper waits 2 seconds between requests to avoid overwhelming the server.

## Usage Example

```python
from scrapers.controller_detail_scraper import ControllerDetailScraper

scraper = ControllerDetailScraper(rate_limit=2.0)

# Scrape details from index metadata
result = scraper.scrape_details(
    index_metadata_path=Path("store/raw/controller/2026-01-12/index/listings_metadata.json"),
    max_listings=10  # Limit for testing
)

# Access extracted data
for detail in result['detail_data']:
    print(f"Model: {detail['aircraft_model']}")
    print(f"Year: {detail['year']}")
    print(f"Price: {detail['asking_price']}")
    print(f"Location: {detail['location']}")
```
