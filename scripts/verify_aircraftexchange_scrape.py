"""Verify AircraftExchange scrape results."""
import json
from pathlib import Path
from datetime import datetime

date_str = datetime.now().strftime("%Y-%m-%d")
base_path = Path("store/raw/aircraftexchange") / date_str

print("=" * 60)
print("AircraftExchange Scrape Verification")
print("=" * 60)

# Check index scraper results
index_path = base_path / "index"
if index_path.exists():
    listings_file = index_path / "listings_metadata.json"
    if listings_file.exists():
        listings = json.loads(listings_file.read_text())
        print(f"\nIndex Scraper Results:")
        print(f"  Total listings extracted: {len(listings)}")
        if listings:
            print(f"  Sample listing:")
            sample = listings[0]
            print(f"    Model: {sample.get('aircraft_model')}")
            print(f"    Year: {sample.get('year')}")
            print(f"    Dealer: {sample.get('dealer_name')}")
            print(f"    URL: {sample.get('listing_url')}")
    
    html_files = list(index_path.glob("page_*.html"))
    print(f"  HTML files saved: {len(html_files)}")
    if html_files:
        total_size = sum(f.stat().st_size for f in html_files)
        print(f"  Total HTML size: {total_size:,} bytes")

# Check manufacturer scraper results
mfg_path = base_path / "manufacturers"
if mfg_path.exists():
    mfg_file = mfg_path / "manufacturers_metadata.json"
    if mfg_file.exists():
        manufacturers = json.loads(mfg_file.read_text())
        print(f"\nManufacturer Scraper Results:")
        print(f"  Total manufacturers found: {len(manufacturers)}")
        if manufacturers:
            print(f"  First 10 manufacturers:")
            for mfg in manufacturers[:10]:
                print(f"    - {mfg['name']} (ID: {mfg['manufacturer_id']})")
    
    html_file = mfg_path / "manufacturers_list.html"
    if html_file.exists():
        print(f"  Manufacturers HTML file size: {html_file.stat().st_size:,} bytes")

print("\n" + "=" * 60)
print("Verification Complete")
print("=" * 60)
