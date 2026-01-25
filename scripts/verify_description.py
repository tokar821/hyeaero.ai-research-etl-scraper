"""Verify description extraction."""
import json
from pathlib import Path

data_file = Path('store/raw/controller/2026-01-12/details/details_metadata.json')
data = json.loads(data_file.read_text())

print("Description extraction verification:")
print("=" * 60)
for i, d in enumerate(data, 1):
    desc = d.get('description')
    url = d.get('listing_url', 'Unknown')
    listing_id = url.split('/')[-2] if '/' in url else 'Unknown'
    
    if desc:
        print(f"Listing {i} (ID: {listing_id}): EXTRACTED")
        print(f"  Length: {len(desc)} characters")
        print(f"  Content: {desc}")
    else:
        print(f"Listing {i} (ID: {listing_id}): MISSING!")
    print()

print(f"\nSummary: {sum(1 for d in data if d.get('description'))}/{len(data)} listings have descriptions extracted")
