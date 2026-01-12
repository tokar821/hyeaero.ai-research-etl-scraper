"""Verify extraction results."""
import json
from pathlib import Path

data_file = Path('store/raw/controller/2026-01-12/details/details_metadata.json')
data = json.loads(data_file.read_text())

print(f"Total listings: {len(data)}")
print(f"\nField extraction summary:")
print(f"  - Listings with location: {sum(1 for d in data if d.get('location'))}")
print(f"  - Listings with clean location (no extra data): {sum(1 for d in data if d.get('location') and len(d.get('location', '').split()) < 5)}")
print(f"  - Listings with props_notes: {sum(1 for d in data if d.get('props_notes'))}")
print(f"  - Listings with additional_equipment: {sum(1 for d in data if d.get('additional_equipment'))}")
print(f"  - Listings with exterior_notes: {sum(1 for d in data if d.get('exterior_notes'))}")
print(f"  - Listings with interior_notes: {sum(1 for d in data if d.get('interior_notes'))}")
print(f"  - Listings with inspection_status: {sum(1 for d in data if d.get('inspection_status'))}")
print(f"  - Listings with total_landings: {sum(1 for d in data if d.get('total_landings'))}")
print(f"  - Listings with engine_1_make_model: {sum(1 for d in data if d.get('engine_1_make_model'))}")
print(f"  - Listings with engine_2_make_model: {sum(1 for d in data if d.get('engine_2_make_model'))}")
print(f"  - Listings with year_painted: {sum(1 for d in data if d.get('year_painted'))}")
print(f"  - Listings with number_of_seats: {sum(1 for d in data if d.get('number_of_seats'))}")
print(f"  - Listings with galley: {sum(1 for d in data if d.get('galley'))}")

print(f"\nSample location values (checking for clean extraction):")
for d in data:
    loc = d.get('location', '')
    if loc:
        print(f"  - {loc[:80]}")

print(f"\nSample asking_price values (checking for no currency/buttons):")
for d in data:
    price = d.get('asking_price', '')
    if price:
        print(f"  - {price}")
