"""Check description extraction."""
import json
from pathlib import Path

data_file = Path('store/raw/controller/2026-01-12/details/details_metadata.json')
data = json.loads(data_file.read_text())

print("Description extraction status:")
print("=" * 60)
for i, d in enumerate(data, 1):
    desc = d.get('description')
    if desc:
        print(f"Listing {i}: [OK] EXTRACTED")
        print(f"  Length: {len(desc)} characters")
        print(f"  Preview: {desc[:100]}...")
    else:
        print(f"Listing {i}: ✗ MISSING")
    print()
