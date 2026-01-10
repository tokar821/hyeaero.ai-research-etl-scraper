# HyeAero ETL Pipeline

ETL pipeline for data ingestion and normalization from aircraft market data sources.

## Overview

This module handles:
- Scraping data from sources (Controller, AircraftExchange, FAA)
- Raw data storage in Akamai Object Storage
- Data normalization
- Loading into downstream systems

## Architecture

- **Config Module**: Central configuration loader with environment variable support
- **Storage Module**: Akamai Object Storage client wrapper (S3-compatible API)
- **Utils Module**: Logging and other utilities

## Setup

### Prerequisites

- Python 3.12
- Akamai Object Storage account with S3-compatible API access

### Installation

1. Create a virtual environment:
```bash
python -m venv venv
```

2. Activate the virtual environment:
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
# Copy the example env file
cp .env.example .env

# Edit .env with your actual credentials
```

### Environment Variables

Required environment variables (see `.env.example`):

- `ENVIRONMENT`: `dev`, `prod`, or `local` (local enables dry-run mode)
- `DRY_RUN`: `true` or `false` (local defaults to true)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `AKAMAI_ACCESS_KEY`: Your Akamai access key
- `AKAMAI_SECRET_KEY`: Your Akamai secret key
- `AKAMAI_ENDPOINT`: Akamai endpoint URL
- `AKAMAI_BUCKET_NAME`: Bucket name for storage
- `AKAMAI_REGION`: Optional region (defaults to us-east-1)

## Usage

### Basic Example

```python
from config import get_config
from storage import AkamaiStorageClient
from utils import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Load config
config = get_config()
logger.info(f"Environment: {config.environment}, Dry-run: {config.is_dry_run()}")

# Initialize storage client
storage = AkamaiStorageClient()

# Upload raw data
data = b'{"aircraft": "Phenom 300", "year": 2017}'
storage.upload_raw_data(
    source="controller",
    filename="listing_12345.json",
    data=data,
    content_type="application/json"
)

# Upload snapshot
storage.upload_snapshot(
    entity="aircraft",
    filename="phenom300_2017.json",
    data=data,
    content_type="application/json"
)
```

### Storage Path Conventions

- **Raw data**: `raw/{source}/{YYYY-MM-DD}/{filename}`
  - Example: `raw/controller/2024-01-15/listing_12345.json`

- **Snapshots**: `snapshots/{entity}/{YYYY-MM-DD}/{filename}`
  - Example: `snapshots/aircraft/2024-01-15/phenom300_2017.json`

## Dry-Run Mode

When `ENVIRONMENT=local` or `DRY_RUN=true`, the storage client operates in dry-run mode:
- No actual uploads to storage
- All operations are logged as `[DRY-RUN]`
- Useful for testing without affecting production data

## Error Handling

All storage operations raise explicit exceptions:
- `StorageError`: Base exception for storage operations
- `StorageConnectionError`: Connection/authentication failures
- `StorageUploadError`: Upload failures
- `StorageDownloadError`: Download failures

## Logging

Logging includes:
- Timestamps (YYYY-MM-DD HH:MM:SS)
- Module names
- Log levels

Example log output:
```
2024-01-15 14:30:45 | storage.akamai_client | INFO | Storage client initialized successfully for bucket: hyeaero-data
2024-01-15 14:30:46 | storage.akamai_client | INFO | Successfully uploaded raw data: raw/controller/2024-01-15/listing_12345.json
```

## Development

### Project Structure

```
etl-pipeline/
├── config/
│   ├── __init__.py
│   └── config_loader.py
├── storage/
│   ├── __init__.py
│   └── akamai_client.py
├── utils/
│   ├── __init__.py
│   └── logger.py
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## Next Steps

- Implement scrapers for Controller, AircraftExchange, and FAA
- Add data normalization pipelines
- Implement data validation and quality checks
