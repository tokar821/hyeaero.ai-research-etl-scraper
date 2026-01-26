# Environment Setup Guide

## Overview

The ETL pipeline uses environment variables for configuration. You can set these via:
1. `.env` file in the project root (recommended)
2. System environment variables
3. Hardcoded fallbacks (for backward compatibility)

## Quick Setup

1. Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

2. Edit `.env` with your settings:
```env
ENVIRONMENT=local
LOG_LEVEL=INFO
POSTGRES_CONNECTION_STRING=postgres://user:password@host:port/database?sslmode=require
```

## Environment Variables

### General Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ENVIRONMENT` | Environment mode: `local`, `dev`, or `prod` | `local` | No |
| `DRY_RUN` | Enable dry-run mode (no actual operations) | `false` (local: `true`) | No |
| `LOG_LEVEL` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` | `INFO` | No |

### PostgreSQL Configuration

You can configure PostgreSQL connection in two ways:

#### Option 1: Connection String (Recommended)

```env
POSTGRES_CONNECTION_STRING=postgres://username:password@hostname:port/database?sslmode=require
```

**Example:**
```env
POSTGRES_CONNECTION_STRING=postgres://avnadmin:AVNS_IT0JkCtP0vz1x-an3Aj@pg-134dedd1-allevi8marketing-47f2.c.aivencloud.com:13079/defaultdb?sslmode=require
```

#### Option 2: Individual Components

```env
POSTGRES_HOST=pg-134dedd1-allevi8marketing-47f2.c.aivencloud.com
POSTGRES_PORT=13079
POSTGRES_DATABASE=defaultdb
POSTGRES_USER=avnadmin
POSTGRES_PASSWORD=your_password_here
```

**Note:** If neither `POSTGRES_CONNECTION_STRING` nor individual components are set, the database loader will use a hardcoded fallback connection string (for backward compatibility).

## Configuration Priority

The system loads configuration in this order:

1. **Environment variables** (system-level)
2. **`.env` file** (project root)
3. **Hardcoded fallbacks** (for backward compatibility)

## Example `.env` File

```env
# Environment
ENVIRONMENT=local

# Logging
LOG_LEVEL=INFO

# Dry Run Mode
DRY_RUN=false

# PostgreSQL (Option 1: Connection String)
POSTGRES_CONNECTION_STRING=postgres://user:password@host:port/database?sslmode=require

# PostgreSQL (Option 2: Individual Components - uncomment if not using connection string)
# POSTGRES_HOST=localhost
# POSTGRES_PORT=5432
# POSTGRES_DATABASE=defaultdb
# POSTGRES_USER=postgres
# POSTGRES_PASSWORD=your_password
```

## Using Configuration in Code

```python
from config.config_loader import get_config

config = get_config()

# Access PostgreSQL settings
if config.postgres_connection_string:
    # Use connection string
    connection = config.postgres_connection_string
elif config.postgres_host:
    # Build from components
    connection = f"host={config.postgres_host} port={config.postgres_port} ..."
```

## Security Best Practices

1. **Never commit `.env` files** - They contain sensitive credentials
2. **Use `.env.example`** - Document required variables without values
3. **Rotate credentials** - Change passwords regularly
4. **Use environment-specific files** - Different `.env` files for dev/prod
5. **Restrict file permissions** - `chmod 600 .env` on Linux/Mac

## Troubleshooting

### "Connection refused" or "Authentication failed"
- Check PostgreSQL connection string/credentials
- Verify database is accessible from your network
- Check firewall rules

### "Module not found: config"
- Make sure you're running from project root
- Check Python path includes project directory

### "Environment variable not found"
- Check `.env` file exists and is in project root
- Verify variable names match exactly (case-sensitive)
- Restart your terminal/IDE after creating `.env`

## Production Deployment

For production, use system environment variables or a secrets management service:

```bash
# Set environment variables
export POSTGRES_CONNECTION_STRING="postgres://..."
export ENVIRONMENT="prod"
export LOG_LEVEL="INFO"
```

Or use a secrets manager (AWS Secrets Manager, HashiCorp Vault, etc.) and load at runtime.
