"""Central configuration loader for ETL pipeline.

Handles loading of Akamai credentials, bucket configuration, and environment settings.
No hardcoded secrets - all values loaded from environment variables.
"""

import os
from typing import Optional
from enum import Enum
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file if it exists
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


class Environment(str, Enum):
    """Supported environments."""
    DEV = "dev"
    PROD = "prod"
    LOCAL = "local"  # For dry-run mode


@dataclass
class AkamaiConfig:
    """Akamai Object Storage configuration."""
    access_key: str
    secret_key: str
    endpoint: str
    bucket_name: str
    region: Optional[str] = None


@dataclass
class Config:
    """Central configuration for ETL pipeline."""
    environment: Environment
    akamai: AkamaiConfig
    dry_run: bool = False
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables.
        
        Raises:
            ValueError: If required environment variables are missing.
        """
        # Environment
        env_str = os.getenv("ENVIRONMENT", "local").lower()
        try:
            environment = Environment(env_str)
        except ValueError:
            raise ValueError(
                f"Invalid ENVIRONMENT value: {env_str}. "
                f"Must be one of: {[e.value for e in Environment]}"
            )

        # Dry-run mode (local environment defaults to True)
        dry_run = os.getenv("DRY_RUN", str(environment == Environment.LOCAL)).lower() == "true"

        # Akamai credentials
        access_key = os.getenv("AKAMAI_ACCESS_KEY")
        secret_key = os.getenv("AKAMAI_SECRET_KEY")
        endpoint = os.getenv("AKAMAI_ENDPOINT")
        bucket_name = os.getenv("AKAMAI_BUCKET_NAME")
        region = os.getenv("AKAMAI_REGION")

        if not access_key:
            raise ValueError("AKAMAI_ACCESS_KEY environment variable is required")
        if not secret_key:
            raise ValueError("AKAMAI_SECRET_KEY environment variable is required")
        if not endpoint:
            raise ValueError("AKAMAI_ENDPOINT environment variable is required")
        if not bucket_name:
            raise ValueError("AKAMAI_BUCKET_NAME environment variable is required")

        akamai = AkamaiConfig(
            access_key=access_key,
            secret_key=secret_key,
            endpoint=endpoint,
            bucket_name=bucket_name,
            region=region,
        )

        # Log level
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        return cls(
            environment=environment,
            akamai=akamai,
            dry_run=dry_run,
            log_level=log_level,
        )

    def is_dry_run(self) -> bool:
        """Check if running in dry-run mode."""
        return self.dry_run or self.environment == Environment.LOCAL


# Global config instance (lazy-loaded)
_config: Optional[Config] = None


def get_config() -> Config:
    """Get the global configuration instance.
    
    Returns:
        Config: The loaded configuration instance.
        
    Raises:
        ValueError: If configuration cannot be loaded.
    """
    global _config
    if _config is None:
        _config = Config.from_env()
    return _config


def reload_config() -> Config:
    """Reload configuration from environment (useful for testing).
    
    Returns:
        Config: The newly loaded configuration instance.
    """
    global _config
    _config = Config.from_env()
    return _config
