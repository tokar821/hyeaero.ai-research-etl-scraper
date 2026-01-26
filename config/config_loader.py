"""Central configuration loader for ETL pipeline.

Handles loading of environment settings.
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
class Config:
    """Central configuration for ETL pipeline."""
    environment: Environment
    dry_run: bool = False
    log_level: str = "INFO"
    
    # PostgreSQL configuration
    postgres_host: Optional[str] = None
    postgres_port: Optional[int] = None
    postgres_database: Optional[str] = None
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_connection_string: Optional[str] = None

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

        # Log level
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        # PostgreSQL configuration
        postgres_host = os.getenv("POSTGRES_HOST")
        postgres_port = int(os.getenv("POSTGRES_PORT", "5432")) if os.getenv("POSTGRES_PORT") else None
        postgres_database = os.getenv("POSTGRES_DATABASE")
        postgres_user = os.getenv("POSTGRES_USER")
        postgres_password = os.getenv("POSTGRES_PASSWORD")
        postgres_connection_string = os.getenv("POSTGRES_CONNECTION_STRING")

        return cls(
            environment=environment,
            dry_run=dry_run,
            log_level=log_level,
            postgres_host=postgres_host,
            postgres_port=postgres_port,
            postgres_database=postgres_database,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            postgres_connection_string=postgres_connection_string,
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
