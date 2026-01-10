"""Logging utility with timestamps and module names."""

import logging
import sys
from datetime import datetime
from typing import Optional

from config.config_loader import get_config


class TimestampFormatter(logging.Formatter):
    """Custom formatter that includes timestamps and module names."""
    
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


def setup_logging(log_level: Optional[str] = None) -> None:
    """Configure logging for the ETL pipeline.
    
    Args:
        log_level: Optional log level. If None, uses value from config.
    """
    if log_level is None:
        try:
            config = get_config()
            log_level = config.log_level
        except Exception:
            log_level = "INFO"
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(TimestampFormatter())
    
    root_logger.addHandler(console_handler)
    
    logging.info(f"Logging configured at level: {log_level}")


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a module.
    
    Args:
        name: Module name (typically __name__).
        
    Returns:
        Logger instance configured with timestamps and module name.
    """
    return logging.getLogger(name)
