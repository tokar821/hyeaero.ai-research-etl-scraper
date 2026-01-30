"""Logging utility with timestamps and module names."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.config_loader import get_config


class TimestampFormatter(logging.Formatter):
    """Custom formatter that includes timestamps and module names."""
    
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


class FlushingStreamHandler(logging.StreamHandler):
    """StreamHandler that flushes after each emit so console output appears immediately."""
    
    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    log_file_overwrite: bool = True,
) -> None:
    """Configure logging for the ETL pipeline.

    Args:
        log_level: Optional log level. If None, uses value from config.
        log_file: Optional log file path. If provided, logs are written to file and console.
        log_file_overwrite: If True (default), overwrite log file each run. If False, append.
    """
    if log_level is None:
        try:
            config = get_config()
            log_level = config.log_level
        except Exception:
            log_level = "INFO"

    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.handlers.clear()

    console_handler = FlushingStreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(TimestampFormatter())
    root_logger.addHandler(console_handler)

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if log_file_overwrite else "a"
        file_handler = logging.FileHandler(log_file, mode=mode, encoding="utf-8")
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(TimestampFormatter())
        root_logger.addHandler(file_handler)
        action = "overwrite" if log_file_overwrite else "append"
        logging.info(f"Logging configured at level: {log_level} (console + file: {log_file}, {action})")
    else:
        logging.info(f"Logging configured at level: {log_level} (console only)")


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a module.
    
    Args:
        name: Module name (typically __name__).
        
    Returns:
        Logger instance configured with timestamps and module name.
    """
    return logging.getLogger(name)
