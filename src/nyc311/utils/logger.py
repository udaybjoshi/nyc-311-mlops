"""Lightweight JSON-ish console logger for Databricks-friendly logs."""

from __future__ import annotations
import logging, os

def get_logger(name: str = "nyc311") -> logging.Logger:
    """Create or return a configured logger.

    Args:
      name: Logger name.

    Returns:
      logging.Logger: Configured logger with single StreamHandler.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)
    h = logging.StreamHandler()
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    h.setFormatter(logging.Formatter(fmt))
    logger.addHandler(h)
    logger.propagate = False
    return logger
