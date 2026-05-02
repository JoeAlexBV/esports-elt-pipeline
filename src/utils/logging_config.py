"""
Centralized logging configuration for the esports ELT pipeline.

Import get_logger() in any module that needs structured logging.

Usage:
    from src.utils.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Loading %d records into %s", len(records), table_name)
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger with a consistent format.
    Uses the module name as the logger name so log output
    always shows where it came from.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
