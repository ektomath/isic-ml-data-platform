"""Shared data-platform package."""

import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    """Send this project's log messages (the data_platform and ml packages) to stdout as plain
    lines, so notebook cells and the command line show them like print output. Safe to call
    more than once."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    for name in ("data_platform", "ml"):
        package_logger = logging.getLogger(name)
        package_logger.setLevel(level)
        package_logger.handlers = [handler]
        package_logger.propagate = False
