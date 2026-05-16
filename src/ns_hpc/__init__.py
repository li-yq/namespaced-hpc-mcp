import logging

from fastmcp.utilities.logging import configure_logging


def _enable_debug_logging() -> None:
    """Enable debug-level logging for the ``ns-hpc`` logger using FastMCP's
    RichHandler-based logging configuration."""
    configure_logging(
        level="DEBUG",
        logger=logging.getLogger("ns-hpc"),
    )
