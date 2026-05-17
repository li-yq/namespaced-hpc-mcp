import logging
import sys


def _enable_debug_logging() -> None:
    """Enable debug-level logging for the ``ns-hpc`` logger using
    Python's built-in logging module with a concise format."""
    logger = logging.getLogger("ns-hpc")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if called multiple times
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG)
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
