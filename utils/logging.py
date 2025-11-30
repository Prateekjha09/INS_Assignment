import os
import datetime
import logging
from logging.handlers import RotatingFileHandler


def log_object_creation() -> logging.Logger:
    """Create and return a configured rotating log object."""
    # Build log directory path
    log_dir = os.path.join("CheckLogs", "Logs_upto")
    os.makedirs(log_dir, exist_ok=True)

    try:
        # Build file name with current date, e.g. Logs_upto/2025-11-30.log
        date_str = datetime.datetime.now().date().isoformat()
        file_name = os.path.join(log_dir, f"{date_str}.log")

        # Formatter
        log_formatter = logging.Formatter(
            "%(asctime)s|%(levelname)s|%(message)s"
        )

        # Rotating file handler
        handler = RotatingFileHandler(
            file_name,
            mode="a",
            maxBytes=5 * 1024 * 1024,
            backupCount=2,
            encoding="utf-8",
            delay=False,
        )
        handler.setFormatter(log_formatter)
        handler.setLevel(logging.INFO)

        # Logger
        logger = logging.getLogger("app_logger")
        logger.setLevel(logging.INFO)

        # Avoid adding multiple handlers if function is called more than once
        if not logger.handlers:
            logger.addHandler(handler)

        return logger

    except Exception as e:
        # Fallback: basicConfig to stderr
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s|%(levelname)s|%(message)s",
        )
        fallback_logger = logging.getLogger("fallback_logger")
        fallback_logger.error(f"Failed to create rotating log handler: {e}")
        return fallback_logger
