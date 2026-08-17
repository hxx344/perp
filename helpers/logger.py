"""
Trading logger with structured output and error handling.
"""

import os
import csv
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import pytz
from decimal import Decimal
from typing import Optional


DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _positive_env_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


class TradingLogger:
    """Enhanced logging with structured output and error handling."""

    def __init__(
        self,
        exchange: str,
        ticker: str,
        log_to_console: Optional[bool] = None,
        enable_debug: bool = False,
    ):
        self.exchange = exchange
        self.ticker = ticker
        self.enable_debug = enable_debug
        self.log_to_file = _env_bool("LOG_TO_FILE", True)
        self.log_max_bytes = _positive_env_int("LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES)
        self.log_backup_count = _positive_env_int("LOG_BACKUP_COUNT", DEFAULT_LOG_BACKUP_COUNT)
        console_enabled = (
            _env_bool("LOG_TO_CONSOLE", False)
            if log_to_console is None
            else bool(log_to_console)
        )

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        configured_log_dir = os.path.expanduser(os.getenv("LOG_DIR", "logs").strip() or "logs")
        if os.path.isabs(configured_log_dir):
            logs_dir = os.path.abspath(configured_log_dir)
        else:
            logs_dir = os.path.abspath(os.path.join(project_root, configured_log_dir))
        if self.log_to_file:
            os.makedirs(logs_dir, exist_ok=True)

        order_file_name = f"{exchange}_{ticker}_orders.csv"
        debug_log_file_name = f"{exchange}_{ticker}_activity.log"

        account_name = os.getenv('ACCOUNT_NAME')
        if account_name:
            order_file_name = f"{exchange}_{ticker}_{account_name}_orders.csv"
            debug_log_file_name = f"{exchange}_{ticker}_{account_name}_activity.log"

        # Log file paths inside logs directory
        self.log_file = os.path.join(logs_dir, order_file_name)
        self.debug_log_file = os.path.join(logs_dir, debug_log_file_name)
        self.timezone = pytz.timezone(os.getenv('TIMEZONE', 'Asia/Shanghai'))
        self.logger = self._setup_logger(console_enabled)

    def _setup_logger(self, log_to_console: bool) -> logging.Logger:
        """Setup the logger with proper configuration."""
        desired_level = logging.DEBUG if self.enable_debug else logging.INFO
        logger = logging.getLogger(f"trading_bot_{self.exchange}_{self.ticker}")
        logger.setLevel(desired_level)

        # Prevent propagation to root logger to avoid duplicate messages
        logger.propagate = False

        def _apply_handler_levels():
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    handler.setLevel(logging.DEBUG)
                elif isinstance(handler, logging.StreamHandler):
                    handler.setLevel(logging.DEBUG if self.enable_debug else logging.INFO)

        class _DedupFilter(logging.Filter):
            """Filter out exact duplicate messages within a short time window."""

            def __init__(self, window_seconds: float = 1.0):
                super().__init__()
                self.window = float(window_seconds)
                self._last_msg = None
                self._last_level = None
                self._last_time = 0.0

            def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
                try:
                    msg = record.getMessage()
                except Exception:
                    return True
                now = float(getattr(record, "created", 0.0) or 0.0)
                if (
                    self._last_msg == msg
                    and self._last_level == record.levelno
                    and (now - self._last_time) <= self.window
                ):
                    return False
                self._last_msg = msg
                self._last_level = record.levelno
                self._last_time = now
                return True

        class TimeZoneFormatter(logging.Formatter):
            def __init__(self, fmt=None, datefmt=None, tz=None):
                super().__init__(fmt=fmt, datefmt=datefmt)
                self.tz = tz

            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                if datefmt:
                    return dt.strftime(datefmt)
                return dt.isoformat()

        formatter = TimeZoneFormatter(
            "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            tz=self.timezone
        )

        # Reconfigure our named logger deterministically if it was already created.
        for existing_filter in list(logger.filters):
            if getattr(existing_filter, "_trading_logger_dedup", False):
                logger.removeFilter(existing_filter)

        try:
            window = float(os.getenv("LOG_DEDUP_WINDOW", "0.0"))
        except Exception:
            window = 0.0
        if window > 0:
            dedup_filter = _DedupFilter(window)
            dedup_filter._trading_logger_dedup = True  # type: ignore[attr-defined]
            logger.addFilter(dedup_filter)

        expected_file = os.path.abspath(self.debug_log_file)
        for handler in list(logger.handlers):
            if isinstance(handler, logging.FileHandler):
                compatible = (
                    self.log_to_file
                    and isinstance(handler, RotatingFileHandler)
                    and os.path.abspath(handler.baseFilename) == expected_file
                    and handler.maxBytes == self.log_max_bytes
                    and handler.backupCount == self.log_backup_count
                )
                if compatible:
                    handler.setFormatter(formatter)
                else:
                    logger.removeHandler(handler)
                    handler.close()
            elif isinstance(handler, logging.NullHandler):
                logger.removeHandler(handler)
                handler.close()

        if self.log_to_file and not any(
            isinstance(handler, logging.FileHandler) for handler in logger.handlers
        ):
            file_handler = RotatingFileHandler(
                self.debug_log_file,
                maxBytes=self.log_max_bytes,
                backupCount=self.log_backup_count,
                encoding="utf-8",
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        console_handlers = [
            handler
            for handler in logger.handlers
            if isinstance(handler, logging.StreamHandler)
            and not isinstance(handler, logging.FileHandler)
            and not isinstance(handler, logging.NullHandler)
        ]
        if log_to_console:
            if not console_handlers:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.DEBUG if self.enable_debug else logging.INFO)
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)
            else:
                for handler in console_handlers:
                    handler.setFormatter(formatter)
        else:
            for handler in console_handlers:
                logger.removeHandler(handler)
                handler.close()

        if not logger.handlers:
            logger.addHandler(logging.NullHandler())

        _apply_handler_levels()

        return logger

    def log(self, message: str, level: str = "INFO"):
        """Log a message with the specified level."""
        formatted_message = f"[{self.exchange.upper()}_{self.ticker.upper()}] {message}"
        if level.upper() == "DEBUG":
            self.logger.debug(formatted_message)
        elif level.upper() == "INFO":
            self.logger.info(formatted_message)
        elif level.upper() == "WARNING":
            self.logger.warning(formatted_message)
        elif level.upper() == "ERROR":
            self.logger.error(formatted_message)
        else:
            self.logger.info(formatted_message)

    def log_transaction(self, order_id: str, side: str, quantity: Decimal, price: Decimal, status: str):
        """Log a transaction to CSV file."""
        if not self.log_to_file:
            return
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, order_id, side, quantity, price, status]

            # Check if file exists to write headers
            file_exists = os.path.isfile(self.log_file)

            with open(self.log_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['Timestamp', 'OrderID', 'Side', 'Quantity', 'Price', 'Status'])
                writer.writerow(row)

        except Exception as e:
            self.log(f"Failed to log transaction: {e}", "ERROR")
