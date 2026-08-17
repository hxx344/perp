import csv
import logging
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from helpers.logger import (
    DEFAULT_LOG_BACKUP_COUNT,
    DEFAULT_LOG_MAX_BYTES,
    TradingLogger,
)


def _close_handlers(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


@pytest.fixture
def logger_tmp_path():
    with TemporaryDirectory(prefix=".trading-logger-test-", dir=Path(__file__).parent) as path:
        yield Path(path)


def test_activity_log_uses_configured_rotation_and_log_directory(monkeypatch, logger_tmp_path):
    tmp_path = logger_tmp_path
    log_dir = tmp_path / "bounded-logs"
    monkeypatch.setenv("LOG_TO_FILE", "true")
    monkeypatch.setenv("LOG_DIR", str(log_dir))
    monkeypatch.setenv("LOG_MAX_BYTES", "256")
    monkeypatch.setenv("LOG_BACKUP_COUNT", "2")
    monkeypatch.delenv("ACCOUNT_NAME", raising=False)

    trading_logger = TradingLogger("logger-test", "ROTATE", log_to_console=False)
    try:
        file_handlers = [
            handler
            for handler in trading_logger.logger.handlers
            if isinstance(handler, RotatingFileHandler)
        ]
        assert len(file_handlers) == 1
        assert file_handlers[0].maxBytes == 256
        assert file_handlers[0].backupCount == 2
        assert trading_logger.debug_log_file == str(log_dir / "logger-test_ROTATE_activity.log")

        for index in range(30):
            trading_logger.log(f"rotation record {index}: " + ("x" * 80))
        file_handlers[0].flush()

        assert (log_dir / "logger-test_ROTATE_activity.log").is_file()
        assert (log_dir / "logger-test_ROTATE_activity.log.1").is_file()
    finally:
        _close_handlers(trading_logger.logger)


def test_log_to_file_false_disables_activity_and_transaction_files(monkeypatch, logger_tmp_path):
    tmp_path = logger_tmp_path
    log_dir = tmp_path / "disabled-logs"
    monkeypatch.setenv("LOG_TO_FILE", "off")
    monkeypatch.setenv("LOG_TO_CONSOLE", "true")
    monkeypatch.setenv("LOG_DIR", str(log_dir))

    trading_logger = TradingLogger("logger-test", "DISABLED")
    try:
        assert not any(
            isinstance(handler, logging.FileHandler)
            for handler in trading_logger.logger.handlers
        )
        assert any(
            isinstance(handler, logging.StreamHandler)
            and not isinstance(handler, logging.FileHandler)
            for handler in trading_logger.logger.handlers
        )
        trading_logger.log("this should not create a file")
        trading_logger.log_transaction(
            "order-1",
            "buy",
            Decimal("0.001"),
            Decimal("50000"),
            "FILLED",
        )
        assert not log_dir.exists()
    finally:
        _close_handlers(trading_logger.logger)


def test_invalid_rotation_settings_fall_back_to_bounded_defaults(monkeypatch, logger_tmp_path):
    tmp_path = logger_tmp_path
    monkeypatch.setenv("LOG_TO_FILE", "yes")
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_MAX_BYTES", "not-an-integer")
    monkeypatch.setenv("LOG_BACKUP_COUNT", "0")

    trading_logger = TradingLogger("logger-test", "DEFAULTS", log_to_console=False)
    try:
        handler = next(
            handler
            for handler in trading_logger.logger.handlers
            if isinstance(handler, RotatingFileHandler)
        )
        assert handler.maxBytes == DEFAULT_LOG_MAX_BYTES
        assert handler.backupCount == DEFAULT_LOG_BACKUP_COUNT
    finally:
        _close_handlers(trading_logger.logger)


def test_transaction_csv_keeps_existing_columns_in_configured_directory(monkeypatch, logger_tmp_path):
    log_dir = logger_tmp_path / "csv-logs"
    monkeypatch.setenv("LOG_TO_FILE", "true")
    monkeypatch.setenv("LOG_DIR", str(log_dir))
    monkeypatch.delenv("ACCOUNT_NAME", raising=False)

    trading_logger = TradingLogger("logger-test", "CSV", log_to_console=False)
    try:
        trading_logger.log_transaction(
            "order-7",
            "sell",
            Decimal("0.002"),
            Decimal("50123.4"),
            "FILLED",
        )
        with open(trading_logger.log_file, newline="", encoding="utf-8") as csv_file:
            rows = list(csv.reader(csv_file))

        assert rows[0] == ["Timestamp", "OrderID", "Side", "Quantity", "Price", "Status"]
        assert rows[1][1:] == ["order-7", "sell", "0.002", "50123.4", "FILLED"]
    finally:
        _close_handlers(trading_logger.logger)
