import sys
import logging
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from utils.logger.logger import get_logger


def _flush(logger: logging.Logger) -> None:
    """Flush handlers so file contents are written before we read them."""
    for h in logger.handlers:
        try:
            h.flush()
        except Exception:
            pass


def _has_console_handler(logger: logging.Logger) -> bool:
    """
    True if logger has a StreamHandler that is NOT a FileHandler.
    (FileHandler subclasses StreamHandler, so we must exclude it.)
    """
    return any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in logger.handlers
    )


def test_logger_creates_files(tmp_path, monkeypatch):

    monkeypatch.chdir(tmp_path)

    logger = get_logger("bronze", console=False)
    logger.info("info message")
    logger.error("error message")

    _flush(logger)

    info_file = tmp_path / "logs" / "bronze" / "bronze.log"
    error_file = tmp_path / "logs" / "bronze" / "bronze_error.log"

    assert info_file.exists()
    assert error_file.exists()

    info_text = info_file.read_text(encoding="utf-8")
    err_text = error_file.read_text(encoding="utf-8")

    assert "info message" in info_text
    assert "error message" in err_text


def test_logger_no_duplicate_handlers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    log1 = get_logger("silver", console=False)
    count1 = len(log1.handlers)

    log2 = get_logger("silver", console=False)
    count2 = len(log2.handlers)

    assert count1 == count2


def test_console_handler_toggle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    lg_console = get_logger("gold", console=True)
    assert _has_console_handler(lg_console)

    lg_no_console = get_logger("ingestion", console=False)
    assert not _has_console_handler(lg_no_console)