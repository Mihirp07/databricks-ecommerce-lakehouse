"""
src/utils/logger.py
-------------------
Structured logger for the Databricks Lakehouse pipeline.

Usage:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Processing started", extra={"layer": "silver", "table": "orders"})
"""

import logging
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """
    Emits log records as single-line JSON — compatible with
    Databricks Log Analytics, AWS CloudWatch, and Azure Monitor.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
            "module":    record.module,
            "line":      record.lineno,
        }

        # Merge any extra fields passed via logger.info(..., extra={...})
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            ):
                log_obj[key] = value

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj, default=str)


def get_logger(
    name: str,
    level: str = "INFO",
    structured: bool = True,
) -> logging.Logger:
    """
    Returns a configured logger.

    Args:
        name:       Module name — pass __name__ from the calling module.
        level:      Log level string: DEBUG | INFO | WARNING | ERROR | CRITICAL
        structured: If True, emit JSON lines. If False, use human-readable format.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Avoid duplicate handlers if module is re-imported (common in notebooks)
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))

    if structured:
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    logger.addHandler(handler)
    logger.propagate = False
    return logger


class PipelineLogger:
    """
    Context-aware logger that automatically injects pipeline metadata
    (layer, table, batch_id, run_id) into every log record.

    Usage:
        pl = PipelineLogger(layer="silver", table="orders", batch_id="abc123")
        pl.info("Deduplication complete", rows_before=1000, rows_after=980)
    """

    def __init__(
        self,
        layer: str,
        table: str,
        batch_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ):
        self._logger = get_logger(f"pipeline.{layer}.{table}")
        self._context = {
            "layer":    layer,
            "table":    table,
            "batch_id": batch_id or "unknown",
            "run_id":   run_id or "unknown",
        }

    def _log(self, level: str, msg: str, **kwargs: Any) -> None:
        extra = {**self._context, **kwargs}
        getattr(self._logger, level)(msg, extra=extra)

    def debug(self, msg: str, **kwargs: Any)    -> None: self._log("debug",    msg, **kwargs)
    def info(self, msg: str, **kwargs: Any)     -> None: self._log("info",     msg, **kwargs)
    def warning(self, msg: str, **kwargs: Any)  -> None: self._log("warning",  msg, **kwargs)
    def error(self, msg: str, **kwargs: Any)    -> None: self._log("error",    msg, **kwargs)
    def critical(self, msg: str, **kwargs: Any) -> None: self._log("critical", msg, **kwargs)
