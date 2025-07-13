"""
Simple structlog configuration for MLB Airflow Data Pipeline.

This module provides a clean, production-ready logging setup using structlog
that outputs structured JSON logs for easier debugging and analysis.
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path

import structlog


def setup_logging(
    log_level: str = "INFO",
    log_dir: str | None = None,
    module_name: str | None = None,
    league: str | None = None,
) -> structlog.stdlib.BoundLogger:
    """
    Configure structured logging with JSON output.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory for log files (defaults to project_root/logs)
        module_name: Name of the module for context
        league: League context (AL/NL) if applicable

    Returns:
        Configured structlog logger
    """

    # Set up log directory
    log_path: Path
    if log_dir is None:
        # Use relative path from project root for portability
        project_root = Path(__file__).parent.parent
        log_path = project_root / "logs"
    else:
        log_path = Path(log_dir)

    log_path.mkdir(exist_ok=True)

    # Create separate formatters for file and console
    # File handler with clean JSON format
    file_handler = logging.handlers.RotatingFileHandler(
        log_path / "mlb_pipeline.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)

    # Configure stdlib logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        handlers=[file_handler, console_handler],
        force=True,  # Override any existing configuration
    )

    # Configure structlog with JSON for files
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Use JSON renderer for clean, structured logs
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Create logger with automatic context
    logger = structlog.get_logger()

    # Add context information
    context = {}
    if module_name:
        context["module"] = module_name
    if league:
        context["league"] = league.lower()

    # Add execution environment context
    if "AIRFLOW_CTX_DAG_ID" in os.environ:
        context["dag_id"] = os.environ["AIRFLOW_CTX_DAG_ID"]
    if "AIRFLOW_CTX_TASK_ID" in os.environ:
        context["task_id"] = os.environ["AIRFLOW_CTX_TASK_ID"]
    if "AIRFLOW_CTX_EXECUTION_DATE" in os.environ:
        context["execution_date"] = os.environ["AIRFLOW_CTX_EXECUTION_DATE"]

    # Bind context to logger
    if context:
        logger = logger.bind(**context)

    return logger


def get_logger(
    module_name: str, league: str | None = None
) -> structlog.stdlib.BoundLogger:
    """
    Get a logger configured for a specific module and league.

    Args:
        module_name: Name of the module
        league: League context (AL/NL) if applicable

    Returns:
        Configured logger with module and league context
    """
    return setup_logging(module_name=module_name, league=league)
