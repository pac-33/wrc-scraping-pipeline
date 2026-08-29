"""Structured JSON logging for the whole process, third-party libraries included.

structlog renders our own log calls; the stdlib root handler is replaced with a
`ProcessorFormatter` whose `foreign_pre_chain` routes records from libraries
that log through stdlib `logging` (Scrapy internals, pymongo, botocore) through
the same JSON renderer — one uniform stream of JSON lines per process.
"""

import logging
import logging.config
from typing import Literal

import structlog

_NOISY_LOGGERS = {
    "pymongo": logging.WARNING,
    "botocore": logging.WARNING,
    "boto3": logging.WARNING,
    "urllib3": logging.WARNING,
    "filelock": logging.WARNING,
}

_SHARED_PROCESSORS: list[structlog.typing.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
]


def setup_logging(level: str = "INFO", log_format: Literal["json", "console"] = "json") -> None:
    """Configure structlog + stdlib logging. Safe to call more than once."""
    renderer: structlog.typing.Processor
    if log_format == "console":
        renderer = structlog.dev.ConsoleRenderer()
        traceback_processors: list[structlog.typing.Processor] = []
    else:
        renderer = structlog.processors.JSONRenderer()
        traceback_processors = [structlog.processors.dict_tracebacks]

    structlog.configure(
        processors=[
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "structured": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        *traceback_processors,
                        renderer,
                    ],
                    # ExtraAdder lifts `extra={...}` fields from stdlib log
                    # calls (e.g. spider.logger) into the JSON event.
                    "foreign_pre_chain": [*_SHARED_PROCESSORS, structlog.stdlib.ExtraAdder()],
                },
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": "structured",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {"handlers": ["default"], "level": level.upper()},
        }
    )

    for name, noisy_level in _NOISY_LOGGERS.items():
        logging.getLogger(name).setLevel(noisy_level)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
