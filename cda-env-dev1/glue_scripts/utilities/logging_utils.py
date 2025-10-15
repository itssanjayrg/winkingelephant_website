import structlog


def get_structlog_logger(logger) -> structlog.BoundLogger:
    # noinspection PyTypeChecker
    return structlog.wrap_logger(
        logger,
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.contextvars.merge_contextvars,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.dict_tracebacks,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.BoundLogger,
    )
