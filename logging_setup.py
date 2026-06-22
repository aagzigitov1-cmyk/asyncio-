import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def configure_logging(
    *,
    log_file: str | None = None,
    level: str | int = "INFO",
    max_bytes: int = 5_000_000,
    backup_count: int = 3,
) -> list[logging.Handler]:
    numeric_level = (
        getattr(logging, level.upper())
        if isinstance(level, str)
        else level
    )
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handlers: list[logging.Handler] = []

    console = logging.StreamHandler()
    console.setLevel(numeric_level)
    console.setFormatter(formatter)
    handlers.append(console)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        rotating = RotatingFileHandler(
            path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        rotating.setLevel(numeric_level)
        rotating.setFormatter(formatter)
        handlers.append(rotating)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(numeric_level)
    for handler in handlers:
        root.addHandler(handler)
    return handlers
