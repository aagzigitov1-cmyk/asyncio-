import json
from pathlib import Path

from storage import CSVStorage, JSONStorage, SQLiteStorage


def load_config(filename: str) -> dict:
    path = Path(filename)
    if path.suffix.lower() != ".json":
        raise ValueError("Only JSON configuration files are supported")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("configuration root must be an object")
    return data


def create_storage(config: dict | None):
    if not config:
        return None
    storage_format = str(config.get("format", "json")).lower()
    path = config.get("path")
    if not path:
        raise ValueError("storage.path is required")
    options = dict(config.get("options") or {})

    if storage_format in ("json", "jsonl"):
        options.setdefault("json_lines", storage_format == "jsonl")
        return JSONStorage(path, **options)
    if storage_format == "csv":
        return CSVStorage(path, **options)
    if storage_format in ("sqlite", "db"):
        return SQLiteStorage(path, **options)
    raise ValueError(f"unsupported storage format: {storage_format}")


def storage_from_output(filename: str):
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        return CSVStorage(filename)
    if suffix in (".db", ".sqlite", ".sqlite3"):
        return SQLiteStorage(filename)
    if suffix == ".json":
        return JSONStorage(filename, json_lines=False)
    return JSONStorage(filename, json_lines=True)
