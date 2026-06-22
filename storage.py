import asyncio
import csv
import io
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import aiosqlite


STANDARD_FIELDS = (
    "url",
    "title",
    "text",
    "links",
    "metadata",
    "crawled_at",
    "status_code",
    "content_type",
)


def normalize_crawl_data(data: dict) -> dict:
    normalized = {
        "url": str(data.get("url", "")),
        "title": str(data.get("title", "")),
        "text": str(data.get("text", "")),
        "links": list(data.get("links") or []),
        "metadata": dict(data.get("metadata") or {}),
        "crawled_at": data.get("crawled_at")
        or datetime.now(timezone.utc),
        "status_code": int(data.get("status_code") or 0),
        "content_type": str(data.get("content_type", "")),
    }
    return normalized


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _serialize_datetime(value) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class DataStorage(ABC):
    def __init__(self):
        self.saved_count = 0
        self.error_count = 0

    @abstractmethod
    async def save(self, data: dict) -> None:
        raise NotImplementedError

    async def save_many(self, items: list[dict]) -> None:
        for item in items:
            await self.save(item)

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    def get_stats(self) -> dict[str, int]:
        return {
            "saved": self.saved_count,
            "errors": self.error_count,
        }


class JSONStorage(DataStorage):
    def __init__(
        self,
        path: str,
        *,
        json_lines: bool = True,
        indent: int = 2,
        buffer_size: int = 100,
        encoding: str = "utf-8",
    ):
        super().__init__()
        if buffer_size <= 0:
            raise ValueError("buffer_size must be greater than zero")
        self.path = Path(path)
        self.json_lines = json_lines
        self.indent = indent
        self.buffer_size = buffer_size
        self.encoding = encoding
        self._buffer: list[dict] = []
        self._pretty_items: list[dict] = []
        self._lock = asyncio.Lock()
        self._closed = False

    async def save(self, data: dict) -> None:
        if self._closed:
            raise RuntimeError("storage is closed")
        item = normalize_crawl_data(data)

        async with self._lock:
            if self.json_lines:
                self._buffer.append(item)
                if len(self._buffer) >= self.buffer_size:
                    await self._flush_lines_locked()
            else:
                self._pretty_items.append(item)
            self.saved_count += 1

    async def _flush_lines_locked(self) -> None:
        if not self._buffer:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        content = "".join(
            json.dumps(
                item,
                ensure_ascii=False,
                default=_json_default,
            )
            + "\n"
            for item in self._buffer
        )
        async with aiofiles.open(
            self.path,
            "a",
            encoding=self.encoding,
        ) as file:
            await file.write(content)
        self._buffer.clear()

    async def flush(self) -> None:
        async with self._lock:
            if self.json_lines:
                await self._flush_lines_locked()
            else:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                content = json.dumps(
                    self._pretty_items,
                    ensure_ascii=False,
                    indent=self.indent,
                    default=_json_default,
                )
                async with aiofiles.open(
                    self.path,
                    "w",
                    encoding=self.encoding,
                ) as file:
                    await file.write(content)

    async def read_all(self) -> list[dict]:
        await self.flush()
        if not self.path.exists():
            return []
        async with aiofiles.open(
            self.path,
            "r",
            encoding=self.encoding,
        ) as file:
            content = await file.read()
        if not content.strip():
            return []
        if self.json_lines:
            return [
                json.loads(line)
                for line in content.splitlines()
                if line.strip()
            ]
        return json.loads(content)

    async def close(self) -> None:
        if not self._closed:
            await self.flush()
            self._closed = True


class CSVStorage(DataStorage):
    def __init__(
        self,
        path: str,
        *,
        encoding: str = "utf-8-sig",
        buffer_size: int = 100,
    ):
        super().__init__()
        if buffer_size <= 0:
            raise ValueError("buffer_size must be greater than zero")
        self.path = Path(path)
        self.encoding = encoding
        self.buffer_size = buffer_size
        self._buffer: list[dict] = []
        self._headers: list[str] | None = None
        self._lock = asyncio.Lock()
        self._initialized = self.path.exists() and self.path.stat().st_size > 0
        self._closed = False

    def _csv_row(self, item: dict) -> dict:
        row = dict(item)
        row["links"] = json.dumps(row["links"], ensure_ascii=False)
        row["metadata"] = json.dumps(row["metadata"], ensure_ascii=False)
        row["crawled_at"] = _serialize_datetime(row["crawled_at"])
        return row

    async def save(self, data: dict) -> None:
        if self._closed:
            raise RuntimeError("storage is closed")
        item = normalize_crawl_data(data)

        async with self._lock:
            if self._headers is None:
                self._headers = list(item.keys())
            self._buffer.append(item)
            if len(self._buffer) >= self.buffer_size:
                await self._flush_locked()
            self.saved_count += 1

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        output = io.StringIO(newline="")
        writer = csv.DictWriter(
            output,
            fieldnames=self._headers or list(STANDARD_FIELDS),
            extrasaction="ignore",
        )
        if not self._initialized:
            writer.writeheader()
            self._initialized = True
        writer.writerows(self._csv_row(item) for item in self._buffer)

        async with aiofiles.open(
            self.path,
            "a",
            encoding=self.encoding,
            newline="",
        ) as file:
            await file.write(output.getvalue())
        self._buffer.clear()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def read_all(self) -> list[dict]:
        await self.flush()
        if not self.path.exists():
            return []
        async with aiofiles.open(
            self.path,
            "r",
            encoding=self.encoding,
            newline="",
        ) as file:
            content = await file.read()
        rows = list(csv.DictReader(io.StringIO(content)))
        for row in rows:
            row["links"] = json.loads(row["links"] or "[]")
            row["metadata"] = json.loads(row["metadata"] or "{}")
            row["status_code"] = int(row["status_code"] or 0)
        return rows

    async def close(self) -> None:
        if not self._closed:
            await self.flush()
            self._closed = True


class SQLiteStorage(DataStorage):
    def __init__(
        self,
        path: str,
        *,
        batch_size: int = 100,
    ):
        super().__init__()
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        self.path = Path(path)
        self.batch_size = batch_size
        self._connection: aiosqlite.Connection | None = None
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()
        self._closed = False

    async def init_db(self) -> None:
        if self._connection is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(self.path)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                text TEXT NOT NULL,
                links TEXT NOT NULL,
                metadata TEXT NOT NULL,
                crawled_at TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                content_type TEXT NOT NULL
            )
            """
        )
        await self._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url)"
        )
        await self._connection.commit()

    async def save(self, data: dict) -> None:
        if self._closed:
            raise RuntimeError("storage is closed")
        item = normalize_crawl_data(data)

        async with self._lock:
            await self.init_db()
            self._buffer.append(item)
            if len(self._buffer) >= self.batch_size:
                await self._flush_locked()
            self.saved_count += 1

    async def save_many(self, items: list[dict]) -> None:
        if self._closed:
            raise RuntimeError("storage is closed")
        normalized = [normalize_crawl_data(item) for item in items]
        async with self._lock:
            await self.init_db()
            self._buffer.extend(normalized)
            await self._flush_locked()
            self.saved_count += len(normalized)

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return
        await self.init_db()
        rows = [
            (
                item["url"],
                item["title"],
                item["text"],
                json.dumps(item["links"], ensure_ascii=False),
                json.dumps(item["metadata"], ensure_ascii=False),
                _serialize_datetime(item["crawled_at"]),
                item["status_code"],
                item["content_type"],
            )
            for item in self._buffer
        ]
        await self._connection.executemany(
            """
            INSERT INTO pages (
                url, title, text, links, metadata,
                crawled_at, status_code, content_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self._connection.commit()
        self._buffer.clear()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def read_all(self) -> list[dict]:
        await self.flush()
        await self.init_db()
        cursor = await self._connection.execute(
            """
            SELECT url, title, text, links, metadata,
                   crawled_at, status_code, content_type
            FROM pages ORDER BY id
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            {
                "url": row["url"],
                "title": row["title"],
                "text": row["text"],
                "links": json.loads(row["links"]),
                "metadata": json.loads(row["metadata"]),
                "crawled_at": row["crawled_at"],
                "status_code": row["status_code"],
                "content_type": row["content_type"],
            }
            for row in rows
        ]

    async def close(self) -> None:
        if self._closed:
            return
        await self.flush()
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        self._closed = True
