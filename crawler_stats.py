import html
import json
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


class CrawlerStats:
    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.successful = 0
        self.failed = 0
        self.status_codes: Counter[int] = Counter()
        self.domains: Counter[str] = Counter()

    def start(self) -> None:
        self.reset()
        self.started_at = time.monotonic()

    def finish(self) -> None:
        self.finished_at = time.monotonic()

    def record_success(self, url: str, status_code: int = 200) -> None:
        self.successful += 1
        self.status_codes[int(status_code or 200)] += 1
        domain = urlparse(url).netloc.lower()
        if domain:
            self.domains[domain] += 1

    def record_failure(
        self,
        url: str,
        status_code: int | None = None,
    ) -> None:
        self.failed += 1
        if status_code:
            self.status_codes[int(status_code)] += 1
        domain = urlparse(url).netloc.lower()
        if domain:
            self.domains[domain] += 1

    def runtime(self) -> float:
        if self.started_at is None:
            return 0.0
        endpoint = self.finished_at or time.monotonic()
        return max(0.0, endpoint - self.started_at)

    def get_stats(self) -> dict:
        total_pages = self.successful + self.failed
        runtime = self.runtime()
        return {
            "total_pages": total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "average_speed": total_pages / runtime if runtime else 0.0,
            "status_codes": {
                str(code): count
                for code, count in sorted(self.status_codes.items())
            },
            "top_domains": [
                {"domain": domain, "pages": count}
                for domain, count in self.domains.most_common(10)
            ],
            "runtime_seconds": runtime,
        }

    def progress(
        self,
        *,
        max_pages: int,
        active_tasks: int,
        queued: int,
    ) -> dict:
        stats = self.get_stats()
        completed = stats["total_pages"]
        percentage = (
            min(100.0, completed / max_pages * 100)
            if max_pages > 0
            else 100.0
        )
        remaining = max(0, max_pages - completed)
        speed = stats["average_speed"]
        eta = remaining / speed if speed else None
        return {
            "completed": completed,
            "max_pages": max_pages,
            "percentage": percentage,
            "speed": speed,
            "eta_seconds": eta,
            "active_tasks": active_tasks,
            "queued": queued,
        }

    def export_to_json(self, filename: str) -> None:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.get_stats(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def export_to_html_report(self, filename: str) -> None:
        stats = self.get_stats()
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)

        status_max = max(
            (int(value) for value in stats["status_codes"].values()),
            default=1,
        )
        status_rows = "".join(
            (
                "<tr>"
                f"<td>{html.escape(code)}</td>"
                f"<td>{count}</td>"
                "<td><div class='bar' "
                f"style='width:{count / status_max * 100:.1f}%'></div></td>"
                "</tr>"
            )
            for code, count in stats["status_codes"].items()
        )
        domain_rows = "".join(
            (
                "<tr>"
                f"<td>{html.escape(item['domain'])}</td>"
                f"<td>{item['pages']}</td>"
                "</tr>"
            )
            for item in stats["top_domains"]
        )

        document = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Crawler report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #172033; }}
    .cards {{ display: flex; flex-wrap: wrap; gap: 1rem; }}
    .card {{ padding: 1rem; min-width: 160px; border: 1px solid #d8deea;
             border-radius: 10px; background: #f8faff; }}
    .value {{ font-size: 1.8rem; font-weight: 700; }}
    table {{ border-collapse: collapse; width: 100%; margin: 1rem 0 2rem; }}
    th, td {{ border-bottom: 1px solid #d8deea; padding: .6rem; text-align: left; }}
    .bar {{ height: .8rem; min-width: 2px; background: #3578e5; border-radius: 4px; }}
  </style>
</head>
<body>
  <h1>Async crawler report</h1>
  <div class="cards">
    <div class="card"><div>Total pages</div><div class="value">{stats['total_pages']}</div></div>
    <div class="card"><div>Successful</div><div class="value">{stats['successful']}</div></div>
    <div class="card"><div>Failed</div><div class="value">{stats['failed']}</div></div>
    <div class="card"><div>Pages/sec</div><div class="value">{stats['average_speed']:.2f}</div></div>
  </div>
  <h2>Status codes</h2>
  <table><thead><tr><th>Status</th><th>Count</th><th>Distribution</th></tr></thead>
  <tbody>{status_rows}</tbody></table>
  <h2>Top domains</h2>
  <table><thead><tr><th>Domain</th><th>Pages</th></tr></thead>
  <tbody>{domain_rows}</tbody></table>
  <p>Runtime: {stats['runtime_seconds']:.2f} seconds</p>
</body>
</html>"""
        path.write_text(document, encoding="utf-8")
