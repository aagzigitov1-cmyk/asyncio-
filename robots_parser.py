import aiohttp
from urllib.parse import urlparse


class RobotsParser:
    def __init__(self):
        self.cache = {}

    async def fetch_robots(self, base_url: str, session: aiohttp.ClientSession):
        if base_url in self.cache:
            return self.cache[base_url]

        robots_url = base_url.rstrip("/") + "/robots.txt"

        rules = {
            "disallow": [],
            "crawl_delay": 0.0,
        }

        try:
            async with session.get(robots_url) as resp:
                if resp.status != 200:
                    self.cache[base_url] = rules
                    return rules

                text = await resp.text()

        except Exception:
            self.cache[base_url] = rules
            return rules

        for line in text.splitlines():
            line = line.strip()

            if line.lower().startswith("disallow:"):
                rules["disallow"].append(line.split(":")[1].strip())

            if "crawl-delay" in line.lower():
                try:
                    rules["crawl_delay"] = float(line.split(":")[1].split()[0])
                except:
                    pass

        self.cache[base_url] = rules
        return rules

    def can_fetch(self, url: str, rules: dict) -> bool:
        path = urlparse(url).path

        for rule in rules["disallow"]:
            if rule and path.startswith(rule):
                return False

        return True

    def get_delay(self, rules: dict) -> float:
        return rules.get("crawl_delay", 0.0)