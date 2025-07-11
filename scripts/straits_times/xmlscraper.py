from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List
import httpx
from bs4 import BeautifulSoup

## scrapes from the xml sitemap, takes only monthly feedsusing the below re patter
## excludes this month's feed


MONTH_FEED_RE = re.compile(r"/(\d{4})/(\d{2})/feeds\.xml$")   # keep only YYYY/MM feeds

@dataclass
class XMLScraper:
    index_url: str
    out_dir: Path | str = "/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/data/straitsTimes/st_sitemaps"
    timeout: float = 15.0
    polite_delay: float = 1.0
    max_concurrency: int = 5
    abbrev: str = "st"

    # ────────────────────────── public helpers ──────────────────────────────
    async def dump_async(self) -> None:
        """Asynchronously download every past month and save to .txt files."""
        self.out_dir = Path(self.out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            month_feeds = await self._sitemap_links(client)

            if not month_feeds:
                print("No month feeds found (or all filtered out).")
                return

            sem = asyncio.Semaphore(self.max_concurrency)
            tasks = [
                asyncio.create_task(self._process_month(feed_url, client, sem))
                for feed_url in month_feeds
            ]
            await asyncio.gather(*tasks)

    def dump(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:           # no loop → we're in a vanilla script
            loop = None

        if loop and loop.is_running():
            # notebook / web-server context → create and return a Task
            return asyncio.create_task(self.dump_async())
        else:
            # classic script → safe to spin up a fresh loop
            asyncio.run(self.dump_async())

    # ────────────────────────── internals ───────────────────────────────────
    async def _sitemap_links(self, client: httpx.AsyncClient) -> List[str]:
        """Return monthly feeds, filtering out current month & sections.xml."""
        r = await client.get(self.index_url)
        r.raise_for_status()

        soup = BeautifulSoup(r.content, "xml")
        raw_links = [
            loc.get_text(strip=True)
            for loc in soup.find_all("loc")
            if loc.parent.name == "sitemap"
        ]

        # figure out YYYY/MM for 'today' (Singapore time is irrelevant for month test)
        y_now, m_now = datetime.now(timezone.utc).year, datetime.now(timezone.utc).month

        feeds: list[str] = []
        for link in raw_links:
            m = MONTH_FEED_RE.search(link)
            if not m:                          # skips sections.xml & anything odd
                continue
            yr, mo = int(m.group(1)), int(m.group(2))
            if (yr, mo) == (y_now, m_now):     # skip current month
                continue
            feeds.append(link)

        return feeds

    async def _month_urls(
        self, feed_url: str, client: httpx.AsyncClient
    ) -> Iterable[str]:
        """Return every <loc> article URL from a single feeds.xml."""
        r = await client.get(feed_url)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "xml")
        return (loc.get_text(strip=True) for loc in soup.find_all("loc"))

    async def _process_month(
        self,
        feed_url: str,
        client: httpx.AsyncClient,
        sem: asyncio.Semaphore,
    ) -> None:
        """Download one month feed, write out its TXT file."""
        async with sem:                 # limit concurrent requests
            try:
                urls = list(await self._month_urls(feed_url, client))
            except httpx.HTTPError as e:
                print("   ERR ·", feed_url, "→", e)
                return

            if not urls:
                print("   0   · (empty) ·", feed_url)
                return

            # derive filename st_YYYY_MM.txt from the URL
            m = MONTH_FEED_RE.search(feed_url)
            fname = f"{self.abbrev}_{m.group(1)}_{m.group(2)}.txt"
            outpath = self.out_dir / fname
            outpath.write_text("\n".join(urls), encoding="utf-8")

            print(f"{len(urls):5d} · {outpath.relative_to(self.out_dir)}")
            await asyncio.sleep(self.polite_delay)  # respectful crawl 
            
def main():
    extractor = XMLScraper(
        index_url="https://www.straitstimes.com/sitemap.xml",
        timeout=10,
        polite_delay=0.5,
        max_concurrency=8,
        out_dir="/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/data/straits_times/unseen",
        abbrev="st"
    )
    extractor.dump()

if __name__ == "__main__":
    main()