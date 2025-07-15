from __future__ import annotations
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List
import httpx
from bs4 import BeautifulSoup

# match URLs ending in "?year=YYYY"
YEAR_FEED_RE = re.compile(r"[?&]year=(\d{4})$")


@dataclass
class BH_XMLScraper:
    index_url: str
    out_dir: Path | str = "./data/beritaharian"
    timeout: float = 15.0
    polite_delay: float = 1.0
    max_concurrency: int = 5
    abbrev: str = "bh"

    async def dump_async(self) -> None:
        """Fetch the sitemap index, then each year-feed, dumping URLs to text files."""
        self.out_dir = Path(self.out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            year_feeds = await self._sitemap_links(client)
            if not year_feeds:
                print("No year feeds found.")
                return

            sem = asyncio.Semaphore(self.max_concurrency)
            tasks = [
                asyncio.create_task(self._process_year(url, client, sem))
                for url in year_feeds
            ]
            await asyncio.gather(*tasks)

    def dump(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            return asyncio.create_task(self.dump_async())
        else:
            asyncio.run(self.dump_async())

    async def _sitemap_links(self, client: httpx.AsyncClient) -> List[str]:
        """Return only those sitemap URLs whose loc ends with ?year=YYYY."""
        resp = await client.get(self.index_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")

        raw = [
            loc.text.strip()
            for loc in soup.find_all("loc")
            if loc.parent.name == "sitemap"
        ]
        # filter to only year feeds
        feeds: list[str] = []
        for link in raw:
            if YEAR_FEED_RE.search(link):
                feeds.append(link)
        return feeds

    async def _year_urls(
        self, feed_url: str, client: httpx.AsyncClient
    ) -> Iterable[str]:
        """Fetch one year-sitemap and yield each <loc> URL."""
        resp = await client.get(feed_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")
        return (loc.text.strip() for loc in soup.find_all("loc"))

    async def _process_year(
        self, feed_url: str, client: httpx.AsyncClient, sem: asyncio.Semaphore
    ) -> None:
        """Grab all URLs for one year and write them to bh_<year>.txt."""
        async with sem:
            try:
                urls = list(await self._year_urls(feed_url, client))
            except httpx.HTTPError as e:
                print(f"ERR · {feed_url} → {e}")
                return

            if not urls:
                print(f"0 · (empty) · {feed_url}")
                return

            # extract the year
            m = YEAR_FEED_RE.search(feed_url)
            year = m.group(1)
            fname = f"{self.abbrev}_{year}.txt"
            outpath = self.out_dir / fname
            outpath.write_text("\n".join(urls), encoding="utf-8")

            print(f"{len(urls):4d} URLs · {outpath.name}")
            await asyncio.sleep(self.polite_delay)


if __name__ == "__main__":
    scraper = BH_XMLScraper(
        index_url="https://www.beritaharian.sg/sitemap.xml",
        out_dir="/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/barita_harian/unseen",
        timeout=30.0,
        polite_delay=1,
        max_concurrency=10,
        abbrev="bh",
    )
    scraper.dump()
