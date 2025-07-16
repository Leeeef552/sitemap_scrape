from __future__ import annotations
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List
import httpx
from bs4 import BeautifulSoup

# match sitemap files like sitemap-1.xml, sitemap-2.xml, etc.
SITEMAP_RE = re.compile(r"sitemap-(\d+)\.xml$")


@dataclass
class ZB_XMLScraper:
    index_url: str
    out_dir: Path | str = "./data/beritaharian"
    timeout: float = 15.0
    polite_delay: float = 1.0
    max_concurrency: int = 5
    abbrev: str = "zb"

    async def dump_async(self) -> None:
        """Fetch the sitemap index, then each numbered sitemap, dumping URLs to text files."""
        self.out_dir = Path(self.out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            sitemap_urls = await self._sitemap_links(client)
            if not sitemap_urls:
                print("No sitemaps found.")
                return

            sem = asyncio.Semaphore(self.max_concurrency)
            tasks = [
                asyncio.create_task(self._process_sitemap(url, client, sem))
                for url in sitemap_urls
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
        """Return all sitemap URLs except sitemap-0.xml."""
        resp = await client.get(self.index_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")

        all_sitemaps = [
            loc.text.strip()
            for loc in soup.find_all("loc")
            if loc.parent.name == "sitemap"
        ]
        # filter out sitemap-0.xml
        filtered = [
            url for url in all_sitemaps
            if SITEMAP_RE.search(url)
        ]
        return filtered

    async def _sitemap_urls(
        self, sitemap_url: str, client: httpx.AsyncClient
    ) -> Iterable[str]:
        """Fetch one sitemap-N.xml and yield each URL in its CDATA <loc>."""
        resp = await client.get(sitemap_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")
        # loc tags contain <![CDATA[ ... ]]>
        for loc in soup.find_all("loc"):
            url = loc.text.strip()
            if url:
                yield url

    async def _process_sitemap(
        self, sitemap_url: str, client: httpx.AsyncClient, sem: asyncio.Semaphore
    ) -> None:
        """Grab all URLs for one sitemap and write them to zb_<n>.txt."""
        async with sem:
            try:
                # collect all entries from the async generator
                urls = [url async for url in self._sitemap_urls(sitemap_url, client)]
            except httpx.HTTPError as e:
                print(f"ERR · {sitemap_url} → {e}")
                return

            if not urls:
                print(f"0 · (empty) · {sitemap_url}")
                return

            # extract the sitemap number
            m = SITEMAP_RE.search(sitemap_url)
            idx = m.group(1) if m else Path(sitemap_url).stem

            fname = f"{self.abbrev}_{idx}.txt"
            outpath = self.out_dir / fname
            outpath.write_text("\n".join(urls), encoding="utf-8")

            print(f"{len(urls):4d} URLs · {outpath.name}")
            await asyncio.sleep(self.polite_delay)



if __name__ == "__main__":
    scraper = ZB_XMLScraper(
        index_url="https://www.zaobao.com.sg/sitemap.xml",
        out_dir="/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao/unseen",
        timeout=30.0,
        polite_delay=0.5,
        max_concurrency=10,
        abbrev="zb",
    )
    scraper.dump()
