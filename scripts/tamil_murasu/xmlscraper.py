from ..berita_harian.bh_xmlscraper import BH_XMLScraper
from ...utils.logger import logger
import httpx
from tqdm.auto import tqdm
from typing import Iterable, List
from urllib.parse import urlparse, urlunparse
from bs4 import BeautifulSoup


class TM_XMLScraper(BH_XMLScraper):
    async def _year_urls(self, feed_url: str, client: httpx.AsyncClient) -> Iterable[str]:
        parsed = urlparse(feed_url)
        if not parsed.netloc.startswith("www."):
            # prepend 'www.' to the hostname
            parsed = parsed._replace(netloc="www." + parsed.netloc)
            feed_url = urlunparse(parsed)

        resp = await client.get(feed_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")
        return (loc.text.strip() for loc in soup.find_all("loc"))

if __name__ == "__main__":
    scraper = TM_XMLScraper(
        index_url="https://www.tamilmurasu.com.sg/sitemap.xml",
        out_dir="/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/tamil_murasu/unseen",
        timeout=30.0,
        polite_delay=1,
        max_concurrency=10,
        abbrev="tm",
    )
    scraper.dump()