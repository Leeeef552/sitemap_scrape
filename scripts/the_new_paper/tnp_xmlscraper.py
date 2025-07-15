from ..berita_harian.bh_xmlscraper import BH_XMLScraper
from ...utils.logger import logger
import httpx
from tqdm.auto import tqdm
from typing import Iterable, List
from urllib.parse import urlparse, urlunparse
from bs4 import BeautifulSoup

if __name__ == "__main__":
    scraper = BH_XMLScraper(
        index_url="https://www.tnp.sg/sitemap.xml",
        out_dir="/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/the_new_paper/unseen",
        timeout=30.0,
        polite_delay=1,
        max_concurrency=10,
        abbrev="tnp",
    )
    scraper.dump()