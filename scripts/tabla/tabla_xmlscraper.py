from __future__ import annotations
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List
import httpx
from bs4 import BeautifulSoup
from ..berita_harian.bh_xmlscraper import BH_XMLScraper

if __name__ == "__main__":
    scraper = BH_XMLScraper(
        index_url="https://www.tabla.com.sg/sitemap.xml",
        out_dir="/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/tabla/unseen",
        timeout=30.0,
        polite_delay=1,
        max_concurrency=5,
        abbrev="tabla",
    )
    scraper.dump()