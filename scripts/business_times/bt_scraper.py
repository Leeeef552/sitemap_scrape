from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback
from tqdm.auto import tqdm
from openai import OpenAI
from playwright.async_api import async_playwright, Browser, BrowserContext
from ...utils.logger import logger  # Ensure this logger is configured
from ..straits_times.st_scraper import ST_Scraper, process_txt_async
import re
from urllib.parse import urljoin, urlparse, parse_qs


class BT_Scraper(ST_Scraper):
    async def scrape_single_url(self, url: str) -> Dict[str, Any]:
        # 1) get everything except images
        data = await super().scrape_single_url(url)

        # if we failed, just pass it along
        if not isinstance(data, dict) or "article_url" not in data:
            return data

        # 2) fetch the DOM again so we can re-extract images
        soup = await self._fetch_page_content(url)
        if soup is None:
            return data

        article = soup.find("article")
        if article is None:
            return data

        # 3) build a new images list by picking the highest-res candidate
        images: List[Dict[str, Any]] = []
        for picture in article.find_all("picture"):
            best = self._best_image_from_picture(picture, url)
            if best:
                images.append(best)

        data["images"] = images
        return data

    def _is_tiny_author_image(self, url: str) -> bool:
        """
        Return True if the URL has w=100, h=100 and dpr=1 in its query string.
        """
        q = parse_qs(urlparse(url).query)
        return q.get("w", [""])[0] == "100" and \
               q.get("h", [""])[0] == "100" and \
               q.get("dpr", [""])[0] == "1"

    def _best_image_from_picture(self, picture: Tag, page_url: str) -> Optional[Dict[str,Any]]:
        """
        Parse every <source>/@srcset and pick the URL with the largest 'w' value.
        Fallback to the <img>/@src if no valid srcset entries found.
        """
        candidates = []  # list of (width:int, url:str)

        # pattern:  capture URL (\S+), then optional whitespace+digits+w
        pattern = re.compile(r'(\S+)(?:\s+(\d+)w)?')

        for source in picture.find_all("source"):
            srcset = source.get("srcset", "")
            for part in srcset.split(","):
                part = part.strip()
                if not part:
                    continue
                m = pattern.match(part)
                if not m:
                    continue
                url_part, w_str = m.groups()
                full_url = urljoin(page_url, url_part)
                width = int(w_str) if w_str else 0
                candidates.append((width, full_url))

        # fallback to <img> if nothing valid in srcset
        if not candidates:
            img = picture.find("img")
            if img and img.get("src"):
                candidates.append((0, urljoin(page_url, img["src"])))

        if not candidates:
            return None

        # pick the highest width
        _, best_url = max(candidates, key=lambda x: x[0])
        img = picture.find("img")
        alt = img.get("alt", "").strip() or None if img else None

        if self._is_tiny_author_image(best_url):
            best_url = None
            alt = None
        return {"image_url": best_url, "alt_text": alt}


def main():
    BASE_DIR = pathlib.Path("/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/data/business_times")
    UNSEEN_DIR = BASE_DIR / "unseen"  # Original .txt files here
    SEEN_DIR = BASE_DIR / "seen"      # Processed .txt files moved here
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    # Ensure directories exist
    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    CONCURRENCY = 75  # Increased from 5 - URLs processed concurrently per file

    txt_files = list(UNSEEN_DIR.glob("*.txt"))

    if not txt_files:
        logger.warning(f"No .txt files found in {UNSEEN_DIR}")
        return

    logger.info(f"Found {len(txt_files)} files to process")

    # Process files one by one (you can modify this to process multiple files concurrently if needed)
    for txt_file in tqdm(txt_files, desc="Processing urls"):
        try:
            result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY, BT_Scraper))
            if result:
                logger.info(f"Processed {txt_file.name} with result {result}")
            
            # Move processed file to seen directory
            seen_path = SEEN_DIR / txt_file.name
            txt_file.rename(seen_path)
            
        except Exception as e:
            logger.error(f"Error processing {txt_file}: {e}", exc_info=True)

    logger.info("All files processed!")


if __name__ == "__main__":
    import time
    start = time.time()
    main()
    end = time.time()
    print(f"total time taken {end-start}")