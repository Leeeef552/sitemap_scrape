from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback
from tqdm.auto import tqdm
from playwright.async_api import async_playwright, Browser, BrowserContext
from ...utils.logger import logger  # Ensure this logger is configured
from ..straits_times.st_scraper import ST_Scraper, process_txt_async
import re
from urllib.parse import urljoin, urlparse, parse_qs
import concurrent.futures, os, functools


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



def _run_one_file(txt_path: pathlib.Path,
                  out_dir: pathlib.Path,
                  err_dir: pathlib.Path,
                  seen_dir: pathlib.Path,
                  concurrency: int) -> str:
    try:
        asyncio.run(
            process_txt_async(
                txt_path,
                out_dir,
                err_dir,
                concurrency,
                BT_Scraper
            )
        )
        # atomic rename so we can't double-process
        txt_path.rename(seen_dir / txt_path.name)
        return f"✔ {txt_path.name}"
    except Exception as e:
        logger.error(f"Worker failed on {txt_path}: {e}", exc_info=True)
        return f"✖ {txt_path.name}: {e}"

def main() -> None:
    BASE_DIR = pathlib.Path(
        "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/business_times"
    )
    UNSEEN_DIR = BASE_DIR / "unseen"
    SEEN_DIR   = BASE_DIR / "seen"
    OUT_DIR    = BASE_DIR / "scraped"
    ERR_DIR    = BASE_DIR / "unsuccessful"

    # make sure the dirs exist
    for d in (UNSEEN_DIR, SEEN_DIR, OUT_DIR, ERR_DIR):
        d.mkdir(parents=True, exist_ok=True)

    CONCURRENCY_IN_FILE    = 20
    MAX_PARALLEL_TXT_FILES = 4

    txt_files = list(UNSEEN_DIR.glob("*.txt"))
    if not txt_files:
        logger.warning("No .txt files to process")
        return

    logger.info(f"Submitting {len(txt_files)} files "
                f"to a pool of {MAX_PARALLEL_TXT_FILES} workers")

    # freeze the arguments other than txt_path
    worker = functools.partial(
        _run_one_file,
        out_dir=OUT_DIR,
        err_dir=ERR_DIR,
        seen_dir=SEEN_DIR,
        concurrency=CONCURRENCY_IN_FILE,
    )

    # ── use processes for real parallelism (each owns a Chromium) ──
    with concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_PARALLEL_TXT_FILES
    ) as pool:
        for status in tqdm(pool.map(worker, txt_files),
                           total=len(txt_files),
                           desc="Files"):
            logger.info(status)

    logger.info("All files processed!")

if __name__ == "__main__":
    import time
    start = time.time()
    main()
    end = time.time()
    print(f"total time taken {end-start}")