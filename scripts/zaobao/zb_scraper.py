from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback, random, re
from tqdm.auto import tqdm
from playwright.async_api import async_playwright, Browser, BrowserContext
from ...utils.logger import logger
from ..straits_times.st_scraper import ST_Scraper, process_txt_async

class ZB_Scraper(ST_Scraper):

    def _extract_images(self, container: Tag, page_url: str) -> List[Dict[str, Any]]:
        """
        1) Grab exactly one hero <img> from any button containing w-full class
        2) Grab all <img> inside the div.articleBody
        3) Dedupe by URL and skip any data: URIs
        """
        images: List[Dict[str, Any]] = []
        seen_srcs = set()
        
        btn = container.find("button", class_=lambda c: c and "w-full" in c.split()) if container else None
        if btn:
            img = btn.find("img", src=True)
            if img:
                src = urljoin(page_url, img["src"])
                if not src.startswith("data:") and src not in seen_srcs:
                    seen_srcs.add(src)
                    images.append({
                        "image_url": src,
                        "alt_text": img.get("alt", "").strip() or None,
                        "caption": None
                    })
        

        # --- 2) In-article images ---
        body = container.find("div", class_=lambda c: c and "articleBody" in c) if container else None
        if body:
            for img in body.find_all("img", src=True):
                src = urljoin(page_url, img["src"])
                # skip icons, duplicates, data URIs
                if src.startswith("data:") or src in seen_srcs:
                    continue

                seen_srcs.add(src)
                # optional figcaption if you ever wrap it
                caption = None
                if img.parent and img.parent.name.lower() == "figure":
                    figcap = img.parent.find("figcaption")
                    if figcap:
                        caption = figcap.get_text(strip=True)

                images.append({
                    "image_url": src,
                    "alt_text": img.get("alt", "").strip() or None,
                    "caption": caption,
                })

        return images    

    async def scrape_single_url(self, url: str) -> Dict[str, Any]:
        """Scrape a single URL for article content, metadata, images, and generate summary."""
        logger.debug(f"Starting scrape for URL: {url}")
        
        try:
            max_retries = 3
            for attempt in range(max_retries):
                soup = await self._fetch_page_content(url)
                if soup and soup.find("article"):
                    logger.info(f"Article successfully found {url}")
                    break
                logger.info(f"Retrying {url}")
                
            article = soup.find("article")
            if not article:
                logger.error(f"No <article> tag found in {url}")
                raise RuntimeError("No <article> tag found")

            # Title
            h1 = article.find("h1")
            title = (
                h1.get_text(strip=True)
                if h1
                else (soup.title.string.strip() if soup.title else "(untitled)")
            )

            # ——— Published date (from the URL) ———
            pub_date = None
            m = re.search(r"story(\d{8})", url)
            if m:
                try:
                    # parse “YYYYMMDD” into a date and iso-format it
                    dt = dateparser.parse(m.group(1))
                    pub_date = dt.date().isoformat()
                except Exception:
                    pub_date = None

            # Extract and clean content
            content = self._clean_content(article)

            # Use custom image extraction method
            images = self._extract_images(article, url)

            return {
                "article_url":   url,
                "site_title":    title,
                "publish_date":  pub_date,
                "content":       content,
                "images":        images,
            }
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}", exc_info=True)
            return ("ERROR", url, repr(e), traceback.format_exc())


import concurrent.futures

def process_single_file(txt_file: pathlib.Path, OUT_DIR, ERR_DIR, SEEN_DIR, CONCURRENCY):
    try:
        result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY, ZB_Scraper, False))
        seen_path = SEEN_DIR / txt_file.name
        txt_file.rename(seen_path)
        return f"Processed {txt_file.name}"
    except Exception as e:
        logger.error(f"Error processing {txt_file}: {e}", exc_info=True)
        return f"Error in {txt_file.name}: {e}"

def main():
    BASE_DIR = pathlib.Path("/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao")
    UNSEEN_DIR = BASE_DIR / "unseen"
    SEEN_DIR = BASE_DIR / "seen"
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    CONCURRENCY = 50
    MAX_PARALLEL_TXT_FILES = 4  # Number of files to process in parallel

    txt_files = list(UNSEEN_DIR.glob("*.txt"))
    if not txt_files:
        logger.warning(f"No .txt files found in {UNSEEN_DIR}")
        return

    logger.info(f"Found {len(txt_files)} files to process")

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_TXT_FILES) as executor:
        futures = [
            executor.submit(process_single_file, txt_file, OUT_DIR, ERR_DIR, SEEN_DIR, CONCURRENCY)
            for txt_file in txt_files
        ]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Parallel processing"):
            result = future.result()
            logger.info(result)

    logger.info("All files processed!")



if __name__ == "__main__":
    import time
    start = time.time()
    main()
    end = time.time()
    print(f"total time taken {end-start}")