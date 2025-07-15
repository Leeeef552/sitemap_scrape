from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback, random, re
from tqdm.auto import tqdm
import concurrent.futures
from ...utils.logger import logger
from ..straits_times.st_scraper import ST_Scraper, process_txt_async

class TM_SCraper(ST_Scraper):

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
                    # parse “YYYYMMDD” into a date and iso‑format it
                    dt = dateparser.parse(m.group(1))
                    pub_date = dt.date().isoformat()
                except Exception:
                    pub_date = None

            # ——— Fallback: scrape the on‑page <p data-testid="date"> ———
            if not pub_date:
                # assume you've already done: soup = BeautifulSoup(html, "html.parser")
                date_tag = soup.find("p", {"data-testid": "date"})
                if date_tag:
                    # extract text like “01 Jul 2025 - 8:39 pm”
                    raw = date_tag.get_text(" ", strip=True)
                    try:
                        dt2 = dateparser.parse(raw)
                        # if you only want the date part:
                        pub_date = dt2.date().isoformat()
                        # or for full timestamp: pub_date = dt2.isoformat()
                    except Exception:
                        pub_date = None

            # Extract and clean content
            content = self._clean_content(article)

            images: List[Dict[str, Any]] = []

            # find every carousel wrapper whose class matches article-carousel-wrapper-<digits>
            for wrapper in article.find_all("div", class_=re.compile(r"article-carousel-wrapper-\d+")):
                # 1) the <a> tag’s href is the real image URL
                a = wrapper.find("a", href=True)
                if not a:
                    continue
                img_url = urljoin(url, a["href"])

                # 2) alt text comes from the <img> inside that <a>
                img_tag = a.find("img")
                alt = img_tag.get("alt", "").strip() or None if img_tag else None

                # 3) caption is in the div with data-testid="image-caption-wrapper"
                cap_div = wrapper.find("div", {"data-testid": "image-caption-wrapper"})
                caption = cap_div.get_text(" ", strip=True) or None if cap_div else None

                images.append({
                    "image_url": img_url,
                    "alt_text":   alt,
                    "caption":    caption,
                })

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


def process_single_file(txt_file: pathlib.Path, OUT_DIR, ERR_DIR, SEEN_DIR, CONCURRENCY):
    try:
        result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY, TM_SCraper, False))
        seen_path = SEEN_DIR / txt_file.name
        txt_file.rename(seen_path)
        return f"Processed {txt_file.name}"
    except Exception as e:
        logger.error(f"Error processing {txt_file}: {e}", exc_info=True)
        return f"Error in {txt_file.name}: {e}"

def main():
    BASE_DIR = pathlib.Path("/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/tamil_murasu")
    UNSEEN_DIR = BASE_DIR / "test"
    SEEN_DIR = BASE_DIR / "seen"
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    CONCURRENCY = 5
    MAX_PARALLEL_TXT_FILES = 2  # Number of files to process in parallel

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