from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback, random, re
from tqdm.auto import tqdm
import concurrent.futures
from ...utils.logger import logger
from ..straits_times.st_scraper import ST_Scraper, process_txt_async


class TNP_Scraper(ST_Scraper):

    # ------------------------------------------------------------------ #
    #  NEW helper: reject placeholders & reaction GIFs (case‑insensitive) #
    # ------------------------------------------------------------------ #
    _UNWANTED_PAT = re.compile(
        r"""
            /assets/image-placeholder-.*\.(?:png|jpe?g|webp)$   |   # placeholders
            /reactions/\d+\.(?:gif|png|webp)$                   |   # reaction images
            (?:^|//|://)            # start, proto‑relative, or normal scheme
            [^/]*\boutbrainimg\.com/ # any sub‑domain, whole word match
        """,
        re.I | re.X,
    )

    def _is_unwanted_image(self, u: str) -> bool:
        return bool(self._UNWANTED_PAT.search(u))

    def _is_data_uri(self, u: str) -> bool:
        return u.strip().lower().startswith("data:")

    def _pick_largest_from_srcset(self, srcset: str) -> str | None:
        best_url = None
        best_w   = -1

        for candidate in srcset.split(","):
            url, *rest = candidate.strip().split()

            # skip data‑URIs and our new unwanted patterns
            if self._is_data_uri(url) or self._is_unwanted_image(url):
                continue

            w = int(rest[0][:-1]) if rest and rest[0].endswith("w") else -1
            if w > best_w:
                best_url, best_w = url, w

        return best_url

    async def scrape_single_url(self, url: str) -> Dict[str, Any]:        
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

            pub_date = None
            time_tag = soup.find("time", {"data-testid": "date"}) or soup.find("time")

            if time_tag and time_tag.get_text(strip=True):
                raw = time_tag.get_text(" ", strip=True)
                try:
                    dt = dateparser.parse(raw)
                    pub_date = dt.isoformat() 
                except Exception:
                    pass

            # Extract and clean content
            content = self._clean_content(article)

            images = []
            seen   = set()

            for img in article.find_all("img", src=True):
                # 1) choose the candidate URL ---------------------------------
                if "srcset" in img.attrs:
                    img_url = self._pick_largest_from_srcset(img["srcset"])
                else:
                    img_url = img["src"]

                # 2) reject bad URLs ------------------------------------------
                if (
                    not img_url
                    or self._is_data_uri(img_url)
                    or self._is_unwanted_image(img_url)
                ):
                    continue

                # 3) normalise, dedupe, and collect ---------------------------
                img_url = urljoin(url, img_url)
                if img_url in seen:
                    continue
                seen.add(img_url)

                alt = img.get("alt") or None
                cap_div = img.find_parent().find(
                    "div", {"data-testid": "image-caption-wrapper"}
                )
                if cap_div:
                    caption = cap_div.get_text(" ", strip=True)
                else:
                    figcap = img.find_next("figcaption")
                    caption = figcap.get_text(" ", strip=True) if figcap else None

                images.append(
                    {
                        "image_url": img_url,
                        "alt_text":  alt,
                        "caption":   caption or None,
                    }
                )

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
        result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY, TNP_Scraper, False))
        seen_path = SEEN_DIR / txt_file.name
        txt_file.rename(seen_path)
        return f"Processed {txt_file.name}"
    except Exception as e:
        logger.error(f"Error processing {txt_file}: {e}", exc_info=True)
        return f"Error in {txt_file.name}: {e}"

def main():
    BASE_DIR = pathlib.Path("/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/the_new_paper")
    UNSEEN_DIR = BASE_DIR / "unseen"
    SEEN_DIR = BASE_DIR / "seen"
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    txt_files = list(UNSEEN_DIR.glob("*.txt"))
    if not txt_files:
        logger.warning(f"No .txt files found in {UNSEEN_DIR}")
        return
    
    CONCURRENCY = 50
    MAX_PARALLEL_TXT_FILES = len(txt_files)  # Number of files to process in parallel

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