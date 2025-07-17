import re
from urllib.parse import urljoin
from dateutil import parser as dateparser
from bs4 import BeautifulSoup, Tag
from typing import Any, Dict, List, Optional
import traceback
import asyncio, json, pathlib, traceback, random, re
from ..straits_times.st_scraper import ST_Scraper, process_txt_async  # adjust import path as needed
import concurrent.futures
from ...utils.logger import logger
from tqdm.auto import tqdm


class Tabla_Scraper(ST_Scraper):
    """Extends ST_Scraper to extract dates from dropdown buttons
    and images from article carousels."""

    async def scrape_single_url(self, url: str) -> Dict[str, Any]:
        result = await super().scrape_single_url(url)
        # If super returned an error tuple, propagate
        if isinstance(result, tuple) and result and result[0] == "ERROR":
            return result

        try:
            soup: BeautifulSoup = await self._fetch_page_content(url)
            article = soup.find("article")
            if not article:
                raise RuntimeError("No <article> tag found for custom scraping")

            # Override publish_date extraction
            pub_date = None
            btn = article.find("button", class_="dropdown-button flex leading-7")
            if btn:
                p_tag = btn.find("p")
                if p_tag and p_tag.get_text(strip=True):
                    raw = p_tag.get_text(strip=True)
                    try:
                        pub_date = dateparser.parse(raw).isoformat()
                    except (ValueError, TypeError):
                        pub_date = None

            # Override images extraction
            images: List[Dict[str, Any]] = []
            wrappers = article.find_all(
                "div", class_=re.compile(r"^article-carousel-wrapper-\d+$")
            )
            for wrapper in wrappers:
                # find caption text from div.text-grey-200 if present
                caption_tag = wrapper.find("div", class_="text-grey-200")
                caption = caption_tag.get_text(strip=True) if caption_tag else None

                # collect all img tags inside this wrapper
                for img in wrapper.find_all("img"):
                    # parse highest-res URL from srcset if present
                    url_to_use = None
                    if img.has_attr("srcset"):
                        candidates = [s.strip() for s in img["srcset"].split(",")]
                        max_width = 0
                        for cand in candidates:
                            parts = cand.split()
                            if len(parts) >= 2 and parts[-1].endswith("w"):
                                try:
                                    width = int(parts[-1][:-1])
                                except ValueError:
                                    width = 0
                                if width > max_width:
                                    max_width = width
                                    url_to_use = parts[0]
                        # fallback: first URL
                        if not url_to_use and candidates:
                            url_to_use = candidates[0].split()[0]
                    else:
                        url_to_use = img.get("src") or img.get("data-src")

                    if url_to_use:
                        full_url = urljoin(url, url_to_use)
                        images.append({
                            "image_url": full_url,
                            "alt_text": caption
                        })

            # Merge into result
            result["publish_date"] = pub_date
            result["images"] = images
            return result

        except Exception as e:
            return ("ERROR", url, repr(e), traceback.format_exc())


def process_single_file(txt_file: pathlib.Path, OUT_DIR, ERR_DIR, SEEN_DIR, CONCURRENCY):
    try:
        result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY, Tabla_Scraper, False))
        seen_path = SEEN_DIR / txt_file.name
        txt_file.rename(seen_path)
        return f"Processed {txt_file.name}"
    except Exception as e:
        logger.error(f"Error processing {txt_file}: {e}", exc_info=True)
        return f"Error in {txt_file.name}: {e}"
    
def main():
    BASE_DIR = pathlib.Path("/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/tabla")
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
    
    CONCURRENCY = 20
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