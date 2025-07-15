from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback, random, re
from tqdm.auto import tqdm
import concurrent.futures
from ...utils.logger import logger
from ..straits_times.st_scraper import process_txt_async
from ..tamil_murasu.tm_scraper import TM_SCraper


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
    BASE_DIR = pathlib.Path("/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/the_new_paper")
    UNSEEN_DIR = BASE_DIR / "test"
    SEEN_DIR = BASE_DIR / "seen"
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    CONCURRENCY = 2
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