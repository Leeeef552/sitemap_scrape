import aiofiles                 # NEW  ──────── async file I/O
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback
from tqdm.auto import tqdm
from playwright.async_api import async_playwright, Browser, BrowserContext
from ...utils.logger import logger  # Ensure this logger is configured
import random
import re
import concurrent.futures, os, functools


class ST_Scraper:
    """Scrapes articles using Playwright with batch processing and context reuse"""

    def __init__(self, concurrency: int = 5):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.browser: Optional[Browser] = None
        self.contexts: List[BrowserContext] = []
        self.context_semaphore = asyncio.Semaphore(concurrency)

    async def __aenter__(self):
        """Async context manager entry"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", 
                "--disable-gpu",
                "--disable-dev-shm-usage",  # Reduce memory usage
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor"
            ],
        )
        
        # Pre-create browser contexts for reuse
        logger.info(f"Creating {self.concurrency} browser contexts for reuse")
        for i in range(self.concurrency):
            context = await self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                ),
            )
            self.contexts.append(context)
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        # Close all contexts
        for context in self.contexts:
            await context.close()
        
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    def _clean_content(self, article: Tag) -> str:
        """Extract and clean text content from article tag"""
        # Remove unwanted elements

        content = article.get_text(separator="\n\n", strip=True)
        if not content.strip():
            logger.warning("No text content extracted after cleaning.")
        return content

        
    def _extract_image_src(self, img_tag: Tag, page_url: str) -> Optional[str]:
        """
        Resolve relative/absolute URLs for the given <img>.
        """
        src = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original")
        return urljoin(page_url, src) if src else None

    async def _get_available_context(self) -> BrowserContext:
        """Get an available browser context from the pool"""
        async with self.context_semaphore:
            # Simple round-robin selection
            context_index = len(self.contexts) - self.context_semaphore._value - 1
            return self.contexts[context_index % len(self.contexts)]

    async def _fetch_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch page content using Playwright with context reuse"""
        if not self.browser:
            raise RuntimeError("Browser not started. Use 'async with'.")
        
        async with self.semaphore:
            await asyncio.sleep(random.uniform(0.1, 0.75))
            context = await self._get_available_context()
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)  # Reduced timeout
                await page.wait_for_selector("article", timeout=30000)
                
                html = await page.content()
                logger.debug(f"Successfully fetched content from {url}")
                return BeautifulSoup(html, "html.parser")
                
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                return None
            finally:
                await page.close()  # Close the page but keep context alive

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

            # Published date (rich logic)
            pub_date: Optional[str] = None
            time_tag = article.find("time")
            if time_tag and time_tag.has_attr("datetime"):
                try:
                    pub_date = dateparser.parse(time_tag["datetime"]).isoformat()
                except (ValueError, TypeError):
                    pass
            elif time_tag:
                try:
                    pub_date = dateparser.parse(time_tag.get_text(strip=True)).isoformat()
                except (ValueError, TypeError):
                    pass
            else:
                # 1) Look for <span>Published Thu, May 29, 2014 · 10:00 PM</span>
                span = article.find("span", string=re.compile(r"\bPublished\b", re.IGNORECASE))
                if span:
                    raw = span.get_text(strip=True)
                    # remove leading "Published", optional colon/dot and whitespace
                    cleaned = re.sub(r"^[Pp]ublished[:·\s]*", "", raw)
                    try:
                        pub_date = dateparser.parse(cleaned).isoformat()
                    except (ValueError, TypeError):
                        pass

                else:
                    # 2) Fallback to meta[property="article:published_time"]
                    meta = soup.find("meta", {"property": "article:published_time"})
                    if meta and meta.has_attr("content"):
                        try:
                            pub_date = dateparser.parse(meta["content"]).isoformat()
                        except (ValueError, TypeError):
                            pass

            # Extract and clean content
            content = self._clean_content(article)

            # Collect images with alt text and caption
            images: List[Dict[str, Any]] = []
            
            for picture in article.find_all("picture"):
                # grab alt text from the <img> if present
                img_tag = picture.find("img")
                alt = img_tag.get("alt", "").strip() or None if img_tag else None

                # now pull every <source> and that <img>
                for tag in picture.find_all(["img"]):
                    src = self._extract_image_src(tag, url)
                    if not src:
                        continue
                    images.append({
                        "image_url": src,
                        "alt_text": alt
                    })

            return {
                "article_url": url,
                "site_title": title,
                "publish_date": pub_date,
                "content": content,
                "images": images,
            }
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}", exc_info=True)
            return ("ERROR", url, repr(e), traceback.format_exc())

    async def scrape_urls_batch(self, urls: List[str]) -> List[Any]:
        """Scrape multiple URLs concurrently"""
        logger.info(f"Starting batch scrape of {len(urls)} URLs")
        
        tasks = []
        for url in urls:
            await asyncio.sleep(0.15)   # 200 ms between task-spawns
            tasks.append(self.scrape_single_url(url))
        results = await asyncio.gather(*tasks)
        logger.info(f"Completed batch scrape of {len(urls)} URLs")
        return results


async def process_txt_async(
    txt_path: pathlib.Path,
    out_dir: pathlib.Path,
    err_dir: pathlib.Path,
    concurrency: int = 5,
    scraper_class: type=ST_Scraper,
    ensure_ascii: bool=True
) -> str | None:
    year_month = txt_path.stem
    out_file = out_dir / f"{year_month}.jsonl"
    err_file = err_dir / f"{year_month}_errors.jsonl"

    logger.info(f"Processing {txt_path.name}…")

    # ── 1) Read all URLs from the .txt ────────────────────────────────────────
    try:
        urls = [ln.strip() for ln in txt_path.read_text().splitlines() if ln.strip()]
    except Exception as e:
        logger.error(f"Error reading {txt_path}: {e}", exc_info=True)
        return None

    if not urls:
        logger.warning(f"No URLs found in {txt_path.name}")
        return year_month

    # ── 2) Filter out URLs we've already scraped successfully ───────────────
    processed_urls = set()
    if out_file.exists():
        for line in out_file.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
                if isinstance(rec, dict) and "article_url" in rec:
                    processed_urls.add(rec["article_url"])
            except json.JSONDecodeError:
                continue

    # Keep only the ones not yet done
    urls = [u for u in urls if u not in processed_urls]
    if not urls:
        logger.info(f"All URLs in {txt_path.name} are already processed; skipping.")
        return year_month

    # ── 3) Now urls contains only new entries; proceed as before ───────────
    success_count = error_count = 0

    async with scraper_class(concurrency=concurrency) as scraper, \
               aiofiles.open(out_file, "a", encoding="utf-8") as ok_f, \
               aiofiles.open(err_file, "a", encoding="utf-8") as er_f:

        # Kick off all scrapes
        scrape_tasks = [asyncio.create_task(scraper.scrape_single_url(u)) for u in urls]

        # Wrap the completion iterator in tqdm
        with tqdm(
            total=len(scrape_tasks),
            desc=f"Scraping URLs in {txt_path.name}",
            unit="url",
            leave=False
        ) as pbar:
            for coro in asyncio.as_completed(scrape_tasks):
                item = await coro

                # Determine which file to write to
                is_error = isinstance(item, tuple) and item and item[0] == "ERROR"
                target_f = er_f if is_error else ok_f

                # Write & flush
                await target_f.write(json.dumps(item,  ensure_ascii=ensure_ascii, default=str) + "\n")
                await target_f.flush()

                # Logging and counters
                if is_error:
                    error_count += 1
                    _, bad_url, msg, _tb = item
                    logger.error(f"Error scraping {bad_url}: {msg}")
                else:
                    success_count += 1
                    logger.debug(f"Saved {item['article_url']}")

                # Advance the progress bar
                pbar.update(1)

    logger.info(
        f"Completed {txt_path.name}: {success_count} ok, {error_count} errors"
    )
    return year_month

def _run_one_file(txt_file: pathlib.Path,
                  out_dir: pathlib.Path,
                  err_dir: pathlib.Path,
                  seen_dir: pathlib.Path,
                  concurrency: int) -> str:
    """
    This is executed in a *separate* process.
    We call asyncio.run(...) because process pools are not async-aware.
    """
    try:
        # do the actual scraping
        asyncio.run(
            process_txt_async(txt_file, out_dir, err_dir,
                              concurrency, ST_Scraper)
        )
        # move to seen/  (atomic rename)
        txt_file.rename(seen_dir / txt_file.name)
        return f"✔ {txt_file.name}"
    except Exception as e:
        logger.error(f"Worker failed on {txt_file}: {e}", exc_info=True)
        return f"✖ {txt_file.name}: {e}"


def main() -> None:
    BASE_DIR = pathlib.Path("/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/straits_times")
    UNSEEN_DIR = BASE_DIR / "unseen"
    SEEN_DIR   = BASE_DIR / "seen"
    OUT_DIR    = BASE_DIR / "scraped"
    ERR_DIR    = BASE_DIR / "unsuccessful"

    # ensure dirs exist
    for d in (UNSEEN_DIR, SEEN_DIR, OUT_DIR, ERR_DIR):
        d.mkdir(parents=True, exist_ok=True)

    CONCURRENCY_IN_FILE        = 50   # pages per file
    MAX_PARALLEL_TXT_FILES     = 4  # change as you like

    txt_files = list(UNSEEN_DIR.glob("*.txt"))
    if not txt_files:
        logger.warning("No .txt files to process")
        return

    logger.info(f"Submitting {len(txt_files)} files to the pool "
                f"({MAX_PARALLEL_TXT_FILES} workers)")

    worker = functools.partial(
        _run_one_file,
        out_dir=OUT_DIR,
        err_dir=ERR_DIR,
        seen_dir=SEEN_DIR,
        concurrency=CONCURRENCY_IN_FILE,
    )

    with concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_PARALLEL_TXT_FILES,
            mp_context=None  # use default 'spawn' on Windows, 'fork' on *nix
    ) as pool:
        for result in tqdm(pool.map(worker, txt_files),
                           total=len(txt_files),
                           desc="Files"):
            logger.info(result)

    logger.info("All files processed!")



if __name__ == "__main__":
    import time
    start = time.time()
    main()
    end = time.time()
    print(f"total time taken {end-start}")