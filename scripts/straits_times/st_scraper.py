# st_scraper_cleanup.py
import aiofiles
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback
from tqdm.auto import tqdm
from playwright.async_api import async_playwright, Browser, BrowserContext
from ...utils.logger import logger          # make sure this logger is configured
import random, re, concurrent.futures, os, functools, gc

# ──────────────────────────────────────────────────────────────────────────────
#  ST_Scraper with automatic browser "showers"
# ──────────────────────────────────────────────────────────────────────────────
class ST_Scraper:
    def __init__(
        self,
        concurrency: int = 5,
        pages_before_restart: int = 1_500
    ):
        self.concurrency = concurrency
        self.pages_before_restart = pages_before_restart
        self._pages_processed = 0
        self.semaphore = asyncio.Semaphore(concurrency)
        self.browser: Optional[Browser] = None
        self.contexts: List[BrowserContext] = []
        self.context_semaphore = asyncio.Semaphore(concurrency)

    # ── async context management ────────────────────────────────────────────
    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        await self._launch_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._shutdown_browser()

    # ── browser lifecycle helpers ────────────────────────────────────────────
    async def _launch_browser(self):
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
            ],
        )
        logger.info("Browser launched")
        for _ in range(self.concurrency):
            ctx = await self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                ),
            )
            self.contexts.append(ctx)
        self._pages_processed = 0

    async def _shutdown_browser(self):
        for ctx in self.contexts:
            try:
                await ctx.close()
            except Exception:
                pass
        self.contexts.clear()
        if self.browser:
            await self.browser.close()
        self.browser = None
        gc.collect()
        logger.info("Browser shut down and memory GC’ed")

    async def _restart_browser(self):
        logger.warning(
            f"Restarting browser after {self._pages_processed} pages to release memory"
        )
        await self._shutdown_browser()
        await self._launch_browser()

    # ── helpers ──────────────────────────────────────────────────────────────
    def _clean_content(self, article: Tag) -> str:
        content = article.get_text(separator="\n\n", strip=True)
        if not content.strip():
            logger.warning("No text content extracted after cleaning.")
        return content

    def _extract_image_src(self, img_tag: Tag, page_url: str) -> Optional[str]:
        src = (
            img_tag.get("src")
            or img_tag.get("data-src")
            or img_tag.get("data-original")
        )
        return urljoin(page_url, src) if src else None

    async def _get_available_context(self) -> BrowserContext:
        async with self.context_semaphore:
            if not self.contexts:
                logger.error("No browser contexts available!")
                raise RuntimeError("No browser contexts available!")
            idx = len(self.contexts) - self.context_semaphore._value - 1
            return self.contexts[idx % len(self.contexts)]

    async def _fetch_page_content(self, url: str) -> Optional[BeautifulSoup]:
        if not self.browser:
            raise RuntimeError("Browser not started. Use 'async with'.")

        async with self.semaphore:
            await asyncio.sleep(random.uniform(0.05, 0.2))
            context = await self._get_available_context()
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_selector("article", timeout=30_000)
                html = await page.content()
                return BeautifulSoup(html, "html.parser")

            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                return None

            finally:
                try:
                    await page.close()
                except Exception:
                    pass

                self._pages_processed += 1
                if self._pages_processed >= self.pages_before_restart:
                    await self._restart_browser()

    # ── scraping logic (unchanged except docstrings trimmed) ────────────────
    async def scrape_single_url(self, url: str) -> Dict[str, Any]:
        try:
            for _ in range(2):  # retries
                soup = await self._fetch_page_content(url)
                if soup and soup.find("article"):
                    break

            article = soup.find("article") if soup else None
            if not article:
                raise RuntimeError("No <article> tag found")

            h1 = article.find("h1")
            title = h1.get_text(strip=True) if h1 else (
                soup.title.string.strip() if soup and soup.title else "(untitled)"
            )

            # publish date detection … (unchanged from your original)
            pub_date: Optional[str] = None
            time_tag = article.find("time")
            if time_tag and time_tag.has_attr("datetime"):
                try:
                    pub_date = dateparser.parse(time_tag["datetime"]).isoformat()
                except Exception:
                    pass
            elif time_tag:
                try:
                    pub_date = dateparser.parse(time_tag.get_text(strip=True)).isoformat()
                except Exception:
                    pass
            else:
                span = article.find("span", string=re.compile(r"\bPublished\b", re.I))
                if span:
                    cleaned = re.sub(r"^[Pp]ublished[:·\s]*", "", span.get_text(strip=True))
                    try:
                        pub_date = dateparser.parse(cleaned).isoformat()
                    except Exception:
                        pass
                else:
                    meta = soup.find("meta", {"property": "article:published_time"}) if soup else None
                    if meta and meta.has_attr("content"):
                        try:
                            pub_date = dateparser.parse(meta["content"]).isoformat()
                        except Exception:
                            pass

            content = self._clean_content(article)
            images: List[Dict[str, Any]] = []
            for picture in article.find_all("picture"):
                img_tag = picture.find("img")
                alt = img_tag.get("alt", "").strip() or None if img_tag else None
                for tag in picture.find_all(["img"]):
                    src = self._extract_image_src(tag, url)
                    if src:
                        images.append({"image_url": src, "alt_text": alt})

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
        tasks = [self.scrape_single_url(u) for u in urls]
        return await asyncio.gather(*tasks)

async def process_txt_async(
    txt_path: pathlib.Path,
    out_dir: pathlib.Path,
    err_dir: pathlib.Path,
    concurrency: int = 5,
    scraper_class: type=ST_Scraper,
    ensure_ascii: bool=True,
    pages_before_restart: int=1500
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

    async with scraper_class(concurrency=concurrency, pages_before_restart=pages_before_restart) as scraper, \
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

    CONCURRENCY_IN_FILE        = 10   # pages per file
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
