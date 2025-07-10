import aiofiles                 # NEW  ──────── async file I/O
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback
from tqdm.auto import tqdm
from openai import OpenAI
from playwright.async_api import async_playwright, Browser, BrowserContext
from ...utils.logger import logger  # Ensure this logger is configured
import random


class AsyncScraper:
    """Scrapes articles using Playwright with batch processing and context reuse"""

    def __init__(
        self, 
        llm_endpoint: str = "http://localhost:8124/v1", 
        model: str = "unsloth/Llama-3.2-3B-Instruct",
        concurrency: int = 5
    ):
        self.llm_client = OpenAI(base_url=llm_endpoint, api_key="no-api-key-required")
        self.model = model
        self.system_prompt = "You are a news summarization assistant. Provide a concise 100-word or less summary of the article content. Focus on key facts, events, and conclusions. Respond with the summary directly without saying anything else."
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
        for element in article.find_all(['script', 'style', 'footer', 'nav', 'aside', 'header', 'button', 'form', 'input']):
            element.decompose()

        content = article.get_text(separator="\n\n", strip=True)
        if not content.strip():
            logger.warning("No text content extracted after cleaning.")
        return content

    async def _generate_summary(self, content: str) -> str:
        try:
            # Run the blocking LLM call in a separate thread
            response = await asyncio.to_thread(
                self.llm_client.chat.completions.create,
                model=self.model,
                messages=[
                    {"role": "user", "content": f"{self.system_prompt}\nArticle contents:\n{content}"}
                ],
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"LLM summary generation error: {e}", exc_info=True)
            return f"Summary generation failed: {str(e)}"
        
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
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)  # Reduced timeout
                await page.wait_for_selector("article", timeout=15000)
                # Removed image navigation clicking - just get the current page content
                
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
            max_retries = 2
            for attempt in range(max_retries):
                soup = await self._fetch_page_content(url)
                if soup and soup.find("article"):
                    break
                logger.info(f"Retrying {url} (attempt {attempt+2}/{max_retries+1})")
                
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
                meta = soup.find("meta", {"property": "article:published_time"})
                if meta and meta.has_attr("content"):
                    try:
                        pub_date = dateparser.parse(meta["content"]).isoformat()
                    except (ValueError, TypeError):
                        pass

            # Extract and clean content
            content = self._clean_content(article)

            # Truncate if too long
            truncated = False
            if len(content) > 12000:
                content = content[:12000]
                truncated = True
                logger.info(f"Content for {url} was truncated to 12k characters")

            # Generate summary
            summary = await self._generate_summary(content)
            if summary.startswith("Summary generation failed:"):
                logger.error(f"Summary generation failed for {url}: {summary}")

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
                "summary": summary,
                "truncated": truncated,
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

    async with AsyncScraper(concurrency=concurrency) as scraper, \
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
                await target_f.write(json.dumps(item, default=str) + "\n")
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


def main():
    BASE_DIR = pathlib.Path("/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/data/straitsTimes")
    UNSEEN_DIR = BASE_DIR / "unseen"  # Original .txt files here
    SEEN_DIR = BASE_DIR / "seen"      # Processed .txt files moved here
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    # Ensure directories exist
    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    CONCURRENCY = 100  # Increased from 5 - URLs processed concurrently per file

    txt_files = list(UNSEEN_DIR.glob("*.txt"))

    if not txt_files:
        logger.warning(f"No .txt files found in {UNSEEN_DIR}")
        return

    logger.info(f"Found {len(txt_files)} files to process")

    # Process files one by one (you can modify this to process multiple files concurrently if needed)
    for txt_file in tqdm(txt_files, desc="Processing urls"):
        try:
            result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY))
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