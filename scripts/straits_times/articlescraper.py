import re
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, concurrent.futures, json, pathlib, textwrap, traceback
from functools import partial
from tqdm.auto import tqdm
from openai import OpenAI
from ...utils.logger import logger  # Ensure this logger is configured


class ArticleScraper:
    """Scrapes a page for article content and generates summaries using an LLM"""

    def __init__(self, llm_endpoint: str = "http://localhost:8124/v1", model: str = "unsloth/Llama-3.2-3B-Instruct"):
        self.llm_client = OpenAI(base_url=llm_endpoint, api_key="no-api-key-required")
        self.model = model
        self.system_prompt = "You are a news summarization assistant. Provide a concise 100-word or less summary of the article content. Focus on key facts, events, and conclusions. Respond with the summary directly without saying anything else."

    def _clean_content(self, article: Tag) -> str:
        """Extract and clean text content from article tag"""
        # Remove unwanted elements
        for element in article.find_all(['script', 'style', 'footer', 'nav', 'aside', 'header', 'button', 'form', 'input']):
            element.decompose()

        content = article.get_text(separator="\n\n", strip=True)
        if not content.strip():
            logger.warning("No text content extracted after cleaning.")
        return content

    def _generate_summary(self, content: str) -> str:
        """Generate article summary using LLM"""
        try:
            response = self.llm_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": f"{self.system_prompt}\nArticle contents:\n{content}"}
                ],
                max_tokens=150,
                temperature=0.3
            )
            summary = response.choices[0].message.content.strip()
            logger.debug(f"Generated summary for content of length {len(content)}")
            return summary
        except Exception as e:
            logger.error(f"LLM summary generation error: {e}", exc_info=True)
            return f"Summary generation failed: {str(e)}"
        
    def _extract_image_src(self, img_tag: Tag, page_url: str) -> Optional[str]:
        """
        Resolve relative/absolute URLs for the given <img>.
        """
        src = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original")
        return urljoin(page_url, src) if src else None
    

    def scrape(self, url: str) -> Dict[str, Any]:
        """Scrape a single URL for article content, metadata, images, and generate summary."""
        logger.debug(f"Starting scrape for URL: {url}")
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            logger.info(f"Successfully fetched {url}")
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to fetch {url}: {e}")

        soup = BeautifulSoup(resp.content, "html.parser")
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
        summary = self._generate_summary(content)
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
            "article_url":    url,
            "site_title":     title,
            "publish_date":   pub_date,
            "summary":        summary,
            "truncated":      truncated,
            "images":         images,
        }


async def scrape_url(url: str, loop: asyncio.AbstractEventLoop, executor: concurrent.futures.Executor, scraper: ArticleScraper):
    """Asynchronous URL processing wrapper."""
    try:
        return await loop.run_in_executor(executor, scraper.scrape, url)
    except Exception as exc:
        logger.error(f"Error processing URL {url}: {exc}", exc_info=True)
        return ("ERROR", url, repr(exc), traceback.format_exc())


def process_txt(txt_path: pathlib.Path, out_dir: pathlib.Path, err_dir: pathlib.Path, url_parallel_workers: int):
    """Read URLs, scrape them concurrently, write .jsonl + error log."""
    year_month = txt_path.stem
    out_file = out_dir / f"{year_month}.jsonl"
    err_file = err_dir / f"{year_month}_errors.jsonl"

    logger.info(f"Processing {txt_path.name}...")

    try:
        urls = [ln.strip() for ln in txt_path.read_text().splitlines() if ln.strip()]
    except Exception as e:
        logger.error(f"Error reading {txt_path}: {e}", exc_info=True)
        return None

    if not urls:
        logger.warning(f"No URLs found in {txt_path.name}")
        return year_month

    # Create scraper instance for this worker
    scraper = ArticleScraper()

    # Create event loop for this worker
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # ThreadPool for blocking work
        with concurrent.futures.ThreadPoolExecutor(max_workers=url_parallel_workers) as tp:
            sem = asyncio.Semaphore(url_parallel_workers)

            async def worker(url):
                async with sem:
                    return await scrape_url(url, loop, tp, scraper)

            tasks = [worker(u) for u in urls]
            gathered = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    except Exception as e:
        logger.error(f"Exception during scraping URLs in {txt_path}: {e}", exc_info=True)
        return None
    finally:
        loop.close()

    # Process results
    success_count = 0
    error_count = 0

    try:
        with out_file.open("w", encoding="utf-8") as ok_f, err_file.open("w", encoding="utf-8") as er_f:
            for item in gathered:
                if isinstance(item, Exception):
                    logger.error(f"Unknown exception occurred: {item}")
                    json.dump({"url": "unknown", "error": repr(item), "traceback": traceback.format_exc()}, er_f)
                    er_f.write("\n")
                    error_count += 1
                elif isinstance(item, tuple) and item and item[0] == "ERROR":
                    _, url, msg, tb = item
                    logger.error(f"Error processing URL {url}: {msg}")
                    json.dump({"url": url, "error": msg, "traceback": tb}, er_f)
                    er_f.write("\n")
                    error_count += 1
                else:
                    logger.debug(f"Writing successful result for {item['article_url']}")
                    json.dump(item, ok_f, default=str)
                    ok_f.write("\n")
                    success_count += 1
    except Exception as e:
        logger.error(f"Error writing output files for {year_month}: {e}", exc_info=True)
        return None

    logger.info(f"Completed {txt_path.name}: {success_count} articles, {error_count} errors")
    return year_month


def main():
    BASE_DIR = pathlib.Path("/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/data/straitsTimes")
    UNSEEN_DIR = BASE_DIR/"test"  # Original .txt files here
    SEEN_DIR = BASE_DIR/"seen"      # Processed .txt files moved here
    OUT_DIR = BASE_DIR/"scraped"
    ERR_DIR = BASE_DIR/"unsuccessful"

    # Ensure directories exist
    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    MAX_FILES_PARALLEL = 2   # Number of txt files processed at once
    MAX_URLS_PARALLEL = 10   # Lowered for LLM concurrency control

    txt_files = list(UNSEEN_DIR.glob("*.txt"))

    if not txt_files:
        logger.warning(f"No .txt files found in {UNSEEN_DIR}")
        return

    logger.info(f"Found {len(txt_files)} files to process")

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_FILES_PARALLEL) as executor:
        with tqdm(total=len(txt_files), desc="Processing files") as pbar:
            futures = {
                executor.submit(process_txt, f, OUT_DIR, ERR_DIR, MAX_URLS_PARALLEL): f
                for f in txt_files
            }

            for future in concurrent.futures.as_completed(futures):
                file_path = futures[future]
                try:
                    result = future.result()
                    if result:
                        logger.info(f"Processed {file_path.name} with result {result}")
                    seen_path = SEEN_DIR / file_path.name
                    file_path.rename(seen_path)
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}", exc_info=True)
                finally:
                    pbar.update(1)

    logger.info("All files processed!")


if __name__ == "__main__":
    main()