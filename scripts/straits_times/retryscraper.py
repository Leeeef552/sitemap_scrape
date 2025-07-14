from typing import Any, List
import asyncio, json, pathlib
from tqdm.auto import tqdm
from ...utils.logger import logger 
from .st_scraper import AsyncScraper


class RetryScraper:
    """Retries previously failed scrapes from error .jsonl files."""

    def __init__(self, err_dir: pathlib.Path, out_dir: pathlib.Path, concurrency: int = 1):
        self.err_dir = err_dir
        self.out_dir = out_dir
        self.concurrency = concurrency

    def load_error_file(self, file_path: pathlib.Path) -> List[List[Any]]:
        """Load failed entries from a JSONL file."""
        lines = []
        try:
            with file_path.open("r", encoding="utf-8") as f:
                for line in f:
                    try:
                        parsed = json.loads(line)
                        if isinstance(parsed, list) and len(parsed) > 1:
                            lines.append(parsed)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON line in {file_path.name}: {line[:100]}")
        except Exception as e:
            logger.error(f"Failed to read error file {file_path.name}: {e}")
        return lines

    def save_success(self, result: dict, out_file: pathlib.Path):
        """Append successful scrape to the correct output JSONL file."""
        with out_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(result, default=str) + "\n")
            f.flush()

    def overwrite_errors(self, err_file: pathlib.Path, remaining_lines: List[List[Any]]):
        """Write back only the remaining failed lines."""
        with err_file.open("w", encoding="utf-8") as f:
            for line in remaining_lines:
                f.write(json.dumps(line, default=str) + "\n")
            f.flush()

    async def retry_file(self, err_file: pathlib.Path):
        """Retry scraping all failed lines in a given error file."""
        logger.info(f"Retrying file: {err_file.name}")
        lines = self.load_error_file(err_file)
        if not lines:
            logger.info(f"No valid retry entries in {err_file.name}")
            return

        year_month = err_file.stem.replace("_errors", "")
        out_file = self.out_dir / f"{year_month}.jsonl"

        async with AsyncScraper(concurrency=self.concurrency) as scraper:
            remaining = []
            for entry in tqdm(lines, desc=f"Retrying {err_file.name}", leave=False):
                url = entry[1]
                try:
                    result = await scraper.scrape_single_url(url)
                    if isinstance(result, dict) and "article_url" in result:
                        self.save_success(result, out_file)
                        logger.info(f"Successfully re-scraped: {url}")
                    else:
                        logger.warning(f"Failed again: {url}")
                        remaining.append(entry)
                except Exception as e:
                    logger.error(f"Retry scrape error for {url}: {e}")
                    remaining.append(entry)

        # Overwrite error file with remaining failed lines
        self.overwrite_errors(err_file, remaining)

    def retry_all(self):
        """Retry all error files in the directory."""
        err_files = list(self.err_dir.glob("*_errors.jsonl"))
        if not err_files:
            logger.info("No error files to retry.")
            return

        for err_file in tqdm(err_files, desc="Retrying failed scrapes"):
            try:
                asyncio.run(self.retry_file(err_file))
            except Exception as e:
                logger.error(f"Error retrying {err_file.name}: {e}", exc_info=True)

def main():
    BASE_DIR = pathlib.Path("/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/temp")
    ERR_DIR = BASE_DIR / "unsuccessful"
    OUT_DIR = BASE_DIR / "scraped"

    retry_scraper = RetryScraper(err_dir=ERR_DIR, out_dir=OUT_DIR)
    retry_scraper.retry_all()

if __name__ == "__main__":
    main()