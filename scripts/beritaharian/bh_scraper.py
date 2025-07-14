from ..business_times.bt_scraper import BT_Scraper, process_txt_async
from ...utils.logger import logger  # Ensure this logger is configured
import asyncio, json, pathlib, traceback
from tqdm.auto import tqdm


def main():
    BASE_DIR = pathlib.Path("/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/data/beritaharian")
    UNSEEN_DIR = BASE_DIR / "test"  # Original .txt files here
    SEEN_DIR = BASE_DIR / "seen"      # Processed .txt files moved here
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    # Ensure directories exist
    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    CONCURRENCY = 3  # Increased from 5 - URLs processed concurrently per file

    txt_files = list(UNSEEN_DIR.glob("*.txt"))

    if not txt_files:
        logger.warning(f"No .txt files found in {UNSEEN_DIR}")
        return

    logger.info(f"Found {len(txt_files)} files to process")

    # Process files one by one (you can modify this to process multiple files concurrently if needed)
    for txt_file in tqdm(txt_files, desc="Processing urls"):
        try:
            result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY, BT_Scraper))
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