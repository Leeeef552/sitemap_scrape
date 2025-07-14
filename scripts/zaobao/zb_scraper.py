from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
import asyncio, json, pathlib, traceback, random, re
from tqdm.auto import tqdm
from playwright.async_api import async_playwright, Browser, BrowserContext
from ...utils.logger import logger
from ..straits_times.st_scraper import ST_Scraper, process_txt_async

class ZB_Scraper(ST_Scraper):

    def _extract_images(self, container: Tag, page_url: str) -> List[Dict[str, Any]]:
        """
        1) Grab exactly one hero <img> from any button containing w-full class
        2) Grab all <img> inside the div.articleBody
        3) Dedupe by URL and skip any data: URIs
        """
        images: List[Dict[str, Any]] = []
        seen_srcs = set()

        # DEBUG: Print container info
        print(f"=== DEBUG: Container type: {type(container)}, exists: {container is not None}")
        
        if not container:
            print("DEBUG: No container found, returning empty images")
            return images

        # DEBUG: Print all buttons found
        all_buttons = container.find_all("button")
        print(f"DEBUG: Found {len(all_buttons)} buttons total")
        
        for i, btn in enumerate(all_buttons):
            classes = btn.get("class", [])
            print(f"DEBUG: Button {i} classes: {classes}")
            
            # Check for images in this button
            imgs_in_btn = btn.find_all("img")
            print(f"DEBUG: Button {i} has {len(imgs_in_btn)} images")
            
            for j, img in enumerate(imgs_in_btn):
                src = img.get("src")
                print(f"DEBUG: Button {i}, Image {j} src: {src}")

        # DEBUG: Print all images found anywhere in container
        all_images = container.find_all("img")
        print(f"DEBUG: Found {len(all_images)} images total in container")
        
        for i, img in enumerate(all_images):
            src = img.get("src")
            parent_tag = img.parent.name if img.parent else "None"
            parent_classes = img.parent.get("class", []) if img.parent else []
            print(f"DEBUG: Image {i} src: {src}, parent: {parent_tag}, parent_classes: {parent_classes}")

        # --- 1) Hero image - try multiple approaches ---
        hero_found = False
        
        # Approach 1: Find button with w-full class directly
        btn = container.find("button", class_=lambda c: c and "w-full" in c.split())
        print(f"DEBUG: Found w-full button: {btn is not None}")
        
        if btn:
            img = btn.find("img", src=True)
            print(f"DEBUG: Found img in w-full button: {img is not None}")
            if img:
                src = urljoin(page_url, img["src"])
                print(f"DEBUG: Hero image src: {src}")
                if not src.startswith("data:") and src not in seen_srcs:
                    seen_srcs.add(src)
                    images.append({
                        "image_url": src,
                        "alt_text": img.get("alt", "").strip() or None,
                        "caption": None
                    })
                    hero_found = True
                    print("DEBUG: Added hero image!")
        
        # Approach 2: If not found, try all buttons
        if not hero_found:
            print("DEBUG: Trying all buttons approach")
            for i, btn in enumerate(all_buttons):
                img = btn.find("img", src=True)
                if img:
                    src = urljoin(page_url, img["src"])
                    print(f"DEBUG: Found image in button {i}: {src}")
                    if not src.startswith("data:") and src not in seen_srcs:
                        seen_srcs.add(src)
                        images.append({
                            "image_url": src,
                            "alt_text": img.get("alt", "").strip() or None,
                            "caption": None
                        })
                        hero_found = True
                        print("DEBUG: Added hero image from button scan!")
                        break  # Take only the first one found
        
        # Approach 3: If still not found, try finding any img in header-like divs
        if not hero_found:
            print("DEBUG: Trying header divs approach")
            header_divs = container.find_all("div", class_=lambda c: c and any(cls in c for cls in ["header", "hero", "banner", "flex-col"]))
            print(f"DEBUG: Found {len(header_divs)} header-like divs")
            
            for i, header_div in enumerate(header_divs):
                img = header_div.find("img", src=True)
                if img:
                    src = urljoin(page_url, img["src"])
                    print(f"DEBUG: Found image in header div {i}: {src}")
                    if not src.startswith("data:") and src not in seen_srcs:
                        seen_srcs.add(src)
                        images.append({
                            "image_url": src,
                            "alt_text": img.get("alt", "").strip() or None,
                            "caption": None
                        })
                        print("DEBUG: Added hero image from header div!")
                        break  # Take only the first one found

        # --- 2) In-article images ---
        body = container.find("div", class_=lambda c: c and "articleBody" in c)
        print(f"DEBUG: Found articleBody: {body is not None}")
        
        if body:
            body_images = body.find_all("img", src=True)
            print(f"DEBUG: Found {len(body_images)} images in articleBody")
            
            for i, img in enumerate(body_images):
                src = urljoin(page_url, img["src"])
                print(f"DEBUG: ArticleBody image {i}: {src}")
                
                # skip icons, duplicates, data URIs
                if src.startswith("data:") or src in seen_srcs:
                    print(f"DEBUG: Skipping image {i} (data URI or duplicate)")
                    continue

                seen_srcs.add(src)
                # optional figcaption if you ever wrap it
                caption = None
                if img.parent and img.parent.name.lower() == "figure":
                    figcap = img.parent.find("figcaption")
                    if figcap:
                        caption = figcap.get_text(strip=True)

                images.append({
                    "image_url": src,
                    "alt_text": img.get("alt", "").strip() or None,
                    "caption": caption,
                })
                print(f"DEBUG: Added articleBody image {i}!")

        print(f"DEBUG: Final images count: {len(images)}")
        return images


def main():
    BASE_DIR = pathlib.Path("/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao")
    UNSEEN_DIR = BASE_DIR / "test"  # Original .txt files here
    SEEN_DIR = BASE_DIR / "seen"      # Processed .txt files moved here
    OUT_DIR = BASE_DIR / "scraped"
    ERR_DIR = BASE_DIR / "unsuccessful"

    # Ensure directories exist
    UNSEEN_DIR.mkdir(exist_ok=True, parents=True)
    SEEN_DIR.mkdir(exist_ok=True, parents=True)
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    ERR_DIR.mkdir(exist_ok=True, parents=True)

    CONCURRENCY = 5  # Increased from 5 - URLs processed concurrently per file

    txt_files = list(UNSEEN_DIR.glob("*.txt"))
    print(len(txt_files))

    if not txt_files:
        logger.warning(f"No .txt files found in {UNSEEN_DIR}")
        return

    logger.info(f"Found {len(txt_files)} files to process")

    # Process files one by one (you can modify this to process multiple files concurrently if needed)
    for txt_file in tqdm(txt_files, desc="Processing urls"):
        try:
            result = asyncio.run(process_txt_async(txt_file, OUT_DIR, ERR_DIR, CONCURRENCY, ZB_Scraper, False))
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