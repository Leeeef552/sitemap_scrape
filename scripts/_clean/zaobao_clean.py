#!/usr/bin/env python3
import os
import re
import json
import glob

# === Configuration ===
# Update these paths directly to point at your input/output directories:
TEXT_INPUT_DIR   = "/path/to/your/input_txt_dir"
TEXT_OUTPUT_DIR  = "/path/to/your/output_txt_dir"
JSON_INPUT_DIR   = "/path/to/your/input_jsonl_dir"
JSON_OUTPUT_DIR  = "/path/to/your/output_jsonl_dir"
ERRORS_INPUT_DIR = "/path/to/your/input_errors_dir"
ERRORS_OUTPUT_DIR= "/path/to/your/output_errors_dir"

# Regex to extract the year from Zaobao story URLs
YEAR_RE = re.compile(r'story(\d{4})')


def ensure_dir(path):
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def group_text(input_dir, output_dir):
    """Read zb_*.txt files, bucket URLs by year into zb_YYYY.txt."""
    ensure_dir(output_dir)
    writers = {}
    pattern = os.path.join(input_dir, 'zb_*.txt')
    for filepath in glob.glob(pattern):
        with open(filepath, encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if not url:
                    continue
                m = YEAR_RE.search(url)
                if not m:
                    continue
                year = m.group(1)
                if year not in writers:
                    out_path = os.path.join(output_dir, f'zb_{year}.txt')
                    writers[year] = open(out_path, 'w', encoding='utf-8')
                writers[year].write(url + '\n')
    for w in writers.values():
        w.close()


def group_json(input_dir, output_dir, key, suffix):
    """
    Generic JSONL grouper:
      - key: 'article_url' for article files or '__ERROR__' for error lists
      - suffix: '.jsonl' or '_errors.jsonl'
    """
    ensure_dir(output_dir)
    writers = {}
    pattern = os.path.join(input_dir, f'zb_*{suffix}')

    for filepath in glob.glob(pattern):
        with open(filepath, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if key == '__ERROR__':
                    # obj is a list, URL at index 1
                    if not (isinstance(obj, list) and len(obj) > 1):
                        continue
                    url = obj[1]
                else:
                    url = obj.get(key, '')

                m = YEAR_RE.search(url)
                if not m:
                    continue
                year = m.group(1)
                if year not in writers:
                    out_path = os.path.join(output_dir, f'zb_{year}{suffix}')
                    writers[year] = open(out_path, 'w', encoding='utf-8')
                writers[year].write(json.dumps(obj, ensure_ascii=False) + '\n')
    for w in writers.values():
        w.close()


def main():
    # # Group text URLs by year
    # group_text(
    #     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao_temp/seen",
    #     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao_temp/seen_out"
    # )
    # # Group JSON articles by year
    # group_json(
    #     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao_temp/scraped",
    #     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao_temp/scraped_out",
    #     key='article_url',
    #     suffix='.jsonl'
    # )
    # # Group error entries by year
    # group_json(
    #     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao_temp/unsuccessful",
    #     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao_temp/unsuccessful_out",
    #     key='__ERROR__',
    #     suffix='_errors.jsonl'
    # )
    # print("Bucketing by year complete.")


if __name__ == '__main__':
    main()
