import os
import json
from tabulate import tabulate
import glob
import re

def read_jsonl_urls(jsonl_files):
    urls = []
    for path in jsonl_files:
        if not os.path.exists(path):
            continue
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line.strip())
                    url = obj.get("article_url")
                    if url:
                        urls.append(url)
                except json.JSONDecodeError:
                    continue
    return urls

def read_error_urls(error_files):
    urls = []
    for path in error_files:
        if not os.path.exists(path):
            continue
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    lst = json.loads(line.strip())
                    if isinstance(lst, list) and len(lst) > 1:
                        urls.append(lst[1])
                except json.JSONDecodeError:
                    continue
    return urls

def extract_year(base):
    match = re.search(r'_(\d{4})$', base)
    if match:
        return int(match.group(1))
    return 0  # Put bases without a year at the start

def compare_txt_jsonl(txt_dir, jsonl_dir, errors_dir):
    txt_files = [f for f in os.listdir(txt_dir) if f.endswith('.txt')]
    summary = []

    for txt_file in txt_files:
        base = txt_file[:-4]  # remove '.txt'

        # Sharded and non-sharded jsonl
        jsonl_pattern = os.path.join(jsonl_dir, f"{base}*.jsonl")
        jsonl_files = [
            f for f in glob.glob(jsonl_pattern)
            if not f.endswith('_errors.jsonl')
        ]

        # Sharded and non-sharded errors
        error_pattern = os.path.join(errors_dir, f"{base}*_errors.jsonl")
        error_files = glob.glob(error_pattern)

        txt_path = os.path.join(txt_dir, txt_file)
        with open(txt_path, 'r', encoding='utf-8') as f_txt:
            txt_urls = [line.strip() for line in f_txt if line.strip()]
        txt_set = set(txt_urls)
        txt_duplicates = len(txt_urls) - len(txt_set)

        # Aggregate URLs from all jsonl files (shard + non-shard)
        jsonl_urls = read_jsonl_urls(jsonl_files)
        jsonl_set = set(jsonl_urls)
        jsonl_duplicates = len(jsonl_urls) - len(jsonl_set)

        # Aggregate URLs from all error files (shard + non-shard)
        error_urls = read_error_urls(error_files)
        error_set = set(error_urls)
        error_duplicates = len(error_urls) - len(error_set)

        # Calculate missing
        accounted_for = jsonl_set.union(error_set)
        unaccounted = txt_set - accounted_for

        year = extract_year(base)
        summary.append([
            year, base,
            len(txt_set), len(jsonl_urls), len(error_urls),
            len(unaccounted),
            txt_duplicates, jsonl_duplicates, error_duplicates
        ])

    # Sort by year
    summary.sort(key=lambda x: x[0])

    headers = [
        "Year", "File Base", "TXT URLs", "Scraped URLs", "Error URLs",
        "Unaccounted URLs", "TXT Duplicates", "Scraped Duplicates", "Error Duplicates"
    ]

    print("\n" + tabulate(summary, headers=headers, tablefmt="grid"))

# Example usage

outlet = "straits_times"

compare_txt_jsonl(
    txt_dir=f'/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/{outlet}/seen',
    jsonl_dir=f'/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/{outlet}/scraped',
    errors_dir=f'/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/{outlet}/unsuccessful'
)