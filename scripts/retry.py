#!/usr/bin/env python3

import os
import json
import shutil

# Configuration: set the directory and threshold here
ERROR_DIR = "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/business_times/unsuccessful"
THRESHOLD = 50
UNSEEN = "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/business_times/unseen"
SEEN = "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/business_times/seen"


def count_lists(obj):
    """
    Recursively count the number of list instances in a JSON object.
    """
    if isinstance(obj, list):
        return 1 + sum(count_lists(item) for item in obj)
    elif isinstance(obj, dict):
        return sum(count_lists(value) for value in obj.values())
    else:
        return 0


def process_file(filepath):
    """
    Read a JSONL file and count the total number of lists across all records.
    """
    total_lists = 0
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON in {filepath}: {e}")
                continue
            total_lists += count_lists(data)
    return total_lists


def main():
    # Use configured variables
    directory = ERROR_DIR
    threshold = THRESHOLD

    # Iterate through JSONL files in the error directory
    for filename in os.listdir(directory):
        if not filename.lower().endswith('.jsonl'):
            continue
        filepath = os.path.join(directory, filename)
        count = process_file(filepath)
        if count > threshold:
            # Derive the .txt filename and print it
            file_name = filename[:-13] + ".txt"
            print(file_name)
            
            # # Attempt to move the .txt from seen to unseen
            # src = os.path.join(SEEN, file_name)
            # dst = os.path.join(UNSEEN, file_name)
            # if os.path.exists(src):
            #     try:
            #         shutil.move(src, dst)
            #         print(f"Moved: {src} -> {dst}")
            #     except Exception as e:
            #         print(f"Error moving {file_name}: {e}")
            # else:
            #     print(f"File not found in seen directory: {src}")

            # # Remove the JSONL file after processing
            # try:
            #     os.remove(filepath)
            #     print(f"Removed JSONL file: {filepath}")
            # except Exception as e:
            #     print(f"Error removing JSONL file {filepath}: {e}")


if __name__ == "__main__":
    main()
