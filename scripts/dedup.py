#!/usr/bin/env python3
import os
import glob
import json
import re

# ─── CONFIGURE HERE ─────────────────────────────────────────────────────────────
# Directory containing your .jsonl files:
INPUT_DIR = "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/temp"

# Output directory for cleaned files.
# If you want to overwrite originals, set this to None.
OUTPUT_DIR ="/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/temp/cleaned" # e.g. "/path/to/cleaned_jsonl_directory"

# The regex pattern to match unwanted image URLs:
# any URL containing "cassette.sphdigital"
PATTERN = re.compile(r"cassette\.sphdigital")
# ────────────────────────────────────────────────────────────────────────────────

def clean_images(input_dir, output_dir=None):
    in_place = (output_dir is None)
    if not in_place:
        os.makedirs(output_dir, exist_ok=True)

    for filepath in glob.glob(os.path.join(input_dir, "*.jsonl")):
        out_path = filepath if in_place else os.path.join(output_dir, os.path.basename(filepath))
        with open(filepath, 'r', encoding='utf-8') as infile, \
             open(out_path, 'w', encoding='utf-8') as outfile:

            for line_num, line in enumerate(infile, start=1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"⚠️ Skipping invalid JSON in {os.path.basename(filepath)} at line {line_num}: {e}")
                    continue

                images = record.get("images", [])
                filtered = []
                for img in images:
                    url = img.get("image_url", "")
                    if PATTERN.search(url):
                        # matched the unwanted pattern → drop it
                        print(f"Removed   : {url} (file: {os.path.basename(filepath)}, line {line_num})")
                    else:
                        filtered.append(img)

                record["images"] = filtered
                outfile.write(json.dumps(record, ensure_ascii=False) + "\n")

    print("\n✅ Done. Processed files in:", input_dir)
    if not in_place:
        print("✅ Cleaned files written to:", output_dir)

if __name__ == "__main__":
    clean_images(INPUT_DIR, OUTPUT_DIR)
