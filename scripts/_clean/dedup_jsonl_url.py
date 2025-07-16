import os
import json

def remove_duplicates_jsonl_by_url(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.jsonl'):
            filepath = os.path.join(directory, filename)
            seen_urls = set()
            unique_lines = []

            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        url = obj.get("article_url")
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            unique_lines.append(line)
                    except json.JSONDecodeError:
                        print(f"Skipping invalid JSON line in {filename}: {line}")

            # Write deduped data to new file
            new_filename = filename.replace('.jsonl', '_deduped.jsonl')
            new_filepath = os.path.join(directory, new_filename)
            with open(new_filepath, 'w', encoding='utf-8') as f_out:
                for unique_line in unique_lines:
                    f_out.write(unique_line + '\n')

            print(f'Processed {filename}: {len(unique_lines)} unique URLs written to {new_filename}')

# Example usage
directory_path = '/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/the_new_paper/lol'  # Replace with your folder path
remove_duplicates_jsonl_by_url(directory_path)
