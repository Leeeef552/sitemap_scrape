import os
from collections import defaultdict
import math


def merge_txt_by_year(input_dir, output_dir=None):
    if output_dir is None:
        output_dir = input_dir

    files_by_group = defaultdict(list)

    for fname in os.listdir(input_dir):
        if fname.endswith(".txt") and "_" in fname:
            parts = fname.split("_")
            if len(parts) == 3:  # e.g. bt_2021_01.txt
                prefix = parts[0]
                year = parts[1]
                key = f"{prefix}_{year}"
                files_by_group[key].append(fname)

    for key, files in files_by_group.items():
        files.sort()
        output_path = os.path.join(output_dir, f"{key}.txt")
        with open(output_path, 'w', encoding='utf-8') as fout:
            for fname in files:
                with open(os.path.join(input_dir, fname), 'r', encoding='utf-8') as fin:
                    contents = fin.read()
                    fout.write(contents)
                    if not contents.endswith("\n"):
                        fout.write("\n")
        print(f"Merged {len(files)} files into {output_path}")


def merge_and_shard_jsonl_by_year(input_dir, output_dir=None, shards=4):
    if output_dir is None:
        output_dir = input_dir

    files_by_group = defaultdict(list)

    for fname in os.listdir(input_dir):
        if fname.endswith(".jsonl") and "_" in fname:
            parts = fname.split("_")
            if len(parts) >= 3:
                prefix = parts[0]
                year = parts[1]
                key = f"{prefix}_{year}"
                files_by_group[key].append(fname)

    for key, files in files_by_group.items():
        files.sort()
        all_lines = []
        for fname in files:
            with open(os.path.join(input_dir, fname), 'r', encoding='utf-8') as fin:
                for line in fin:
                    if line.strip():
                        all_lines.append(line if line.endswith('\n') else line + '\n')

        total_lines = len(all_lines)
        print(f"Merging {len(files)} files for {key} → {total_lines} lines")

        if shards == 1:
            output_path = os.path.join(output_dir, f"{key}.jsonl")
            with open(output_path, 'w', encoding='utf-8') as fout:
                fout.writelines(all_lines)
            print(f"  → Wrote to {output_path}")
        else:
            lines_per_shard = math.ceil(total_lines / shards)
            for i in range(shards):
                shard_lines = all_lines[i * lines_per_shard : (i + 1) * lines_per_shard]
                output_path = os.path.join(output_dir, f"{key}_part{i+1}.jsonl")
                with open(output_path, 'w', encoding='utf-8') as fout:
                    fout.writelines(shard_lines)
                print(f"  → Wrote {len(shard_lines)} lines to {output_path}")


def merge_errors_jsonl_by_year(input_dir, output_dir=None):
    if output_dir is None:
        output_dir = input_dir

    files_by_group = defaultdict(list)

    for fname in os.listdir(input_dir):
        if fname.endswith("_errors.jsonl") and "_" in fname:
            parts = fname.split("_")
            if len(parts) >= 3:
                prefix = parts[0]
                year = parts[1]
                key = f"{prefix}_{year}"
                files_by_group[key].append(fname)

    for key, files in files_by_group.items():
        files.sort()
        output_path = os.path.join(output_dir, f"{key}_errors.jsonl")
        with open(output_path, 'w', encoding='utf-8') as fout:
            for fname in files:
                with open(os.path.join(input_dir, fname), 'r', encoding='utf-8') as fin:
                    for line in fin:
                        fout.write(line if line.endswith('\n') else line + '\n')
        print(f"Merged {len(files)} files into {output_path}")


def shard_all_jsonl_files_in_dir(input_dir, output_dir=None, shards=4):
    if output_dir is None:
        output_dir = input_dir

    for fname in os.listdir(input_dir):
        # Only process files like "prefix_year.jsonl"
        if (
            fname.endswith(".jsonl")
            and "_part" not in fname
            and not fname.endswith("_errors.jsonl")
        ):
            # Check pattern: prefix_year.jsonl (two parts before extension)
            parts = fname.split("_")
            if len(parts) != 2 or not parts[1].endswith(".jsonl"):
                print(f"Skipping {fname} (does not match prefix_year.jsonl)")
                continue

            file_path = os.path.join(input_dir, fname)
            base_name = fname[:-6]  # Remove ".jsonl", keep "prefix_year"

            with open(file_path, 'r', encoding='utf-8') as fin:
                lines = [line if line.endswith('\n') else line + '\n' for line in fin if line.strip()]

            total_lines = len(lines)
            if total_lines == 0:
                print(f"Skipping empty file: {fname}")
                continue

            lines_per_shard = math.ceil(total_lines / shards)
            for i in range(shards):
                shard_lines = lines[i * lines_per_shard: (i + 1) * lines_per_shard]
                shard_fname = f"{base_name}_part{i+1}.jsonl"
                shard_path = os.path.join(output_dir, shard_fname)
                with open(shard_path, 'w', encoding='utf-8') as fout:
                    fout.writelines(shard_lines)
                print(f"Sharded {len(shard_lines)} lines → {shard_path}")


# merge_txt_by_year(
#     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/straits_times/seen",
#     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/straits_times/temp1"
# )


# merge_and_shard_jsonl_by_year(
#     "/workspace/eefun/webscraping/sitemap/backups/data_backup/zaobao/temp1",
#     "/workspace/eefun/webscraping/sitemap/backups/data_backup/zaobao/temp2",
#     shards=4
# )

# merge_errors_jsonl_by_year(
#     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/straits_times/unsuccessful",
#     "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/straits_times/temp"
# )

shard_all_jsonl_files_in_dir(
    "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao/scraped",
    "/workspace/eefun/webscraping/sitemap/sitemap_scrape/data/zaobao/scraped/temp",
    shards=4
)