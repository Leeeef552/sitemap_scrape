# sitemap_scrape
scraping sitemap news for htx

## Has Sitemaps (so can get archives)
1. straits times
2. business times
3. Zaobao --> but some access denied
4. Berita Harian
5. Tamil Murasu
6. The New Paper (TNP)
7. tabla (only until 2022)

## No Sitemap and uses Google News (limited news) --> or use click button to navigate
1. CNA (https://www.channelnewsasia.com/api/v1/sitemap-news-feed?_format=xml)
2. Today Online (https://www.channelnewsasia.com/api/v1/sitemap-news-feed?_format=xml)
3. shinmin (https://www.shinmin.sg/sitemap.xml) has a sitemap but kinda sus since a lot on horseracing

# VLLM run command
```bash
CUDA_VISIBLE_DEVICES=0 vllm serve unsloth/Llama-3.2-3B-Instruct --port 8124 --gpu-memory-utilization 0.65 --chat-template /home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/utils/chat_template.jinja
```