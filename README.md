# sitemap_scrape
scraping sitemap news for htx

## Has Sitemaps (so can get archives)
1. straits times        (DONE)
2. business times       (DONE)
3. Zaobao               (DONE)
4. Berita Harian        (havent started / no subscription gives very little text)  
5. Tamil Murasu         (rate limited)
6. The New Paper        (in progress)
7. tabla                (havent started)

## No Sitemap and uses Google News Api - (limit to last 1000 articles or last 2 days) --> live scraper / use browser to navigate site for archives
1. CNA                  (cannot find archives)
2. Today Online         (cannot find archives)
3. shinmin              (cannot find archives)

## No Sitemap and uses Google News (limited news) --> or use click button to navigate
1. CNA (https://www.channelnewsasia.com/api/v1/sitemap-news-feed?_format=xml)
2. Today Online (https://www.channelnewsasia.com/api/v1/sitemap-news-feed?_format=xml)
3. shinmin (https://www.shinmin.sg/sitemap.xml) has a sitemap but kinda sus since a lot on horseracingsn


<!-- # VLLM run command
```bash
CUDA_VISIBLE_DEVICES=0 vllm serve unsloth/Llama-3.2-3B-Instruct --port 8124 --gpu-memory-utilization 0.65 --chat-template /home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/utils/chat_template.jinja
``` -->