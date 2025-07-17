# Sitemap Scraper for Historical News Articles

A project for scraping historical news articles from Singaporean publications using sitemaps and APIs.

---

## 🗺️ Sitemap Status

### ✅ Publications with Sitemaps (Archive Access)
1. [Straits Times](https://www.straitstimes.com ) ✔️ *(Completed)*  
2. [Business Times](https://www.businesstimes.com.sg ) ✔️ *(Completed)*  
3. [Zaobao](https://www.zaobao.com.sg ) ✔️ *(Completed)*  
4. [The New Paper](https://www.tnp.sg ) ✔️ *(Completed)*  
5. [Tamil Murasu](https://www.tamilmurasu.org ) ⚠️ *(Rate-limited)*  
6. [Berita Harian](https://www.beritaharian.sg ) ⚠️ *(Limited access without subscription)*  
7. [Tabla](https://tabla.sg ) ⏳ *(In progress)*  

### ⚠️ Publications Without Sitemaps
These use Google News API with limitations:
- **CNA** (Last 1000 articles or 2 days)
- **Today Online** (Last 1000 articles or 2 days)
- **Shinmin** (Last 1000 articles or 2 days)

*Suspected sitemap: [Shinmin Sitemap](https://www.shinmin.sg/sitemap.xml ) (mostly horseracing content)*

---

## 📊 Scraping Statistics

### Straits Times (2015–2025)
| Year | File Base | TXT URLs | Scraped URLs | Error URLs | Unaccounted URLs | TXT Duplicates | Scraped Duplicates | Error Duplicates |
|------|-----------|----------|--------------|------------|------------------|----------------|--------------------|------------------|
| 2015 | st_2015   | 54,440   | 54,339       | 101        | 0                | 0              | 0                  | 0                |
| 2016 | st_2016   | 54,477   | 54,370       | 107        | 0                | 0              | 0                  | 0                |
| 2017 | st_2017   | 49,618   | 49,469       | 150        | 0                | 1              | 1                  | 0                |
| 2018 | st_2018   | 54,393   | 54,306       | 87         | 0                | 0              | 0                  | 0                |
| 2019 | st_2019   | 55,186   | 55,139       | 51         | 0                | 0              | 0                  | 4                |
| 2020 | st_2020   | 59,999   | 59,939       | 61         | 0                | 1              | 1                  | 0                |
| 2021 | st_2021   | 60,000   | 59,981       | 19         | 0                | 0              | 0                  | 0                |
| 2022 | st_2022   | 54,075   | 54,032       | 49         | 0                | 3              | 3                  | 3                |
| 2023 | st_2023   | 54,602   | 54,379       | 223        | 0                | 0              | 0                  | 0                |
| 2024 | st_2024   | 59,991   | 59,803       | 197        | 0                | 9              | 9                  | 0                |
| 2025 | st_2025   | 29,979   | 29,891       | 109        | 0                | 21             | 21                 | 0                |

*Total: 583,759 articles | Success Rate: 99.75%*

### Business Times (2013–2025)
| Year | File Base | TXT URLs | Scraped URLs | Error URLs | Unaccounted URLs | TXT Duplicates | Scraped Duplicates | Error Duplicates |
|------|-----------|----------|--------------|------------|------------------|----------------|--------------------|------------------|
| 2013 | bt_2013   | 2,413    | 2,406        | 7          | 0                | 0              | 0                  | 0                |
| 2014 | bt_2014   | 17,683   | 17,666       | 17         | 0                | 0              | 0                  | 0                |
| 2015 | bt_2015   | 49,282   | 49,255       | 27         | 0                | 0              | 0                  | 0                |
| 2016 | bt_2016   | 50,469   | 50,448       | 25         | 0                | 0              | 0                  | 0                |
| 2017 | bt_2017   | 45,575   | 45,557       | 18         | 0                | 0              | 0                  | 0                |
| 2018 | bt_2018   | 41,615   | 41,536       | 85         | 0                | 0              | 0                  | 0                |
| 2019 | bt_2019   | 41,590   | 41,577       | 13         | 0                | 0              | 0                  | 0                |
| 2020 | bt_2020   | 43,252   | 43,237       | 15         | 0                | 0              | 0                  | 0                |
| 2021 | bt_2021   | 35,161   | 35,105       | 56         | 0                | 0              | 0                  | 0                |
| 2022 | bt_2022   | 34,386   | 34,365       | 21         | 0                | 0              | 0                  | 0                |
| 2023 | bt_2023   | 31,568   | 31,541       | 27         | 0                | 0              | 0                  | 0                |
| 2024 | bt_2024   | 30,156   | 30,116       | 42         | 0                | 2              | 2                  | 0                |
| 2025 | bt_2025   | 11,855   | 11,839       | 16         | 0                | 0              | 0                  | 0                |

*Total: 394,945 articles | Success Rate: 99.9%*

### The New Paper (2016–2025)
| Year | File Base | TXT URLs | Scraped URLs | Error URLs | Unaccounted URLs | TXT Duplicates | Scraped Duplicates | Error Duplicates |
|------|-----------|----------|--------------|------------|------------------|----------------|--------------------|------------------|
| 2016 | tnp_2016  | 6,305    | 6,305        | 0          | 0                | 0              | 0                  | 0                |
| 2017 | tnp_2017  | 3,524    | 3,519        | 5          | 0                | 0              | 0                  | 0                |
| 2018 | tnp_2018  | 13,630   | 13,616       | 14         | 0                | 0              | 0                  | 0                |
| 2019 | tnp_2019  | 12,267   | 12,185       | 82         | 0                | 0              | 0                  | 0                |
| 2020 | tnp_2020  | 10,798   | 10,760       | 38         | 0                | 1              | 0                  | 0                |
| 2021 | tnp_2021  | 10,016   | 9,998        | 18         | 0                | 0              | 0                  | 0                |
| 2022 | tnp_2022  | 10,634   | 10,576       | 58         | 0                | 0              | 0                  | 0                |
| 2023 | tnp_2023  | 8,442    | 8,433        | 9          | 0                | 0              | 0                  | 0                |
| 2024 | tnp_2024  | 6,782    | 6,780        | 2          | 0                | 0              | 0                  | 0                |
| 2025 | tnp_2025  | 2,986    | 2,985        | 1          | 0                | 0              | 0                  | 0                |

*Total: 74,384 articles | Success Rate: 99.7%*

### Zaobao (2016–2025)
| Year | File Base | TXT URLs | Scraped URLs | Error URLs | Unaccounted URLs | TXT Duplicates | Scraped Duplicates | Error Duplicates |
|------|-----------|----------|--------------|------------|------------------|----------------|--------------------|------------------|
| 2016 | zb_2016   | 71,911   | 71,909       | 2          | 0                | 0              | 0                  | 0                |
| 2017 | zb_2017   | 84,977   | 84,976       | 1          | 0                | 0              | 0                  | 0                |
| 2018 | zb_2018   | 86,690   | 86,651       | 39         | 0                | 0              | 0                  | 0                |
| 2019 | zb_2019   | 86,777   | 86,682       | 95         | 0                | 0              | 0                  | 0                |
| 2020 | zb_2020   | 86,669   | 86,478       | 191        | 0                | 0              | 0                  | 0                |
| 2021 | zb_2021   | 81,534   | 81,549       | 42         | 0                | 57             | 57                 | 0                |
| 2022 | zb_2022   | 80,396   | 80,284       | 112        | 0                | 0              | 0                  | 0                |
| 2023 | zb_2023   | 76,279   | 74,087       | 2,192      | 0                | 0              | 0                  | 0                |
| 2024 | zb_2024   | 74,665   | 74,025       | 640        | 0                | 0              | 0                  | 0                |
| 2025 | zb_2025   | 43,250   | 43,192       | 58         | 0                | 0              | 0                  | 0                |

*Note: 2021 shows >100% due to duplicate resolution  
Total: 773,348 articles | Success Rate: 99.3%*

### Tabla (2023–2025)
| Year | File Base   | TXT URLs | Scraped URLs | Error URLs | Unaccounted URLs | TXT Duplicates | Scraped Duplicates | Error Duplicates |
|------|------------|----------|--------------|------------|------------------|----------------|--------------------|------------------|
| 2023 | tabla_2023 | 623      | 622          | 5          | 0                | 0              | 0                  | 1                |
| 2024 | tabla_2024 | 772      | 772          | 3          | 0                | 0              | 0                  | 0                |
| 2025 | tabla_2025 | 572      | 572          | 0          | 0                | 0              | 0                  | 0                |

*Total: 1,967 articles | Success Rate: 99.95%*

---

## ⚙️ Technical Details
### VLLM Server Command
```bash
CUDA_VISIBLE_DEVICES=0 vllm serve unsloth/Llama-3.2-3B-Instruct \
  --port 8124 \
  --gpu-memory-utilization 0.65 \
  --chat-template /path/to/chat_template.jinja