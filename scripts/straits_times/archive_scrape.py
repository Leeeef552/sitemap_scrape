import re
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser
from typing import Any, Dict, List, Optional
from ...utils.logger import logger

    

class ArticleScraper:
    def _extract_image_src(self, img_tag: Tag, page_url: str) -> Optional[str]:
        # extract by src tag
        src = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original")
        return urljoin(page_url, src) if src else None

    def _extract_caption(self, img_tag: Tag) -> Optional[str]:
        #Return the best-guess caption for `img_tag`.

        # 1. Ideal case: <figure> → <figcaption>
        fig = img_tag.find_parent("figure")
        if fig:
            figcap = fig.find("figcaption")
            if figcap:
                text = figcap.get_text(" ", strip=True)
                if text:
                    return text
        # 2. Immediate siblings (<figcaption>, <p>, <span>)
        for sib in (img_tag.find_next_sibling(), img_tag.find_previous_sibling()):
            if sib and isinstance(sib, Tag) and sib.name in {"figcaption", "p", "span"}:
                text = sib.get_text(" ", strip=True)
                if text:
                    return text
        # 3. Any ancestor with caption-like class
        parent = img_tag.parent
        while parent and parent.name not in {"article", "body"}:
            classes = parent.get("class", [])
            if any(re.search(r"(caption|credit)", c, re.I) for c in classes):
                text = parent.get_text(" ", strip=True)
                if text:
                    return text
            parent = parent.parent
        # 4. Up to 3 forward/back block-level siblings
        for direction in ("next", "previous"):
            sib_iter = (
                img_tag.next_siblings if direction == "next" else img_tag.previous_siblings
            )
            count = 0
            for sib in sib_iter:
                if isinstance(sib, Tag) and sib.name in {"p", "div", "figcaption", "span"}:
                    text = sib.get_text(" ", strip=True)
                    if text:
                        return text
                    count += 1
                    if count == 3:  # stop after 3 hops
                        break
        # 5. Last resort: alt text
        alt = img_tag.get("alt", "").strip()
        return alt or None


    def scrape(self, url: str) -> List[Dict[str, Any]]:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        article = soup.find("article")
        if not article:
            raise RuntimeError("No <article> tag found")

        # Title
        h1 = article.find("h1")
        title = (
            h1.get_text(strip=True)
            if h1
            else (soup.title.string.strip() if soup.title else "(untitled)")
        )

        # Published date
        pub_date: Optional[str] = None
        time_tag = article.find("time")
        if time_tag and time_tag.has_attr("datetime"):
            pub_date = dateparser.parse(time_tag["datetime"]).isoformat()
        elif time_tag:
            pub_date = dateparser.parse(time_tag.get_text(strip=True)).isoformat()
        else:
            meta = soup.find("meta", {"property": "article:published_time"})
            if meta and meta.has_attr("content"):
                pub_date = dateparser.parse(meta["content"]).isoformat()

        # Collect images
        results: List[Dict[str, Any]] = []
        for img in article.find_all("img"):
            src = self._extract_image_src(img, url)
            if not src:
                continue

            results.append(
                {
                    "site_title": title,
                    "publish_date": pub_date,
                    "image_url": src,
                    "alt_text": img.get("alt", "").strip() or None,
                    "caption": self._extract_caption(img),
                }
            )
        return results


# if __name__ == "__main__":
#     scraper = ArticleScraper()
#     images = scraper.scrape(
#         "https://www.straitstimes.com/opinion/budget-2015-beware-the-trust-fund-kids-mindset"
#     )
#     for img in images:
#         print(img)
