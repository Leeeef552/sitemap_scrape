from ..straits_times.xmlscraper import XMLScraper


def main():
    extractor = XMLScraper(
        index_url="https://www.businesstimes.com.sg/sitemap.xml",
        timeout=10,
        polite_delay=0.5,
        max_concurrency=10,
        out_dir="/home/leeeefun681/volume/eefun/webscraping/sitemap/sitemap_scrape/data/business_times/unseen",
        abbrev="bt"
    )
    extractor.dump()

if __name__ == "__main__":
    main()