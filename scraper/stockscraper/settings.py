# scraper/scraper/settings.py
BOT_NAME = "stockscraper"
SPIDER_MODULES = ["stockscraper.spiders"]
NEWSPIDER_MODULE = "stockscraper.spiders"

ROBOTSTXT_OBEY = True
DOWNLOAD_DELAY = 0.5
CONCURRENT_REQUESTS = 8
AUTOTHROTTLE_ENABLED = True

USER_AGENT = "damian-uni-project/1.0 (contact: damian.liew@u.nus.edu)"
DEFAULT_REQUEST_HEADERS = {
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
}

ITEM_PIPELINES = {
    "stockscraper.pipelines.OpenAIPipeline": 200,
    "stockscraper.pipelines.DuckDBPipeline": 300,
    "stockscraper.pipelines.ParquetPipeline": 400,
}

# in settings.py
CLOSESPIDER_ITEMCOUNT = 20   # stop after 20 items
