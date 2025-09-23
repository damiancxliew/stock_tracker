# scraper/stockscraper/spiders/yahoo_news_rss.py
import feedparser
import scrapy
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from stockscraper.items import NewsItem

class YahooNewsRSSSpider(scrapy.Spider):
    name = "yahoo_news_rss"
    
    def __init__(self, ticker=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ticker = (ticker or "AAPL").upper()
        if not ticker:
            raise ValueError("Provide -a ticker=SYMBOL")
        self.ticker = ticker.upper()

    def start_requests(self):
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={self.ticker}&region=US&lang=en-US"
        yield scrapy.Request(url, callback=self.parse_rss)

    def parse_rss(self, response):
        feed = feedparser.parse(response.body)
        for e in feed.entries:
            published = None
            if getattr(e, "published", None):
                try:
                    published = datetime(*e.published_parsed[:6], tzinfo=timezone.utc).isoformat()
                except Exception:
                    published = None
            
            # +++ NEW: Follow the link to parse the full article +++
            yield response.follow(e.link, self.parse_article, meta={
                "ticker": self.ticker,
                "source": "YahooFinanceRSS",
                "title": e.title,
                "link": e.link,
                "published": published,
                "summary": getattr(e, "summary", ""),
            })

    # +++ NEW: Callback to parse the article's HTML content +++
    def parse_article(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        # Yahoo finance news is often in a 'div' with class 'caas-body'
        article_body = soup.find("div", class_="caas-body")
        article_text = article_body.get_text(separator="\n", strip=True) if article_body else ""
        truncated_text = " ".join(article_text.split()[:2000])
        
        allowed = {"ticker","source","title","link","published","summary","article_text"}
        data = {k: v for k, v in response.meta.items() if k in allowed}
        item = NewsItem(**data)
        item["article_text"] = truncated_text
        yield item