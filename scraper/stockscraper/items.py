# scraper/stockscraper/items.py
import scrapy

class SecFilingItem(scrapy.Item):
    cik = scrapy.Field()
    ticker = scrapy.Field()
    company_name = scrapy.Field()
    form = scrapy.Field()
    filing_date = scrapy.Field()
    accession_no = scrapy.Field()
    primary_doc = scrapy.Field()
    report_url = scrapy.Field()
    report_text = scrapy.Field()       
    summary_ai = scrapy.Field()
    sentiment = scrapy.Field()
    sentiment_score = scrapy.Field()

class NewsItem(scrapy.Item):
    ticker = scrapy.Field()
    source = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    published = scrapy.Field()
    summary = scrapy.Field()
    article_text = scrapy.Field()       
    summary_ai = scrapy.Field()
    sentiment = scrapy.Field()
    sentiment_score = scrapy.Field()