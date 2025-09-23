# Navigate to the scraper directory first
cd /Users/damianliew/Desktop/Projects/stock_tracker/scraper;

# Then run scrapy commands directly (not with python -v)
python -m scrapy crawl sec_filings -a cik=0000320193 -a ticker=AAPL -L INFO;
python -m scrapy crawl yahoo_news_rss -a ticker=AAPL -L INFO;