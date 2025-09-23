# scraper/stockscraper/spiders/sec_filings.py
import json
import scrapy
import requests
from bs4 import BeautifulSoup
from stockscraper.items import SecFilingItem

# FIXED: Proper ticker to CIK conversion
def to_cik(ticker: str) -> str:
    """Convert ticker to CIK using SEC's company tickers JSON"""
    try:
        # Get the company tickers JSON from SEC
        headers = {
            'User-Agent': 'damian-uni-project/1.0 (contact: damian.liew@u.nus.edu)',
            'Accept': 'application/json'
        }
        response = requests.get(
            'https://www.sec.gov/files/company_tickers.json', 
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        
        companies = response.json()
        
        # Search for the ticker
        for company_data in companies.values():
            if company_data['ticker'].upper() == ticker.upper():
                return str(company_data['cik_str']).zfill(10)  # Pad to 10 digits
        
        print(f"WARNING: Ticker {ticker} not found in SEC database")
        return None
        
    except Exception as e:
        print(f"ERROR: Failed to convert ticker {ticker} to CIK: {e}")
        return None

class SecFilingsSpider(scrapy.Spider):
    name = "sec_filings"
    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "USER_AGENT": "damian-uni-project/1.0 (contact: damian.liew@u.nus.edu)"
    }

    def __init__(self, cik=None, ticker=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ticker = (ticker or "AAPL").upper()
        if cik:
            self.cik = str(cik).zfill(10)  # Pad to 10 digits
            self.ticker = ticker or self.cik
        elif ticker:
            self.ticker = ticker.upper()
            self.cik = to_cik(self.ticker)
            if not self.cik:
                raise ValueError(f"Could not find CIK for ticker {self.ticker}")
        else:
            raise ValueError("Provide -a cik=... or -a ticker=...")
        
        self.logger.info(f"Initialized SEC spider - Ticker: {self.ticker}, CIK: {self.cik}")

    def start_requests(self):
        # Use the padded CIK
        url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
        
        headers = {
            'User-Agent': 'damian-uni-project/1.0 (contact: damian.liew@u.nus.edu)',
            'Accept': 'application/json'
        }
        
        self.logger.info(f"Requesting SEC data from: {url}")
        yield scrapy.Request(
            url, 
            callback=self.parse, 
            headers=headers,
            dont_filter=True
        )

    def parse(self, response):
        self.logger.info(f"SEC API Response status: {response.status}")
        self.logger.info(f"Response length: {len(response.text)} characters")
        
        if response.status != 200:
            self.logger.error(f"SEC API returned status {response.status}")
            return
        
        try:
            data = json.loads(response.text)
            self.logger.info(f"Successfully parsed JSON response")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            self.logger.error(f"Response text: {response.text[:500]}...")
            return
        
        # Log the structure we received
        cik = data.get("cik")
        company = data.get("name")
        
        self.logger.info(f"Company: {company}")
        self.logger.info(f"CIK: {cik}")
        
        filings = data.get("filings", {})
        if not filings:
            self.logger.error("No 'filings' key in response")
            return
            
        recent = filings.get("recent", {})
        if not recent:
            self.logger.error("No 'recent' filings found")
            return
        
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        
        self.logger.info(f"Found {len(forms)} total filings")
        
        # Log some sample data
        if forms:
            self.logger.info(f"Sample forms: {forms[:5]}")
            self.logger.info(f"Sample dates: {dates[:5]}")
        
        relevant_count = 0
        for i, (form, date, acc, pdoc) in enumerate(zip(forms, dates, accessions, primary_docs)):
            if form not in ("10-K", "10-Q", "8-K"):
                continue
                
            relevant_count += 1
            self.logger.info(f"Processing {form} filing from {date}")
            
            acc_nodashes = acc.replace("-", "")
            report_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodashes}/{pdoc}"
            
            # Store metadata for the report parsing
            meta = {
                "cik": str(cik),
                "ticker": self.ticker,
                "company_name": company,
                "form": form,
                "filing_date": date,
                "accession_no": acc,
                "primary_doc": pdoc,
                "report_url": report_url,
            }
            
            self.logger.info(f"Following report URL: {report_url}")
            
            yield response.follow(
                report_url, 
                self.parse_report, 
                meta=meta,
                headers={
                    'User-Agent': 'damian-uni-project/1.0 (contact: damian.liew@u.nus.edu)',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                },
                dont_filter=True
            )
        
        self.logger.info(f"Found {relevant_count} relevant filings (10-K, 10-Q, 8-K)")
        
        if relevant_count == 0:
            self.logger.warning("No relevant filings found!")

    def parse_report(self, response):
        self.logger.info(f"Parsing report: {response.url}")
        self.logger.info(f"Report response status: {response.status}")
        
        if response.status != 200:
            self.logger.error(f"Failed to fetch report: {response.status}")
            return
        
        try:
            # Parse the HTML content
            soup = BeautifulSoup(response.body, "lxml")
            report_text = soup.get_text(separator="\n", strip=True)
            
            if not report_text:
                self.logger.warning(f"No text content found in report: {response.url}")
                return
            
            # Truncate to reasonable size (4000 words)
            truncated_text = " ".join(report_text.split()[:4000])
            
            self.logger.info(f"Extracted {len(report_text)} characters of text (truncated to {len(truncated_text)})")
            
            # Get metadata from the request
            meta_data = response.meta
            
            # Create the item
            item = SecFilingItem()
            for key, value in meta_data.items():
                if hasattr(item, key) or key in item.fields:
                    item[key] = value
            
            item["report_text"] = truncated_text
            
            self.logger.info(f"Created SecFilingItem: {item.get('ticker')} - {item.get('form')} - {item.get('filing_date')}")
            
            yield item
            
        except Exception as e:
            self.logger.error(f"Error parsing report {response.url}: {e}")