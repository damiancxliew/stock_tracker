# scraper/stockscraper/pipelines.py
import os
import duckdb
import pandas as pd
from datetime import datetime
import openai
import json
from scrapy.exceptions import DropItem
from dotenv import load_dotenv
from itemadapter import ItemAdapter
import logging

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Setup OpenAI ---
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    logger.warning("OPENAI_API_KEY environment variable not set. AI pipeline will be skipped.")

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data"))
LAKE_DIR = os.path.join(DATA_DIR, "lake")
DB_PATH = os.path.join(DATA_DIR, "warehouse.duckdb")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LAKE_DIR, exist_ok=True)

logger.info(f"Data directory: {DATA_DIR}")
logger.info(f"Database path: {DB_PATH}")

class OpenAIPipeline:
    def open_spider(self, spider):
        logger.info("OpenAI Pipeline opened")

    def process_item(self, item, spider):
        logger.info(f"OpenAI Pipeline processing item: {type(item).__name__}")
        
        if not openai.api_key:
            logger.info("Skipping OpenAI processing - no API key")
            return item

        adapter = ItemAdapter(item)
        text_content = (
            adapter.get("report_text")
            or adapter.get("article_text") 
            or f"{adapter.get('title', '')} — {adapter.get('summary', '')}"
        )

        if not text_content or len(text_content.strip()) < 10:
            logger.info("No meaningful text content found, skipping AI analysis")
            return item

        try:
            logger.info("Calling OpenAI API...")
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a financial analyst. Analyze the following text."},
                    {"role": "user", "content": f"""
                        From the text below, provide these three things in a JSON format with keys "summary", "sentiment", and "sentiment_score":
                        1. A concise one-sentence summary.
                        2. The overall sentiment (options: Positive, Negative, Neutral).
                        3. A sentiment score from -1.0 (very negative) to 1.0 (very positive).

                        Text: "{text_content[:1000]}"
                    """}
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
            )
            
            analysis = json.loads(response.choices[0].message.content)
            adapter["summary_ai"] = analysis.get("summary", "")
            adapter["sentiment"] = analysis.get("sentiment", "Unknown")
            adapter["sentiment_score"] = float(analysis.get("sentiment_score", 0.0))
            logger.info("OpenAI analysis completed successfully")

        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            adapter["summary_ai"] = "Error in analysis."
            adapter["sentiment"] = "Unknown"
            adapter["sentiment_score"] = 0.0
            
        return item

    def close_spider(self, spider):
        logger.info("OpenAI Pipeline closed")


class DuckDBPipeline:
    def __init__(self):
        self.items = []

    def open_spider(self, spider):
        logger.info("DuckDB Pipeline opened")
        try:
            # Test database connection
            self.con = duckdb.connect(DB_PATH)
            
            # Create tables with explicit schemas
            self.con.execute("""
            CREATE TABLE IF NOT EXISTS sec_filings (
              cik VARCHAR, 
              ticker VARCHAR, 
              company_name VARCHAR,
              form VARCHAR, 
              filing_date DATE, 
              accession_no VARCHAR,
              primary_doc VARCHAR, 
              report_url VARCHAR, 
              report_text TEXT,
              summary_ai TEXT, 
              sentiment VARCHAR, 
              sentiment_score DOUBLE
            );
            """)
            
            self.con.execute("""
            CREATE TABLE IF NOT EXISTS news (
              ticker VARCHAR, 
              source VARCHAR, 
              title TEXT,
              link VARCHAR, 
              published TIMESTAMP, 
              summary TEXT, 
              article_text TEXT,
              summary_ai TEXT, 
              sentiment VARCHAR, 
              sentiment_score DOUBLE
            );
            """)
            
            logger.info("DuckDB tables created successfully")
            
            # Test insert
            self.con.execute("SELECT COUNT(*) FROM sec_filings")
            self.con.execute("SELECT COUNT(*) FROM news")
            logger.info("Database connection test successful")
            
        except Exception as e:
            logger.error(f"Failed to setup DuckDB: {e}")
            raise

    def process_item(self, item, spider):
        logger.info(f"DuckDB Pipeline processing item: {type(item).__name__}")
        
        # Convert item to dict and store
        item_dict = dict(ItemAdapter(item))
        
        # Log what we're storing
        logger.info(f"Item keys: {list(item_dict.keys())}")
        
        self.items.append(item_dict)
        logger.info(f"Total items collected: {len(self.items)}")
        
        return item

    def close_spider(self, spider):
        logger.info(f"DuckDB Pipeline closing with {len(self.items)} items")
        
        if not self.items:
            logger.warning("No items to process!")
            self.con.close()
            return

        # Separate SEC filings from news
        sec_items = []
        news_items = []
        
        for item in self.items:
            if "form" in item:  # SEC filing
                sec_items.append(item)
            else:  # News
                news_items.append(item)
        
        logger.info(f"Found {len(sec_items)} SEC filings and {len(news_items)} news items")

        # Process SEC filings
        if sec_items:
            try:
                logger.info("Processing SEC filings...")
                df = pd.DataFrame(sec_items)
                
                # Define expected columns for SEC filings
                sec_columns = [
                    "cik", "ticker", "company_name", "form", "filing_date", 
                    "accession_no", "primary_doc", "report_url", "report_text",
                    "summary_ai", "sentiment", "sentiment_score"
                ]
                
                # Ensure all columns exist
                for col in sec_columns:
                    if col not in df.columns:
                        df[col] = None
                
                df = df[sec_columns]  # Reorder columns
                
                # Convert types
                df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce").dt.date
                df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
                
                # Fill NaN values
                df = df.fillna({
                    'cik': '', 'ticker': '', 'company_name': '', 'form': '',
                    'accession_no': '', 'primary_doc': '', 'report_url': '', 
                    'report_text': '', 'summary_ai': '', 'sentiment': 'Unknown',
                    'sentiment_score': 0.0
                })
                
                logger.info(f"DataFrame shape: {df.shape}")
                logger.info(f"DataFrame columns: {list(df.columns)}")
                
                # Insert into database
                self.con.register("sec_temp", df)
                result = self.con.execute("INSERT INTO sec_filings SELECT * FROM sec_temp")
                
                # Verify insertion
                count = self.con.execute("SELECT COUNT(*) FROM sec_filings").fetchone()[0]
                logger.info(f"Successfully inserted {count} SEC filings")
                
            except Exception as e:
                logger.error(f"Failed to process SEC filings: {e}")
                logger.error(f"DataFrame info: {df.info() if 'df' in locals() else 'No DataFrame'}")

        # Process news items
        if news_items:
            try:
                logger.info("Processing news items...")
                df = pd.DataFrame(news_items)
                
                # Define expected columns for news (NO report_text!)
                news_columns = [
                    "ticker", "source", "title", "link", "published", 
                    "summary", "article_text", "summary_ai", "sentiment", "sentiment_score"
                ]
                
                # Ensure all columns exist
                for col in news_columns:
                    if col not in df.columns:
                        df[col] = None
                
                df = df[news_columns]  # Reorder columns
                
                # Convert types
                df["published"] = pd.to_datetime(df["published"], errors="coerce")
                df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
                
                # Fill NaN values
                df = df.fillna({
                    'ticker': '', 'source': '', 'title': '', 'link': '',
                    'summary': '', 'article_text': '', 'summary_ai': '', 
                    'sentiment': 'Unknown', 'sentiment_score': 0.0
                })
                
                logger.info(f"DataFrame shape: {df.shape}")
                logger.info(f"DataFrame columns: {list(df.columns)}")
                
                # Insert into database
                self.con.register("news_temp", df)
                result = self.con.execute("INSERT INTO news SELECT * FROM news_temp")
                
                # Verify insertion
                count = self.con.execute("SELECT COUNT(*) FROM news").fetchone()[0]
                logger.info(f"Successfully inserted {count} news items")
                
            except Exception as e:
                logger.error(f"Failed to process news items: {e}")
                logger.error(f"DataFrame info: {df.info() if 'df' in locals() else 'No DataFrame'}")

        # Close connection
        try:
            self.con.close()
            logger.info("DuckDB connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing DuckDB connection: {e}")


class ParquetPipeline:
    def __init__(self):
        self.items = []

    def open_spider(self, spider):
        logger.info("Parquet Pipeline opened")

    def process_item(self, item, spider):
        logger.info(f"Parquet Pipeline processing item: {type(item).__name__}")
        self.items.append(dict(ItemAdapter(item)))
        return item

    def close_spider(self, spider):
        logger.info(f"Parquet Pipeline closing with {len(self.items)} items")
        
        if not self.items:
            logger.warning("No items to save to Parquet!")
            return

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Separate SEC filings from news
        sec_items = [item for item in self.items if "form" in item]
        news_items = [item for item in self.items if "form" not in item]
        
        try:
            if sec_items:
                filepath = os.path.join(LAKE_DIR, f"sec_filings_{ts}.parquet")
                pd.DataFrame(sec_items).to_parquet(filepath)
                logger.info(f"Saved {len(sec_items)} SEC filings to {filepath}")
                
            if news_items:
                filepath = os.path.join(LAKE_DIR, f"news_{ts}.parquet")
                pd.DataFrame(news_items).to_parquet(filepath)
                logger.info(f"Saved {len(news_items)} news items to {filepath}")
                
        except Exception as e:
            logger.error(f"Error saving Parquet files: {e}")