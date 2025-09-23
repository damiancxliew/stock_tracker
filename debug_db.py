#!/usr/bin/env python3
# debug_db.py - Run this script to check your database
import os
import duckdb

# Path to your database
DB_PATH = "data/warehouse.duckdb"

def check_database():
    print(f"Checking database at: {os.path.abspath(DB_PATH)}")
    
    if not os.path.exists(DB_PATH):
        print("❌ Database file doesn't exist!")
        return
    
    print("✅ Database file exists")
    print(f"File size: {os.path.getsize(DB_PATH)} bytes")
    
    try:
        con = duckdb.connect(DB_PATH)
        
        # Check tables
        tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        print(f"\n📊 Tables in database: {[t[0] for t in tables]}")
        
        # Check SEC filings
        try:
            sec_count = con.execute("SELECT COUNT(*) FROM sec_filings").fetchone()[0]
            print(f"📈 SEC filings: {sec_count} records")
            
            if sec_count > 0:
                sample = con.execute("SELECT ticker, form, filing_date FROM sec_filings LIMIT 3").fetchall()
                print("Sample SEC records:")
                for record in sample:
                    print(f"  - {record}")
        except Exception as e:
            print(f"❌ Error checking sec_filings: {e}")
        
        # Check news
        try:
            news_count = con.execute("SELECT COUNT(*) FROM news").fetchone()[0]
            print(f"📰 News articles: {news_count} records")
            
            if news_count > 0:
                sample = con.execute("SELECT ticker, source, title FROM news LIMIT 3").fetchall()
                print("Sample news records:")
                for record in sample:
                    print(f"  - {record}")
        except Exception as e:
            print(f"❌ Error checking news: {e}")
        
        con.close()
        
    except Exception as e:
        print(f"❌ Database connection error: {e}")

def check_data_directory():
    data_dir = "data"
    print(f"\n📁 Checking data directory: {os.path.abspath(data_dir)}")
    
    if not os.path.exists(data_dir):
        print("❌ Data directory doesn't exist!")
        return
    
    print("✅ Data directory exists")
    
    # Check contents
    for root, dirs, files in os.walk(data_dir):
        level = root.replace(data_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            print(f"{subindent}{file} ({file_size} bytes)")

if __name__ == "__main__":
    print("🔍 Database Debug Report")
    print("=" * 50)
    
    check_data_directory()
    print()
    check_database()
    
    print("\n" + "=" * 50)
    print("Debug complete!")