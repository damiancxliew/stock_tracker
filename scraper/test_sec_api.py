#!/usr/bin/env python3
# test_sec_api.py - Test SEC API connection
import requests
import json

def test_sec_api():
    print("🔍 Testing SEC API Connection")
    print("=" * 50)
    
    # Test 1: Get company tickers
    print("1. Testing company tickers endpoint...")
    try:
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
        print(f"✅ Found {len(companies)} companies")
        
        # Find AAPL
        aapl_cik = None
        for company_data in companies.values():
            if company_data['ticker'].upper() == 'AAPL':
                aapl_cik = str(company_data['cik_str']).zfill(10)
                print(f"✅ Found AAPL: CIK = {aapl_cik}, Company = {company_data['title']}")
                break
        
        if not aapl_cik:
            print("❌ AAPL not found in company tickers!")
            return
            
    except Exception as e:
        print(f"❌ Error fetching company tickers: {e}")
        return
    
    # Test 2: Get AAPL filings
    print(f"\n2. Testing AAPL filings endpoint...")
    try:
        url = f"https://data.sec.gov/submissions/CIK{aapl_cik}.json"
        print(f"URL: {url}")
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"✅ Successfully fetched AAPL data")
        print(f"Company: {data.get('name')}")
        print(f"CIK: {data.get('cik')}")
        
        filings = data.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])
        dates = filings.get("filingDate", [])
        
        print(f"Total filings: {len(forms)}")
        
        # Count relevant forms
        relevant_forms = [f for f in forms if f in ("10-K", "10-Q", "8-K")]
        print(f"Relevant filings (10-K, 10-Q, 8-K): {len(relevant_forms)}")
        
        # Show recent relevant filings
        recent_relevant = []
        for i, (form, date) in enumerate(zip(forms, dates)):
            if form in ("10-K", "10-Q", "8-K"):
                recent_relevant.append((form, date))
            if len(recent_relevant) >= 5:
                break
        
        print("Recent relevant filings:")
        for form, date in recent_relevant:
            print(f"  - {form} on {date}")
        
        if len(relevant_forms) == 0:
            print("❌ No relevant filings found!")
        else:
            print("✅ Relevant filings found!")
            
    except Exception as e:
        print(f"❌ Error fetching AAPL filings: {e}")
        return
    
    # Test 3: Test a sample report URL
    print(f"\n3. Testing sample report access...")
    try:
        if recent_relevant:
            form, date = recent_relevant[0]
            # Find the corresponding accession and primary doc
            form_index = forms.index(form)
            accession = filings.get("accessionNumber", [])[form_index]
            primary_doc = filings.get("primaryDocument", [])[form_index]
            
            acc_nodashes = accession.replace("-", "")
            report_url = f"https://www.sec.gov/Archives/edgar/data/{int(aapl_cik)}/{acc_nodashes}/{primary_doc}"
            
            print(f"Testing report URL: {report_url}")
            
            response = requests.head(report_url, headers=headers, timeout=10)  # Use HEAD to check if accessible
            
            if response.status_code == 200:
                print("✅ Report URL is accessible")
            else:
                print(f"⚠️  Report URL returned status {response.status_code}")
                
    except Exception as e:
        print(f"❌ Error testing report URL: {e}")
    
    print("\n" + "=" * 50)
    print("SEC API test complete!")

if __name__ == "__main__":
    test_sec_api()