# app.py
import os, sys, subprocess, duckdb, pandas as pd, yfinance as yf, streamlit as st, openai
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "warehouse.duckdb")

st.set_page_config(page_title="US Stocks Intel", layout="wide")
st.title("US Stocks – Trends & Filings Intel")

openai_api_key = st.secrets.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
openai.api_key = openai_api_key

# ---------- Helpers ----------
def load_ticker_list() -> list[str]:
    """Return distinct tickers we have in DuckDB (from either table)."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute("""
            SELECT DISTINCT UPPER(ticker) AS t FROM (
                SELECT ticker FROM sec_filings
                UNION ALL
                SELECT ticker FROM news
            )
            WHERE ticker IS NOT NULL AND ticker <> ''
            ORDER BY t
        """).df()
        con.close()
        items = df["t"].dropna().astype(str).tolist()
        return sorted(set(items + ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]))  # ensure some defaults
    except Exception:
        return ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]

def run_spider(spider: str, ticker: str, item_limit: int = 30, cwd: str = None) -> tuple[bool, str]:
    """
    Run a Scrapy spider in-process env using the current Python (virtualenv-safe).
    Adjust 'cwd' to the folder where scrapy.cfg lives (your repo root or scraper/).
    """
    env = os.environ.copy()
    # throttle costs a bit
    cmd = [
        sys.executable, "-m", "scrapy", "crawl", spider,
        "-a", f"ticker={ticker}",
        "-s", f"CLOSESPIDER_ITEMCOUNT={item_limit}",
        "-s", "AUTOTHROTTLE_ENABLED=True",
        "-s", "CONCURRENT_REQUESTS=8",
        "-s", "DOWNLOAD_DELAY=0.5",
    ]
    try:
        res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False, timeout=600)
        ok = (res.returncode == 0)
        log = (res.stdout or "") + "\n" + (res.stderr or "")
        return ok, log
    except Exception as e:
        return False, f"Failed to run spider {spider}: {e}"

def fetch_db(ticker: str):
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        sec_df = con.execute("""
            SELECT form, filing_date, report_url, summary_ai, sentiment, sentiment_score
            FROM sec_filings WHERE ticker = ? ORDER BY filing_date DESC LIMIT 50
        """, [ticker]).df()
        news_df = con.execute("""
            SELECT published, title, link, summary_ai, sentiment, sentiment_score
            FROM news WHERE ticker = ? ORDER BY published DESC NULLS LAST LIMIT 200
        """, [ticker]).df()
        con.close()
    except Exception as e:
        st.error(f"Database error: {e}")
        sec_df, news_df = pd.DataFrame(), pd.DataFrame()

    # coerce
    if not sec_df.empty:
        sec_df["filing_date"] = pd.to_datetime(sec_df["filing_date"], errors="coerce")
        sec_df["sentiment_score"] = pd.to_numeric(sec_df["sentiment_score"], errors="coerce")
    if not news_df.empty:
        news_df["published"] = pd.to_datetime(news_df["published"], errors="coerce")
        news_df["sentiment_score"] = pd.to_numeric(news_df["sentiment_score"], errors="coerce")
    return sec_df, news_df

def build_unified(sec_df, news_df):
    parts = []
    if sec_df is not None and not sec_df.empty:
        a = sec_df.copy()
        a["kind"] = "FILING"
        a["when"] = pd.to_datetime(a["filing_date"], errors="coerce")
        a["title_disp"] = a["form"].fillna("Filing")
        a["url"] = a["report_url"]
        a["summary_disp"] = a["summary_ai"]
        parts.append(a[["kind","when","title_disp","url","summary_disp","sentiment","sentiment_score"]])
    if news_df is not None and not news_df.empty:
        b = news_df.copy()
        b["kind"] = "NEWS"
        b["when"] = pd.to_datetime(b["published"], errors="coerce")
        b["title_disp"] = b["title"].fillna("News")
        b["url"] = b["link"]
        b["summary_disp"] = b["summary_ai"]
        parts.append(b[["kind","when","title_disp","url","summary_disp","sentiment","sentiment_score"]])
    if not parts:
        return pd.DataFrame(columns=["kind","when","title_disp","url","summary_disp","sentiment","sentiment_score"])
    out = pd.concat(parts, ignore_index=True)
    out["sentiment_score"] = pd.to_numeric(out["sentiment_score"], errors="coerce")
    return out.dropna(subset=["sentiment_score"])

def fmt_summary(text, max_len=120):
    s = "" if text is None else str(text)
    return s if len(s) <= max_len else s[:max_len] + "…"

# ---------- Sidebar controls ----------
st.sidebar.header("Controls")

# 1) dropdown with manual override
tickers = load_ticker_list()
sel = st.sidebar.selectbox("Choose a ticker", options=tickers, index=min(0, len(tickers)-1))
manual = st.sidebar.text_input("…or type a ticker", "").upper().strip()
ticker = (manual or sel).upper()

period = st.sidebar.selectbox("Price period", ["1mo","3mo","6mo","1y","2y"], index=2)
interval = st.sidebar.selectbox("Candle interval", ["1d","1h"], index=0)
item_limit = st.sidebar.slider("Scrape max items (per spider)", 5, 100, 25, step=5)

# 2) generate button
generate_clicked = st.sidebar.button("⚡ Generate latest data")

# ---------- Run spiders on demand ----------
if generate_clicked:
    st.sidebar.info(f"Running spiders for {ticker} (limit {item_limit})…")
    # set cwd to your scrapy project root (where scrapy.cfg lives)
    SCRAPY_CWD = os.path.abspath(os.path.join(os.path.dirname(__file__), "../scraper"))
    with st.status("Scraping in progress…", expanded=True) as status:
        ok1, log1 = run_spider("sec_filings", ticker, item_limit=item_limit, cwd=SCRAPY_CWD)
        st.write("SEC filings spider finished." if ok1 else "SEC filings spider failed.")
        st.code(log1[-4000:])  # tail logs

        ok2, log2 = run_spider("yahoo_news_rss", ticker, item_limit=item_limit, cwd=SCRAPY_CWD)
        st.write("Yahoo News spider finished." if ok2 else "Yahoo News spider failed.")
        st.code(log2[-4000:])

        if ok1 or ok2:
            status.update(label="Scraping done. Reloading data…", state="complete")
        else:
            status.update(label="Scraping failed.", state="error")

# ---------- Price chart ----------
st.subheader(f"Price – {ticker}")
price = yf.download(ticker, period=period, interval=interval, progress=False)
if not price.empty and "Close" in price.columns:
    st.line_chart(price["Close"])
else:
    st.warning(f"Could not fetch price data for {ticker}.")

# ---------- Load & show data ----------
sec_df, news_df = fetch_db(ticker)

combined = build_unified(sec_df, news_df)
if not combined.empty:
    st.subheader("Most Extreme Sentiments (Filings + News)")
    max_row = combined.sort_values(by=["sentiment_score","when"], ascending=[False, False]).iloc[0]
    min_row = combined.sort_values(by=["sentiment_score","when"], ascending=[True, False]).iloc[0]
    c1, c2 = st.columns(2)
    with c1:
        st.success("📈 Most Positive")
        st.write(f"**{max_row['title_disp']}** ({max_row['kind']}) — {max_row['when'].date() if pd.notna(max_row['when']) else 'N/A'}")
        st.write(f"Sentiment: {max_row.get('sentiment','Unknown')} ({float(max_row['sentiment_score']):.2f})")
        with st.expander("View Summary"):
            st.write(max_row.get("summary_disp") or "No summary available.")
            if pd.notna(max_row.get("url", None)): st.write(f"[Open Link]({max_row['url']})")
    with c2:
        st.error("📉 Most Negative")
        st.write(f"**{min_row['title_disp']}** ({min_row['kind']}) — {min_row['when'].date() if pd.notna(min_row['when']) else 'N/A'}")
        st.write(f"Sentiment: {min_row.get('sentiment','Unknown')} ({float(min_row['sentiment_score']):.2f})")
        with st.expander("View Summary"):
            st.write(min_row.get("summary_disp") or "No summary available.")
            if pd.notna(min_row.get("url", None)): st.write(f"[Open Link]({min_row['url']})")
    st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Recent SEC Filings")
    if sec_df.empty:
        st.info("No filings yet. Run the SEC spider.")
    else:
        st.write("**All Recent Filings**")
        for _, row in sec_df.iterrows():
            score = row.get("sentiment_score")
            if pd.notna(score):
                color = "🟢" if float(score) > 0.1 else ("🔴" if float(score) < -0.1 else "🟡")
                text = f"{color} {row.get('sentiment','Unknown')} ({float(score):.2f})"
            else:
                text = f"⚪ {row.get('sentiment','Unknown')}"
            with st.container():
                cL, cR = st.columns([3,1])
                with cL:
                    date_str = row["filing_date"].strftime("%Y-%m-%d") if pd.notna(row.get("filing_date")) else "N/A"
                    st.markdown(f"**{row.get('form','N/A')}** — {date_str}")
                    st.caption(fmt_summary(row.get("summary_ai")))
                with cR:
                    st.markdown(text)
                    if pd.notna(row.get("report_url", None)):
                        st.markdown(f"[📄 View]({row['report_url']})")
                st.markdown("---")

with col2:
    st.subheader("Latest News")
    if news_df.empty:
        st.info("No news yet. Run the RSS spider.")
    else:
        for _, row in news_df.iterrows():
            title = str(row.get("title","Untitled"))
            link = row.get("link")
            st.markdown(f"**{title}**" + (f" — [Link]({link})" if pd.notna(link) else ""))
            with st.expander("AI Summary"):
                st.write(row.get("summary_ai") or "No summary available.")
                s = row.get("sentiment_score")
                if pd.isna(s):
                    st.write(f"**Sentiment:** {row.get('sentiment','Unknown')}")
                else:
                    st.write(f"**Sentiment:** {row.get('sentiment','Unknown')} ({float(s):.2f})")

# ---------- Sentiment over time ----------
st.subheader("News Sentiment Over Time")
if not news_df.empty and "published" in news_df.columns and "sentiment_score" in news_df.columns:
    s = pd.to_numeric(news_df["sentiment_score"], errors="coerce")
    t = pd.to_datetime(news_df["published"], errors="coerce")
    ts = pd.Series(s.values, index=t).dropna().rolling(window=5, min_periods=1).mean()
    if not ts.empty:
        st.line_chart(ts)
    else:
        st.info("No sentiment data available for chart.")
else:
    st.info("No news with sentiment scores to analyse.")

# ---------- AI Insights ----------
st.subheader("Actionable Insights (Generated by AI)")
if not openai.api_key:
    st.warning("Enter your OPENAI_API_KEY in .env to generate insights.")
elif news_df.empty and sec_df.empty:
    st.info("Not enough data to generate insights.")
else:
    with st.spinner("Generating insights..."):
        try:
            # titles
            titles = "No recent news available"
            if not news_df.empty and "title" in news_df.columns:
                titles_list = news_df["title"].astype(str).replace("nan","").fillna("").head(10).tolist()
                titles_list = [t for t in (t.strip() for t in titles_list) if t]
                titles = "\n".join(f"- {t}" for t in titles_list) or "No recent news available"

            # forms
            forms = "None"
            if not sec_df.empty and "form" in sec_df.columns:
                forms_list = pd.Series(sec_df["form"]).dropna().astype(str).head(5).unique().tolist()
                forms = ", ".join(forms_list) if forms_list else "None"

            # price change
            price_change_str = "N/A"
            if not price.empty and "Close" in price.columns and price["Close"].shape[0] >= 2:
                pct = float(price["Close"].pct_change().iloc[-1])
                if pd.notna(pct):
                    price_change_str = f"{pct * 100:.2f}%"

            prompt = (
                "You are a senior financial analyst for a hedge fund.\n"
                f"Given the latest data for the stock {ticker}, provide a bulleted list of 3-5 key actionable insights.\n"
                "Be concise and direct. Focus on what a trader or investor should be aware of.\n\n"
                f"Latest News Headlines:\n{titles}\n\n"
                f"Recent SEC Filings: {forms}\n"
                f"Last Price Change: {price_change_str}\n\n"
                "Generate the insights now."
            )
            resp = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
            )
            st.markdown(str(resp.choices[0].message.content))
        except Exception as e:
            st.error(f"Failed to generate AI insights: {e}")
