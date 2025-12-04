import os
from datetime import datetime
import pandas as pd
import yfinance as yf
import feedparser

def fetch_news(ticker, max_articles=None, source="yfinance"):
    """Fetch news articles for a given ticker."""
    articles = []
    
    if source == "yfinance":
        try:
            stock = yf.Ticker(ticker)
            news = stock.news
            
            for article in news:
                # yfinance news has nested structure with 'content' key
                content = article.get("content", {})
                provider = content.get("provider", {})  # provider is inside content
                canonical_url = content.get("canonicalUrl", {})  # canonicalUrl is inside content
                click_through_url = content.get("clickThroughUrl", {})
                
                # Parse published date
                published = None
                if content.get("pubDate"):
                    try:
                        published = pd.to_datetime(content.get("pubDate"))
                    except:
                        pass
                
                # Use clickThroughUrl if canonicalUrl is not available
                link = ""
                if canonical_url and canonical_url.get("url"):
                    link = canonical_url.get("url", "")
                elif click_through_url and click_through_url.get("url"):
                    link = click_through_url.get("url", "")
                
                article_data = {
                    "ticker": ticker,
                    "title": content.get("title", ""),
                    "description": content.get("description", ""),
                    "summary": content.get("summary", ""),
                    "link": link,
                    "published": published,
                    "publisher": provider.get("displayName", "") if provider else "",
                    "type": content.get("contentType", ""),
                    "uuid": article.get("id", "")
                }
                articles.append(article_data)
                
                if max_articles and len(articles) >= max_articles:
                    break
        except Exception as e:
            print(f"Error fetching news from yfinance: {e}")
            # Fallback to Google News
            return fetch_news(ticker, max_articles, source="google")
    
    elif source == "google":
        try:
            query = f"{ticker} stock"
            feed_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries:
                article_data = {
                    "ticker": ticker,
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "publisher": entry.get("source", {}).get("title", "") if hasattr(entry, "source") else "",
                    "type": "google_news",
                    "uuid": ""
                }
                articles.append(article_data)
                
                if max_articles and len(articles) >= max_articles:
                    break
        except Exception as e:
            print(f"Error fetching news from Google: {e}")
    
    df = pd.DataFrame(articles)
    if not df.empty and "published" in df.columns:
        # Convert published to datetime if it's a string
        df["published"] = pd.to_datetime(df["published"], errors="coerce")
        # Sort by published date (most recent first)
        df = df.sort_values("published", ascending=False).reset_index(drop=True)
    
    return df

def fetch_news_and_save(ticker, max_articles=None, save_dir="data/raw/news", source="yfinance"):
    """Fetch news and save to parquet file."""
    os.makedirs(save_dir, exist_ok=True)
    
    df = fetch_news(ticker, max_articles, source)
    
    if not df.empty:
        filepath = os.path.join(save_dir, f"{ticker}_news.parquet")
        df.to_parquet(filepath, index=False)
        print(f"Saved {len(df)} articles to {filepath}")
    
    return df