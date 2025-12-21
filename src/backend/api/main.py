"""
FastAPI application for serving processed financial data and triggering ETL pipelines.
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
import math

from etl.orchestrator import run_etl_pipeline
from etl.auto_orchestrator import run_autonomous
from etl.config import ETLConfig
from retrieval.retrieval_service import get_retrieval_service
from agents.research_agent import ResearchAgent
from pydantic import BaseModel

app = FastAPI(title="Financial Data ETL API", version="1.0.0")
config = ETLConfig()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://localhost:5174"],  # Vite default ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_parquet_file(filepath: Path) -> pd.DataFrame:
    """Helper to load parquet file with error handling."""
    if not filepath.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {filepath}")
    try:
        return pd.read_parquet(filepath)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")


def clean_dataframe_for_json(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-serializable format, handling NaN values."""
    # Convert to dict first, then clean (more reliable than DataFrame operations)
    records = df.to_dict(orient='records')
    
    # Clean each record, handling NaN and inf values
    def clean_dict(d):
        if isinstance(d, dict):
            cleaned = {}
            for k, v in d.items():
                if pd.isna(v):
                    cleaned[k] = None
                elif isinstance(v, (float, int)):
                    if not math.isfinite(v):
                        cleaned[k] = None
                    else:
                        cleaned[k] = v
                elif isinstance(v, pd.Timestamp):
                    cleaned[k] = v.isoformat() if pd.notna(v) else None
                else:
                    cleaned[k] = v
            return cleaned
        elif isinstance(d, list):
            return [clean_dict(item) for item in d]
        else:
            if pd.isna(d):
                return None
            elif isinstance(d, (float, int)) and not math.isfinite(d):
                return None
            return d
    
    return [clean_dict(record) for record in records]


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Financial Data ETL API",
        "version": "1.0.0",
        "endpoints": {
            "features": "/api/ticker/{ticker}/features",
            "prices": "/api/ticker/{ticker}/prices",
            "news": "/api/ticker/{ticker}/news",
            "fundamentals": "/api/ticker/{ticker}/fundamentals",
            "run_etl": "/api/etl/run/{ticker}",
            "search": "/api/search?query=...",
            "search_news": "/api/search/news?query=...",
            "search_filings": "/api/search/filings?query=...",
            "search_transcripts": "/api/search/transcripts?query=...",
            "agent_query": "/api/agent/query",
            "agent_research": "/api/agent/research",
            "agent_status": "/api/agent/status",
        }
    }


@app.get("/api/ticker/{ticker}/features")
async def get_features(ticker: str):
    """Get processed features for a ticker."""
    try:
        # Load features file (contains all tickers)
        df = load_parquet_file(config.FEATURES_FILE)
        
        # Filter by ticker
        ticker_data = df[df["ticker"] == ticker.upper()]
        
        if ticker_data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No features found for ticker {ticker}"
            )
        
        return {
            "ticker": ticker.upper(),
            "count": len(ticker_data),
            "data": clean_dataframe_for_json(ticker_data)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ticker/{ticker}/prices")
async def get_prices(ticker: str):
    """Get processed prices for a ticker."""
    try:
        # Try combined file first
        if config.PROCESSED_PRICES_FILE.exists():
            df = load_parquet_file(config.PROCESSED_PRICES_FILE)
            ticker_data = df[df["ticker"] == ticker.upper()]
        else:
            # Try individual file
            filepath = config.PROCESSED_PRICES_DIR / f"{ticker.upper()}.parquet"
            df = load_parquet_file(filepath)
            ticker_data = df
        
        if ticker_data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No prices found for ticker {ticker}"
            )
        
        return {
            "ticker": ticker.upper(),
            "count": len(ticker_data),
            "data": clean_dataframe_for_json(ticker_data)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ticker/{ticker}/news")
async def get_news(ticker: str):
    """Get processed news for a ticker."""
    try:
        # Try combined file first
        if config.PROCESSED_NEWS_FILE.exists():
            df = load_parquet_file(config.PROCESSED_NEWS_FILE)
            ticker_data = df[df["ticker"] == ticker.upper()]
        else:
            # Try individual file
            filepath = config.PROCESSED_NEWS_DIR / f"{ticker.upper()}_news.parquet"
            df = load_parquet_file(filepath)
            ticker_data = df
        
        if ticker_data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No news found for ticker {ticker}"
            )
        
        # Remove embedding column for API response (too large)
        if "embedding" in ticker_data.columns:
            ticker_data = ticker_data.drop(columns=["embedding"])
        
        return {
            "ticker": ticker.upper(),
            "count": len(ticker_data),
            "data": clean_dataframe_for_json(ticker_data)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ticker/{ticker}/fundamentals")
async def get_fundamentals(ticker: str):
    """Get processed fundamentals for a ticker."""
    try:
        # Try combined file first
        if config.PROCESSED_FUNDAMENTALS_FILE.exists():
            df = load_parquet_file(config.PROCESSED_FUNDAMENTALS_FILE)
            # Filter by ticker if column exists
            if "ticker" in df.columns:
                ticker_data = df[df["ticker"] == ticker.upper()]
            else:
                ticker_data = df
        else:
            # Try individual file
            filepath = config.PROCESSED_FUNDAMENTALS_DIR / f"{ticker.upper()}_fundamentals.parquet"
            df = load_parquet_file(filepath)
            ticker_data = df
        
        if ticker_data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No fundamentals found for ticker {ticker}"
            )
        
        return {
            "ticker": ticker.upper(),
            "count": len(ticker_data),
            "data": clean_dataframe_for_json(ticker_data)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def run_etl_background(ticker: str) -> Dict[str, Any]:
    """Run ETL pipeline in background."""
    try:
        results = run_etl_pipeline(ticker.upper())
        return results
    except Exception as e:
        return {
            "ticker": ticker.upper(),
            "overall_success": False,
            "error": str(e)
        }


@app.post("/api/etl/run/{ticker}")
async def trigger_etl(ticker: str, background_tasks: BackgroundTasks):
    """Trigger ETL pipeline for a ticker."""
    ticker = ticker.upper()
    
    # Run ETL in background
    background_tasks.add_task(run_etl_background, ticker)
    
    return {
        "message": f"ETL pipeline started for {ticker}",
        "ticker": ticker,
        "status": "processing",
        "note": "Check back in a few moments for results"
    }


@app.get("/api/etl/status/{ticker}")
async def get_etl_status(ticker: str):
    """Check if processed data exists for a ticker."""
    ticker = ticker.upper()
    status = {
        "ticker": ticker,
        "features": False,
        "prices": False,
        "news": False,
        "fundamentals": False,
    }
    
    # Check features
    if config.FEATURES_FILE.exists():
        try:
            df = pd.read_parquet(config.FEATURES_FILE)
            status["features"] = ticker in df["ticker"].values if "ticker" in df.columns else False
        except:
            pass
    
    # Check prices
    if config.PROCESSED_PRICES_FILE.exists():
        try:
            df = pd.read_parquet(config.PROCESSED_PRICES_FILE)
            status["prices"] = ticker in df["ticker"].values if "ticker" in df.columns else False
        except:
            pass
    elif (config.PROCESSED_PRICES_DIR / f"{ticker}.parquet").exists():
        status["prices"] = True
    
    # Check news
    if config.PROCESSED_NEWS_FILE.exists():
        try:
            df = pd.read_parquet(config.PROCESSED_NEWS_FILE)
            status["news"] = ticker in df["ticker"].values if "ticker" in df.columns else False
        except:
            pass
    elif (config.PROCESSED_NEWS_DIR / f"{ticker}_news.parquet").exists():
        status["news"] = True
    
    # Check fundamentals
    if config.PROCESSED_FUNDAMENTALS_FILE.exists():
        try:
            df = pd.read_parquet(config.PROCESSED_FUNDAMENTALS_FILE)
            status["fundamentals"] = ticker in df["ticker"].values if "ticker" in df.columns else True
        except:
            pass
    elif (config.PROCESSED_FUNDAMENTALS_DIR / f"{ticker}_fundamentals.parquet").exists():
        status["fundamentals"] = True
    
    return status


@app.get("/api/search")
async def search(
    query: str,
    doc_type: Optional[str] = None,
    ticker: Optional[str] = None,
    k: int = 10,
    min_score: float = 0.0,
    auto_etl: bool = False,
    rebuild_index: bool = True,
):
    """Search over financial documents with automatic ETL pipeline.
    
    When ticker is provided, results for that ticker are prioritized (shown first)
    but other relevant results are still included if there aren't enough ticker matches.
    """
    try:
        auto_status = None
        if auto_etl:
            # Potentially expensive: fetch/process data and rebuild indices (depending on orchestrator settings)
            auto_status = run_autonomous(query, ticker_hint=ticker)
        if rebuild_index:
            # Explicit rebuild (expensive). Prefer calling /api/search/rebuild-indices out of band.
            service = get_retrieval_service()
            service.rebuild_indices(ticker=None)
        
        # Perform search
        service = get_retrieval_service()
        results = service.search(
            query=query,
            doc_type=doc_type,
            ticker=ticker,
            k=k,
            min_score=min_score
        )
        
        return {
            "query": query,
            "count": len(results),
            "results": results,
            "auto_etl": auto_status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/api/search/news")
async def search_news(
    query: str,
    ticker: Optional[str] = None,
    k: int = 10
):
    """Search news articles."""
    try:
        service = get_retrieval_service()
        results = service.search_news(query, ticker=ticker, k=k)
        return {
            "query": query,
            "count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News search failed: {str(e)}")


@app.get("/api/search/filings")
async def search_filings(
    query: str,
    ticker: Optional[str] = None,
    k: int = 10
):
    """Search SEC filings."""
    try:
        service = get_retrieval_service()
        results = service.search_filings(query, ticker=ticker, k=k)
        return {
            "query": query,
            "count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Filings search failed: {str(e)}")


@app.get("/api/search/transcripts")
async def search_transcripts(
    query: str,
    ticker: Optional[str] = None,
    k: int = 10
):
    """Search earnings call transcripts."""
    try:
        service = get_retrieval_service()
        results = service.search_transcripts(query, ticker=ticker, k=k)
        return {
            "query": query,
            "count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transcripts search failed: {str(e)}")


@app.post("/api/search/rebuild-indices")
async def rebuild_indices(ticker: Optional[str] = None, doc_types: Optional[str] = None):
    """Rebuild vector indices (useful after ETL updates)."""
    try:
        service = get_retrieval_service()
        doc_type_set = None
        if doc_types:
            doc_type_set = {dt.strip() for dt in doc_types.split(",") if dt.strip()}
        service.rebuild_indices(ticker=ticker, doc_types=doc_type_set)
        return {
            "status": "success",
            "message": f"Indices rebuilt for ticker: {ticker or 'all'}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Index rebuild failed: {str(e)}")


class DocumentRequest(BaseModel):
    """Request model for retrieving full document."""
    doc_type: str
    ticker: Optional[str] = None
    index: Optional[int] = None
    filing_file: Optional[str] = None
    transcript_file: Optional[str] = None


@app.post("/api/document")
async def get_document(request: DocumentRequest):
    """Retrieve full document by metadata from search result."""
    try:
        doc_type = request.doc_type
        ticker = request.ticker
        
        # Remove embedding column for API response (too large)
        def remove_embedding(data: dict) -> dict:
            if isinstance(data, dict):
                return {k: v for k, v in data.items() if k != 'embedding'}
            return data
        
        def select_row(df: pd.DataFrame) -> dict:
            """Select a row by index label if possible; otherwise fall back to positional iloc."""
            if df is None or df.empty:
                raise HTTPException(status_code=404, detail="No data found")
            if request.index is not None:
                # Prefer label-based match if the stored index came from iterrows() (often a label index)
                try:
                    if request.index in df.index:
                        row = df.loc[request.index]
                        if isinstance(row, pd.DataFrame):
                            row = row.iloc[0]
                        return row.to_dict()
                except Exception:
                    pass
                # Fall back to positional index
                if 0 <= request.index < len(df):
                    return df.iloc[request.index].to_dict()
            return df.iloc[0].to_dict()

        if doc_type == 'news':
            # Load news parquet file
            if config.PROCESSED_NEWS_FILE.exists():
                df = pd.read_parquet(config.PROCESSED_NEWS_FILE)
                if ticker and 'ticker' in df.columns:
                    df = df[df['ticker'].str.upper() == ticker.upper()]
            else:
                filepath = config.PROCESSED_NEWS_DIR / f"{ticker.upper()}_news.parquet"
                if not filepath.exists():
                    raise HTTPException(status_code=404, detail=f"News file not found for {ticker}")
                df = pd.read_parquet(filepath)
            
            if df.empty:
                raise HTTPException(status_code=404, detail="No news data found")
            
            doc = select_row(df)
            
            return {
                "doc_type": "news",
                "document": clean_dataframe_for_json([doc])[0]
            }
        
        elif doc_type == 'filing':
            if not request.filing_file:
                raise HTTPException(status_code=400, detail="filing_file is required for filing documents")
            
            # Load specific filing file
            filepath = config.PROCESSED_FILINGS_DIR / f"{request.filing_file}.parquet"
            if not filepath.exists():
                raise HTTPException(status_code=404, detail=f"Filing file not found: {request.filing_file}")
            
            df = pd.read_parquet(filepath)
            if df.empty:
                raise HTTPException(status_code=404, detail="Filing file is empty")
            
            doc = select_row(df)
            
            return {
                "doc_type": "filing",
                "document": clean_dataframe_for_json([doc])[0]
            }
        
        elif doc_type == 'transcript':
            if not request.transcript_file:
                raise HTTPException(status_code=400, detail="transcript_file is required for transcript documents")
            
            # Load specific transcript file
            filepath = config.PROCESSED_TRANSCRIPTS_DIR / f"{request.transcript_file}.parquet"
            if not filepath.exists():
                raise HTTPException(status_code=404, detail=f"Transcript file not found: {request.transcript_file}")
            
            df = pd.read_parquet(filepath)
            if df.empty:
                raise HTTPException(status_code=404, detail="Transcript file is empty")
            
            doc = select_row(df)
            
            return {
                "doc_type": "transcript",
                "document": clean_dataframe_for_json([doc])[0]
            }
        
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported document type: {doc_type}")
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve document: {str(e)}")


# Agent endpoints
class AgentQueryRequest(BaseModel):
    """Request model for agent queries."""
    query: str
    ticker: Optional[str] = None
    agent_type: str = "research"
    auto_etl: bool = False
    rebuild_index: bool = False


# Global agent instance
_research_agent = None

def get_research_agent() -> ResearchAgent:
    """Get or create global research agent instance."""
    global _research_agent
    if _research_agent is None:
        _research_agent = ResearchAgent()
    return _research_agent


@app.post("/api/agent/query")
async def agent_query(request: AgentQueryRequest):
    """Query the research agent with a natural language question."""
    try:
        auto_status = None
        if request.auto_etl:
            auto_status = run_autonomous(request.query, ticker_hint=request.ticker)
        if request.rebuild_index:
            service = get_retrieval_service()
            service.rebuild_indices(ticker=None)
        agent = get_research_agent()
        
        context = {}
        # Use inferred ticker from auto_status if available, otherwise use provided ticker
        inferred_ticker = auto_status.get("ticker") if isinstance(auto_status, dict) else None
        ticker_to_use = inferred_ticker or request.ticker
        if ticker_to_use:
            context["ticker"] = ticker_to_use.upper()
        
        response = await agent.process_query(
            query=request.query,
            context=context if context else None
        )
        
        return {"auto": auto_status, "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent query failed: {str(e)}")


@app.post("/api/agent/research")
async def research_topic(
    topic: str,
    ticker: Optional[str] = None,
    doc_types: Optional[str] = None
):
    """Research a specific topic using the research agent."""
    try:
        agent = get_research_agent()
        
        doc_type_list = None
        if doc_types:
            doc_type_list = [dt.strip() for dt in doc_types.split(",")]
        
        response = await agent.research_topic(
            topic=topic,
            ticker=ticker.upper() if ticker else None,
            doc_types=doc_type_list
        )
        
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Research failed: {str(e)}")


@app.get("/api/agent/status")
async def agent_status():
    """Get status of the agent system."""
    try:
        agent = get_research_agent()
        return {
            "status": "active",
            "agent": agent.name,
            "model": agent.model,
            "tools_available": len(agent.tools),
            "memory_size": len(agent.memory)
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=config.API_HOST,
        port=config.API_PORT
    )

