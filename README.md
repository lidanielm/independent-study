# Financial Data ETL & Analytics Platform

A comprehensive financial data pipeline and analytics platform that extracts, processes, and analyzes stock market data with semantic search capabilities.

## Features

### Data Ingestion & Processing
- Price Data: Historical stock prices from Yahoo Finance (5+ years)
- News Data: Financial news articles with sentiment analysis
- SEC Filings: 10-K and 10-Q filings with section extraction (MD&A, Risk Factors)
- Earnings Transcripts: Earnings call transcripts with speaker segmentation
- Fundamentals: Financial statement data (income statements, balance sheets)

### ETL Pipeline
- Automated Workflow: End-to-end Extract, Transform, Load pipeline
- Data Cleaning: Normalized column names, date handling, gap filling
- Feature Engineering: 
  - Technical indicators (returns, momentum, volatility)
  - News sentiment aggregation
  - Text embeddings for semantic search
- Vector Indices: FAISS-based vector stores for efficient similarity search

### Semantic Search
- Vector Retrieval: Search across news, filings, and transcripts using natural language
- Similarity Search: Find relevant documents based on semantic meaning
- Filtered Search: Search by ticker or document type
- Index Management: Rebuild indices on-demand

### Data Visualization
- Interactive Charts: Price charts with technical indicators
- Data Tables: Formatted tables for features, news, and fundamentals
- Number Formatting: Human-readable number formatting (K/M suffixes, decimals)

### Web Interface
- ETL Dashboard: Trigger and monitor ETL pipelines
- Data Explorer: Browse processed data by ticker
- Search Interface: Semantic search across financial documents
- Status Monitor: Real-time pipeline status tracking

## Architecture

```
src/
├── backend/
│   ├── api/           # FastAPI REST API
│   ├── etl/           # ETL orchestration
│   ├── ingestion/     # Data extraction modules
│   ├── processing/    # Data transformation
│   ├── retrieval/     # Vector search system
│   └── utils/         # NLP and text processing utilities
└── frontend/          # React + Vite frontend
```

## Quick Start

### Prerequisites
- Python 3.9+
- Node.js 16+
- API keys (optional):
  - Alpha Vantage API key (for fundamentals) - set in `.env` as `AV_API_KEY`

### Backend Setup

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables (optional):
```bash
# Create .env file
echo "AV_API_KEY=your_alpha_vantage_key" > .env
```

4. Start API server:
```bash
cd src/backend/api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend Setup

1. Install dependencies:
```bash
cd src/frontend
npm install
```

2. Start development server:
```bash
npm run dev
```

The frontend will be available at `http://localhost:5173`

### Running ETL Pipeline

Via API:
```bash
curl -X POST http://localhost:8000/api/etl/run/AAPL
```

Via Python:
```bash
python test_etl.py
```

## API Endpoints

### Data Endpoints
- `GET /api/ticker/{ticker}/features` - Get processed features
- `GET /api/ticker/{ticker}/prices` - Get price data
- `GET /api/ticker/{ticker}/news` - Get news articles
- `GET /api/ticker/{ticker}/fundamentals` - Get fundamentals

### ETL Endpoints
- `POST /api/etl/run/{ticker}` - Run ETL pipeline
- `GET /api/etl/status/{ticker}` - Get pipeline status

### Search Endpoints
- `GET /api/search?query=...&k=10` - Semantic search across all documents
- `GET /api/search/news?query=...&ticker=AAPL` - Search news
- `GET /api/search/filings?query=...&ticker=AAPL` - Search filings
- `GET /api/search/transcripts?query=...&ticker=AAPL` - Search transcripts
- `POST /api/search/rebuild-indices` - Rebuild vector indices

## Technology Stack

### Backend
- FastAPI: REST API framework
- Pandas: Data processing
- FAISS: Vector similarity search
- Sentence Transformers: Text embeddings
- VADER: Sentiment analysis
- yfinance: Stock price data
- secedgar: SEC filings access

### Frontend
- React: UI framework
- Vite: Build tool
- Tailwind CSS: Styling
- Recharts: Data visualization
- Axios: HTTP client

## Data Flow

1. Extract: Fetch data from APIs (Yahoo Finance, SEC EDGAR, earningscall)
2. Transform: Clean, normalize, and compute features
3. Load: Save processed data and build vector indices
4. Search: Query vector indices for semantic search

## Project Structure

```
.
├── src/
│   ├── backend/
│   │   ├── api/              # FastAPI application
│   │   ├── etl/              # ETL orchestration
│   │   ├── ingestion/        # Data extraction
│   │   ├── processing/       # Data transformation
│   │   ├── retrieval/        # Vector search
│   │   └── utils/            # Utilities
│   └── frontend/             # React application
├── data/                     # Data storage (gitignored)
├── test_etl.py              # ETL test script
├── test_vector_retrieval.py # Vector search test
└── README.md                # This file
```

## AI Usage

This project was developed with moderate assistance from AI tools. AI was used to:
- Generate boilerplate code for API endpoints and React components
- Debug and fix data processing issues
- Refactor and clean up code structure
- Write documentation and README content

