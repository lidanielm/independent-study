# Financial Research Platform

A financial research platform that extracts, processes, and analyzes stock market data with semantic search and an AI-powered research agent.

## Features

- **Data Ingestion**: Stock prices, financial news, SEC filings, earnings transcripts, fundamentals
- **ETL Pipeline**: Automated data processing with feature engineering and vector indexing
- **Semantic Search**: Vector-based search across financial documents
- **AI Research Agent**: Natural language queries with automatic document retrieval
- **Web Interface**: React frontend for data exploration and agent interaction

## Architecture

```
src/
├── backend/
│   ├── api/           # FastAPI REST API
│   ├── agents/        # AI research agent
│   ├── etl/           # ETL orchestration
│   ├── ingestion/     # Data extraction
│   ├── processing/    # Data transformation
│   ├── retrieval/     # Vector search
│   └── utils/         # NLP utilities
└── frontend/          # React + Vite
```

## Quick Start

### Prerequisites

- Python 3.9+
- Node.js 16+
- API keys: `OPENAI_API_KEY`, `AV_API_KEY`, `API_NINJAS_KEY`

### Setup

**Backend:**
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create .env file
echo "OPENAI_API_KEY=your_key" > .env

# Start API server
cd src/backend/api
uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd src/frontend
npm install
npm run dev
```

**Run ETL:**
```bash
curl -X POST http://localhost:8000/api/etl/run/AAPL
```

## API Endpoints

**Data:**
- `GET /api/ticker/{ticker}/features` - Processed features
- `GET /api/ticker/{ticker}/prices` - Price data
- `GET /api/ticker/{ticker}/news` - News articles
- `GET /api/ticker/{ticker}/fundamentals` - Fundamentals

**ETL:**
- `POST /api/etl/run/{ticker}` - Run ETL pipeline
- `GET /api/etl/status/{ticker}` - Pipeline status

**Search:**
- `GET /api/search?query=...&ticker=...&k=10` - Search all documents
- `GET /api/search/news?query=...` - Search news
- `GET /api/search/filings?query=...` - Search filings
- `GET /api/search/transcripts?query=...` - Search transcripts

**Agent:**
- `POST /api/agent/query` - Natural language query
- `POST /api/agent/research` - Research topic
- `GET /api/agent/status` - Agent status

## Technology Stack

**Backend:** FastAPI, Pandas, FAISS, Sentence Transformers, VADER, OpenAI, yfinance, secedgar  
**Frontend:** React, Vite, TailwindCSS, Axios  
**Storage:** Parquet, FAISS indices
