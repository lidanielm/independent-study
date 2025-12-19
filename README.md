# Financial Research Platform

A comprehensive financial research platform that extracts, processes, and analyzes stock market data with search capabilities.

## Features
Data ingestion & processing, ETL pipeline, search, AI research agent, data visualization, frontend UI
<!-- ### Data Ingestion & Processing
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

### Search
- Vector Retrieval: Search across news, filings, and transcripts using natural language
- Similarity Search: Find relevant documents based on semantic meaning
- Filtered Search: Search by ticker or document type
- Index Management: Rebuild indices on-demand

### AI Research Agent
- Natural Language Queries: Ask questions in plain English
- Intelligent Synthesis: Combines information from multiple sources
- Document Search: Automatically searches news, filings, and transcripts
- Source Citations: Provides references to source documents
- Conversational Interface: Chat-based interaction with context awareness

### Data Visualization
- Interactive Charts: Price charts with technical indicators
- Data Tables: Formatted tables for features, news, and fundamentals
- Number Formatting: Human-readable number formatting (K/M suffixes, decimals)

### Web Interface
- ETL Dashboard: Trigger and monitor ETL pipelines
- Data Explorer: Browse processed data by ticker
- Search Interface: Search across financial documents
- AI Agent Chat: Conversational interface for research queries
- Status Monitor: Real-time pipeline status tracking -->

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
- API keys:
  - OpenAI API key (for AI agent) - set in `.env` as `OPENAI_API_KEY` (required for agent)
  - Alpha Vantage API key (for fundamentals) - set in `.env` as `AV_API_KEY` (optional)

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

3. Set up environment variables:
```bash
# Create .env file
echo "OPENAI_API_KEY=your_openai_key" > .env
echo "AV_API_KEY=your_alpha_vantage_key" >> .env  # Optional
```

**Note**: The OpenAI API key is required for the AI Agent feature. Get your key from https://platform.openai.com/api-keys

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
<!-- 
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
- `GET /api/search?query=...&k=10` - Search across all documents
- `GET /api/search/news?query=...&ticker=AAPL` - Search news
- `GET /api/search/filings?query=...&ticker=AAPL` - Search filings
- `GET /api/search/transcripts?query=...&ticker=AAPL` - Search transcripts
- `POST /api/search/rebuild-indices` - Rebuild vector indices

### Agent Endpoints
- `POST /api/agent/query` - Query the research agent with natural language
- `POST /api/agent/research` - Research a specific topic
- `GET /api/agent/status` - Get agent system status -->

## Technology Stack

### Backend
FastAPI, Pandas, VADER, OpenAI, yfinance, secedgar
<!-- - FastAPI: REST API framework
- Pandas: Data processing
- FAISS: Vector similarity search
- Sentence Transformers: Text embeddings
- VADER: Sentiment analysis
- OpenAI: LLM for AI agents
- yfinance: Stock price data
- secedgar: SEC filings access -->

### Frontend
React, Vite, TailwindCSS, Axios
<!-- - React: UI framework
- Vite: Build tool
- Tailwind CSS: Styling
- Recharts: Data visualization
- Axios: HTTP client -->

<!-- ## Data Flow

1. Extract: Fetch data from APIs (Yahoo Finance, SEC EDGAR, earningscall)
2. Transform: Clean, normalize, and compute features
3. Load: Save processed data and build vector indices
4. Search: Query vector indices for semantic search -->

## AI Usage

This project was developed with moderate assistance from AI tools. AI was used to:
- Generate boilerplate code for API endpoints and React components
- Debug and fix data processing issues
- Refactor and clean up code structure
- Write documentation and README content
- Implement the agentic AI system for financial research

