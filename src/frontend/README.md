# ETL Workflow Frontend

React frontend application for the ETL workflow system.

## Setup

1. Install dependencies:
```bash
npm install
```

2. Create `.env` file (optional, defaults to http://localhost:8000):
```bash
cp .env.example .env
```

3. Start development server:
```bash
npm run dev
```

The app will be available at http://localhost:5173

## Build for Production

```bash
npm run build
```

## Features

- **ETL Pipeline Page**: Trigger and monitor ETL pipelines
- **Data Explorer**: View processed financial data (features, prices, news, fundamentals)
- **Dashboard**: Overview and quick access to features

## API Connection

The frontend connects to the FastAPI backend. Make sure the backend is running on port 8000 (or update VITE_API_URL in .env).
