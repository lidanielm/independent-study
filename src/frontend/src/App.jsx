import { useState } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import ETLTrigger from './components/ETLTrigger';
import StatusMonitor from './components/StatusMonitor';
import DataViewer from './components/DataViewer';
import SearchInterface from './components/SearchInterface';
import AgentChat from './components/AgentChat';

const Navigation = () => {
  const location = useLocation();
  
  const navItems = [
    { path: '/', label: 'Dashboard' },
    { path: '/search', label: 'Search' },
    { path: '/agent', label: 'AI Agent' },
    { path: '/etl', label: 'ETL Pipeline' },
    { path: '/data', label: 'Data Explorer' },
  ];

  return (
    <nav className="bg-white shadow-sm border-b border-gray-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex">
            <div className="flex-shrink-0 flex items-center">
              <h1 className="text-xl font-bold text-gray-900">Financial Research System</h1>
            </div>
            <div className="hidden sm:ml-6 sm:flex sm:space-x-8">
              {navItems.map((item) => (
                <Link
                  key={item.path}
                  to={item.path}
                  className={`inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium ${
                    location.pathname === item.path
                      ? 'border-blue-500 text-gray-900'
                      : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700'
                  }`}
                >
                  {item.label}
                </Link>
              ))}
            </div>
          </div>
        </div>
      </div>
    </nav>
  );
};

const ETLPipelinePage = () => {
  const [selectedTicker, setSelectedTicker] = useState('');

  const handleETLTriggered = (ticker) => {
    setSelectedTicker(ticker);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">ETL Pipeline</h1>
          <p className="mt-2 text-gray-600">Trigger and monitor ETL pipelines for financial data</p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ETLTrigger onTriggered={handleETLTriggered} />
          <StatusMonitor ticker={selectedTicker} autoRefresh={true} />
        </div>
      </div>
    </div>
  );
};

const DataExplorerPage = () => {
  const [ticker, setTicker] = useState('');

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Data Explorer</h1>
          <p className="mt-2 text-gray-600">Explore processed financial data</p>
        </div>

        <div className="mb-6">
          <label htmlFor="ticker-select" className="block text-sm font-medium text-gray-700 mb-2">
            Select Ticker
          </label>
          <input
            type="text"
            id="ticker-select"
            value={ticker}
            onChange={(e) => setTicker(e.target.value.toUpperCase())}
            placeholder="Enter ticker symbol (e.g., AAPL)"
            className="w-full max-w-xs px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
        </div>

        {ticker && <DataViewer ticker={ticker} />}
        {!ticker && (
          <div className="bg-white rounded-lg shadow-md p-6">
            <p className="text-gray-500">Enter a ticker symbol to view data</p>
          </div>
        )}
      </div>
    </div>
  );
};

const SearchPage = () => {
  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Search</h1>
          <p className="mt-2 text-gray-600">Search across news, SEC filings, and earnings transcripts using natural language</p>
        </div>
        <SearchInterface />
      </div>
    </div>
  );
};

const AgentPage = () => {
  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">AI Research Agent</h1>
          <p className="mt-2 text-gray-600">Ask questions and get intelligent answers synthesized from financial documents</p>
        </div>
        <AgentChat />
      </div>
    </div>
  );
};

const DashboardPage = () => {
  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Financial Research Dashboard</h1>
          <p className="mt-2 text-gray-600">Welcome to your financial data research platform. Get started by exploring the tools below.</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="bg-white rounded-lg shadow-md p-6">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">ETL Pipeline</h3>
            <p className="text-sm text-gray-600 mb-4">Trigger and monitor ETL pipelines</p>
            <Link
              to="/etl"
              className="inline-block bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 transition-colors"
            >
              Go to ETL Pipeline
            </Link>
          </div>

          <div className="bg-white rounded-lg shadow-md p-6">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">Data Explorer</h3>
            <p className="text-sm text-gray-600 mb-4">View processed financial data</p>
            <Link
              to="/data"
              className="inline-block bg-green-600 text-white px-4 py-2 rounded-md hover:bg-green-700 transition-colors"
            >
              Go to Data Explorer
            </Link>
          </div>

          <div className="bg-white rounded-lg shadow-md p-6">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">Search</h3>
            <p className="text-sm text-gray-600 mb-4">Search documents using natural language</p>
            <Link
              to="/search"
              className="inline-block bg-purple-600 text-white px-4 py-2 rounded-md hover:bg-purple-700 transition-colors"
            >
              Go to Search
            </Link>
          </div>

          <div className="bg-white rounded-lg shadow-md p-6">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">AI Research Agent</h3>
            <p className="text-sm text-gray-600 mb-4">Chat with AI to research financial topics</p>
            <Link
              to="/agent"
              className="inline-block bg-indigo-600 text-white px-4 py-2 rounded-md hover:bg-indigo-700 transition-colors"
            >
              Go to AI Agent
            </Link>
          </div>

          {/* <div className="bg-white rounded-lg shadow-md p-6">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">API Documentation</h3>
            <p className="text-sm text-gray-600 mb-4">View API endpoints and usage</p>
            <a
              href="http://localhost:8000/docs"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-block bg-gray-600 text-white px-4 py-2 rounded-md hover:bg-gray-700 transition-colors"
            >
              Open API Docs
            </a>
          </div> */}
        </div>

        {/* <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-800 mb-4">Getting Started</h2>
          <ol className="list-decimal list-inside space-y-2 text-gray-600">
            <li>Navigate to the <Link to="/etl" className="text-blue-600 hover:text-blue-800">ETL Pipeline</Link> page</li>
            <li>Enter a ticker symbol (e.g., AAPL, MSFT, GOOG)</li>
            <li>Click "Trigger ETL Pipeline" to start processing</li>
            <li>Monitor the status in real-time</li>
            <li>Once complete, explore the data in the <Link to="/data" className="text-green-600 hover:text-green-800">Data Explorer</Link></li>
            <li>Use <Link to="/search" className="text-purple-600 hover:text-purple-800">Search</Link> or the <Link to="/agent" className="text-indigo-600 hover:text-indigo-800">AI Agent</Link> to query the data</li>
          </ol>
        </div> */}
      </div>
    </div>
  );
};

function App() {
  return (
    <Router>
      <div className="min-h-screen bg-gray-50">
        <Navigation />
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/agent" element={<AgentPage />} />
          <Route path="/etl" element={<ETLPipelinePage />} />
          <Route path="/data" element={<DataExplorerPage />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
