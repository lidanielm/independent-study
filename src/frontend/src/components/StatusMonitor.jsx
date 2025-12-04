import { useState, useEffect } from 'react';
import { getStatus } from '../services/api';

const StatusMonitor = ({ ticker, autoRefresh = true, refreshInterval = 5000 }) => {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchStatus = async () => {
    if (!ticker) return;

    setLoading(true);
    setError(null);

    try {
      const data = await getStatus(ticker);
      setStatus(data);
    } catch (err) {
      setError(err.message || 'Failed to fetch status');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (ticker) {
      fetchStatus();
    }
  }, [ticker]);

  useEffect(() => {
    if (!autoRefresh || !ticker) return;

    const interval = setInterval(fetchStatus, refreshInterval);
    return () => clearInterval(interval);
  }, [ticker, autoRefresh, refreshInterval]);

  if (!ticker) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-2xl font-bold mb-4 text-gray-800">Status Monitor</h2>
        <p className="text-gray-500">Enter a ticker symbol to check status</p>
      </div>
    );
  }

  const StatusBadge = ({ available, label }) => (
    <div className="flex items-center justify-between p-3 bg-gray-50 rounded-md">
      <span className="text-sm font-medium text-gray-700">{label}</span>
      {available ? (
        <span className="px-2 py-1 text-xs font-semibold text-green-800 bg-green-100 rounded-full">
          Available
        </span>
      ) : (
        <span className="px-2 py-1 text-xs font-semibold text-gray-800 bg-gray-200 rounded-full">
          Not Available
        </span>
      )}
    </div>
  );

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold text-gray-800">Status Monitor</h2>
        <button
          onClick={fetchStatus}
          disabled={loading}
          className="px-4 py-2 text-sm font-medium text-blue-600 bg-blue-50 rounded-md hover:bg-blue-100 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50"
        >
          {loading ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      <div className="mb-4">
        <p className="text-sm text-gray-600">
          Monitoring: <span className="font-semibold text-gray-800">{ticker}</span>
        </p>
        {autoRefresh && (
          <p className="text-xs text-gray-500 mt-1">
            Auto-refreshing every {refreshInterval / 1000} seconds
          </p>
        )}
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded-md">
          {error}
        </div>
      )}

      {loading && !status ? (
        <div className="flex items-center justify-center py-8">
          <svg className="animate-spin h-8 w-8 text-blue-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
        </div>
      ) : status ? (
        <div className="space-y-3">
          <StatusBadge available={status.features} label="Features" />
          <StatusBadge available={status.prices} label="Prices" />
          <StatusBadge available={status.news} label="News" />
          <StatusBadge available={status.fundamentals} label="Fundamentals" />
        </div>
      ) : null}
    </div>
  );
};

export default StatusMonitor;

