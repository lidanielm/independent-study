import { useState } from 'react';
import { triggerETL } from '../services/api';

const ETLTrigger = ({ onTriggered }) => {
  const [ticker, setTicker] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  const [skipExtract, setSkipExtract] = useState(false);
  const [skipTransform, setSkipTransform] = useState(false);
  const [skipLoad, setSkipLoad] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!ticker.trim()) {
      setError('Please enter a ticker symbol');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(false);

    try {
      const result = await triggerETL(ticker.trim());
      setSuccess(true);
      if (onTriggered) {
        onTriggered(ticker.trim());
      }
      // Reset form after 2 seconds
      setTimeout(() => {
        setTicker('');
        setSuccess(false);
      }, 2000);
    } catch (err) {
      setError(err.message || 'Failed to trigger ETL pipeline');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <h2 className="text-2xl font-bold mb-4 text-gray-800">Trigger ETL Pipeline</h2>
      
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="ticker" className="block text-sm font-medium text-gray-700 mb-2">
            Ticker Symbol
          </label>
          <input
            type="text"
            id="ticker"
            value={ticker}
            onChange={(e) => setTicker(e.target.value.toUpperCase())}
            placeholder="e.g., AAPL"
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            disabled={loading}
          />
        </div>

        <div className="space-y-2">
          <p className="text-sm font-medium text-gray-700">Skip Steps (Optional):</p>
          <label className="flex items-center space-x-2">
            <input
              type="checkbox"
              checked={skipExtract}
              onChange={(e) => setSkipExtract(e.target.checked)}
              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
              disabled={loading}
            />
            <span className="text-sm text-gray-600">Skip Extract</span>
          </label>
          <label className="flex items-center space-x-2">
            <input
              type="checkbox"
              checked={skipTransform}
              onChange={(e) => setSkipTransform(e.target.checked)}
              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
              disabled={loading}
            />
            <span className="text-sm text-gray-600">Skip Transform</span>
          </label>
          <label className="flex items-center space-x-2">
            <input
              type="checkbox"
              checked={skipLoad}
              onChange={(e) => setSkipLoad(e.target.checked)}
              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
              disabled={loading}
            />
            <span className="text-sm text-gray-600">Skip Load</span>
          </label>
        </div>

        <button
          type="submit"
          disabled={loading || !ticker.trim()}
          className="w-full bg-blue-600 text-white py-2 px-4 rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
        >
          {loading ? (
            <span className="flex items-center justify-center">
              <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              Triggering ETL...
            </span>
          ) : (
            'Trigger ETL Pipeline'
          )}
        </button>
      </form>

      {error && (
        <div className="mt-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded-md">
          {error}
        </div>
      )}

      {success && (
        <div className="mt-4 p-3 bg-green-100 border border-green-400 text-green-700 rounded-md">
          ETL pipeline started successfully! Check the status monitor for progress.
        </div>
      )}
    </div>
  );
};

export default ETLTrigger;

