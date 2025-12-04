import { useState } from 'react';
import { searchDocuments, searchNews, searchFilings, searchTranscripts, rebuildIndices } from '../services/api';

const SearchInterface = () => {
  const [query, setQuery] = useState('');
  const [docType, setDocType] = useState('all');
  const [ticker, setTicker] = useState('');
  const [k, setK] = useState(10);
  const [minScore, setMinScore] = useState(0.0);
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [rebuilding, setRebuilding] = useState(false);

  const handleSearch = async () => {
    if (!query.trim()) {
      setError('Please enter a search query');
      return;
    }

    setLoading(true);
    setError(null);
    setResults(null);

    try {
      let data;
      if (docType === 'all') {
        data = await searchDocuments(query, null, ticker || null, k, minScore);
      } else if (docType === 'news') {
        data = await searchNews(query, ticker || null, k);
      } else if (docType === 'filing') {
        data = await searchFilings(query, ticker || null, k);
      } else if (docType === 'transcript') {
        data = await searchTranscripts(query, ticker || null, k);
      }

      setResults(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRebuild = async () => {
    setRebuilding(true);
    setError(null);
    try {
      await rebuildIndices(ticker || null);
      alert('Indices rebuilt successfully!');
    } catch (err) {
      setError(err.message);
    } finally {
      setRebuilding(false);
    }
  };

  const formatDate = (dateString) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleDateString();
    } catch {
      return dateString;
    }
  };

  const getDocTypeColor = (type) => {
    const colors = {
      news: 'bg-blue-100 text-blue-800',
      filing: 'bg-green-100 text-green-800',
      transcript: 'bg-purple-100 text-purple-800',
    };
    return colors[type] || 'bg-gray-100 text-gray-800';
  };

  return (
    <div className="space-y-6">
      {/* Search Form */}
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Semantic Search</h2>
        
        <div className="space-y-4">
          {/* Query Input */}
          <div>
            <label htmlFor="query" className="block text-sm font-medium text-gray-700 mb-2">
              Search Query
            </label>
            <input
              type="text"
              id="query"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
              placeholder="e.g., artificial intelligence, revenue growth, risk factors"
              className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>

          {/* Filters */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div>
              <label htmlFor="docType" className="block text-sm font-medium text-gray-700 mb-2">
                Document Type
              </label>
              <select
                id="docType"
                value={docType}
                onChange={(e) => setDocType(e.target.value)}
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              >
                <option value="all">All Documents</option>
                <option value="news">News</option>
                <option value="filing">SEC Filings</option>
                <option value="transcript">Transcripts</option>
              </select>
            </div>

            <div>
              <label htmlFor="ticker" className="block text-sm font-medium text-gray-700 mb-2">
                Ticker (Optional)
              </label>
              <input
                type="text"
                id="ticker"
                value={ticker}
                onChange={(e) => setTicker(e.target.value.toUpperCase())}
                placeholder="AAPL, MSFT, etc."
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>

            <div>
              <label htmlFor="k" className="block text-sm font-medium text-gray-700 mb-2">
                Results (k)
              </label>
              <input
                type="number"
                id="k"
                value={k}
                onChange={(e) => setK(parseInt(e.target.value) || 10)}
                min="1"
                max="50"
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>

            <div>
              <label htmlFor="minScore" className="block text-sm font-medium text-gray-700 mb-2">
                Min Score
              </label>
              <input
                type="number"
                id="minScore"
                value={minScore}
                onChange={(e) => setMinScore(parseFloat(e.target.value) || 0.0)}
                min="0"
                max="1"
                step="0.1"
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
          </div>

          {/* Actions */}
          <div className="flex gap-3">
            <button
              onClick={handleSearch}
              disabled={loading}
              className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-blue-400 disabled:cursor-not-allowed transition-colors"
            >
              {loading ? 'Searching...' : 'Search'}
            </button>
            <button
              onClick={handleRebuild}
              disabled={rebuilding}
              className="px-6 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
            >
              {rebuilding ? 'Rebuilding...' : 'Rebuild Indices'}
            </button>
          </div>
        </div>
      </div>

      {/* Error Message */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-md p-4">
          <p className="text-red-800">{error}</p>
        </div>
      )}

      {/* Results */}
      {results && (
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-lg font-semibold text-gray-900">
              Search Results ({results.count || 0})
            </h3>
            {results.query && (
              <span className="text-sm text-gray-500">Query: "{results.query}"</span>
            )}
          </div>

          {results.results && results.results.length > 0 ? (
            <div className="space-y-4">
              {results.results.map((result, index) => (
                <div
                  key={index}
                  className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
                >
                  <div className="flex items-start justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className={`px-2 py-1 rounded text-xs font-medium ${getDocTypeColor(result.doc_type)}`}>
                        {result.doc_type}
                      </span>
                      {result.ticker && (
                        <span className="px-2 py-1 bg-gray-100 text-gray-700 rounded text-xs font-medium">
                          {result.ticker}
                        </span>
                      )}
                      {result.similarity_score && (
                        <span className="text-xs text-gray-500">
                          Score: {result.similarity_score.toFixed(3)}
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Title or Section */}
                  {(result.title || result.section) && (
                    <h4 className="font-semibold text-gray-900 mb-2">
                      {result.title || result.section}
                    </h4>
                  )}

                  {/* Text Preview */}
                  {result.text && (
                    <p className="text-gray-700 text-sm mb-2 line-clamp-3">
                      {result.text.length > 300 ? `${result.text.substring(0, 300)}...` : result.text}
                    </p>
                  )}

                  {/* Metadata */}
                  <div className="flex flex-wrap gap-4 text-xs text-gray-500 mt-2">
                    {result.published && (
                      <span>Published: {formatDate(result.published)}</span>
                    )}
                    {result.publisher && (
                      <span>Publisher: {result.publisher}</span>
                    )}
                    {result.speaker && (
                      <span>Speaker: {result.speaker}</span>
                    )}
                    {result.sentiment !== undefined && (
                      <span>
                        Sentiment: {result.sentiment > 0 ? '😊' : result.sentiment < 0 ? '😟' : '😐'} {result.sentiment.toFixed(2)}
                      </span>
                    )}
                  </div>

                  {/* Link */}
                  {result.link && (
                    <a
                      href={result.link}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-blue-600 hover:text-blue-800 text-sm mt-2 inline-block"
                    >
                      View Source →
                    </a>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-8 text-gray-500">
              <p>No results found. Try a different query or rebuild the indices.</p>
            </div>
          )}
        </div>
      )}

      {/* No Results Message */}
      {results && results.count === 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
          <p className="text-yellow-800">
            No results found. Make sure you've run the ETL pipeline to build indices, or try rebuilding them.
          </p>
        </div>
      )}
    </div>
  );
};

export default SearchInterface;

