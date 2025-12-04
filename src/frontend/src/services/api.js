import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * Trigger ETL pipeline for a ticker
 * @param {string} ticker - Stock ticker symbol
 * @returns {Promise} API response
 */
export const triggerETL = async (ticker) => {
  try {
    const response = await api.post(`/api/etl/run/${ticker.toUpperCase()}`);
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Failed to trigger ETL');
  }
};

/**
 * Get ETL status for a ticker
 * @param {string} ticker - Stock ticker symbol
 * @returns {Promise} Status object with data availability
 */
export const getStatus = async (ticker) => {
  try {
    const response = await api.get(`/api/etl/status/${ticker.toUpperCase()}`);
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Failed to get status');
  }
};

/**
 * Get processed features for a ticker
 * @param {string} ticker - Stock ticker symbol
 * @returns {Promise} Features data
 */
export const getFeatures = async (ticker) => {
  try {
    const response = await api.get(`/api/ticker/${ticker.toUpperCase()}/features`);
    return response.data;
  } catch (error) {
    if (error.response?.status === 404) {
      return null; // No data available
    }
    throw new Error(error.response?.data?.detail || error.message || 'Failed to get features');
  }
};

/**
 * Get processed prices for a ticker
 * @param {string} ticker - Stock ticker symbol
 * @returns {Promise} Prices data
 */
export const getPrices = async (ticker) => {
  try {
    const response = await api.get(`/api/ticker/${ticker.toUpperCase()}/prices`);
    return response.data;
  } catch (error) {
    if (error.response?.status === 404) {
      return null; // No data available
    }
    throw new Error(error.response?.data?.detail || error.message || 'Failed to get prices');
  }
};

/**
 * Get processed news for a ticker
 * @param {string} ticker - Stock ticker symbol
 * @returns {Promise} News data
 */
export const getNews = async (ticker) => {
  try {
    const response = await api.get(`/api/ticker/${ticker.toUpperCase()}/news`);
    return response.data;
  } catch (error) {
    if (error.response?.status === 404) {
      return null; // No data available
    }
    throw new Error(error.response?.data?.detail || error.message || 'Failed to get news');
  }
};

/**
 * Get processed fundamentals for a ticker
 * @param {string} ticker - Stock ticker symbol
 * @returns {Promise} Fundamentals data
 */
export const getFundamentals = async (ticker) => {
  try {
    const response = await api.get(`/api/ticker/${ticker.toUpperCase()}/fundamentals`);
    return response.data;
  } catch (error) {
    if (error.response?.status === 404) {
      return null; // No data available
    }
    throw new Error(error.response?.data?.detail || error.message || 'Failed to get fundamentals');
  }
};

/**
 * Semantic search across all document types
 * @param {string} query - Search query
 * @param {string} docType - Optional document type filter (news, filing, transcript)
 * @param {string} ticker - Optional ticker filter
 * @param {number} k - Number of results (default: 10)
 * @param {number} minScore - Minimum similarity score (default: 0.0)
 * @returns {Promise} Search results
 */
export const searchDocuments = async (query, docType = null, ticker = null, k = 10, minScore = 0.0) => {
  try {
    const params = new URLSearchParams({ query, k: k.toString(), min_score: minScore.toString() });
    if (docType) params.append('doc_type', docType);
    if (ticker) params.append('ticker', ticker.toUpperCase());
    
    const response = await api.get(`/api/search?${params.toString()}`);
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Search failed');
  }
};

/**
 * Search news articles
 * @param {string} query - Search query
 * @param {string} ticker - Optional ticker filter
 * @param {number} k - Number of results (default: 10)
 * @returns {Promise} News search results
 */
export const searchNews = async (query, ticker = null, k = 10) => {
  try {
    const params = new URLSearchParams({ query, k: k.toString() });
    if (ticker) params.append('ticker', ticker.toUpperCase());
    
    const response = await api.get(`/api/search/news?${params.toString()}`);
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'News search failed');
  }
};

/**
 * Search SEC filings
 * @param {string} query - Search query
 * @param {string} ticker - Optional ticker filter
 * @param {number} k - Number of results (default: 10)
 * @returns {Promise} Filings search results
 */
export const searchFilings = async (query, ticker = null, k = 10) => {
  try {
    const params = new URLSearchParams({ query, k: k.toString() });
    if (ticker) params.append('ticker', ticker.toUpperCase());
    
    const response = await api.get(`/api/search/filings?${params.toString()}`);
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Filings search failed');
  }
};

/**
 * Search earnings call transcripts
 * @param {string} query - Search query
 * @param {string} ticker - Optional ticker filter
 * @param {number} k - Number of results (default: 10)
 * @returns {Promise} Transcripts search results
 */
export const searchTranscripts = async (query, ticker = null, k = 10) => {
  try {
    const params = new URLSearchParams({ query, k: k.toString() });
    if (ticker) params.append('ticker', ticker.toUpperCase());
    
    const response = await api.get(`/api/search/transcripts?${params.toString()}`);
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Transcripts search failed');
  }
};

/**
 * Rebuild vector indices
 * @param {string} ticker - Optional ticker filter
 * @returns {Promise} Rebuild status
 */
export const rebuildIndices = async (ticker = null) => {
  try {
    const params = ticker ? new URLSearchParams({ ticker: ticker.toUpperCase() }) : new URLSearchParams();
    const response = await api.post(`/api/search/rebuild-indices?${params.toString()}`);
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Failed to rebuild indices');
  }
};

export default api;

