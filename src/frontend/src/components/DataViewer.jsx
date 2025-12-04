import { useState, useEffect } from 'react';
import { getFeatures, getPrices, getNews, getFundamentals } from '../services/api';
import PriceChart from './PriceChart';
import NewsList from './NewsList';
import FundamentalsTable from './FundamentalsTable';

const DataViewer = ({ ticker }) => {
  const [activeTab, setActiveTab] = useState('features');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!ticker) {
      setData(null);
      return;
    }

    const fetchData = async () => {
      setLoading(true);
      setError(null);

      try {
        let result = null;
        switch (activeTab) {
          case 'features':
            result = await getFeatures(ticker);
            break;
          case 'prices':
            result = await getPrices(ticker);
            break;
          case 'news':
            result = await getNews(ticker);
            break;
          case 'fundamentals':
            result = await getFundamentals(ticker);
            break;
          default:
            result = null;
        }
        setData(result);
      } catch (err) {
        setError(err.message || 'Failed to fetch data');
        setData(null);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [ticker, activeTab]);

  if (!ticker) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-2xl font-bold mb-4 text-gray-800">Data Viewer</h2>
        <p className="text-gray-500">Select a ticker to view data</p>
      </div>
    );
  }

  const tabs = [
    { id: 'features', label: 'Features' },
    { id: 'prices', label: 'Prices' },
    { id: 'news', label: 'News' },
    { id: 'fundamentals', label: 'Fundamentals' },
  ];

  const renderContent = () => {
    if (loading) {
      return (
        <div className="flex items-center justify-center py-12">
          <svg className="animate-spin h-8 w-8 text-blue-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
        </div>
      );
    }

    if (error) {
      return (
        <div className="p-4 bg-red-100 border border-red-400 text-red-700 rounded-md">
          {error}
        </div>
      );
    }

    if (!data) {
      return (
        <div className="p-4 bg-yellow-100 border border-yellow-400 text-yellow-700 rounded-md">
          No data available for {ticker}. Run the ETL pipeline first.
        </div>
      );
    }

    switch (activeTab) {
      case 'features':
        return (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  {data.data && data.data.length > 0 && Object.keys(data.data[0]).map((key) => (
                    <th key={key} className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {data.data && data.data.slice(0, 100).map((row, idx) => {
                  const keys = Object.keys(row);
                  return (
                    <tr key={idx}>
                      {keys.map((key, i) => {
                        const value = row[key];
                        let displayValue;
                        
                        if (value === null || value === undefined) {
                          displayValue = 'N/A';
                        } else if (typeof value === 'number') {
                          // Format numbers based on their magnitude
                          if (Math.abs(value) >= 1000000) {
                            displayValue = (value / 1000000).toFixed(2) + 'M';
                          } else if (Math.abs(value) >= 1000) {
                            displayValue = (value / 1000).toFixed(2) + 'K';
                          } else if (Math.abs(value) < 1 && value !== 0) {
                            // Small decimals (like returns, sentiment)
                            displayValue = value.toFixed(4);
                          } else {
                            // Regular numbers (prices, volumes)
                            displayValue = value.toLocaleString('en-US', { 
                              minimumFractionDigits: 2, 
                              maximumFractionDigits: 2 
                            });
                          }
                        } else if (typeof value === 'object') {
                          displayValue = JSON.stringify(value);
                        } else {
                          displayValue = String(value);
                        }
                        
                        return (
                          <td key={i} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                            {displayValue}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {data.count > 100 && (
              <p className="mt-4 text-sm text-gray-500">Showing first 100 of {data.count} records</p>
            )}
          </div>
        );
      case 'prices':
        return <PriceChart data={data.data || []} />;
      case 'news':
        return <NewsList data={data.data || []} />;
      case 'fundamentals':
        return <FundamentalsTable data={data.data || []} />;
      default:
        return null;
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <h2 className="text-2xl font-bold mb-4 text-gray-800">Data Viewer</h2>
      <p className="text-sm text-gray-600 mb-4">Viewing data for: <span className="font-semibold">{ticker}</span></p>

      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === tab.id
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      <div className="mt-6">
        {renderContent()}
      </div>
    </div>
  );
};

export default DataViewer;

