const NewsList = ({ data }) => {
  if (!data || data.length === 0) {
    return (
      <div className="p-4 text-center text-gray-500">
        No news data available
      </div>
    );
  }

  const getSentimentColor = (sentiment) => {
    if (!sentiment) return 'bg-gray-200 text-gray-700';
    if (sentiment > 0.1) return 'bg-green-100 text-green-800';
    if (sentiment < -0.1) return 'bg-red-100 text-red-800';
    return 'bg-yellow-100 text-yellow-800';
  };

  const formatDate = (dateString) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleDateString();
    } catch {
      return dateString;
    }
  };

  return (
    <div className="space-y-4">
      {data.map((article, idx) => (
        <div key={idx} className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow">
          <div className="flex items-start justify-between mb-2">
            <h3 className="text-lg font-semibold text-gray-800 flex-1">
              {article.title || 'No title'}
            </h3>
            {article.sentiment !== undefined && (
              <span className={`ml-4 px-2 py-1 text-xs font-semibold rounded-full ${getSentimentColor(article.sentiment)}`}>
                {article.sentiment > 0 ? 'Positive' : article.sentiment < 0 ? 'Negative' : 'Neutral'}
              </span>
            )}
          </div>
          
          {article.summary && (
            <p className="text-sm text-gray-600 mb-2">{article.summary}</p>
          )}
          
          <div className="flex items-center justify-between text-xs text-gray-500">
            <div className="flex items-center space-x-4">
              {article.publisher && (
                <span>Source: {article.publisher}</span>
              )}
              {article.published && (
                <span>{formatDate(article.published)}</span>
              )}
            </div>
            {article.link && (
              <a 
                href={article.link} 
                target="_blank" 
                rel="noopener noreferrer"
                className="text-blue-600 hover:text-blue-800"
              >
                Read more →
              </a>
            )}
          </div>
        </div>
      ))}
    </div>
  );
};

export default NewsList;

