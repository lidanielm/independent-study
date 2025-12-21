import { useEffect } from 'react';

const DocumentModal = ({ document: doc, isOpen, onClose }) => {
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = 'unset';
    }
    return () => {
      document.body.style.overflow = 'unset';
    };
  }, [isOpen]);

  if (!isOpen || !doc) return null;

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
    <div className="fixed inset-0 z-50 overflow-y-auto">
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black bg-opacity-50 transition-opacity"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="flex min-h-full items-center justify-center p-4">
        <div
          className="relative bg-white rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-hidden flex flex-col"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between p-6 border-b border-gray-200">
            <div className="flex items-center gap-3">
              <span className={`px-3 py-1 rounded text-sm font-medium ${getDocTypeColor(doc.doc_type)}`}>
                {doc.doc_type}
              </span>
              {doc.document.ticker && (
                <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded text-sm font-medium">
                  {doc.document.ticker}
                </span>
              )}
            </div>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600 transition-colors"
            >
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-y-auto p-6">
            {/* Title or Section */}
            {(doc.document.title || doc.document.section) && (
              <h2 className="text-2xl font-bold text-gray-900 mb-4">
                {doc.document.title || doc.document.section}
              </h2>
            )}

            {/* Metadata */}
            <div className="flex flex-wrap gap-4 text-sm text-gray-600 mb-6 pb-4 border-b border-gray-200">
              {doc.document.published && (
                <span>Published: {formatDate(doc.document.published)}</span>
              )}
              {doc.document.publisher && (
                <span>Publisher: {doc.document.publisher}</span>
              )}
              {doc.document.speaker && (
                <span>Speaker: {doc.document.speaker}</span>
              )}
              {doc.document.sentiment !== undefined && (
                <span>
                  Sentiment: {doc.document.sentiment > 0 ? '😊' : doc.document.sentiment < 0 ? '😟' : '😐'} {doc.document.sentiment.toFixed(2)}
                </span>
              )}
              {doc.document.sentiment_score !== undefined && (
                <span>
                  Sentiment: {doc.document.sentiment_score > 0 ? '😊' : doc.document.sentiment_score < 0 ? '😟' : '😐'} {doc.document.sentiment_score.toFixed(2)}
                </span>
              )}
            </div>

            {/* Full Text */}
            <div className="prose max-w-none">
              {doc.document.text && (
                <div className="whitespace-pre-wrap text-gray-700 leading-relaxed">
                  {doc.document.text}
                </div>
              )}
              {doc.document.description && (
                <div className="text-gray-700 leading-relaxed">
                  {doc.document.description}
                </div>
              )}
              {!doc.document.text && !doc.document.description && (
                <pre className="whitespace-pre-wrap text-gray-700 leading-relaxed bg-gray-50 border border-gray-200 rounded p-3 text-sm overflow-x-auto">
                  {JSON.stringify(doc.document, null, 2)}
                </pre>
              )}
            </div>

            {/* Link */}
            {doc.document.link && (
              <div className="mt-6 pt-4 border-t border-gray-200">
                <a
                  href={doc.document.link}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:text-blue-800 font-medium"
                >
                  View Original Source →
                </a>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default DocumentModal;

