import { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import { queryAgent, getDocument } from '../services/api';
import DocumentModal from './DocumentModal';

const AgentChat = () => {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: "Hello! I'm your financial research assistant. I can help you search through news articles, SEC filings, and earnings transcripts. What would you like to know?",
      sources: []
    }
  ]);
  const [input, setInput] = useState('');
  const [ticker, setTicker] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedDocument, setSelectedDocument] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [loadingDocument, setLoadingDocument] = useState(false);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || loading) return;

    const userMessage = input.trim();
    setInput('');
    setError(null);

    // Add user message
    const newUserMessage = {
      role: 'user',
      content: userMessage,
      sources: []
    };
    setMessages(prev => [...prev, newUserMessage]);
    setLoading(true);

    try {
      const response = await queryAgent(userMessage, ticker || null);

      // Normalize backend shapes: backend returns { auto, response }
      const agentResp = response?.response || response;
      const content =
        agentResp?.answer ||
        response?.answer ||
        agentResp?.error ||
        response?.error ||
        'No response received';

      const assistantMessage = {
        role: 'assistant',
        content,
        sources: agentResp?.sources || response?.sources || [],
        tool_calls: agentResp?.tool_calls || [],
        error: Boolean(agentResp?.error || response?.error)
      };
      
      setMessages(prev => [...prev, assistantMessage]);
    } catch (err) {
      setError(err.message);
      const errorMessage = {
        role: 'assistant',
        content: `Error: ${err.message}`,
        sources: [],
        error: true
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const formatSourceType = (type) => {
    const types = {
      news: '📰 News',
      filing: '📄 Filing',
      transcript: '🎙️ Transcript'
    };
    return types[type] || type;
  };

  const handleSourceClick = async (source) => {
    const docType = source?.doc_type || source?.type;

    // If we can't fetch the document, fall back to opening a link if present.
    if (!docType) {
      if (source?.link) window.open(source.link, '_blank', 'noopener,noreferrer');
      return;
    }

    // Only attempt /api/document if we have the metadata it expects.
    const hasEnoughMetadata =
      docType === 'news' ||
      (docType === 'filing' && Boolean(source?.filing_file)) ||
      (docType === 'transcript' && Boolean(source?.transcript_file));

    if (!hasEnoughMetadata) {
      if (source?.link) window.open(source.link, '_blank', 'noopener,noreferrer');
      return;
    }

    setLoadingDocument(true);
    setError(null);
    try {
      const fullDocument = await getDocument({
        doc_type: docType,
        ticker: source?.ticker,
        index: source?.index,
        filing_file: source?.filing_file,
        transcript_file: source?.transcript_file,
      });
      setSelectedDocument(fullDocument);
      setIsModalOpen(true);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoadingDocument(false);
    }
  };

  const handleCloseModal = () => {
    setIsModalOpen(false);
    setSelectedDocument(null);
  };

  const clearChat = () => {
    setMessages([
      {
        role: 'assistant',
        content: "Hello! I'm your financial research assistant. I can help you search through news articles, SEC filings, and earnings transcripts. What would you like to know?",
        sources: []
      }
    ]);
    setError(null);
  };

  return (
    <div className="flex flex-col h-[calc(100vh-200px)] bg-white rounded-lg shadow-md">
      {/* Header */}
      <div className="border-b border-gray-200 p-4 bg-gradient-to-r from-blue-600 to-blue-700 text-white">
        <div className="flex justify-between items-center">
          <div>
            <h2 className="text-xl font-bold">Research Agent</h2>
            <p className="text-sm text-blue-100">Ask questions about financial data</p>
          </div>
          {/* <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <label htmlFor="ticker-filter" className="text-sm font-medium">
                Ticker:
              </label>
              <input
                type="text"
                id="ticker-filter"
                value={ticker}
                onChange={(e) => setTicker(e.target.value.toUpperCase())}
                placeholder="AAPL"
                className="w-20 px-2 py-1 rounded text-gray-900 text-sm"
                maxLength={5}
              />
            </div>
            <button
              onClick={clearChat}
              className="px-3 py-1 bg-blue-500 hover:bg-blue-400 rounded text-sm transition-colors"
            >
              Clear
            </button>
          </div> */}
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((message, index) => (
          <div
            key={index}
            className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
          >
            <div
              className={`max-w-3xl rounded-lg p-4 ${
                message.role === 'user'
                  ? 'bg-blue-600 text-white'
                  : message.error
                  ? 'bg-red-50 border border-red-200 text-red-800'
                  : 'bg-gray-100 text-gray-900'
              }`}
            >
              <div className="markdown-content">
                <ReactMarkdown
                  components={{
                    p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
                    h1: ({ children }) => <h1 className="text-xl font-bold mb-2 mt-4 first:mt-0">{children}</h1>,
                    h2: ({ children }) => <h2 className="text-lg font-bold mb-2 mt-3 first:mt-0">{children}</h2>,
                    h3: ({ children }) => <h3 className="text-base font-semibold mb-1 mt-2 first:mt-0">{children}</h3>,
                    ul: ({ children }) => <ul className="list-disc list-inside mb-2 space-y-1">{children}</ul>,
                    ol: ({ children }) => <ol className="list-decimal list-inside mb-2 space-y-1">{children}</ol>,
                    li: ({ children }) => <li className="ml-2">{children}</li>,
                    code: ({ children, className }) => {
                      const isInline = !className;
                      return isInline ? (
                        <code className="bg-gray-200 px-1 py-0.5 rounded text-sm font-mono">{children}</code>
                      ) : (
                        <code className="block bg-gray-200 p-2 rounded text-sm font-mono overflow-x-auto mb-2">{children}</code>
                      );
                    },
                    blockquote: ({ children }) => (
                      <blockquote className="border-l-4 border-gray-300 pl-3 italic my-2">{children}</blockquote>
                    ),
                    strong: ({ children }) => <strong className="font-semibold">{children}</strong>,
                    em: ({ children }) => <em className="italic">{children}</em>,
                    a: ({ href, children }) => (
                      <a href={href} className="text-blue-600 hover:underline" target="_blank" rel="noopener noreferrer">
                        {children}
                      </a>
                    ),
                  }}
                >
                  {message.content}
                </ReactMarkdown>
              </div>
              
              {/* Sources */}
              {message.sources && message.sources.length > 0 && (
                <div className="mt-3 pt-3 border-t border-gray-300">
                  <div className="text-xs font-semibold mb-2 text-gray-600">
                    Sources ({message.sources.length}):
                  </div>
                  <div className="space-y-2">
                    {message.sources.map((source, idx) => (
                      <div
                        key={idx}
                        className="text-xs bg-white rounded p-2 border border-gray-200 hover:border-gray-300 hover:shadow-sm transition cursor-pointer"
                        onClick={() => handleSourceClick(source)}
                      >
                        <div className="flex items-center gap-2 mb-1">
                          <span className="font-medium">
                            {formatSourceType(source.doc_type || source.type)}
                          </span>
                          {source.ticker && (
                            <span className="px-1.5 py-0.5 bg-gray-200 rounded text-gray-700">
                              {source.ticker}
                            </span>
                          )}
                          {source.score && (
                            <span className="text-gray-500">
                              (Score: {source.score.toFixed(3)})
                            </span>
                          )}
                        </div>
                        {source.title && (
                          <div className="font-medium text-gray-800 mb-1">
                            {source.title}
                          </div>
                        )}
                        {source.text_preview && (
                          <div className="text-gray-600 line-clamp-2">
                            {source.text_preview}...
                          </div>
                        )}
                        {!source.text_preview && source.link && (
                          <div className="text-gray-600">
                            Click to open the original link
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        ))}
        
        {loading && (
          <div className="flex justify-start">
            <div className="bg-gray-100 rounded-lg p-4">
              <div className="flex items-center gap-2">
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
                <span className="text-gray-600">Researching...</span>
              </div>
            </div>
          </div>
        )}

        {loadingDocument && (
          <div className="flex justify-start">
            <div className="bg-gray-100 rounded-lg p-3">
              <div className="flex items-center gap-2">
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
                <span className="text-gray-600">Loading source…</span>
              </div>
            </div>
          </div>
        )}
        
        <div ref={messagesEndRef} />
      </div>

      {/* Error Message */}
      {error && (
        <div className="px-4 py-2 bg-red-50 border-t border-red-200">
          <p className="text-sm text-red-800">{error}</p>
        </div>
      )}

      {/* Input */}
      <div className="border-t border-gray-200 p-4">
        <div className="flex gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Ask a question about financial data... (e.g., 'What are the main risks for AAPL?' or 'Search for information about AI investments')"
            className="flex-1 px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 resize-none"
            rows={2}
            disabled={loading}
          />
          <button
            onClick={handleSend}
            disabled={loading || !input.trim()}
            className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-blue-400 disabled:cursor-not-allowed transition-colors font-medium"
          >
            Send
          </button>
        </div>
        <div className="mt-2 text-xs text-gray-500">
          Tip: You can specify a ticker in the filter above, or mention it in your question
        </div>
      </div>

      <DocumentModal
        document={selectedDocument}
        isOpen={isModalOpen}
        onClose={handleCloseModal}
      />
    </div>
  );
};

export default AgentChat;

