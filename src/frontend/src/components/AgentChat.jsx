import { useState, useRef, useEffect } from 'react';
import { queryAgent } from '../services/api';

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
      
      // Add assistant response
      const assistantMessage = {
        role: 'assistant',
        content: response.answer || response.error || 'No response received',
        sources: response.sources || [],
        tool_calls: response.tool_calls || [],
        error: response.error || false
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
          <div className="flex items-center gap-3">
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
          </div>
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
              <div className="whitespace-pre-wrap">{message.content}</div>
              
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
                        className="text-xs bg-white rounded p-2 border border-gray-200"
                      >
                        <div className="flex items-center gap-2 mb-1">
                          <span className="font-medium">
                            {formatSourceType(source.type)}
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
    </div>
  );
};

export default AgentChat;

