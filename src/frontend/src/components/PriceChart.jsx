import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const PriceChart = ({ data }) => {
  if (!data || data.length === 0) {
    return (
      <div className="p-4 text-center text-gray-500">
        No price data available
      </div>
    );
  }

  // Custom tooltip formatter
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-300 rounded shadow-lg">
          <p className="font-semibold mb-2">{label}</p>
          {payload.map((entry, index) => (
            <p key={index} style={{ color: entry.color }} className="text-sm">
              {entry.name}: ${typeof entry.value === 'number' 
                ? entry.value.toLocaleString('en-US', { 
                    minimumFractionDigits: 2, 
                    maximumFractionDigits: 2 
                  })
                : entry.value}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  // Transform data for chart
  const chartData = data.map((item) => {
    const date = item.date || item.Date || item.published;
    return {
      date: date ? new Date(date).toLocaleDateString() : 'N/A',
      close: item.close || item.Close || 0,
      open: item.open || item.Open || 0,
      high: item.high || item.High || 0,
      low: item.low || item.Low || 0,
    };
  }).slice(-50); // Show last 50 data points

  // Format Y-axis values
  const formatYAxis = (value) => {
    return `$${value.toLocaleString('en-US', { 
      minimumFractionDigits: 0, 
      maximumFractionDigits: 0 
    })}`;
  };

  return (
    <div className="w-full h-96">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis 
            dataKey="date" 
            angle={-45}
            textAnchor="end"
            height={80}
          />
          <YAxis tickFormatter={formatYAxis} />
          <Tooltip content={<CustomTooltip />} />
          <Legend />
          <Line type="monotone" dataKey="close" stroke="#3b82f6" name="Close" />
          <Line type="monotone" dataKey="open" stroke="#10b981" name="Open" />
          <Line type="monotone" dataKey="high" stroke="#ef4444" name="High" />
          <Line type="monotone" dataKey="low" stroke="#f59e0b" name="Low" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default PriceChart;

