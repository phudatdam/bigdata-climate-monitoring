import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import type { HourlyData } from '../types/weather';
import { formatVietnamTimestamp } from '../utils/observationTime';

interface HumidityChartProps {
  data: HourlyData[];
  selectedTimestamp: string;
}

export function HumidityChart({ data, selectedTimestamp }: HumidityChartProps) {
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">Humidity Levels (24 Hours)</h3>
      <ResponsiveContainer width="100%" height={300}>
        <AreaChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis
            dataKey="timestamp"
            stroke="#6b7280"
            tick={{ fontSize: 12 }}
            tickFormatter={formatVietnamTimestamp}
          />
          <YAxis
            stroke="#6b7280"
            tick={{ fontSize: 12 }}
            label={{ value: '%', angle: -90, position: 'insideLeft' }}
          />
          <Tooltip
            labelFormatter={(value) => formatVietnamTimestamp(String(value))}
            contentStyle={{
              backgroundColor: '#fff',
              border: '1px solid #e5e7eb',
              borderRadius: '8px',
            }}
          />
          <Area
            type="monotone"
            dataKey="humidity"
            stroke="#3b82f6"
            fill="#93c5fd"
            strokeWidth={2}
            name="Humidity"
          />
          <ReferenceLine x={selectedTimestamp} stroke="#3b82f6" strokeDasharray="4 2" />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
