import { Droplets, Sun, Thermometer, Cloud } from 'lucide-react';
import type { CurrentWeather } from '../types/weather';

interface WeatherMetricsProps {
  current: CurrentWeather;
}

export function WeatherMetrics({ current }: WeatherMetricsProps) {
  const display = (value: number | null, suffix: string) =>
    value === null ? 'Unavailable' : `${value}${suffix}`;

  const metrics = [
    {
      label: 'Precipitation',
      value: display(current.precipitation, ' mm'),
      icon: Droplets,
      color: 'text-blue-500',
      bgColor: 'bg-blue-50',
    },
    {
      label: 'Cloud Cover',
      value: display(current.cloudCover, '%'),
      icon: Cloud,
      color: 'text-gray-500',
      bgColor: 'bg-gray-50',
    },
    {
      label: 'Soil Temperature',
      value: display(current.soilTemperature, '°C'),
      icon: Thermometer,
      color: 'text-orange-500',
      bgColor: 'bg-orange-50',
    },
    {
      label: 'Solar Radiation',
      value: display(current.shortwaveRadiation, ' W/m²'),
      icon: Sun,
      color: 'text-yellow-500',
      bgColor: 'bg-yellow-50',
    },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {metrics.map((metric) => (
        <div key={metric.label} className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-4">
            <div className={`p-3 rounded-lg ${metric.bgColor}`}>
              <metric.icon className={`w-6 h-6 ${metric.color}`} />
            </div>
            <div>
              <p className="text-sm text-gray-600">{metric.label}</p>
              <p className="text-2xl font-bold text-gray-900">{metric.value}</p>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
