import { CurrentWeather } from './CurrentWeather';
import { TemperatureChart } from './TemperatureChart';
import { HumidityChart } from './HumidityChart';
import { AirQualityChart } from './AirQualityChart';
import { WeatherMetrics } from './WeatherMetrics';
import { WeatherAlerts } from './WeatherAlerts';
import type { WeatherData } from '../types/weather';
import { closestAirQualityObservation } from '../utils/observationTime';

interface WeatherDashboardProps {
  data: WeatherData;
  locationName: string;
}

export function WeatherDashboard({ data, locationName }: WeatherDashboardProps) {
  const currentAirQuality = closestAirQualityObservation(data.airQuality, data.current.timestamp);

  return (
    <div className="space-y-6">
      {/* Current Weather */}
      <CurrentWeather current={data.current} locationName={locationName} />

      {/* Rule-based Advisories */}
      <WeatherAlerts
        current={data.current}
        hourly={data.hourly}
        latestAirQuality={currentAirQuality}
      />

      {/* Weather Metrics Grid */}
      <WeatherMetrics current={data.current} />

      {/* Charts Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <TemperatureChart data={data.hourly} selectedTimestamp={data.current.timestamp} />
        <HumidityChart data={data.hourly} selectedTimestamp={data.current.timestamp} />
      </div>

      {/* Air Quality Chart */}
      <AirQualityChart data={data.airQuality} selectedEntry={currentAirQuality} />
    </div>
  );
}
