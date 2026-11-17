export interface CurrentWeather {
  source: string | null;
  schemaVersion: string | null;
  temperature: number | null;
  humidity: number | null;
  windSpeed: number | null;
  windDir: number | null;
  pressure: number | null;
  precipitation: number | null;
  cloudCover: number | null;
  soilTemperature: number | null;
  shortwaveRadiation: number | null;
  condition: string;
  timestamp: string;
}

// HourlyData carries all fields so current conditions can be derived
// from the cached array without an extra server round-trip.
export interface HourlyData {
  source: string | null;
  schemaVersion: string | null;
  timestamp: string;
  time: string;
  temperature: number | null;
  humidity: number | null;
  windSpeed: number | null;
  windDir: number | null;
  pressure: number | null;
  precipitation: number | null;
  cloudCover: number | null;
  soilTemperature: number | null;
  shortwaveRadiation: number | null;
  condition: string;
}

export interface AirQualityData {
  source: string | null;
  schemaVersion: string | null;
  timestamp: string;
  time: string;
  aqi: number | null;
  pm25: number | null;
  pm25Mean12h: number | null;
  isStagnantAir: boolean | null;
}

export interface WeatherData {
  current: CurrentWeather;
  hourly: HourlyData[];
  airQuality: AirQualityData[];
}

export interface ForecastHour {
  hour: number;          // 1–6 (offset from base)
  timestamp: string;     // UTC forecast timestamp
  time: string;          // "HH:00" of the forecast hour
  temperature:   number | null;
  cloudCover:    number | null;
  precipitation: number | null;
  pressure:      number | null;
  windSpeed:     number | null;
  pm25:          number | null;
}

export interface ForecastCurrent {
  temperature:      number | null;
  humidity:         number | null;
  windSpeed:        number | null;
  pressure:         number | null;
  cloudCover:       number | null;
  precipitation:    number | null;
  pm25:             number | null;
  dewPoint:         number | null;
  pressureDelta3h:  number | null;
  tempMean6h:       number | null;
}

export interface ForecastData {
  baseHour:      number;
  baseTime:      string;
  baseTimestamp: string;
  current:       ForecastCurrent;
  hours:         ForecastHour[];
}
