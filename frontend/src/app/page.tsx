
'use client'; 

import { useState, useEffect, useCallback } from 'react';
import { Sun, Cloud, CloudRain, CloudSnow, Thermometer, Wind, Droplet, Gauge, Eye, Sunrise, Sunset, CalendarDays, Maximize, Minimize } from 'lucide-react'; // Icons


interface CurrentWeatherData {
  timestamp: string;
  temperature: number;
  feels_like: number;
  pressure: number;
  humidity: number;
  wind_speed: number;
  wind_deg: number;
  weather_description: string;
  sunrise_utc: number;
  sunset_utc: number;
  rain_1h?: number;
  snow_1h?: number;
  visibility: number; 
}

interface HourlyForecastData {
  timestamp: string;
  temperature: number;
  pop: number; // Probability of Precipitation
}

interface DailyForecastData {
  timestamp: string;
  temp_min: number;
  temp_max: number;
  pop: number; 
}

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:5000'; 

export default function WeatherDashboard() {
  const [locations, setLocations] = useState<string[]>([]);
  const [selectedLocation, setSelectedLocation] = useState<string | null>(null);
  const [currentWeather, setCurrentWeather] = useState<CurrentWeatherData | null>(null);
  const [hourlyForecast, setHourlyForecast] = useState<HourlyForecastData[]>([]);
  const [dailyForecast, setDailyForecast] = useState<DailyForecastData[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Function to fetch all available locations
  const fetchLocations = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await fetch(`${API_BASE_URL}/api/locations`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data: string[] = await response.json();
      setLocations(data);
      if (data.length > 0 && !selectedLocation) {
        setSelectedLocation(data[0]); // Select the first location by default
      }
    } catch (err: unknown) { // Changed 'any' to 'unknown'
      let errorMessage = 'An unknown error occurred while fetching locations.';
      if (err instanceof Error) {
        errorMessage = `Failed to fetch locations: ${err.message}`;
      } else if (typeof err === 'string') {
        errorMessage = `Failed to fetch locations: ${err}`;
      }
      setError(errorMessage);
      console.error("Failed to fetch locations:", err);
    } finally {
      setLoading(false);
    }
  }, [selectedLocation]); 
  const fetchWeatherData = useCallback(async (location: string) => {
    if (!location) return;

    try {
      setLoading(true);
      setError(null);

      // Fetch current weather
      const currentResponse = await fetch(`${API_BASE_URL}/api/current/${location}`);
      if (!currentResponse.ok) {
        // If 404 (no data), set null, don't throw error for this specific case
        if (currentResponse.status === 404) {
          setCurrentWeather(null);
        } else {
          throw new Error(`Failed to fetch current weather for ${location}: ${currentResponse.statusText}`);
        }
      } else {
        const data: CurrentWeatherData = await currentResponse.json();
        setCurrentWeather(data);
      }

      // Fetch hourly forecast
      const hourlyResponse = await fetch(`${API_BASE_URL}/api/hourly/${location}`);
      if (!hourlyResponse.ok) {
        if (hourlyResponse.status === 404) {
          setHourlyForecast([]);
        } else {
          throw new Error(`Failed to fetch hourly forecast for ${location}: ${hourlyResponse.statusText}`);
        }
      } else {
        const data: HourlyForecastData[] = await hourlyResponse.json();
        setHourlyForecast(data);
      }

      // Fetch daily forecast
      const dailyResponse = await fetch(`${API_BASE_URL}/api/daily/${location}`);
      if (!dailyResponse.ok) {
        if (dailyResponse.status === 404) {
          setDailyForecast([]);
        } else {
          throw new Error(`Failed to fetch daily forecast for ${location}: ${dailyResponse.statusText}`);
        }
      } else {
        const data: DailyForecastData[] = await dailyResponse.json();
        setDailyForecast(data);
      }

    } catch (err: unknown) { // Changed 'any' to 'unknown'
      let errorMessage = 'An unknown error occurred while fetching weather data.';
      if (err instanceof Error) {
        errorMessage = `Failed to fetch weather data: ${err.message}`;
      } else if (typeof err === 'string') {
        errorMessage = `Failed to fetch weather data: ${err}`;
      }
      setError(errorMessage);
      console.error(`Failed to fetch weather data for ${location}:`, err);
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch for locations on component mount
  useEffect(() => {
    fetchLocations();
  }, [fetchLocations]);

  // Fetch weather data when selected location changes
  useEffect(() => {
    if (selectedLocation) {
      fetchWeatherData(selectedLocation);
    }
  }, [selectedLocation, fetchWeatherData]);

  // Utility to get weather icon based on description or main
  const getWeatherIcon = (description: string | undefined, main: string | undefined, size: number = 24) => {
    if (!description && !main) return <Cloud size={size} />; // Default
    const lowerDesc = description?.toLowerCase();
    const lowerMain = main?.toLowerCase();

    if (lowerDesc?.includes('rain') || lowerMain?.includes('rain')) return <CloudRain size={size} />;
    if (lowerDesc?.includes('cloud') || lowerMain?.includes('cloud')) return <Cloud size={size} />;
    if (lowerDesc?.includes('snow') || lowerMain?.includes('snow')) return <CloudSnow size={size} />;
    if (lowerDesc?.includes('clear') || lowerMain?.includes('clear')) return <Sun size={size} />;
    // Add more conditions as needed
    return <Cloud size={size} />; // Fallback
  };

  // Utility to format timestamp
  const formatTimestamp = (timestamp: string, options: Intl.DateTimeFormatOptions = { hour: 'numeric', minute: '2-digit', hour12: true }) => {
    return new Date(timestamp).toLocaleTimeString('en-US', options);
  };

  const formatDailyDate = (timestamp: string) => {
    return new Date(timestamp).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
  };

  const formatSunTime = (timestamp_unix: number) => {
    if (!timestamp_unix) return 'N/A';
    return new Date(timestamp_unix * 1000).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true });
  }

  if (loading && locations.length === 0) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-400 to-indigo-600 text-white p-4">
        <div className="text-2xl font-bold">Loading Locations...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-400 to-indigo-600 text-white p-4">
        <div className="bg-red-500 p-6 rounded-lg shadow-xl flex items-center space-x-4">
          <span className="text-3xl">⚠️</span>
          <div>
            <h2 className="text-xl font-bold mb-2">Error:</h2>
            <p>{error}</p>
            <button
              onClick={fetchLocations}
              className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-400 to-indigo-600 text-white p-4 flex flex-col items-center">
      <div className="w-full max-w-4xl bg-white bg-opacity-20 backdrop-blur-md rounded-2xl shadow-xl p-6 mb-8 mt-8">
        <h1 className="text-4xl font-extrabold text-center mb-6 text-shadow">Weather Dashboard</h1>

        <div className="mb-6 text-center">
          <label htmlFor="location-select" className="block text-lg font-semibold mb-2">
            Select Location:
          </label>
          <div className="relative inline-block w-full max-w-xs">
            <select
              id="location-select"
              className="block appearance-none w-full bg-white bg-opacity-30 border border-white border-opacity-50 text-white py-3 px-4 pr-8 rounded-lg shadow-sm leading-tight focus:outline-none focus:ring-2 focus:ring-blue-300 focus:border-transparent transition-all duration-300"
              value={selectedLocation || ''}
              onChange={(e) => setSelectedLocation(e.target.value)}
            >
              {locations.length === 0 ? (
                <option value="">No locations available</option>
              ) : (
                locations.map((loc) => (
                  <option key={loc} value={loc}>
                    {loc.replace(/-/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} {/* Format for display */}
                  </option>
                ))
              )}
            </select>
            <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-white">
              <svg className="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 6.757 7.586 5.343 9z"/></svg>
            </div>
          </div>
        </div>

        {selectedLocation && (
          <>
            <h2 className="text-3xl font-bold text-center mb-6 text-shadow-md">
              {selectedLocation.replace(/-/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} Forecast
            </h2>

            {loading ? (
              <div className="text-center text-xl">Loading Forecast...</div>
            ) : (
              <>
                {/* Current Weather */}
                <div className="bg-white bg-opacity-20 p-6 rounded-xl shadow-lg mb-8">
                  <h3 className="text-2xl font-bold mb-4 flex items-center">
                    <Thermometer className="mr-2" size={28} /> Current Weather
                  </h3>
                  {currentWeather ? (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-lg">
                      <div className="flex items-center"><Thermometer className="mr-2" /> Temperature: {currentWeather.temperature}°C</div>
                      <div className="flex items-center"><Thermometer className="mr-2" /> Feels Like: {currentWeather.feels_like}°C</div>
                      <div className="flex items-center"><Gauge className="mr-2" /> Pressure: {currentWeather.pressure} hPa</div>
                      <div className="flex items-center"><Droplet className="mr-2" /> Humidity: {currentWeather.humidity}%</div>
                      <div className="flex items-center"><Wind className="mr-2" /> Wind: {currentWeather.wind_speed} m/s ({currentWeather.wind_deg}°C)</div>
                      <div className="flex items-center">{getWeatherIcon(currentWeather.weather_description, currentWeather.weather_description)} {currentWeather.weather_description}</div>
                      <div className="flex items-center"><Eye className="mr-2" /> Visibility: {(currentWeather.visibility / 1000).toFixed(1)} km</div>
                      {currentWeather.rain_1h !== undefined && currentWeather.rain_1h > 0 && (
                        <div className="flex items-center"><CloudRain className="mr-2" /> Rain (1h): {currentWeather.rain_1h} mm</div>
                      )}
                      {currentWeather.snow_1h !== undefined && currentWeather.snow_1h > 0 && (
                        <div className="flex items-center"><CloudSnow className="mr-2" /> Snow (1h): {currentWeather.snow_1h} mm</div>
                      )}
                      <div className="flex items-center"><Sunrise className="mr-2" /> Sunrise: {formatSunTime(currentWeather.sunrise_utc)}</div>
                      <div className="flex items-center"><Sunset className="mr-2" /> Sunset: {formatSunTime(currentWeather.sunset_utc)}</div>
                      <div className="col-span-1 md:col-span-2 text-sm italic text-right">
                        Last updated: {new Date(currentWeather.timestamp).toLocaleString()}
                      </div>
                    </div>
                  ) : (
                    <p className="text-lg text-center opacity-80">No current weather data available for this location.</p>
                  )}
                </div>

                {/* Hourly Forecast */}
                <div className="bg-white bg-opacity-20 p-6 rounded-xl shadow-lg mb-8">
                  <h3 className="text-2xl font-bold mb-4 flex items-center">
                    <Thermometer className="mr-2" /> Hourly Forecast (Next 24h)
                  </h3>
                  {hourlyForecast.length > 0 ? (
                    <div className="overflow-x-auto pb-4">
                      <div className="flex space-x-4">
                        {hourlyForecast.map((hour, index) => (
                          <div key={index} className="flex-shrink-0 w-32 p-3 bg-white bg-opacity-15 rounded-lg text-center border border-white border-opacity-30 shadow-md">
                            <p className="font-semibold text-lg">{formatTimestamp(hour.timestamp)}</p>
                            <p className="text-3xl font-bold mb-1">{hour.temperature}°</p>
                            <div className="flex items-center justify-center mb-1">
                                {getWeatherIcon(undefined, undefined, 20)} {/* Placeholder icon for now */}
                            </div>
                            <p className="text-sm">POP: {(hour.pop * 100).toFixed(0)}%</p>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : (
                    <p className="text-lg text-center opacity-80">No hourly forecast data available.</p>
                  )}
                </div>

                {/* Daily Forecast */}
                <div className="bg-white bg-opacity-20 p-6 rounded-xl shadow-lg mb-8">
                  <h3 className="text-2xl font-bold mb-4 flex items-center">
                    <CalendarDays className="mr-2" /> Daily Forecast (Next 8 days)
                  </h3>
                  {dailyForecast.length > 0 ? (
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                      {dailyForecast.map((day, index) => (
                        <div key={index} className="p-4 bg-white bg-opacity-15 rounded-lg text-center border border-white border-opacity-30 shadow-md">
                          <p className="font-semibold text-lg mb-1">{formatDailyDate(day.timestamp)}</p>
                          <div className="flex items-center justify-center text-4xl font-bold mb-1">
                            <Minimize size={28} className="mr-1 text-blue-200" /> {day.temp_min}°
                            <span className="mx-2 text-white text-opacity-50">/</span>
                            <Maximize size={28} className="ml-1 text-red-200" /> {day.temp_max}°
                          </div>
                          <p className="text-sm mb-2">POP: {(day.pop * 100).toFixed(0)}%</p>
                          <div className="flex items-center justify-center text-xl">
                            {getWeatherIcon(undefined, undefined, 24)} {/* Placeholder icon */}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="text-lg text-center opacity-80">No daily forecast data available.</p>
                  )}
                </div>
              </>
            )}
          </>
        )}
      </div>
      <footer className="mt-8 text-center text-white text-opacity-70 text-sm">
        <p>&copy; {new Date().getFullYear()} Realtime Weather Dashboard. All rights reserved.</p>
      </footer>
    </div>
  );
}
