"""
Example Python module with two API calls.
This module demonstrates making HTTP requests to different APIs.
"""

import requests
import json
from typing import Dict, Any, Optional
from datetime import datetime


def get_weather_data(city: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch weather data for a given city from a weather API.
    
    Args:
        city: Name of the city to get weather for
        api_key: Optional API key for authentication
        
    Returns:
        Dictionary containing weather data
        
    Example:
        >>> data = get_weather_data("London")
        >>> print(data)
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    
    params = {
        "q": city,
        "appid": api_key or "79f65b3971442a97737764187f770b70",
        "units": "metric"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "status": "failed"}


def get_climate_indicators(country_code: str) -> Dict[str, Any]:
    """
    Fetch population data for a given country from World Bank API.
    
    Args:
        country_code: ISO 3166-1 alpha-3 country code (e.g., 'USA', 'GBR')
        
    Returns:
        Dictionary containing country indicator data
        
    Example:
        >>> data = get_climate_indicators("USA")
        >>> print(data)
    """
    # World Bank API endpoint for population data
    base_url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/SP.POP.TOTL"
    
    params = {
        "format": "json",
        "per_page": 5,
        "date": "2020:2023"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        # World Bank API returns array, second element has data
        if isinstance(data, list) and len(data) > 1:
            return {"data": data[1], "status": "success"}
        return data
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "status": "failed"}


if __name__ == "__main__":
    # Example usage
    print("Fetching weather data for London...")
    weather = get_weather_data("London")
    print(f"Weather: {weather}\n")
    
    print("Fetching population data for USA...")
    climate = get_climate_indicators("USA")
    print(f"Population data: {climate}\n")
    
    # Save data to JSON file
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "weather_data": weather,
        "population_data": climate
    }
    
    output_file = "api_results.json"
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    
    print(f"Data saved to {output_file}")
