"""
City configuration mapping Kalshi tickers to NWS station IDs, timezones, and coordinates.

This is the single source of truth for all city-related configuration.
Weather API clients, Kalshi market discovery, and the prediction engine all reference this.

Kalshi ticker prefixes follow the pattern:
  - KXHIGH{CODE} for high temperature markets
  - KXLOW{CODE}  for low temperature markets

NWS station IDs are used for:
  - Fetching forecasts from api.weather.gov
  - Pulling Daily Climate Reports for settlement verification

Coordinates (lat/lon) are used for:
  - Visual Crossing, PirateWeather, OpenWeatherMap API calls
  - GRIB2 point extraction (Sprint 7)
"""

from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass(frozen=True)
class CityConfig:
    name: str
    kalshi_ticker_code: str
    nws_station_id: str
    timezone: str
    lat: float
    lon: float

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError(f"City name cannot be empty (code={self.kalshi_ticker_code!r})")
        if not self.kalshi_ticker_code or not self.kalshi_ticker_code.strip():
            raise ValueError(f"Kalshi ticker code cannot be empty (name={self.name!r})")
        if not self.nws_station_id or not self.nws_station_id.strip():
            raise ValueError(f"NWS station ID cannot be empty (city={self.name!r})")
        if not (-90 <= self.lat <= 90):
            raise ValueError(f"Latitude must be -90..90, got {self.lat} (city={self.name!r})")
        if not (-180 <= self.lon <= 180):
            raise ValueError(f"Longitude must be -180..180, got {self.lon} (city={self.name!r})")
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, KeyError):
            raise ValueError(f"Invalid timezone {self.timezone!r} (city={self.name!r})")


# fmt: off
# Populated from Kalshi's weather series. NWS stations mapped to nearest WFO gridpoint.
# This will be validated and potentially expanded in Sprint 1 when we query the Kalshi API
# for all active weather series.
CITIES: dict[str, CityConfig] = {
    "ATL": CityConfig("Atlanta",        "ATL", "KATL", "America/New_York",    33.6407, -84.4277),
    "AUS": CityConfig("Austin",         "AUS", "KAUS", "America/Chicago",     30.1975, -97.6664),
    "BAL": CityConfig("Baltimore",      "BAL", "KBWI", "America/New_York",    39.1754, -76.6684),
    "BOS": CityConfig("Boston",         "BOS", "KBOS", "America/New_York",    42.3656, -71.0096),
    "CLT": CityConfig("Charlotte",      "CLT", "KCLT", "America/New_York",    35.2144, -80.9473),
    "CHI": CityConfig("Chicago",        "CHI", "KORD", "America/Chicago",     41.9742, -87.9073),
    "CVG": CityConfig("Cincinnati",     "CVG", "KCVG", "America/New_York",    39.0488, -84.6678),
    "CLE": CityConfig("Cleveland",      "CLE", "KCLE", "America/New_York",    41.4117, -81.8498),
    "CMH": CityConfig("Columbus",       "CMH", "KCMH", "America/New_York",    39.9980, -82.8919),
    "DFW": CityConfig("Dallas",         "DFW", "KDFW", "America/Chicago",     32.8998, -97.0403),
    "DEN": CityConfig("Denver",         "DEN", "KDEN", "America/Denver",      39.8561, -104.6737),
    "DTW": CityConfig("Detroit",        "DTW", "KDTW", "America/Detroit",     42.2124, -83.3534),
    "ELP": CityConfig("El Paso",        "ELP", "KELP", "America/Denver",      31.8072, -106.3776),
    "HOU": CityConfig("Houston",        "HOU", "KIAH", "America/Chicago",     29.9844, -95.3414),
    "IND": CityConfig("Indianapolis",   "IND", "KIND", "America/Indiana/Indianapolis", 39.7173, -86.2944),
    "JAX": CityConfig("Jacksonville",   "JAX", "KJAX", "America/New_York",    30.4941, -81.6879),
    "KC":  CityConfig("Kansas City",    "KC",  "KMCI", "America/Chicago",     39.2976, -94.7139),
    "LAS": CityConfig("Las Vegas",      "LAS", "KLAS", "America/Los_Angeles", 36.0840, -115.1537),
    "LA":  CityConfig("Los Angeles",    "LA",  "KLAX", "America/Los_Angeles", 33.9382, -118.3886),
    "MEM": CityConfig("Memphis",        "MEM", "KMEM", "America/Chicago",     35.0424, -89.9767),
    "MIA": CityConfig("Miami",          "MIA", "KMIA", "America/New_York",    25.7933, -80.2906),
    "MKE": CityConfig("Milwaukee",      "MKE", "KMKE", "America/Chicago",     42.9472, -87.8966),
    "MSP": CityConfig("Minneapolis",    "MSP", "KMSP", "America/Chicago",     44.8831, -93.2289),
    "BNA": CityConfig("Nashville",      "BNA", "KBNA", "America/Chicago",     36.1245, -86.6782),
    "MSY": CityConfig("New Orleans",    "MSY", "KMSY", "America/Chicago",     29.9934, -90.2580),
    "NYC": CityConfig("New York",       "NYC", "KJFK", "America/New_York",    40.6413, -73.7781),
    "OKC": CityConfig("Oklahoma City",  "OKC", "KOKC", "America/Chicago",     35.3931, -97.6007),
    "OMA": CityConfig("Omaha",          "OMA", "KOMA", "America/Chicago",     41.3032, -95.8941),
    "ORL": CityConfig("Orlando",        "ORL", "KMCO", "America/New_York",    28.4312, -81.3081),
    "PHL": CityConfig("Philadelphia",   "PHL", "KPHL", "America/New_York",    39.8721, -75.2411),
    "PHX": CityConfig("Phoenix",        "PHX", "KPHX", "America/Phoenix",     33.4373, -112.0078),
    "PIT": CityConfig("Pittsburgh",     "PIT", "KPIT", "America/New_York",    40.4915, -80.2329),
    "PDX": CityConfig("Portland",       "PDX", "KPDX", "America/Los_Angeles", 45.5898, -122.5951),
    "RDU": CityConfig("Raleigh",        "RDU", "KRDU", "America/New_York",    35.8801, -78.7880),
    "SAC": CityConfig("Sacramento",     "SAC", "KSMF", "America/Los_Angeles", 38.6954, -121.5908),
    "SLC": CityConfig("Salt Lake City", "SLC", "KSLC", "America/Denver",      40.7884, -111.9778),
    "SAT": CityConfig("San Antonio",    "SAT", "KSAT", "America/Chicago",     29.5337, -98.4698),
    "SD":  CityConfig("San Diego",      "SD",  "KSAN", "America/Los_Angeles", 32.7336, -117.1831),
    "SF":  CityConfig("San Francisco",  "SF",  "KSFO", "America/Los_Angeles", 37.6213, -122.3790),
    "SEA": CityConfig("Seattle",        "SEA", "KSEA", "America/Los_Angeles", 47.4502, -122.3088),
    "STL": CityConfig("St. Louis",      "STL", "KSTL", "America/Chicago",     38.7487, -90.3700),
    "TPA": CityConfig("Tampa",          "TPA", "KTPA", "America/New_York",    27.9756, -82.5333),
    "DCA": CityConfig("Washington DC",  "DCA", "KDCA", "America/New_York",    38.8512, -77.0402),
}
# fmt: on


def get_city(code: str) -> CityConfig:
    """Get city config by Kalshi ticker code. Raises KeyError if not found."""
    city = CITIES.get(code)
    if city is None:
        raise KeyError(f"Unknown city code: {code!r}. Valid codes: {sorted(CITIES.keys())}")
    return city


_ALL_CITIES_SORTED: list[CityConfig] = sorted(CITIES.values(), key=lambda c: c.name)


def all_cities() -> list[CityConfig]:
    """Return all city configs, sorted by name. Cached at module load time."""
    return list(_ALL_CITIES_SORTED)
