![NewsGlobe Screenshot](screenshot.png)

# NewsGlobe

A multi-source live world events aggregation system that collects, processes, and visualizes events from multiple data sources on an interactive map.

## Features

- **Multi-source ingestion**: RSS, Telegram, Mastodon, GDELT, GDACS, USGS
- **Real-time processing**: NLP/geocoding, event clustering, and streaming to frontend
- **Interactive map**: Web-based visualization with Cesium/Leaflet
- **Flight & maritime tracking**: ADSB aircraft and AIS vessel data integration

## Prerequisites

Before running News Globe, you'll need:

1. **API Keys:** (optional, but recommended)
   - Mapbox Access Token: Required for nicer map tiles. Get from [https://account.mapbox.com/access-tokens/](https://account.mapbox.com/access-tokens/)
   - AIS Stream API Key: Required for vessel tracking. Get from [https://aisstream.io/](https://aisstream.io/)
   - Cesium Ion API Token: Required for nice Cesium 3D tiles. Get from [https://cesium.com/ion/](https://cesium.com/ion/)
   - OpenWeatherMap API key: Required for drawing weather on map. Get from [https://openweathermap.org/api](https://openweathermap.org/api)

2. **System Requirements:**
   - **Docker & Docker Compose** (required for full system)
   - Python 3.x (for local development or debugging)

## Quick Start

1. **Clone and configure:**
   ```bash
   git clone https://github.com/mihasm/news-globe.git
   cd news-globe
   cp env.example .env
   # Edit .env with your API keys
   ```

2. **Start services with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

NOTICE: First run can take A WHILE. It is building the location cache database. That process can take up to 15 minutes or more.

3. **Access the application:**
   http://localhost:88

## Configuration

### Environment Variables

Copy `env.example` to `.env` in root folder (next to docker-compose.yml) and configure the following variables:

- `AISSTREAM_API_KEY`: Your AIS Stream API key
- `MAPBOX_TOKEN`: Your Mapbox access token
- `CESIUM_ION_TOKEN`: Your Cesium Ion API token
- `OPENWEATHERMAP_API_KEY`: Your OpenWeatherMap API key

The frontend will automatically load API tokens from the API at runtime.

## Data Pipeline

1. **Collection**:
   - Supervisor service orchestrates data collection from connectors (GDELT, GDACS, USGS, Telegram, Mastodon, RSS)
   - Raw data is converted to unified `IngestionRecord` format and stored in Memory Store

2. **Ingestion**:
   - Ingestion service continuously polls Memory Store for new records
   - Performs NLP-based geocoding for location extraction from unstructured text using spaCy
   - Resolves location names using the Location Service (GeoNames-based geocoding)
   - Validates and normalizes data, stores in PostgreSQL with deduplication

3. **Clustering**:
   - Clustering service processes normalized events
   - Groups related events using spaCy sentence vectors and fuzzy string matching
   - Maintains cluster relationships and metadata in database

4. **Serving**:
   - API server provides REST endpoints for clusters, events, and real-time data
   - Frontend queries API via HTTP and displays events on interactive Cesium/Leaflet map
   - Proxy server handles WebSocket connections for real-time updates

## Architecture

- **Connectors**: Unified connectors for GDELT, GDACS, USGS, Telegram, Mastodon, RSS. Managed by the supervisor service.
- **Services**:
  - **API Server**: REST endpoints for data access and configuration
  - **Supervisor**: Orchestrates data collection from all connectors
  - **Ingestion Service**: Processes raw data with NLP/geocoding and deduplication
  - **Clustering Service**: Groups related events using spaCy vectors and fuzzy matching
  - **Location Service**: GeoNames-based geocoding service for location resolution
  - **Memory Store**: In-memory data layer for fast data transfer between services
  - **Proxy Server**: Handles WebSocket connections and service routing
- **Frontend**: Static web app with interactive map visualization using Cesium/Leaflet
- **Database**: PostgreSQL
- **PostgreSQL** (automatically handled by Docker)

## Development

### Testing

Run the test suite using Playwright for browser automation tests:
```bash
python tests/runner.py
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
