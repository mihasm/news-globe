# NewsGlobe

A multi-source live world events aggregation system that collects, processes, and visualizes events from multiple data sources on an interactive map.

## Features

- **Multi-source ingestion**: RSS, Telegram, Mastodon, GDELT, GDACS, USGS
- **Real-time processing**: NLP/geocoding, event clustering, and streaming to frontend
- **Interactive map**: Web-based visualization with Cesium/Leaflet
- **Flight & maritime tracking**: ADSB aircraft and AIS vessel data integration

## Prerequisites

Before running News Globe, you'll need:

1. **API Keys:**
   - **Mapbox Access Token**: Required for map tiles. Get from [https://account.mapbox.com/access-tokens/](https://account.mapbox.com/access-tokens/)
   - **AIS Stream API Key**: Required for vessel tracking. Get from [https://aisstream.io/](https://aisstream.io/)

2. **System Requirements:**
   - Docker & Docker Compose (recommended)
   - Python 3.x (for local development)
   - PostgreSQL with PostGIS extension

## Quick Start

1. **Clone and configure:**
   ```bash
   git clone <repository-url>
   cd news-globe
   cp env.example .env
   # Edit .env with your API keys
   ```

2. **Start services with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

3. **Or run locally:**
   ```bash
   pip install -r requirements.txt
   # Set environment variables or use .env file
   python main.py
   ```

4. **Access the application:**
   - Frontend: http://localhost
   - API: http://localhost/api (see [API.md](API.md) for documentation)

## Configuration

### Environment Variables

Copy `env.example` to `.env` and configure the following variables:

- `AISSTREAM_API_KEY`: Your AIS Stream API key from [https://aisstream.io/](https://aisstream.io/)
- `MAPBOX_TOKEN`: Your Mapbox access token from [https://account.mapbox.com/access-tokens/](https://account.mapbox.com/access-tokens/)

The frontend will automatically load the Mapbox token from the API at runtime.

## Data Pipeline

1. **Collection**: 
   - Supervisor runs connectors (GDELT, GDACS, USGS, Telegram, Mastodo, RSS) on schedules, converting data to unified `IngestionRecord` format and sending to Memory Store

2. **Ingestion**: 
   - Ingestion service polls Memory Store for raw items
   - Validates records, performs NLP geocoding for missing locations
   - Stores normalized items in PostgreSQL with deduplication

3. **Clustering**: 
   - Clustering service processes unassigned normalized items
   - Groups related events using spaCy vectors and fuzzy matching
   - Creates/updates clusters in database

4. **Serving**: 
   - API server provides REST endpoints for clusters/events
   - Frontend queries API and displays events on interactive map

## Architecture

- **Connectors**: Unified connectors for GDELT, GDACS, USGS, Telegram, Mastodon, RSS. Ran using the supervisor container.
- **Services**:
  - API server (REST endpoints)
  - Ingestion service (NLP/geocoding)
  - Clustering service (event grouping)
  - Memory store (in-memory data layer)
- **Frontend**: Static web app with map visualization
- **Database**: PostgreSQL

## Requirements

- Docker & Docker Compose (recommended)
- Python 3.x
- PostgreSQL

## Development

### Testing

Run the test suite:
```bash
python tests/runner.py
```

### Frontend Debugging

For development debugging of frontend code:
```bash
python dev_scripts/frontend_debug.py
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
