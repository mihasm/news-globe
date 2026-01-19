# Contributing to NewsGlobe

Thank you for your interest in contributing to NewsGlobe! We welcome contributions from the community.

## Development Setup

1. **Clone and setup:**
   ```bash
   git clone https://github.com/mihasm/news-globe.git
   cd news-globe
   cp env.example .env
   # Edit .env with your API keys
   ```

2. **Start services:**
   ```bash
   docker-compose up -d
   ```

3. **Run locally for development:**
   ```bash
   pip install -r requirements.txt
   python main.py
   ```

## Development Workflow

- Most containers use volume mounts, so code changes are reflected immediately
- Restart containers only when needed: `docker-compose restart <service-name>`
- Only rebuild containers when Dockerfiles or requirements.txt change

## Testing

- Run automated tests: `python tests/runner.py`
- Debug frontend: `python dev_scripts/frontend_debug.py`
- Tests use Playwright with Firefox for browser automation

## Code Style

- Follow existing code patterns and structure
- Use type hints where appropriate
- Keep functions focused and well-documented
- Prefer stdlib and existing dependencies

## Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes with tests
4. Ensure all tests pass
5. Update documentation if needed
6. Submit a pull request with a clear description

## Areas for Contribution

- **Data Sources**: Add new RSS feeds, APIs, or connectors
- **Frontend**: Improve UI/UX, add new map layers, enhance visualization
- **Backend**: Improve clustering algorithms, add new features
- **Documentation**: Improve setup guides, API docs, architecture docs
- **Testing**: Add more test scenarios, improve test coverage

## Architecture Overview

The system consists of:
- **Connectors**: Data ingestion from various sources
- **Services**: Ingestion (NLP/geocoding), clustering (event grouping), API server
- **Frontend**: Web-based map visualization
- **Database**: PostgreSQL with PostGIS

See README.md for detailed architecture information.

## Questions?

Open an issue on GitHub or reach out to the maintainers.
