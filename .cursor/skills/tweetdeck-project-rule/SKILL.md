---
description: "This rule provides standards for frontend components and API validation"
alwaysApply: true
---

Development Workflow

Most containers are mounted with code as volumes. Code changes are immediately reflected in the container filesystem. Only restarting the container is needed for new code to take effect. No Docker rebuild is required for regular code changes. Container names are available in docker-compose.yml.

To restart a container: docker-compose restart <service-name>

Docker rebuild is necessary only if Dockerfiles are changed, requirements.txt files are modified, or other large pieces of infrastructure code are changed. Never rebuild Docker images automatically. Always ask the user first before running docker-compose build or docker-compose up --build. You may, however trigger docker-compose restart.

Debugging

To debug the API, send requests to the public API endpoint: http://localhost/api

To debug backend services, use docker-compose logs to view service logs.

The Cursor agent should never run Python code directly from the host PC to test code. Always connect to the corresponding Docker container instance and run code from there.

Frontend Testing and Debugging

For development debugging of frontend code and console logs during page load:
- Run `python3 dev_scripts/frontend_debug.py` to capture console output, page errors, and failed network requests
- Examine output in console and log file at `dev_scripts/frontend_debug.log`

For final end-to-end testing of frontend functionality:
- Run `python3 tests/runner.py` to execute automated browser tests across multiple scenarios
- Tests run in Playwright (Firefox) with fresh browser contexts for each scenario
- Scenarios include page loading, marker interactions, cluster sidebar functionality, and content validation
- Logs are written to files in `tests/logs/`
- Use pip3 if you want to install anything, not pip

Service-Specific Notes

Services with code volumes (restart only needed): api, supervisor, ingestion, clustering, memory-store.

Services that may need rebuild: any service when Dockerfile changes, any service when requirements.txt changes, infrastructure changes.

Schema sql generation rules

Never write alter statements into sql init file. This file is only used for initialization on first postgres container startup.