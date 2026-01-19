# NewsGlobe API Documentation

## Overview

NewsGlobe provides a REST API for accessing real-time event data and system configuration.

## Endpoints

### GET /api/config
Returns frontend configuration including API keys and settings.

**Response:**
```json
{
  "mapboxToken": "configured|not_configured",
  "cesiumIonToken": "configured|not_configured",
  "openweathermapApiKey": "configured|not_configured"
}
```

### GET /api/clusters
Returns all event clusters with their associated items.

**Query Parameters:**
- `time_filter`: ISO 8601 timestamp for filtering events after this time
- `location_key`: Filter by specific location (lat,lng format)

**Response:**
```json
[
  {
    "cluster_id": "number",
    "location_key": "lat,lng",
    "location_name": "string",
    "items": [
      {
        "id": "number",
        "title": "string",
        "text": "string",
        "source": "string",
        "timestamp": "ISO 8601",
        "url": "string",
        "lat": "number",
        "lon": "number"
      }
    ]
  }
]
```

### GET /api/adsb
Returns ADS-B aircraft data around a location.

**Query Parameters:**
- `lat`: Latitude (required)
- `lon`: Longitude (required)
- `radius_nm`: Search radius in nautical miles (default: 50)

**Response:**
```json
[
  {
    "icao": "string",
    "callsign": "string",
    "lat": "number",
    "lon": "number",
    "altitude": "number",
    "speed": "number",
    "heading": "number"
  }
]
```

### GET /api/ais
Returns AIS vessel data around a location.

**Query Parameters:**
- `lat`: Latitude (required)
- `lon`: Longitude (required)
- `radius_nm`: Search radius in nautical miles (default: 50)

**Response:**
```json
[
  {
    "mmsi": "string",
    "name": "string",
    "lat": "number",
    "lon": "number",
    "speed": "number",
    "heading": "number",
    "type": "string"
  }
]
```

### GET /api/stats
Returns system statistics.

**Response:**
```json
{
  "total_clusters": "number",
  "total_events": "number",
  "last_updated": "ISO 8601",
  "sources": {
    "source_name": "count"
  }
}
```

### DELETE /api/delete-all
Clears all data from the system (development/testing only).

## Authentication

Currently, no authentication is required for API access. API keys for external services (Mapbox, AIS Stream, etc.) should be configured server-side.

## CORS

The API includes CORS headers to allow cross-origin requests from the frontend.

## Rate Limiting

Currently no rate limiting is implemented.

## Error Responses

All endpoints return standard HTTP status codes:
- `200`: Success
- `400`: Bad Request
- `404`: Not Found
- `500`: Internal Server Error

Error responses include a JSON body with error details:
```json
{
  "error": "Error message",
  "details": "Additional information"
}
```