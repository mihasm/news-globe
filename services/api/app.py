#!/usr/bin/env python3
"""
app.py - Unified API server

Endpoints:
- GET    /api/clusters              (GeoJSON clusters, from events_schema models)
- GET    /api/adsb                  (bbox -> external fetch_adsb)
- GET    /api/ais                   (bbox -> external fetch_ais)
- GET    /api/gdacs                 (feed -> GDACS GeoJSON)
- GET    /api/usgs                  (feed -> USGS GeoJSON)
- DELETE /api/delete-all            (delete tweets/news + events clusters/items)

Notes:
- No legacy /api/items /api/locations /api/stats /health.
- No legacy clustering models; uses shared.models.events_schema only for clusters/items.
- All imports at top.
"""

from __future__ import annotations

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from aiohttp import web

from shared.models.database import close_database
from shared.models.models import Cluster, NormalizedItem

logger = logging.getLogger(__name__)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _parse_since_time(since_param: Optional[str]) -> Optional[datetime]:
    """
    Supported:
    - None/empty: no filter
    - Relative: "1h", "24h", "7d"
    - ISO8601: "2026-01-15T10:30:00Z" (or with offset)
    """
    if not since_param:
        return None

    s = since_param.strip()

    # Relative
    try:
        if s.endswith("h"):
            hours = int(s[:-1])
            return datetime.utcnow() - timedelta(hours=hours)
        if s.endswith("d"):
            days = int(s[:-1])
            return datetime.utcnow() - timedelta(days=days)
    except ValueError:
        return None

    # ISO
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_timeout(val: Optional[str], default: int = 10) -> int:
    try:
        t = int(val) if val is not None else default
    except ValueError:
        t = default
    return max(1, min(t, 60))


def _parse_required_bbox(request: web.Request) -> Optional[Tuple[float, float, float, float]]:
    try:
        min_lat = float(request.query.get("min_lat", ""))
        max_lat = float(request.query.get("max_lat", ""))
        min_lon = float(request.query.get("min_lon", ""))
        max_lon = float(request.query.get("max_lon", ""))
    except (ValueError, TypeError):
        return None

    if min_lat >= max_lat or min_lon >= max_lon:
        return None
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        return None
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        return None

    return (min_lat, max_lat, min_lon, max_lon)


def _clusters_to_geojson(clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
    features: List[Dict[str, Any]] = []

    for c in clusters:
        lat = c.get("representative_lat")
        lon = c.get("representative_lon")
        if lat is None or lon is None:
            continue

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "cluster_id": c.get("cluster_id"),
                    "item_count": c.get("item_count"),
                    "title": c.get("title"),
                    "summary": c.get("summary"),
                    "representative_location_name": c.get("representative_location_name"),
                    "location_key": c.get("location_key"),
                    "representative_lat": lat,
                    "representative_lon": lon,
                    "first_seen_at": c.get("first_seen_at"),
                    "last_seen_at": c.get("last_seen_at"),
                    "created_at": c.get("created_at"),
                    "updated_at": c.get("updated_at"),
                    "items": c.get("items", []),
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


async def _query_clusters(since_time: Optional[datetime], limit: int) -> List[Dict[str, Any]]:
    conditions = []
    if since_time is not None:
        conditions.append(Cluster.last_seen_at >= since_time)

    q = Cluster.select()
    if conditions:
        q = q.where(*conditions)
    q = q.order_by(Cluster.last_seen_at.desc()).limit(limit)

    out: List[Dict[str, Any]] = []
    for cluster in q:
        items_query = (
            NormalizedItem.select()
            .where(
                (NormalizedItem.cluster_id == cluster.cluster_id)
            )
            .order_by(NormalizedItem.published_at.desc())
        )

        def format_datetime(dt):
            """Format datetime for JSON, handling both datetime objects and strings."""
            if not dt:
                return None
            if isinstance(dt, str):
                # If it's already a string, return as-is (assuming it's ISO format)
                return dt
            return dt.isoformat()

        items: List[Dict[str, Any]] = []
        for item in items_query:
            items.append(
                {
                    "id": item.id,
                    "source": item.source,
                    "source_id": item.source_id,
                    "collected_at": format_datetime(item.collected_at),
                    "published_at": format_datetime(item.published_at),
                    "title": item.title,
                    "text": item.text,
                    "url": item.url,
                    "media_urls": item.get_media_urls(),
                    "entities": item.get_entities(),
                    "location_name": item.location_name,
                    "lat": item.lat,
                    "lon": item.lon,
                    "cluster_id": str(item.cluster_id) if item.cluster_id else None,
                    "author": item.author,
                }
            )

        out.append(
            {
                "cluster_id": str(cluster.cluster_id),
                "created_at": format_datetime(cluster.created_at),
                "updated_at": format_datetime(cluster.updated_at),
                "item_count": cluster.item_count,
                "title": cluster.title,
                "summary": cluster.summary,
                "representative_lat": cluster.representative_lat,
                "representative_lon": cluster.representative_lon,
                "representative_location_name": cluster.representative_location_name,
                "location_key": cluster.representative_location_name.lower() if cluster.representative_location_name else None,
                "first_seen_at": format_datetime(cluster.first_seen_at),
                "last_seen_at": format_datetime(cluster.last_seen_at),
                "items": items,
            }
        )

    return out


class APIServer:
    def __init__(self, address: str = "0.0.0.0", port: int = 8080):
        self.address = address
        self.port = port
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

        # Initialize database
        from shared.models.database import database, initialize_database
        database.connect(reuse_if_open=True)
        initialize_database()

    async def handle_options(self, request: web.Request) -> web.Response:
        return web.Response(headers=CORS_HEADERS)

    async def get_config(self, request: web.Request) -> web.Response:
        """Serve frontend configuration including API keys."""
        try:
            config = {
                "mapboxToken": os.getenv("MAPBOX_TOKEN", ""),
                "cesiumIonToken": os.getenv("CESIUM_ION_TOKEN", ""),
                "openweathermapApiKey": os.getenv("OPENWEATHERMAP_API_KEY", ""),
            }
            return web.json_response(config, headers=CORS_HEADERS)
        except Exception as e:
            logger.error("Error in get_config: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500, headers=CORS_HEADERS)

    # -------- Data endpoints --------

    async def get_clusters(self, request: web.Request) -> web.Response:
        try:
            since_time = _parse_since_time(request.query.get("since"))

            try:
                limit = int(request.query.get("limit", 2000))
            except ValueError:
                limit = 2000
            limit = max(1, min(limit, 5000))

            clusters = await _query_clusters(since_time=since_time, limit=limit)
            return web.json_response(_clusters_to_geojson(clusters), headers=CORS_HEADERS)
        except Exception as e:
            logger.error("Error in get_clusters: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500, headers=CORS_HEADERS)

    async def get_adsb(self, request: web.Request) -> web.Response:
        try:
            bbox = _parse_required_bbox(request)
            if bbox is None:
                return web.json_response(
                    {"error": "Invalid bbox. Required: min_lat, max_lat, min_lon, max_lon"},
                    status=400,
                    headers=CORS_HEADERS,
                )

            timeout = _parse_timeout(request.query.get("timeout"), default=10)

            from adsb_api import fetch_adsb  # external

            min_lat, max_lat, min_lon, max_lon = bbox
            items = await fetch_adsb(min_lat, max_lat, min_lon, max_lon, timeout=timeout)
            return web.json_response({"count": len(items), "items": items}, headers=CORS_HEADERS)
        except Exception as e:
            logger.error("Error in get_adsb: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500, headers=CORS_HEADERS)

    async def get_ais(self, request: web.Request) -> web.Response:
        try:
            bbox = _parse_required_bbox(request)
            if bbox is None:
                return web.json_response(
                    {"error": "Invalid bbox. Required: min_lat, max_lat, min_lon, max_lon"},
                    status=400,
                    headers=CORS_HEADERS,
                )

            timeout = _parse_timeout(request.query.get("timeout"), default=10)

            from ais_api import fetch_ais  # external

            min_lat, max_lat, min_lon, max_lon = bbox
            items = await fetch_ais(min_lat, max_lat, min_lon, max_lon, timeout=timeout)
            return web.json_response({"count": len(items), "items": items}, headers=CORS_HEADERS)
        except Exception as e:
            logger.error("Error in get_ais: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500, headers=CORS_HEADERS)

    async def get_gdacs(self, request: web.Request) -> web.Response:
        try:
            feed = request.query.get("feed", "geojson")
            if feed not in {"geojson", "rss", "rss_24h", "rss_7d"}:
                return web.json_response(
                    {"error": "Invalid feed. Supported: geojson, rss, rss_24h, rss_7d"},
                    status=400,
                    headers=CORS_HEADERS,
                )

            from gdacs_api import fetch_gdacs  # external

            data = await fetch_gdacs(feed=feed)
            return web.json_response(data, headers=CORS_HEADERS)
        except Exception as e:
            logger.error("Error in get_gdacs: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500, headers=CORS_HEADERS)

    async def get_usgs(self, request: web.Request) -> web.Response:
        try:
            feed = request.query.get("feed", "significant_hour")
            if feed not in {"all_hour", "all_day", "significant_hour", "significant_day"}:
                return web.json_response(
                    {"error": "Invalid feed. Supported: all_hour, all_day, significant_hour, significant_day"},
                    status=400,
                    headers=CORS_HEADERS,
                )

            from usgs_api import fetch_usgs  # external

            data = await fetch_usgs(feed=feed)
            return web.json_response(data, headers=CORS_HEADERS)
        except Exception as e:
            logger.error("Error in get_usgs: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500, headers=CORS_HEADERS)

    # -------- Stats endpoint --------
    async def get_stats(self, request: web.Request) -> web.Response:
        try:
            # Get counts from database
            total_items = NormalizedItem.select().count()
            clustered_items = NormalizedItem.select().where(NormalizedItem.cluster_id.is_null(False)).count()
            total_clusters = Cluster.select().count()

            return web.json_response(
                {
                    "normalized_items_count": total_items,
                    "clustered_items_count": clustered_items,
                    "clusters_count": total_clusters,
                },
                headers=CORS_HEADERS,
            )
        except Exception as e:
            logger.error("Error in get_stats: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500, headers=CORS_HEADERS)

    # -------- Maintenance --------

    async def delete_all_data(self, request: web.Request) -> web.Response:
        """
        DELETE /api/delete-all

        Deletes:
        - All normalized items (NormalizedItem rows)
        - All clusters (Cluster rows)
        """
        try:
            # Count what will be deleted
            cluster_count = Cluster.select().count()
            normalized_item_count = NormalizedItem.select().count()

            # Delete all clusters
            Cluster.delete().execute()

            # Delete all normalized items
            NormalizedItem.delete().execute()

            return web.json_response(
                {
                    "status": "success",
                    "clusters_deleted": cluster_count,
                    "normalized_items_deleted": normalized_item_count,
                },
                headers=CORS_HEADERS,
            )
        except Exception as e:
            logger.error("Error in delete_all_data: %s", e, exc_info=True)
            return web.json_response({"status": "error", "error": str(e)}, status=500, headers=CORS_HEADERS)

    # -------- Server lifecycle --------

    async def start_server(self) -> None:
        self.app = web.Application()

        # Core API
        self.app.router.add_get("/api/config", self.get_config)
        self.app.router.add_get("/api/clusters", self.get_clusters)
        self.app.router.add_get("/api/adsb", self.get_adsb)
        self.app.router.add_get("/api/ais", self.get_ais)
        self.app.router.add_get("/api/gdacs", self.get_gdacs)
        self.app.router.add_get("/api/usgs", self.get_usgs)
        self.app.router.add_get("/api/stats", self.get_stats)
        self.app.router.add_delete("/api/delete-all", self.delete_all_data)

        # CORS preflight (generic)
        self.app.router.add_options("/api/{path:.*}", self.handle_options)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.address, self.port)
        await self.site.start()

        logger.info("API server started on http://%s:%s", self.address, self.port)

    async def stop_server(self) -> None:
        try:
            if self.runner:
                await self.runner.cleanup()
        finally:
            close_database()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    address = os.getenv("API_ADDRESS", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8080"))

    server = APIServer(address=address, port=port)

    try:
        await server.start_server()
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Shutting down API server...")
    finally:
        await server.stop_server()


if __name__ == "__main__":
    asyncio.run(main())
