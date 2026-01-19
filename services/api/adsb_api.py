#!/usr/bin/env python3
"""
adsb_api.py - Standalone ADSB fetcher (no server)

Public async API (expected by your aiohttp handler):
    async def fetch_adsb(min_lat, max_lat, min_lon, max_lon, timeout=10) -> list[dict]

Implementation:
- Uses adsb.lol HTTP endpoints (no API key)
- Probes for a working endpoint template once (cached)
- Queries by center+radius that covers the bbox, then filters back to bbox
- Runs blocking requests in a thread to keep the event loop responsive
"""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
ENDPOINT_TEMPLATES = [
    "https://api.adsb.lol/v2/lat/{lat}/lon/{lon}/dist/{dist}",
    "https://api.adsb.lol/v2/lat/{lat}/lon/{lon}/dist/{dist}/",
    "https://api.adsb.lol/api/aircraft/lat/{lat}/lon/{lon}/dist/{dist}",
    "https://api.adsb.lol/api/aircraft/lat/{lat}/lon/{lon}/dist/{dist}/",
]
NM_PER_KM = 1.0 / 1.852

# Cache a working template after first successful probe
_WORKING_ENDPOINT_TEMPLATE: Optional[str] = None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bbox_center_and_covering_radius_nm(
    lamin: float, lamax: float, lomin: float, lomax: float
) -> Tuple[float, float, float]:
    clat = (lamin + lamax) / 2.0
    clon = (lomin + lomax) / 2.0
    corners = [
        (lamin, lomin),
        (lamin, lomax),
        (lamax, lomin),
        (lamax, lomax),
    ]
    max_km = max(haversine_km(clat, clon, la, lo) for la, lo in corners)
    radius_nm = max_km * NM_PER_KM
    return clat, clon, radius_nm


def in_bbox(lat: float, lon: float, lamin: float, lamax: float, lomin: float, lomax: float) -> bool:
    return lamin <= lat <= lamax and lomin <= lon <= lomax


def _http_get_json(url: str, timeout: int) -> Tuple[Optional[Dict[str, Any]], Optional[requests.Response], Optional[str]]:
    try:
        r = requests.get(
            url,
            headers={"User-Agent": UA, "Accept": "application/json"},
            timeout=timeout,
        )
        if r.status_code in (429, 503):
            return None, r, None
        r.raise_for_status()
        return r.json(), r, None
    except Exception as e:
        return None, None, str(e)


def _parse_aircraft_list(payload: Dict[str, Any]) -> Tuple[Optional[int], List[Dict[str, Any]]]:
    now: Optional[int] = None
    if isinstance(payload.get("now"), (int, float)):
        now = int(payload["now"])
    elif isinstance(payload.get("time"), (int, float)):
        now = int(payload["time"])

    if isinstance(payload.get("ac"), list):
        return now, payload["ac"]
    if isinstance(payload.get("aircraft"), list):
        return now, payload["aircraft"]
    if isinstance(payload.get("states"), list):
        return now, payload["states"]

    return now, []


def _norm_int(x: Any) -> Optional[int]:
    try:
        if x is None or isinstance(x, bool):
            return None
        return int(float(x))
    except Exception:
        return None


def _norm_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None


def _build_aircraft(provider: str, now: Optional[int], d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    icao = (d.get("icao") or d.get("hex") or d.get("icao24") or "").strip().lower()
    if not icao:
        return None

    lat = _norm_float(d.get("lat"))
    lon = _norm_float(d.get("lon"))
    if lat is None or lon is None:
        return None

    callsign = (d.get("flight") or d.get("call") or d.get("callsign") or "").strip() or None

    alt_baro_ft = _norm_int(d.get("alt_baro"))
    alt_geom_ft = _norm_int(d.get("alt_geom"))
    gs_kt = _norm_float(d.get("gs"))
    track_deg = _norm_float(d.get("track"))
    vrt_rate_fpm = _norm_int(d.get("baro_rate") or d.get("geom_rate") or d.get("rate"))
    squawk = (d.get("squawk") or "").strip() or None
    category = (d.get("category") or d.get("t") or "").strip() or None
    seen_pos_sec = _norm_int(d.get("seen_pos"))
    seen_sec = _norm_int(d.get("seen"))
    rssi = _norm_float(d.get("rssi"))

    return {
        "id": icao,
        "icao": icao,
        "callsign": callsign,
        "lat": float(lat),
        "lon": float(lon),
        "alt_baro_ft": alt_baro_ft,
        "alt_geom_ft": alt_geom_ft,
        "speed_knots": gs_kt,
        "heading_deg": track_deg,
        "vertical_rate_fpm": vrt_rate_fpm,
        "squawk": squawk,
        "category": category,
        "seen_pos_sec": seen_pos_sec,
        "seen_sec": seen_sec,
        "rssi": rssi,
        "type": "adsb",
        "source": provider,
        "timestamp": now or int(time.time()),
    }


def _pick_working_endpoint_sync(lat: float, lon: float, dist_nm: float, timeout: int) -> str:
    probe_dist = max(1.0, min(10.0, dist_nm))
    last_err: Optional[str] = None

    for tpl in ENDPOINT_TEMPLATES:
        url = tpl.format(lat=f"{lat:.6f}", lon=f"{lon:.6f}", dist=f"{probe_dist:.0f}")
        data, _resp, err = _http_get_json(url, timeout=timeout)
        if err:
            last_err = err
            continue
        if data is None:
            continue
        if isinstance(data, dict) and any(k in data for k in ("ac", "aircraft", "states")):
            return tpl

    raise RuntimeError(f"Could not find a working adsb.lol endpoint (all probes failed). Last error: {last_err}")


def _fetch_bbox_aircraft_sync(
    lamin: float,
    lamax: float,
    lomin: float,
    lomax: float,
    endpoint_tpl: str,
    timeout: int,
    dist_nm_override: Optional[float] = None,
) -> List[Dict[str, Any]]:
    clat, clon, radius_nm = bbox_center_and_covering_radius_nm(lamin, lamax, lomin, lomax)
    dist_nm = dist_nm_override if dist_nm_override is not None else radius_nm

    url = endpoint_tpl.format(lat=f"{clat:.6f}", lon=f"{clon:.6f}", dist=f"{dist_nm:.0f}")
    payload, resp, err = _http_get_json(url, timeout=timeout)

    if resp is not None and resp.status_code in (429, 503):
        return []
    if err or payload is None or not isinstance(payload, dict):
        return []

    now, raw_list = _parse_aircraft_list(payload)
    out: List[Dict[str, Any]] = []

    for d in raw_list:
        if not isinstance(d, dict):
            continue
        ac = _build_aircraft("adsb.lol", now, d)
        if not ac:
            continue
        if in_bbox(ac["lat"], ac["lon"], lamin, lamax, lomin, lomax):
            out.append(ac)

    return out


def _validate_bbox(min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> None:
    if not (-90.0 <= min_lat <= 90.0 and -90.0 <= max_lat <= 90.0):
        raise ValueError("Latitude must be between -90 and 90")
    if not (-180.0 <= min_lon <= 180.0 and -180.0 <= max_lon <= 180.0):
        raise ValueError("Longitude must be between -180 and 180")
    if min_lat >= max_lat or min_lon >= max_lon:
        raise ValueError("Invalid bounding box: min_lat must be < max_lat and min_lon must be < max_lon")


async def fetch_adsb(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    timeout: int = 10,
) -> List[Dict[str, Any]]:
    """
    Fetch ADSB aircraft in a bounding box.

    Expected signature for your server:
        async def fetch_adsb(min_lat, max_lat, min_lon, max_lon, timeout=10) -> list[dict]
    """
    global _WORKING_ENDPOINT_TEMPLATE

    _validate_bbox(min_lat, max_lat, min_lon, max_lon)

    clat, clon, radius_nm = bbox_center_and_covering_radius_nm(min_lat, max_lat, min_lon, max_lon)

    # Ensure we have a working endpoint cached
    if _WORKING_ENDPOINT_TEMPLATE is None:
        _WORKING_ENDPOINT_TEMPLATE = await asyncio.to_thread(
            _pick_working_endpoint_sync, clat, clon, radius_nm, int(timeout)
        )

    # Fetch using cached endpoint. If it fails (e.g., endpoint changed), re-probe once.
    items = await asyncio.to_thread(
        _fetch_bbox_aircraft_sync,
        min_lat,
        max_lat,
        min_lon,
        max_lon,
        _WORKING_ENDPOINT_TEMPLATE,
        int(timeout),
        None,
    )

    if items:
        return items

    # Re-probe once on empty result set (could be legit empty, but also could be endpoint breakage).
    # If you want "empty means empty", remove this block.
    try:
        new_tpl = await asyncio.to_thread(_pick_working_endpoint_sync, clat, clon, radius_nm, int(timeout))
        if new_tpl != _WORKING_ENDPOINT_TEMPLATE:
            _WORKING_ENDPOINT_TEMPLATE = new_tpl
            items = await asyncio.to_thread(
                _fetch_bbox_aircraft_sync,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
                _WORKING_ENDPOINT_TEMPLATE,
                int(timeout),
                None,
            )
    except Exception:
        pass

    return items
