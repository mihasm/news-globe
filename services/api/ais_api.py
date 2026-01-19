# ais_api.py
from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import websockets

WS_URL = "wss://stream.aisstream.io/v0/stream"


def _validate_bbox(min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> None:
    if not (-90.0 <= min_lat <= 90.0 and -90.0 <= max_lat <= 90.0):
        raise ValueError("Latitude must be between -90 and 90")
    if not (-180.0 <= min_lon <= 180.0 and -180.0 <= max_lon <= 180.0):
        raise ValueError("Longitude must be between -180 and 180")
    if min_lat >= max_lat or min_lon >= max_lon:
        raise ValueError("Invalid bbox: min_lat < max_lat and min_lon < max_lon are required")


def build_subscription(
    api_key: str,
    bounding_boxes: List[List[List[float]]],
    mmsi: Optional[List[str]] = None,
    msg_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    sub: Dict[str, Any] = {
        "APIKey": api_key,
        "BoundingBoxes": bounding_boxes,
    }
    if mmsi:
        sub["FiltersShipMMSI"] = mmsi
    if msg_types:
        sub["FilterMessageTypes"] = msg_types
    return sub


def _extract_body(msg: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    mtype = msg.get("MessageType") or "Unknown"
    container = msg.get("Message") or {}
    body = container.get(mtype) or {}
    if not isinstance(body, dict):
        body = {}
    return mtype, body


def _extract_mmsi(_: str, body: Dict[str, Any]) -> Optional[int]:
    user_id = body.get("UserID")
    if user_id is None:
        return None
    try:
        return int(user_id)
    except (TypeError, ValueError):
        return None


def _merge_vessel_record(
    vessels: Dict[int, Dict[str, Any]],
    mmsi: int,
    msg: Dict[str, Any],
) -> None:
    now = time.time()

    meta = msg.get("Metadata") or {}
    if not isinstance(meta, dict):
        meta = {}

    mtype, body = _extract_body(msg)

    rec = vessels.get(mmsi)
    if rec is None:
        rec = {"mmsi": mmsi, "first_seen": now}
        vessels[mmsi] = rec

    rec["last_seen"] = now
    rec["last_message_type"] = mtype

    lat = meta.get("Latitude")
    lon = meta.get("Longitude")
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        rec["last_position"] = {"lat": float(lat), "lon": float(lon)}

    for k_in, k_out in [
        ("Sog", "sog"),
        ("Cog", "cog"),
        ("Heading", "heading"),
        ("RateOfTurn", "rot"),
        ("NavigationalStatus", "nav_status"),
    ]:
        v = body.get(k_in)
        if v is not None:
            rec[k_out] = v

    for k_in, k_out in [
        ("Name", "name"),
        ("CallSign", "callsign"),
        ("ImoNumber", "imo"),
        ("ShipType", "ship_type"),
        ("Destination", "destination"),
        ("Eta", "eta"),
        ("Draught", "draught"),
        ("DimensionToBow", "dim_to_bow"),
        ("DimensionToStern", "dim_to_stern"),
        ("DimensionToPort", "dim_to_port"),
        ("DimensionToStarboard", "dim_to_starboard"),
    ]:
        v = body.get(k_in)
        if v is not None and v != "":
            rec[k_out] = v

    latest_subset: Dict[str, Any] = {}
    for k, v in body.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            if isinstance(v, str) and len(v) > 200:
                latest_subset[k] = v[:200] + "…"
            else:
                latest_subset[k] = v
    rec["latest_body"] = latest_subset


async def snapshot_vessels(
    api_key: str,
    bounding_boxes: List[List[List[float]]],
    mmsi_filter: Optional[List[str]],
    msg_types: Optional[List[str]],
    *,
    min_duration_s: float,
    stable_window_s: float,
    hard_timeout_s: float,
) -> Dict[str, Any]:
    vessels: Dict[int, Dict[str, Any]] = {}
    started = time.time()
    last_new = started

    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=8 * 1024 * 1024,
    ) as ws:
        sub = build_subscription(api_key, bounding_boxes, mmsi=mmsi_filter, msg_types=msg_types)
        await ws.send(json.dumps(sub))

        while True:
            now = time.time()

            if now - started >= hard_timeout_s:
                break

            if (now - started) >= min_duration_s and (now - last_new) >= stable_window_s:
                break

            try:
                message_json = await asyncio.wait_for(ws.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            try:
                msg = json.loads(message_json)
            except json.JSONDecodeError:
                continue

            if isinstance(msg, dict) and "error" in msg:
                raise RuntimeError(str(msg.get("error")))

            if not isinstance(msg, dict):
                continue

            mtype, body = _extract_body(msg)
            if not isinstance(body, dict):
                continue

            mmsi = _extract_mmsi(mtype, body)
            if mmsi is None:
                continue

            is_new = mmsi not in vessels
            _merge_vessel_record(vessels, mmsi, msg)
            if is_new:
                last_new = time.time()

    vessel_list = [vessels[k] for k in sorted(vessels.keys())]
    return {"vessels": vessel_list}


async def fetch_ais(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    *,
    timeout: float = 1,
) -> List[Dict[str, Any]]:
    """
    Fetch a snapshot of AIS vessels in the given bbox.

    Expected by your API handler:
      async def fetch_ais(min_lat, max_lat, min_lon, max_lon, timeout=10) -> list[dict]
    """
    _validate_bbox(float(min_lat), float(max_lat), float(min_lon), float(max_lon))

    api_key = os.getenv("AISSTREAM_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("AISSTREAM_API_KEY environment variable not set")

    hard_timeout_s = float(timeout)
    if hard_timeout_s <= 0:
        raise ValueError("timeout must be > 0")

    # Heuristics tuned for "snapshot within timeout":
    # - require at least ~half the timeout to collect (capped)
    # - stop early once stable for a short window
    min_duration_s = max(1.0, min(5.0, hard_timeout_s * 0.6))
    stable_window_s = max(0.75, min(2.0, hard_timeout_s * 0.2))

    bounding_boxes = [[[float(min_lat), float(min_lon)], [float(max_lat), float(max_lon)]]]

    result = await snapshot_vessels(
        api_key=api_key,
        bounding_boxes=bounding_boxes,
        mmsi_filter=None,
        msg_types=None,
        min_duration_s=min_duration_s,
        stable_window_s=stable_window_s,
        hard_timeout_s=hard_timeout_s,
    )

    vessels = result.get("vessels", [])
    if not isinstance(vessels, list):
        return []
    return vessels
