"""
USGS connector for authoritative earthquake data.

Provides clean, map-native, authoritative event stream for earthquakes.
"""

import time
import requests
from typing import Iterator, Dict, Any, Optional
from datetime import datetime

from ..base import BaseConnector
from shared.models.models import IngestionRecord


class USGSConnector(BaseConnector):
    """
    USGS connector for earthquake monitoring.

    Polls USGS real-time GeoJSON feeds for new earthquake events.
    """

    USGS_FEEDS = {
        "all_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        "all_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
        "significant_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_hour.geojson",
        "significant_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson",
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__('usgs', config)

        # Configuration
        self.feed_name = config.get('feed', 'significant_hour')
        self.feed_url = self.USGS_FEEDS.get(self.feed_name, self.USGS_FEEDS['significant_hour'])
        self.poll_interval = config.get('poll_interval', 300)  # 5 minutes

    def fetch_earthquakes(self) -> list:
        """
        Fetch latest earthquakes from USGS feed.

        Returns list of earthquake feature dictionaries.
        """
        response = requests.get(self.feed_url, timeout=30)
        response.raise_for_status()

        data = response.json()
        return data.get('features', [])

    def earthquake_to_record(self, feature: Dict[str, Any]) -> IngestionRecord:
        """
        Convert USGS earthquake feature to unified IngestionRecord format.

        USGS features have properties with mag, place, time, etc. and geometry with coordinates.
        """
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})

        # Extract basic properties
        event_id = props.get('code', feature.get('id', ''))
        magnitude = props.get('mag')
        place = props.get('place', '')
        url = props.get('url', '')
        tsunami = bool(props.get('tsunami', 0))
        significance = props.get('sig')

        # Convert timestamp from milliseconds to ISO
        published_at = None
        if props.get('time'):
            try:
                # USGS time is in milliseconds since epoch
                dt = datetime.fromtimestamp(props['time'] / 1000)
                published_at = dt.isoformat()
            except (ValueError, TypeError):
                pass

        # Extract location
        location_data = None
        if geom.get('type') == 'Point' and geom.get('coordinates'):
            lon, lat, depth = geom['coordinates'][:3]  # depth in km
            location_data = {
                'lat': lat,
                'lon': lon,
                'place_name': place,
            }

        # Title combines magnitude and place
        title = f"M{magnitude:.1f} - {place}" if magnitude is not None else place

        return self.create_record(
            source_id=event_id,
            title=title,
            url=url,
            published_at=published_at,
            author=None,
            entities={
                'magnitude': magnitude,
                'depth_km': geom.get('coordinates', [None, None, None])[2] if geom.get('coordinates') else None,
                'tsunami': tsunami,
                'significance': significance,
                'usgs_feed': self.feed_name,
            },
            location_data=location_data,
            raw=feature,  # Keep original for debugging
        )

    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Fetch earthquakes from USGS feed.
        """
        try:
            # Fetch latest earthquakes
            features = self.fetch_earthquakes()

            for feature in features:
                event_id = feature.get('id', '')
                if not event_id:
                    continue

                # Convert to unified record
                record = self.earthquake_to_record(feature)

                # Validate record
                from shared.models.models import validate_record
                errors = validate_record(record)
                if errors:
                    self.logger.warning(f"Invalid USGS record for {event_id}: {errors}")
                    continue

                yield record

            self.logger.info(f"Fetched {len(features)} earthquakes from USGS")

        except Exception as e:
            self.logger.error(f"Error fetching from USGS: {e}")
            raise