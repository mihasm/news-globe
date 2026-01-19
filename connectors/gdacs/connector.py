"""
GDACS connector for authoritative multi-hazard alerts.

Provides official multi-hazard alerting for floods, cyclones, volcanoes, etc.
"""

import time
import requests
from typing import Iterator, Dict, Any, Optional
from datetime import datetime

from ..base import BaseConnector
from shared.models.models import IngestionRecord


class GDACSConnector(BaseConnector):
    """
    GDACS connector for multi-hazard monitoring.

    Polls GDACS public feeds for new hazard events.
    """

    GDACS_FEEDS = {
        "geojson": "https://www.gdacs.org/contentdata/xml/gdacs.geojson",
        "rss": "https://www.gdacs.org/contentdata/xml/rss.xml",
        "rss_24h": "https://www.gdacs.org/contentdata/xml/rss_24h.xml",
        "rss_7d": "https://www.gdacs.org/contentdata/xml/rss_7d.xml",
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__('gdacs', config)

        # Configuration
        self.feed_name = config.get('feed', 'geojson')
        self.feed_url = self.GDACS_FEEDS.get(self.feed_name, self.GDACS_FEEDS['geojson'])
        self.poll_interval = config.get('poll_interval', 600)  # 10 minutes
        self.user_agent = config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)')

    def fetch_events(self) -> list:
        """
        Fetch latest events from GDACS feed.

        Returns list of event dictionaries.
        """
        headers = {'User-Agent': self.user_agent}
        response = requests.get(self.feed_url, headers=headers, timeout=30)
        response.raise_for_status()

        if self.feed_name == 'geojson':
            data = response.json()
            return data.get('features', [])
        else:
            # Parse RSS - simplified implementation
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            events = []

            for item in root.findall('.//item'):
                event = self._parse_rss_item(item)
                if event:
                    events.append(event)

            return events

    def _parse_rss_item(self, item) -> Optional[Dict[str, Any]]:
        """Parse RSS item to event dict."""
        title = item.find('title')
        link = item.find('link')
        description = item.find('description')
        pub_date = item.find('pubDate')

        if not title or not link:
            return None

        return {
            'title': title.text or '',
            'url': link.text or '',
            'description': description.text if description is not None else '',
            'published': pub_date.text if pub_date is not None else '',
            'event_id': link.text or '',  # Use URL as ID
        }

    def event_to_record(self, event: Dict[str, Any]) -> IngestionRecord:
        """
        Convert GDACS event to unified IngestionRecord format.
        """
        if self.feed_name == 'geojson':
            return self._geojson_event_to_record(event)
        else:
            return self._rss_event_to_record(event)

    def _geojson_event_to_record(self, feature: Dict[str, Any]) -> IngestionRecord:
        """Convert GeoJSON feature to record."""
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})

        event_id = props.get('eventid', feature.get('id', ''))
        event_type = props.get('eventtype')
        alert_level = props.get('alertlevel')
        title = props.get('title', '')
        url = props.get('link', '')
        published = props.get('fromdate')

        # Convert published date to ISO if present
        published_at = None
        if published:
            try:
                # Try different date formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        dt = datetime.strptime(published, fmt)
                        published_at = dt.isoformat()
                        break
                    except ValueError:
                        continue
            except:
                pass

        # Extract location
        location_data = None
        if geom.get('type') == 'Point' and geom.get('coordinates'):
            lon, lat = geom['coordinates'][:2]
            location_data = {
                'lat': lat,
                'lon': lon,
                'place_name': props.get('country'),
            }

        return self.create_record(
            source_id=event_id,
            title=title,
            text=props.get('description'),
            url=url,
            published_at=published_at,
            author=None,
            entities={
                'event_type': event_type,
                'alert_level': alert_level,
                'country': props.get('country'),
                'severity': props.get('severity'),
            },
            location_data=location_data,
            raw=feature,
        )

    def _rss_event_to_record(self, event: Dict[str, Any]) -> IngestionRecord:
        """Convert RSS event to record."""
        published_at = None
        if event.get('published'):
            try:
                # Parse RSS date format
                dt = datetime.strptime(event['published'], '%a, %d %b %Y %H:%M:%S %z')
                published_at = dt.isoformat()
            except:
                pass

        return self.create_record(
            source_id=event['url'],  # Use URL as ID
            title=event.get('title'),
            text=event.get('description'),
            url=event.get('url'),
            published_at=published_at,
            author=None,
            entities={
                'feed_type': 'rss',
            },
            raw=event,
        )

    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Fetch events from GDACS feed.
        """
        try:
            # Fetch latest events
            events = self.fetch_events()

            for event in events:
                event_id = event.get('id') or event.get('eventid') or event.get('url', '')
                if not event_id:
                    continue

                # Convert to unified record
                record = self.event_to_record(event)

                # Validate record
                from shared.models.models import validate_record
                errors = validate_record(record)
                if errors:
                    self.logger.warning(f"Invalid GDACS record for {event_id}: {errors}")
                    continue

                yield record

            self.logger.info(f"Fetched {len(events)} events from GDACS")

        except Exception as e:
            self.logger.error(f"Error fetching from GDACS: {e}")
            raise