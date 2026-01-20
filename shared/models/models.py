"""
Database models for Multi-Source Live World Events System.

This module defines all Peewee ORM models and dataclasses used throughout the application.
All database tables are created via init.sql when PostgreSQL container starts.
"""

import uuid
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict, fields

# Helper function for timezone-aware datetime
def utcnow():
    return datetime.now(timezone.utc)

from peewee import (
    Model, CharField, TextField, DateTimeField, DoubleField, IntegerField,
    FloatField, ForeignKeyField
)

from .database import BaseModel, database

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ============================================================================
# Database Models (Peewee ORM)
# ============================================================================

class NormalizedItem(BaseModel):
    """Normalized events in unified format."""
    source = CharField(max_length=32, index=True)
    source_id = TextField(index=True)
    collected_at = DateTimeField(default=utcnow, index=True)
    published_at = DateTimeField(null=True, index=True)  # when the event actually happened
    title = TextField(null=True)
    text = TextField(null=True)
    url = TextField(null=True)
    media_urls = TextField(null=True)  # JSON array of media URLs
    entities = TextField(null=True)  # JSON structured data (magnitude, mmsi, etc.)
    location_name = TextField(null=True)
    lat = DoubleField(null=True)
    lon = DoubleField(null=True)
    cluster_id = TextField(null=True, index=True)  # UUID as string
    author = TextField(null=True)

    class Meta:
        table_name = 'normalized_items'
        indexes = (
            (('source', 'source_id'), True),  # unique constraint
            (('lat', 'lon'), False),  # location index
        )

    def get_media_urls(self) -> List[str]:
        """Get media_urls as a list."""
        if not self.media_urls:
            return []
        try:
            return json.loads(self.media_urls)
        except (json.JSONDecodeError, TypeError):
            return []

    def set_media_urls(self, urls: List[str]) -> None:
        """Set media_urls from a list."""
        self.media_urls = json.dumps(urls) if urls else None

    def get_entities(self) -> Optional[Dict[str, Any]]:
        """Get entities as a dict."""
        if not self.entities:
            return None
        try:
            return json.loads(self.entities)
        except (json.JSONDecodeError, TypeError):
            return None

    def set_entities(self, data: Optional[Dict[str, Any]]) -> None:
        """Set entities from a dict."""
        self.entities = json.dumps(data) if data else None

    def __str__(self):
        return f"NormalizedItem(id={self.id}, source={self.source}, title={self.title[:30] if self.title else None})"


class Cluster(BaseModel):
    """Groups of related events."""
    cluster_id = TextField(primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at = DateTimeField(default=utcnow, index=True)
    updated_at = DateTimeField(default=utcnow, index=True)
    item_count = IntegerField(default=0, index=True)
    tags = TextField(null=True)  # JSON array of descriptive tags
    title = TextField(null=True)
    summary = TextField(null=True)
    representative_lat = DoubleField(null=True)
    representative_lon = DoubleField(null=True)
    representative_location_name = TextField(null=True)
    first_seen_at = DateTimeField(null=True)
    last_seen_at = DateTimeField(null=True)

    class Meta:
        table_name = 'clusters'

    def get_tags(self) -> List[str]:
        """Get tags as a list."""
        if not self.tags:
            return []
        try:
            return json.loads(self.tags)
        except (json.JSONDecodeError, TypeError):
            return []

    def set_tags(self, tag_list: List[str]) -> None:
        """Set tags from a list."""
        self.tags = json.dumps(tag_list) if tag_list else None

    def __str__(self):
        return f"Cluster(id={self.cluster_id}, title={self.title[:30] if self.title else None}, items={self.item_count})"






# ============================================================================
# Dataclasses (for ingestion records, not database models)
# ============================================================================

@dataclass
class IngestionRecord:
    """
    Unified ingestion record schema.

    All connectors must output records in this format for uniform processing.
    """
    source: str  # gdelt|telegram|mastodon|adsb|ais|rss
    source_id: str  # unique per source item (URL, message id, quake id, etc.)
    collected_at: int  # unix timestamp in seconds

    # Optional content fields
    published_at: Optional[str] = None  # ISO datetime string
    title: Optional[str] = None
    text: Optional[str] = None
    url: Optional[str] = None  # canonical link
    media_urls: Optional[List[str]] = None  # videos/images/external links
    author: Optional[str] = None  # author/creator of the content

    # Structured data
    entities: Optional[Dict[str, Any]] = None  # callsign, mmsi, icao, magnitude, alert_level, etc.

    # Geographic information
    location_name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None

    # Debug info
    raw: Optional[Dict[str, Any]] = None  # original payload for debugging

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)

        # Remove None values for cleaner JSON
        return {k: v for k, v in data.items() if v is not None}

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IngestionRecord':
        """Create record from dictionary."""
        # Get valid field names from the dataclass
        valid_fields = {f.name for f in fields(cls)}

        # Filter out unknown fields
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}

        # Validate required fields are present
        required_fields = {'source', 'source_id', 'collected_at'}
        missing_fields = required_fields - set(filtered_data.keys())
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        return cls(**filtered_data)

    @classmethod
    def from_json(cls, json_str: str) -> 'IngestionRecord':
        """Create record from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)

    def get_hash(self) -> str:
        """
        Generate a hash for deduplication based on source and source_id.
        This ensures we don't store duplicate records from the same source.
        """
        import hashlib
        key = f"{self.source}:{self.source_id}"
        return hashlib.sha256(key.encode()).hexdigest()

    def has_location(self) -> bool:
        """Check if record has geographic coordinates."""
        return self.lat is not None and self.lon is not None

    def get_bbox(self) -> Optional[List[float]]:
        """Get bounding box inferred from point."""
        if self.has_location():
            # Create small bbox around point (1km buffer)
            lat, lon = self.lat, self.lon
            buffer = 0.01  # roughly 1km
            return [lat - buffer, lat + buffer, lon - buffer, lon + buffer]
        return None


# Type aliases for clarity
RecordList = List[IngestionRecord]
RecordDict = Dict[str, Any]


def validate_record(record: IngestionRecord) -> List[str]:
    """
    Validate an ingestion record and return list of validation errors.

    Returns empty list if valid.
    """
    errors = []

    if not record.source:
        errors.append("source is required")
    elif record.source not in {'gdelt', 'telegram', 'mastodon', 'adsb', 'ais', 'rss'}:
        errors.append(f"invalid source: {record.source}")

    if not record.source_id:
        errors.append("source_id is required")

    if record.collected_at <= 0:
        errors.append("collected_at must be positive unix timestamp")

    # Validate location coordinates
    if record.lat is not None and record.lon is not None:
        if not (-90 <= record.lat <= 90):
            errors.append("latitude must be between -90 and 90")
        if not (-180 <= record.lon <= 180):
            errors.append("longitude must be between -180 and 180")

    return errors


# ============================================================================
# Utility functions
# ============================================================================

def get_recent_events(hours: int = 24, limit: int = 1000) -> List[NormalizedItem]:
    """Get recent events with location data."""
    since = datetime.utcnow().replace(hour=datetime.utcnow().hour - hours)
    return (NormalizedItem
            .select()
            .where((NormalizedItem.published_at >= since) &
                   (NormalizedItem.lat.is_null(False)) &
                   (NormalizedItem.lon.is_null(False)))
            .order_by(NormalizedItem.published_at.desc())
            .limit(limit))


def get_active_clusters(hours: int = 1, min_items: int = 2) -> List[Cluster]:
    """Get recently active clusters."""
    since = datetime.utcnow().replace(hour=datetime.utcnow().hour - hours)
    return (Cluster
            .select()
            .where((Cluster.updated_at >= since) &
                   (Cluster.item_count >= min_items))
            .order_by(Cluster.updated_at.desc()))
