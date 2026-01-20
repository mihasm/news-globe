"""
Events Ingestion Service - Processes unified ingestion records from connectors

Handles:
1. Storing raw data from connectors
2. Normalizing records into unified format
3. Deduplication based on source + source_id
4. Preparing records for clustering
"""

import os
import logging
import time
import requests
from typing import List, Dict, Optional, Iterator
from datetime import datetime
import traceback

from shared.models.models import IngestionRecord, validate_record
from shared.models.database import database
from location import LocationGetter

import spacy

logger = logging.getLogger(__name__)


class EventsIngestionService:
    """
    Processes IngestionRecord objects from connectors into the events database.

    This service:
    1. Stores raw data in raw_items table
    2. Normalizes and deduplicates records
    3. Enriches missing location using NER -> GeoNames resolver
    """

    def __init__(self, batch_size: int = 50, memory_store_url: str = None):
        self.batch_size = batch_size
        self.memory_store_url = memory_store_url or os.getenv("MEMORY_STORE_URL", "http://memory-store:6379")

        self.stats = {
            "processed": 0,
            "inserted": 0,
            "skipped_duplicates": 0,
            "validation_errors": 0,
            "no_location_data": 0,
            "missing_published_at": 0,
            "unknown_error": 0,
            "location_ner_attempted": 0,
            "location_ner_found": 0,
            "location_resolved": 0,
        }

        database.connect(reuse_if_open=True)
        logger.debug(f"Database connected: {database.database}")

        # Initialize database tables
        from shared.models.database import initialize_database
        initialize_database()

        # Location service resolver
        location_service_url = os.getenv("LOCATION_SERVICE_URL", "http://location:8787")
        self.location_getter = LocationGetter(service_url=location_service_url)

        # spaCy multilingual NER model (installed in entrypoint)
        model_name = os.getenv("SPACY_MODEL", "xx_ent_wiki_sm")
        self.nlp = spacy.load(model_name, disable=["tagger", "parser", "lemmatizer", "attribute_ruler"])
        logger.info(f"spaCy loaded: {model_name}")

        # LOC/GPE labels vary by model; keep both
        self._loc_labels = {"LOC", "GPE"}

        # Optional: common false positives you observed
        self._stop_lower = set(x.strip().lower() for x in os.getenv("LOC_STOPWORDS", "man,it,der").split(",") if x.strip())

        from shared.models.models import NormalizedItem
        self.NormalizedItem = NormalizedItem

    def process_records(self, records: Iterator[IngestionRecord]) -> Dict[str, int]:
        batch: List[IngestionRecord] = []
        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                self._process_batch(batch)
                batch = []
        if batch:
            self._process_batch(batch)
        return self.stats.copy()

    def _process_batch(self, records: List[IngestionRecord]) -> Dict[str, int]:
        if not records:
            return self.stats.copy()

        logger.debug(f"Processing batch of {len(records)} records")

        valid_records: List[IngestionRecord] = []
        for record in records:
            self.stats["processed"] += 1
            errors = validate_record(record)
            if errors:
                logger.warning(f"Invalid record {record.source}:{record.source_id}: {errors}")
                self.stats["validation_errors"] += 1
                continue
            valid_records.append(record)

        if not valid_records:
            return self.stats.copy()

        # Enrich missing location using spaCy NER -> GeoNames resolver
        self._enrich_locations_with_spacy(valid_records)

        # Process each record individually to prevent one bad record from aborting the entire batch
        for record in valid_records:
            try:
                with database.atomic():
                    result = self._store_normalized_item(record)

                    if result == 1:
                        self.stats["inserted"] += 1
                    elif result == 0:
                        self.stats["skipped_duplicates"] += 1
                    elif result == -1:
                        self.stats["no_location_data"] += 1
                    elif result == -2:
                        self.stats["missing_published_at"] += 1
                    else:
                        self.stats["unknown_error"] += 1

            except Exception as e:
                logger.error(f"Error processing individual record {record.source}:{record.source_id}: {e}")
                self.stats["unknown_error"] += 1

        logger.info(f"Processed batch of {len(valid_records)} records; stats={self.stats}")

        return self.stats.copy()

    def _enrich_locations_with_spacy(self, records: List[IngestionRecord]) -> None:
        # Collect texts to NER only for records missing location
        idx_map: List[int] = []
        texts: List[str] = []

        for i, r in enumerate(records):
            if r.has_location():
                continue
            if not (r.title or r.text):
                continue
            title = r.title or ""
            text = r.text or ""
            combined = (title + "\n" + text).strip()
            if not combined:
                continue
            idx_map.append(i)
            texts.append(combined)

        if not texts:
            return

        self.stats["location_ner_attempted"] += len(texts)

        # spaCy pipe for throughput
        for i, doc in enumerate(self.nlp.pipe(texts, batch_size=64)):
            rec = records[idx_map[i]]

            # Extract location entities
            candidates: List[str] = []
            for ent in doc.ents:
                if ent.label_ not in self._loc_labels:
                    continue
                s = ent.text.strip()

                # Filters to kill common junk
                s_lower = s.lower()
                if len(s) < 3:
                    continue
                if s_lower in self._stop_lower:
                    continue
                # Avoid all-lowercase single tokens (common false positives)
                if " " not in s and s.islower():
                    continue

                candidates.append(s)

            # De-dup while preserving order
            seen = set()
            deduped: List[str] = []
            for c in candidates:
                k = c.lower()
                if k not in seen:
                    seen.add(k)
                    deduped.append(c)

            if not deduped:
                continue

            self.stats["location_ner_found"] += 1

            # Resolve candidates with your GeoNames resolver.
            # Assumes LocationGetter can accept raw text; if you have a "resolve_name" method, prefer that.
            # Here we resolve by feeding candidate strings (first match wins).
            try:
                resolved = None
                for cand in deduped[:5]:
                    # If LocationGetter only exposes parse_locations_batch(texts),
                    # we can still call it with the candidate string as "text".
                    res_list = self.location_getter.parse_locations_batch([cand])
                    if res_list and res_list[0]:
                        resolved = res_list[0]
                        break

                if not resolved:
                    continue

                location_name, lat, lng, area, similarity = resolved
                rec.location_name = location_name
                rec.lat = lat
                rec.lon = lng
                self.stats["location_resolved"] += 1

            except Exception as e:
                logger.error(f"Location resolve failed for {rec.source}:{rec.source_id}: {e}")

    def _store_normalized_item(self, record: IngestionRecord) -> int:
        """
        Returns:
          1 inserted
          0 duplicate ignored
         -1 no location
         -2 missing published_at
         -3 error
        """
        if not record.has_location():
            logger.warning(f"No location data for {record.source}:{record.source_id}")
            return -1
        if record.published_at is None:
            logger.warning(f"Missing published_at for {record.source}:{record.source_id}")
            return -2
        if record.source == 'mastodon' and 'emsc' in record.source_id:
            return -3 # ignore emsc tweets

        # Convert timestamps safely
        # TODO: THIS SHOULD BE HANDLED BY THE INGESTION BEFORE THIS FUNCTION IS CALLED
        try:
            collected_at_dt = datetime.fromtimestamp(record.collected_at)
        except (ValueError, OSError) as e:
            logger.warning(f"Invalid collected_at timestamp for {record.source}:{record.source_id}: {record.collected_at}")
            return -3

        # Convert published_at string to datetime if present
        published_at_dt = None
        if record.published_at:
            if isinstance(record.published_at, str):
                try:
                    published_at_dt = datetime.fromisoformat(record.published_at.replace('Z', '+00:00'))
                except ValueError:
                    # If parsing fails, use None
                    published_at_dt = None
                    logger.warning(f"Invalid published_at for {record.source}:{record.source_id}: {record.published_at}")
                    return -3
            else:
                published_at_dt = record.published_at

        try:
            # Create a new NormalizedItem instance to use helper methods
            item = self.NormalizedItem(
                source=record.source,
                source_id=record.source_id,
                collected_at=collected_at_dt,
                published_at=published_at_dt,
                title=record.title,
                text=record.text,
                url=record.url,
                location_name=record.location_name,
                lat=record.lat,
                lon=record.lon,
                author=record.author,
            )

            # Use helper methods for JSON fields
            try:
                item.set_media_urls(record.media_urls)
                item.set_entities(record.entities)
            except (TypeError, ValueError) as e:
                logger.warning(f"JSON serialization error for {record.source}:{record.source_id}: {e}")
                # Set to None if serialization fails
                item.media_urls = None
                item.entities = None

            # Check if record already exists to avoid duplicate key errors
            try:
                existing = (self.NormalizedItem
                           .select()
                           .where((self.NormalizedItem.source == item.source) &
                                  (self.NormalizedItem.source_id == item.source_id))
                           .first())

                if existing:
                    # Record already exists
                    return 0

                # Record doesn't exist, try to insert
                item.save(force_insert=True)
                return 1

            except Exception as e:
                # Log any database errors
                logger.warning(f"Database error storing item {record.source}:{record.source_id}: {e}")
                return 0

        except Exception as e:
            # Catch any remaining exceptions to prevent transaction abortion
            # This includes IntegrityError, DataError, and other Peewee exceptions
            logger.warning(f"Unexpected error storing item {record.source}:{record.source_id}: {e}")
            return 0

    def get_stats(self) -> Dict[str, int]:
        return self.stats.copy()

    def reset_stats(self) -> None:
        self.stats = {
            "processed": 0,
            "inserted": 0,
            "skipped_duplicates": 0,
            "validation_errors": 0,
            "no_location_data": 0,
            "missing_published_at": 0,
            "unknown_error": 0,
            "location_ner_attempted": 0,
            "location_ner_found": 0,
            "location_resolved": 0,
        }

    def read_from_memory_store(self) -> List[IngestionRecord]:
        try:
            response = requests.get(f"{self.memory_store_url}/get/raw_items", timeout=10)
            if response.status_code != 200:
                logger.error(f"Failed to read from memory store: {response.status_code} - {response.text}")
                return []

            data = response.json()
            raw_items = data.get("raw_items", [])
            if not raw_items:
                return []

            records: List[IngestionRecord] = []
            for item_data in raw_items:
                try:
                    records.append(IngestionRecord.from_dict(item_data))
                except Exception as e:
                    logger.warning(f"Failed to parse record from memory store: {e}")
            return records

        except Exception as e:
            logger.error(f"Error reading from memory store: {e}")
            return []

    def process_from_memory_store(self) -> Dict[str, int]:
        records = self.read_from_memory_store()
        if records:
            return self.process_records(iter(records))
        return self.stats.copy()

    def run_continuous_processing(self, poll_interval: int = 5) -> None:
        logger.info(f"Starting continuous ingestion processing, polling every {poll_interval}s")
        while True:
            try:
                stats = self.process_from_memory_store()
                if stats.get("processed", 0) > 0:
                    logger.info(f"Processed batch: {stats}")
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                logger.info("Shutdown signal received")
                break
            except Exception as e:
                logger.error(f"Error in continuous processing loop: {e}")
                traceback.print_exc()
                time.sleep(poll_interval)


def start_continuous_ingestion():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger.info("Starting Events Ingestion Service (memory store mode)")
    EventsIngestionService().run_continuous_processing()


if __name__ == "__main__":
    start_continuous_ingestion()