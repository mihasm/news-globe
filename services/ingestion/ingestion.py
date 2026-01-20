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
from collections import defaultdict
from typing import Iterable, Tuple, Set
from datetime import datetime, timezone
import traceback
from enum import IntEnum
from peewee import DatabaseError, IntegrityError
import psycopg2


from shared.models.models import IngestionRecord, validate_record
from shared.models.database import database
from location import LocationGetter

import spacy

logger = logging.getLogger(__name__)

class StoreResult(IntEnum):
    INSERTED = 1
    DUPLICATE = 0
    NO_LOCATION = -1
    MISSING_PUBLISHED_AT = -2
    IGNORED = -3
    INVALID_COLLECTED_AT = -4
    INVALID_PUBLISHED_AT = -5

def _to_utc_from_epoch_seconds(ts: float | int) -> datetime:
    # Raises ValueError / OSError if out of range; caller decides how to handle.
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


def _parse_published_at(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        # Ensure tz-aware; if naive, assume UTC (or choose to reject).
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Handle Z suffix
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)  # raises ValueError if invalid
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise TypeError(f"published_at must be datetime|str|None, got {type(value)!r}")


class EventsIngestionService:
    """
    Processes IngestionRecord objects from connectors into the events database.

    This service:
    1. Stores raw data in raw_items table
    2. Normalizes and deduplicates records
    3. Enriches missing location using NER -> GeoNames resolver
    """

    def __init__(self, batch_size: int = 250, memory_store_url: str = None):
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
            "ignored":0,
            "parsing_error": 0,
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

    def _process_batch(self, records):
        if not records:
            return self.stats.copy()

        logger.debug("Processing batch of %d records", len(records))

        # 1) Validate first (cheap)
        valid_records: List[IngestionRecord] = []
        for record in records:
            self.stats["processed"] += 1
            errors = validate_record(record)
            if errors:
                logger.warning("Invalid record %s:%s: %s", record.source, record.source_id, errors)
                self.stats["validation_errors"] += 1
                continue
            valid_records.append(record)

        if not valid_records:
            return self.stats.copy()

        # 2) Dedupe within the incoming batch (cheap, avoids double work)
        valid_records = self._dedupe_within_batch(valid_records)
        if not valid_records:
            return self.stats.copy()

        # 3) Dedupe against DB in bulk (before spaCy/location)
        valid_records = self._filter_already_ingested(valid_records)
        if not valid_records:
            return self.stats.copy()

        # 4) Only now do spaCy/location enrichment (expensive)
        self._enrich_locations_with_spacy(valid_records)

        # 5) Store remaining records (upsert still protects races)
        for record in valid_records:
            try:
                with database.atomic():
                    res = self._store_normalized_item(record)
            except Exception:
                logger.exception("Error processing record %s:%s", record.source, record.source_id)
                self.stats["unknown_error"] += 1
                continue

            if res == StoreResult.INSERTED:
                self.stats["inserted"] += 1
            elif res == StoreResult.DUPLICATE:
                # This can still happen due to races between workers or
                # because another process inserted after our precheck.
                self.stats["skipped_duplicates"] += 1
            elif res == StoreResult.NO_LOCATION:
                self.stats["no_location_data"] += 1
            elif res == StoreResult.MISSING_PUBLISHED_AT:
                self.stats["missing_published_at"] += 1
            elif res == StoreResult.INVALID_COLLECTED_AT:
                logger.info("ERROR for record "+str(record))
                logger.info("INVALID COLLECTED AT:"+record.collected_at)
                self.stats["parsing_error"] += 1
            elif res == StoreResult.INVALID_PUBLISHED_AT:
                logger.info("ERROR for record "+str(record))
                logger.info("INVALID PUBLISHED AT:"+record.published_at)
                self.stats["parsing_error"] += 1
            elif res == StoreResult.IGNORED:
                self.stats["ignored"] += 1
            else:
                self.stats["unknown_error"] += 1

        logger.info("Processed batch of %d records; stats=%s", len(valid_records), self.stats)
        return self.stats.copy()

    def _dedupe_within_batch(self, records: List[IngestionRecord]) -> List[IngestionRecord]:
        """
        Removes duplicates inside the incoming batch (same source + source_id).
        Keeps first occurrence, counts the rest as skipped_duplicates.
        """
        seen: Set[Tuple[str, str]] = set()
        out: List[IngestionRecord] = []

        for r in records:
            # validate_record should ensure these exist, but be defensive.
            if not r.source or not r.source_id:
                out.append(r)
                continue

            key = (r.source, r.source_id)
            if key in seen:
                self.stats["skipped_duplicates"] += 1
                continue
            seen.add(key)
            out.append(r)

        return out

    def _filter_already_ingested(self, records: List[IngestionRecord]) -> List[IngestionRecord]:
        """
        Bulk-checks DB for existing (source, source_id) pairs and filters them out
        before doing any expensive enrichment.

        Implementation groups by source so we can use a simple IN(source_id)
        per source (fast and Peewee-friendly).
        """
        # Group source -> list of ids
        by_source: Dict[str, List[str]] = defaultdict(list)
        for r in records:
            if r.source and r.source_id:
                by_source[r.source].append(r.source_id)

        if not by_source:
            return records

        existing: Set[Tuple[str, str]] = set()

        # Query per source (usually a small number of sources), all at once per source.
        for src, ids in by_source.items():
            # Guard against huge IN lists if batch_size grows later.
            # With your default batch_size=250, this is fine.
            q = (
                self.NormalizedItem
                .select(self.NormalizedItem.source, self.NormalizedItem.source_id)
                .where(
                    (self.NormalizedItem.source == src) &
                    (self.NormalizedItem.source_id.in_(ids))
                )
            )
            for row in q:
                existing.add((row.source, row.source_id))

        if not existing:
            return records

        out: List[IngestionRecord] = []
        for r in records:
            if r.source and r.source_id and (r.source, r.source_id) in existing:
                self.stats["skipped_duplicates"] += 1
                continue
            out.append(r)

        return out


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

    def _store_normalized_item(self, record: IngestionRecord) -> StoreResult:
        if record.source == "mastodon" and "emsc" in (record.source_id or ""):
            logger.debug("Ignoring emsc mastodon item %s:%s", record.source, record.source_id)
            return StoreResult.IGNORED

        if not record.has_location():
            logger.debug("No location data for %s:%s", record.source, record.source_id)
            return StoreResult.NO_LOCATION

        if record.published_at is None:
            logger.debug("Missing published_at for %s:%s", record.source, record.source_id)
            return StoreResult.MISSING_PUBLISHED_AT

        try:
            collected_at_dt = _to_utc_from_epoch_seconds(record.collected_at)
        except (ValueError, OSError, TypeError) as e:
            logger.warning(
                "Invalid collected_at for %s:%s (%r): %s",
                record.source, record.source_id, record.collected_at, e
            )
            return StoreResult.INVALID_COLLECTED_AT

        try:
            published_at_dt = _parse_published_at(record.published_at)
        except (ValueError, TypeError) as e:
            logger.warning(
                "Invalid published_at for %s:%s (%r): %s",
                record.source, record.source_id, record.published_at, e
            )
            return StoreResult.INVALID_PUBLISHED_AT

        if published_at_dt is None:
            return StoreResult.MISSING_PUBLISHED_AT

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

        try:
            item.set_media_urls(record.media_urls)
            item.set_entities(record.entities)
        except (TypeError, ValueError) as e:
            logger.warning("JSON serialization error for %s:%s: %s", record.source, record.source_id, e)
            item.media_urls = None
            item.entities = None

        # Prefer Postgres upsert "DO NOTHING" to avoid duplicate exceptions entirely.
        # This requires Postgres (you have it) and Peewee's on_conflict support.
        data = item.__data__.copy()
        pk_name = item._meta.primary_key.name
        data.pop(pk_name, None)

        try:
            ins = (self.NormalizedItem
                .insert(**data)
                .on_conflict(
                    conflict_target=[self.NormalizedItem.source, self.NormalizedItem.source_id],
                    action="IGNORE",
                ))
            res = ins.execute()
            # Peewee returns inserted PK on success; on conflict IGNORE it returns None/0 depending on version.
            return StoreResult.INSERTED if res else StoreResult.DUPLICATE

        except (AttributeError, TypeError):
            # Peewee version doesn’t support action="IGNORE" or on_conflict signature differs -> fallback.
            pass
        except IntegrityError:
            return StoreResult.DUPLICATE
        except psycopg2.errors.UniqueViolation:
            return StoreResult.DUPLICATE
        except DatabaseError:
            logger.exception("Database error storing item %s:%s", record.source, record.source_id)
            raise

        # Fallback path (works everywhere): insert-first and catch duplicates.
        try:
            item.save(force_insert=True)
            return StoreResult.INSERTED
        except IntegrityError:
            return StoreResult.DUPLICATE
        except psycopg2.errors.UniqueViolation:
            return StoreResult.DUPLICATE
        except DatabaseError:
            logger.exception("Database error storing item %s:%s", record.source, record.source_id)
            raise

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
            "ignored":0,
            "parsing_error": 0,
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