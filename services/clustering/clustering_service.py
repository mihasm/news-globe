"""
Events Clustering Service - Database operations and service layer
Handles cluster creation, persistence, and coordination with the core clustering logic.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from shared.models.database import database, initialize_database
from shared.models.models import Cluster, NormalizedItem

from clustering_core import ClusterIndex, ClusterMatcher, Item, canonicalize

logger = logging.getLogger(__name__)


@dataclass
class ClusteringConfig:
    min_cluster_size: int = 1
    max_cluster_items: int = 100


def normalized_item_to_item(x: NormalizedItem) -> Item:
    title = (x.title or "").strip()
    body = (x.text or "").strip()
    text = f"{title} {body}".strip()

    ts = x.published_at or x.collected_at
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            ts = None
    if not ts:
        ts = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return Item(item_type="normalized", item_id=str(x.id), text=text, created_at=ts, url=x.url)


def _cluster_rep_text(c: Cluster) -> str:
    if c.title:
        return c.title

    # fallback: newest item text in the cluster
    try:
        from peewee import fn
        it = (
            NormalizedItem.select()
            .where(NormalizedItem.cluster_id == c.cluster_id)
            .order_by(fn.COALESCE(NormalizedItem.published_at, NormalizedItem.collected_at).desc())
            .first()
        )
        if it:
            t = (it.title or "").strip()
            b = (it.text or "").strip()
            return f"{t} {b}".strip()
    except Exception:
        pass

    return ""


class EventsClusteringService:
    """
    Streamlined clustering:
      - Builds/refreshes an in-memory index of recent clusters
      - Assigns unclustered NormalizedItem rows to existing cluster or creates a new Cluster row
      - Updates cluster timestamps and item_count
    """

    def __init__(self, config: Optional[ClusteringConfig] = None):
        self.config = config or ClusteringConfig()

        database.connect(reuse_if_open=True)
        initialize_database()

        self.nlp = self._load_spacy()
        self.index = ClusterIndex(self.nlp)
        self.matcher = ClusterMatcher(self.nlp, self.index, refresh_callback=self.refresh_index)

        self.stats: Dict[str, int] = {"processed": 0, "clustered": 0, "new_clusters": 0}

    def _load_spacy(self):
        import spacy
        from shared.utils.spacy_setup import PREFERRED_MODELS

        for name in PREFERRED_MODELS:
            try:
                nlp = spacy.load(name)
                logger.info("spaCy model loaded: %s", name)
                return nlp
            except OSError:
                continue
        raise RuntimeError("No suitable spaCy model found. Install en_core_web_md/en_core_web_lg (vectors required).")

    def refresh_index(self, hours: int = 72, limit: int = 5000) -> None:
        """Refresh the cluster index with recent clusters from database."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        clusters = list(
            Cluster.select()
            .where((Cluster.last_seen_at.is_null(False)) & (Cluster.last_seen_at >= cutoff))
            .order_by(Cluster.last_seen_at.desc())
            .limit(limit)
        )

        cluster_data = []
        for c in clusters:
            rep_text = _cluster_rep_text(c)
            cluster_data.append((str(c.cluster_id), rep_text, c.last_seen_at))

        self.index.refresh_from_data(cluster_data)

    def process_unassigned_items(self, batch_size: int = 1000) -> Dict[str, int]:
        q = (
            NormalizedItem.select()
            .where(NormalizedItem.cluster_id.is_null())
            .order_by(NormalizedItem.collected_at.desc())
        )
        total = q.count()
        logger.info("Found %s unassigned items", total)
        if total == 0:
            return dict(self.stats)

        processed = 0
        while processed < total and processed < 10000:
            batch = list(q.offset(processed).limit(batch_size))
            if not batch:
                break
            self._cluster_batch(batch)
            processed += len(batch)

        logger.info("Clustering done: processed=%s stats=%s", processed, self.stats)
        return dict(self.stats)

    def _cluster_batch(self, items: List[NormalizedItem]) -> None:
        for row in items:
            try:
                item = normalized_item_to_item(row)
                cid, sim, how = self.matcher.assign(item)
                if not cid:
                    cid = self._create_cluster(item)
                    how = "new_cluster"
                    sim = None
                self._persist(row, cid)
                self.stats["processed"] += 1
                if how == "new_cluster":
                    self.stats["new_clusters"] += 1
                else:
                    self.stats["clustered"] += 1
            except Exception:
                logger.exception("Error clustering item id=%s", getattr(row, "id", None))

    def _create_cluster(self, item: Item) -> str:
        now = datetime.now(timezone.utc)

        cluster_lat = cluster_lon = cluster_locname = None
        try:
            ni = NormalizedItem.get(NormalizedItem.id == int(item.item_id))
            cluster_lat, cluster_lon, cluster_locname = ni.lat, ni.lon, ni.location_name
        except Exception:
            pass

        title = (item.text or "No title")[:200]
        with database.atomic():
            c = Cluster.create(
                title=title,
                representative_lat=cluster_lat,
                representative_lon=cluster_lon,
                representative_location_name=cluster_locname,
                first_seen_at=item.created_at,
                last_seen_at=item.created_at,
                item_count=0,
                created_at=now,
                updated_at=now,
            )
        self.index.add_or_update_from_data(str(c.cluster_id), _cluster_rep_text(c), c.last_seen_at)
        return str(c.cluster_id)

    def _persist(self, row: NormalizedItem, cluster_id: str) -> None:
        with database.atomic():
            try:
                cluster = Cluster.get(Cluster.cluster_id == cluster_id)
            except Cluster.DoesNotExist:
                # stale index entry: remove and skip
                self.index.entries = [e for e in self.index.entries if e.cluster_id != cluster_id]
                return

            row.cluster_id = cluster_id
            row.save()

            now = datetime.now(timezone.utc)
            cluster.first_seen_at = cluster.first_seen_at or now
            cluster.last_seen_at = now
            cluster.updated_at = now

            cluster.item_count = (
                NormalizedItem.select().where(NormalizedItem.cluster_id == cluster.cluster_id).count()
            )
            cluster.save()

    def cleanup_old_clusters(self, max_age_days: int = 30) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        old = Cluster.select().where(Cluster.last_seen_at < cutoff)

        n = 0
        for c in old:
            with database.atomic():
                NormalizedItem.update(cluster_id=None).where(NormalizedItem.cluster_id == c.cluster_id).execute()
                c.delete_instance()
                n += 1

        logger.info("Cleaned up %s old clusters", n)
        return n

    def recalculate_cluster_stats(self) -> None:
        from peewee import fn

        active = Cluster.select().where(Cluster.updated_at >= datetime.now(timezone.utc) - timedelta(hours=1))
        for c in active:
            items = NormalizedItem.select().where(NormalizedItem.cluster_id == c.cluster_id)

            c.item_count = items.count()

            loc = items.where((NormalizedItem.lat.is_null(False)) & (NormalizedItem.lon.is_null(False)))
            if loc.count():
                c.representative_lat = float(loc.select(fn.AVG(NormalizedItem.lat)).scalar() or 0) or None
                c.representative_lon = float(loc.select(fn.AVG(NormalizedItem.lon)).scalar() or 0) or None
                common = (
                    loc.select(NormalizedItem.location_name)
                    .where(NormalizedItem.location_name.is_null(False))
                    .group_by(NormalizedItem.location_name)
                    .order_by(fn.COUNT(NormalizedItem.id).desc())
                    .first()
                )
                if common:
                    c.representative_location_name = common.location_name

            first = items.order_by(fn.COALESCE(NormalizedItem.published_at, NormalizedItem.collected_at).asc()).first()
            last = items.order_by(fn.COALESCE(NormalizedItem.published_at, NormalizedItem.collected_at).desc()).first()
            if first:
                c.first_seen_at = first.published_at or first.collected_at
            if last:
                c.last_seen_at = last.published_at or last.collected_at

            c.save()

        logger.info("Recalculated stats for %s clusters", len(active))