"""
Events Clustering Service - Groups related events by content similarity
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Literal
from dataclasses import dataclass
import re

import numpy as np
from rapidfuzz import fuzz

from shared.models.database import database
from shared.models.models import NormalizedItem, Cluster
from shared.utils.spacy_setup import ensure_spacy_models

logger = logging.getLogger(__name__)

# Clustering Logic - Generic clustering logic (drop-in replacement) using CANONICALIZED TEXT (no num:123 tags)

ItemType = Literal["tweet", "news", "normalized"]

# ----------------------------
# Canonicalization
# ----------------------------

_RE_URL = re.compile(r"https?://\S+")
_RE_RT = re.compile(r"^\s*RT\s+@[\w_]+:\s*", re.IGNORECASE)
_RE_MENTION = re.compile(r"@\w+")
_RE_HASHTAG = re.compile(r"#\w+")
_RE_EMOJI = re.compile(r"[\U0001F000-\U0001FAFF]+")
_RE_SPACES = re.compile(r"\s+")
_RE_PUNCT_KEEP_PCT = re.compile(r"[^\w\s%]")

_RE_NUMBER = re.compile(r"\b\d{1,3}(?:,\d{3})+\b|\b\d+\b")
_RE_PERCENT = re.compile(r"\b(\d{1,3})\s*%\b")
_RE_TIMEWINDOW = re.compile(r"\b(\d{1,3})\s*(hours?|days?|weeks?|months?|years?)\b", re.IGNORECASE)

# Keep this SMALL and generic. Do NOT include country names or event-specific words.
_STOP = {
    "the", "a", "an", "and", "or", "but",
    "to", "of", "in", "on", "at", "for", "from", "with", "by", "as",
    "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those",
    "it", "its", "they", "them", "their", "we", "you",
    "said", "says", "say", "report", "reports", "reported", "according",
    "via", "new", "latest", "breaking", "news"
}

_UNIT_MAP = {
    "hour": "h",
    "day": "d",
    "week": "w",
    "month": "m",
    "year": "y",
}

def _assert_spacy_has_vectors(spacy_nlp) -> None:
    doc = spacy_nlp("vector probe")
    v = getattr(doc, "vector", None)
    if v is None:
        raise RuntimeError("spaCy model has no doc.vector attribute (unexpected).")
    n = float(np.linalg.norm(np.asarray(v, dtype=np.float32)))
    if n <= 1e-6:
        raise RuntimeError(
            "spaCy vectors are zero/absent. Load a vectors-capable model (e.g., *_md or *_lg)."
        )

def _extract_numbers(raw: str) -> list[str]:
    out: list[str] = []
    for m in _RE_NUMBER.finditer(raw):
        s = m.group(0).replace(",", "")
        # keep only reasonable length to reduce ID noise
        # (still generic: you can adjust bounds)
        if 1 <= len(s) <= 8:
            out.append(s)
    return out

def _extract_percents(raw: str) -> list[str]:
    out: list[str] = []
    for m in _RE_PERCENT.finditer(raw):
        out.append(f"{m.group(1)}%")
    return out

def _extract_timewindows(raw: str) -> list[str]:
    out: list[str] = []
    for m in _RE_TIMEWINDOW.finditer(raw):
        val = m.group(1)
        unit = (m.group(2) or "").lower()
        if unit.endswith("s"):
            unit = unit[:-1]
        u = _UNIT_MAP.get(unit)
        if u:
            out.append(f"{val}{u}")
    return out

def canonicalize_text(raw: str | None) -> tuple[str, set[str]]:
    """
    Returns:
      canonical_string: space-joined sorted unique tokens (words + numbers + percents + timewindows)
      rare_tokens: tokens excluding a small stoplist, used as a generic anti-overmerge guard
    """
    if not raw:
        return "", set()

    t = raw.strip()
    t = _RE_RT.sub("", t)
    t = _RE_URL.sub(" ", t)
    t = _RE_MENTION.sub(" ", t)
    t = _RE_HASHTAG.sub(" ", t)
    t = _RE_EMOJI.sub(" ", t)

    nums = _extract_numbers(t)
    pcts = _extract_percents(t)
    tws = _extract_timewindows(t)

    # strip punctuation (keep %), normalize spaces, lowercase
    t2 = _RE_PUNCT_KEEP_PCT.sub(" ", t)
    t2 = _RE_SPACES.sub(" ", t2).strip().lower()

    words = [w for w in t2.split() if w and w not in _STOP and len(w) >= 3]

    tokens = sorted(set(words + nums + pcts + tws))
    canon = " ".join(tokens)

    # "rare" tokens: remove common stop words; keep >=4 chars OR anything with a digit/% (helps distinctiveness)
    rare: set[str] = set()
    for tok in tokens:
        if tok in _STOP:
            continue
        if any(c.isdigit() for c in tok) or "%" in tok:
            rare.add(tok)
        elif len(tok) >= 4:
            rare.add(tok)

    return canon, rare

def _l2_normalize(v: np.ndarray) -> np.ndarray:
    n = float(np.linalg.norm(v))
    if n <= 1e-12:
        return v.astype(np.float32, copy=False)
    return (v / n).astype(np.float32, copy=False)

def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b))

def _length_ratio(a: str, b: str) -> float:
    la, lb = len(a), len(b)
    if la == 0 or lb == 0:
        return 0.0
    return min(la, lb) / max(la, lb)

def _soft_location_bonus(a_key: str | None, b_key: str | None) -> float:
    if not a_key or not b_key:
        return 0.0
    return 0.02 if a_key == b_key else 0.0

# ----------------------------
# Models
# ----------------------------

@dataclass
class Item:
    item_type: ItemType
    item_id: str
    text: str
    created_at_db: datetime
    location_key: str | None
    quoted_tweet_id: str | None = None
    retweeted_tweet_id: str | None = None
    url: str | None = None

@dataclass
class ClusterIndexEntry:
    cluster_id: str
    rep_type: ItemType
    rep_id: str
    rep_canon: str
    rep_vec: np.ndarray
    rep_rare: set[str]
    last_seen_at: datetime | None
    location_key: str | None

# ----------------------------
# Index
# ----------------------------

class ClusterIndex:
    def __init__(self, spacy_nlp):
        self.spacy_nlp = spacy_nlp
        _assert_spacy_has_vectors(self.spacy_nlp)

        self.entries: list[ClusterIndexEntry] = []
        self.last_refresh: datetime | None = None

    def refresh(self, hours: int = 72, limit: int = 5000) -> None:
        _assert_spacy_has_vectors(self.spacy_nlp)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        q = (Cluster
             .select()
             .where((Cluster.last_seen_at.is_null(False)) & (Cluster.last_seen_at >= cutoff))
             .order_by(Cluster.last_seen_at.desc())
             .limit(limit))
        clusters = list(q)

        rep_canons: list[str] = []
        metas: list[tuple[Cluster, str, set[str]]] = []

        for c in clusters:
            rep_text = self._load_rep_text(c)
            if not rep_text:
                continue
            canon, rare = canonicalize_text(rep_text)
            if not canon:
                continue
            rep_canons.append(canon)
            metas.append((c, canon, rare))

        if not rep_canons:
            self.entries = []
            self.last_refresh = datetime.now(timezone.utc)
            return

        vecs = self._embed_texts(rep_canons)

        out: list[ClusterIndexEntry] = []
        for (c, canon, rare), v in zip(metas, vecs):
            out.append(ClusterIndexEntry(
                cluster_id=str(c.cluster_id),
                rep_type=(c.representative_item_type or "tweet"),
                rep_id=(c.representative_item_id or ""),
                rep_canon=canon,
                rep_vec=v,
                rep_rare=rare,
                last_seen_at=c.last_seen_at,
                location_key=c.representative_location_key,
            ))
        self.entries = out
        self.last_refresh = datetime.now(timezone.utc)

    def _load_rep_text(self, c: Cluster) -> str:
        """Load representative text from cluster. This method may be overridden by subclasses."""
        # Default implementation assumes cluster has title
        # Subclasses should override this for different schemas
        return c.title or ""

    def _embed_texts(self, texts: list[str]) -> np.ndarray:
        docs = list(self.spacy_nlp.pipe(texts, disable=["ner", "parser", "tagger", "lemmatizer"]))
        mat = np.vstack([d.vector for d in docs]).astype(np.float32)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms = np.where(norms <= 1e-12, 1.0, norms)
        return (mat / norms).astype(np.float32)

    def add_cluster_entry(self, cluster: Cluster) -> None:
        _assert_spacy_has_vectors(self.spacy_nlp)

        rep_text = self._load_rep_text(cluster)
        if not rep_text:
            return
        canon, rare = canonicalize_text(rep_text)
        if not canon:
            return

        v = self._embed_texts([canon])
        if v.shape[0] == 0:
            return

        cid = str(cluster.cluster_id)
        self.entries = [e for e in self.entries if e.cluster_id != cid]

        self.entries.insert(0, ClusterIndexEntry(
            cluster_id=cid,
            rep_type=(cluster.representative_item_type or "tweet"),
            rep_id=(cluster.representative_item_id or ""),
            rep_canon=canon,
            rep_vec=v[0],
            rep_rare=rare,
            last_seen_at=cluster.last_seen_at,
            location_key=cluster.representative_location_key,
        ))

# ----------------------------
# Matcher
# ----------------------------

class ClusterMatcher:
    def __init__(self, spacy_nlp, index: ClusterIndex):
        self.spacy_nlp = spacy_nlp
        self.index = index
        _assert_spacy_has_vectors(self.spacy_nlp)

        # Lexical near-dup thresholds (on canonical strings)
        self.near_dup_token_set_ratio = 80
        self.near_dup_partial_ratio = 80

        # Semantic threshold (spaCy vectors on canonical strings)
        self.semantic_cosine_threshold = 0.80

        # Generic guardrails to reduce over-merging
        self.semantic_min_lexical = 0.55          # require some lexical support even for semantic
        self.semantic_min_rare_overlap = 1        # require ≥1 rare-token overlap
        self.min_len_ratio_for_semantic = 0.35    # block extreme length mismatch

        # Candidate filtering
        self.min_token_overlap = 1               # overlap on canonical tokens (cheap filter)

        self.max_age_hours = 72
        self.refresh_interval_seconds = 300

        # Geo behavior
        self.hard_location_gate = False

    def assign_cluster(self, item: Item) -> tuple[str, float | None, str]:
        if (self.index.last_refresh is None or
            (datetime.now(timezone.utc) - self.index.last_refresh).total_seconds() > self.refresh_interval_seconds):
            self.index.refresh(hours=self.max_age_hours)

        hard = self._match_hard_links(item)
        if hard:
            return hard[0], None, hard[1]

        cluster_id, sim, match_type = self._match_by_similarity(item)
        if cluster_id:
            return cluster_id, sim, match_type

        new_id = self._create_new_cluster(item)
        return new_id, None, "new_cluster"

    def _match_hard_links(self, item: Item) -> Optional[tuple[str, str]]:
        # Events clustering doesn't use hard links (retweets/quotes)
        return None

    def _candidate_entries(self, item_location_key: str | None) -> list[ClusterIndexEntry]:
        if not self.hard_location_gate:
            return self.index.entries
        if not item_location_key:
            return self.index.entries
        return [e for e in self.index.entries if (not e.location_key or e.location_key == item_location_key)]

    def _match_by_similarity(self, item: Item) -> tuple[str | None, float | None, str]:
        if not self.index.entries:
            return None, None, ""

        canon, rare = canonicalize_text(item.text)
        if not canon:
            return None, None, ""

        entries = self._candidate_entries(item.location_key)
        if not entries:
            return None, None, ""

        # Cheap candidate prefilter: token overlap on canonical strings
        item_tok = set(canon.split())
        cands: list[ClusterIndexEntry] = []
        for e in entries:
            etok = set(e.rep_canon.split())
            if len(item_tok & etok) >= self.min_token_overlap:
                cands.append(e)

        if not cands:
            cands = entries  # fallback

        # 1) Lexical near-dup
        best_id: str | None = None
        best_score: float = 0.0
        for e in cands:
            score = float(fuzz.token_set_ratio(canon, e.rep_canon))
            if score >= self.near_dup_token_set_ratio and score > best_score:
                best_id = e.cluster_id
                best_score = score
        if best_id:
            return best_id, best_score / 100.0, "near_dup_rapidfuzz"

        best_id = None
        best_score = 0.0
        for e in cands:
            score = float(fuzz.partial_ratio(canon, e.rep_canon))
            if score >= self.near_dup_partial_ratio and score > best_score:
                best_id = e.cluster_id
                best_score = score
        if best_id:
            return best_id, best_score / 100.0, "near_dup_partial"

        # 2) Semantic (spaCy vectors) with generic guardrails
        doc = self.spacy_nlp(canon)
        v = _l2_normalize(doc.vector.astype(np.float32))

        best_sem_id: str | None = None
        best_sem: float = -1.0
        best_rep: ClusterIndexEntry | None = None

        for e in cands:
            # length mismatch guardrail
            if _length_ratio(canon, e.rep_canon) < self.min_len_ratio_for_semantic:
                continue

            s = cosine_sim(v, e.rep_vec) + _soft_location_bonus(item.location_key, e.location_key)
            if s > best_sem:
                best_sem = float(s)
                best_sem_id = e.cluster_id
                best_rep = e

        if best_rep is None or best_sem_id is None:
            return None, None, ""

        if best_sem >= self.semantic_cosine_threshold:
            # lexical guardrail
            lex = float(fuzz.token_set_ratio(canon, best_rep.rep_canon)) / 100.0
            if lex < self.semantic_min_lexical:
                return None, None, ""

            # rare-token overlap guardrail
            if self.semantic_min_rare_overlap > 0:
                if len(rare & best_rep.rep_rare) < self.semantic_min_rare_overlap:
                    return None, None, ""

            return best_sem_id, best_sem, "semantic_spacy"

        return None, None, ""

    def _create_new_cluster(self, item: Item) -> str:
        """Create a new cluster. This method should be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement _create_new_cluster")


def normalized_item_to_item(normalized_item: NormalizedItem) -> Item:
    """Convert a NormalizedItem to an Item for clustering logic."""
    title_text = (normalized_item.title or "").strip()
    body_text = (normalized_item.text or "").strip()
    text = f"{title_text} {body_text}".strip() if title_text or body_text else ""

    created_at = normalized_item.published_at or normalized_item.collected_at
    if created_at:
        # Handle case where created_at might be a string (defensive parsing)
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            except ValueError:
                # If parsing fails, use current time
                created_at = datetime.now(timezone.utc)
        elif created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
    else:
        created_at = datetime.now(timezone.utc)

    location_key = None
    if normalized_item.lat is not None and normalized_item.lon is not None:
        location_key = f"{normalized_item.lat:.6f},{normalized_item.lon:.6f}"

    return Item(
        item_type="normalized",
        item_id=str(normalized_item.id),
        text=text,
        created_at_db=created_at,
        location_key=location_key,
        url=normalized_item.url,
    )


@dataclass
class ClusteringConfig:
    """Configuration for clustering behavior."""
    min_cluster_size: int = 1
    max_cluster_items: int = 100


class EventsClusterIndex(ClusterIndex):
    """Custom ClusterIndex for events clustering."""

    def _load_rep_text(self, c: Cluster) -> str:
        """Load representative text from events Cluster model."""
        if c.title:
            return c.title

        try:
            # Use COALESCE to handle NULL values in ordering
            from peewee import fn
            first_item = (NormalizedItem
                         .select()
                         .where(NormalizedItem.cluster_id == c.cluster_id)
                         .order_by(fn.COALESCE(NormalizedItem.published_at, NormalizedItem.collected_at).desc())
                         .first())
            if first_item:
                title_text = (first_item.title or "").strip()
                body_text = (first_item.text or "").strip()
                return f"{title_text} {body_text}".strip()
        except Exception:
            pass

        return ""

    def refresh(self, hours: int = 72, limit: int = 5000) -> None:
        """Refresh the index with events clusters."""
        _assert_spacy_has_vectors(self.spacy_nlp)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        q = (Cluster
             .select()
             .where((Cluster.last_seen_at.is_null(False)) & (Cluster.last_seen_at >= cutoff))
             .order_by(Cluster.last_seen_at.desc())
             .limit(limit))
        clusters = list(q)

        rep_canons: list[str] = []
        metas: list[tuple[Cluster, str, set[str]]] = []

        for c in clusters:
            rep_text = self._load_rep_text(c)
            if not rep_text:
                continue
            canon, rare = canonicalize_text(rep_text)
            if not canon:
                continue
            rep_canons.append(canon)
            metas.append((c, canon, rare))

        if not rep_canons:
            self.entries = []
            self.last_refresh = datetime.now(timezone.utc)
            return

        vecs = self._embed_texts(rep_canons)

        out: list[ClusterIndexEntry] = []
        for (c, canon, rare), v in zip(metas, vecs):
            out.append(ClusterIndexEntry(
                cluster_id=str(c.cluster_id),
                rep_type="normalized",
                rep_id=str(c.cluster_id),
                rep_canon=canon,
                rep_vec=v,
                rep_rare=rare,
                last_seen_at=c.last_seen_at,
                location_key=None,  # Events clusters don't use location keys
            ))
        self.entries = out
        self.last_refresh = datetime.now(timezone.utc)

    def add_cluster_entry(self, cluster: Cluster) -> None:
        """Override to use events Cluster model."""
        _assert_spacy_has_vectors(self.spacy_nlp)

        rep_text = self._load_rep_text(cluster)
        if not rep_text:
            return
        canon, rare = canonicalize_text(rep_text)
        if not canon:
            return

        v = self._embed_texts([canon])
        if v.shape[0] == 0:
            return

        cid = str(cluster.cluster_id)
        self.entries = [e for e in self.entries if e.cluster_id != cid]

        self.entries.insert(0, ClusterIndexEntry(
            cluster_id=cid,
            rep_type="normalized",
            rep_id=str(cluster.cluster_id),
            rep_canon=canon,
            rep_vec=v[0],
            rep_rare=rare,
            last_seen_at=cluster.last_seen_at,
            location_key=None,  # Events clusters don't use location keys in the same way
        ))


class EventsClusterMatcher(ClusterMatcher):
    """Custom ClusterMatcher for events clustering."""

    def _match_hard_links(self, item: Item) -> Optional[tuple[str, str]]:
        """Events clustering doesn't use hard links (retweets/quotes)."""
        return None

    def _create_new_cluster(self, item: Item) -> str:
        """Create a new cluster using the events schema."""
        title = None
        if item.text:
            title = item.text[:200]
        else:
            title = "No title"

        now = datetime.now(timezone.utc)

        cluster_lat = cluster_lon = cluster_location_name = None
        if item.item_type == "normalized":
            try:
                normalized_item = NormalizedItem.get(NormalizedItem.id == int(item.item_id))
                cluster_lat = normalized_item.lat
                cluster_lon = normalized_item.lon
                cluster_location_name = normalized_item.location_name
            except (NormalizedItem.DoesNotExist, ValueError):
                pass

        with database.atomic():
            c = Cluster.create(
                title=title,
                representative_lat=cluster_lat,
                representative_lon=cluster_lon,
                representative_location_name=cluster_location_name,
                first_seen_at=item.created_at_db,
                last_seen_at=item.created_at_db,
                item_count=0,
                created_at=now,
                updated_at=now,
            )

        logger.debug(f"Created events cluster {c.cluster_id}")
        self.index.add_cluster_entry(c)
        return str(c.cluster_id)


class EventsClusteringService:
    """
    Clusters normalized events based on content similarity.
    """

    def __init__(self, config: Optional[ClusteringConfig] = None):
        self.config = config or ClusteringConfig()

        # Initialize database
        from shared.models.database import database, initialize_database
        database.connect(reuse_if_open=True)
        initialize_database()

        self.nlp = None
        self._init_spacy()

        if self.nlp:
            self.cluster_index = EventsClusterIndex(self.nlp)
            self.cluster_matcher = EventsClusterMatcher(self.nlp, self.cluster_index)
        else:
            self.cluster_index = None
            self.cluster_matcher = None

        self.stats = {'processed': 0, 'clustered': 0, 'new_clusters': 0}

    def _init_spacy(self) -> None:
        """Initialize spaCy model."""
        import spacy
        from shared.utils.spacy_setup import PREFERRED_MODELS

        # Try to load any available model in order of preference
        for model_name in PREFERRED_MODELS:
            try:
                self.nlp = spacy.load(model_name)
                _assert_spacy_has_vectors(self.nlp)
                logger.info(f"spaCy model loaded: {model_name}")
                return
            except OSError:
                continue

        raise RuntimeError("No suitable spaCy model found. Please install en_core_web_md or en_core_web_trf")

    def process_unassigned_items(self, batch_size: int = 1000) -> Dict[str, int]:
        """
        Process unassigned normalized items and cluster them.

        Args:
            batch_size: Maximum number of items to process in one batch.
        """
        base_query = NormalizedItem.select().where(NormalizedItem.cluster_id.is_null())

        # Order by collected_at for consistent processing
        base_query = base_query.order_by(NormalizedItem.collected_at.desc())

        # Get total count first
        total_unassigned = base_query.count()
        logger.info(f"Found {total_unassigned} unassigned items to process")

        if total_unassigned == 0:
            return self.stats.copy()

        # Process in batches to avoid memory issues
        processed_total = 0
        batches_processed = 0

        while processed_total < total_unassigned:
            # Get next batch
            batch_items = list(base_query.offset(processed_total).limit(batch_size))

            if not batch_items:
                break

            logger.info(f"Processing batch {batches_processed + 1}: {len(batch_items)} items (offset: {processed_total})")
            self._cluster_items(batch_items)

            processed_total += len(batch_items)
            batches_processed += 1

            # Safety check - don't process more than 10,000 items in one call
            if processed_total >= 10000:
                logger.warning("Reached safety limit of 10,000 items processed. Stopping to prevent performance issues.")
                break

        logger.info(f"Clustering complete: processed {processed_total} items in {batches_processed} batches. {self.stats}")
        return self.stats.copy()




    def _cluster_items(self, items: List[NormalizedItem]) -> None:
        """Cluster items using clustering logic."""
        if not self.cluster_matcher:
            logger.warning("ClusterMatcher not available")
            return

        for item in items:
            try:
                cluster_item = normalized_item_to_item(item)
                cluster_id, sim, match_type = self.cluster_matcher.assign_cluster(cluster_item)
                self._persist_assignment(item, cluster_id, sim, match_type)

                self.stats['processed'] += 1
                if match_type == "new_cluster":
                    self.stats['new_clusters'] += 1
                else:
                    self.stats['clustered'] += 1

            except Exception as e:
                logger.error(f"Error clustering item {item.id}: {e}")

    def _persist_assignment(self, item: NormalizedItem, cluster_id: str, sim: float | None, match_type: str) -> None:
        """Persist cluster assignment for a NormalizedItem."""
        with database.atomic():
            try:
                cluster = Cluster.get(Cluster.cluster_id == cluster_id)
            except Cluster.DoesNotExist:
                logger.warning(f"Cluster {cluster_id} does not exist, removing from cache")
                # Remove stale cluster from cache
                if self.cluster_index:
                    self.cluster_index.entries = [e for e in self.cluster_index.entries if e.cluster_id != cluster_id]
                return

            # Update item assignment
            item.cluster_id = cluster_id
            item.save()

            # Update cluster metadata
            now = datetime.now(timezone.utc)
            if cluster.first_seen_at is None:
                cluster.first_seen_at = now
            cluster.last_seen_at = now
            cluster.updated_at = now

            # Update item count
            count = (NormalizedItem
                     .select()
                     .where(NormalizedItem.cluster_id == cluster.cluster_id)
                     .count())
            cluster.item_count = count
            cluster.save()



    def cleanup_old_clusters(self, max_age_days: int = 30) -> int:
        """Remove clusters older than specified days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

        old_clusters = Cluster.select().where(Cluster.last_seen_at < cutoff)

        count = 0
        for cluster in old_clusters:
            with database.atomic():
                # Clear cluster_id from items (trigger will update stats)
                NormalizedItem.update(cluster_id=None).where(NormalizedItem.cluster_id == cluster.cluster_id).execute()
                cluster.delete_instance()
                count += 1

        logger.info(f"Cleaned up {count} old clusters")
        return count

    def recalculate_cluster_stats(self) -> None:
        """Recalculate statistics for all active clusters."""
        active_clusters = Cluster.select().where(
            Cluster.updated_at >= datetime.now(timezone.utc) - timedelta(hours=1)
        )

        for cluster in active_clusters:
            # Recalculate item count
            count = (NormalizedItem
                     .select()
                     .where(NormalizedItem.cluster_id == cluster.cluster_id)
                     .count())
            cluster.item_count = count

            # Recalculate location stats
            items_with_location = (NormalizedItem
                                   .select()
                                   .where((NormalizedItem.cluster_id == cluster.cluster_id) &
                                          (NormalizedItem.lat.is_null(False)) &
                                          (NormalizedItem.lon.is_null(False))))

            if items_with_location.count() > 0:
                # Get average location
                from peewee import fn
                avg_lat = items_with_location.select(fn.AVG(NormalizedItem.lat)).scalar()
                avg_lon = items_with_location.select(fn.AVG(NormalizedItem.lon)).scalar()

                cluster.representative_lat = float(avg_lat) if avg_lat else None
                cluster.representative_lon = float(avg_lon) if avg_lon else None

                # Get most common location name
                location_names = (items_with_location
                                  .select(NormalizedItem.location_name)
                                  .where(NormalizedItem.location_name.is_null(False))
                                  .group_by(NormalizedItem.location_name)
                                  .order_by(fn.COUNT(NormalizedItem.id).desc())
                                  .first())
                if location_names:
                    cluster.representative_location_name = location_names.location_name

            # Update timestamps
            # Use COALESCE to prefer published_at over collected_at for ordering
            from peewee import fn
            first_item = (NormalizedItem
                          .select()
                          .where(NormalizedItem.cluster_id == cluster.cluster_id)
                          .order_by(fn.COALESCE(NormalizedItem.published_at, NormalizedItem.collected_at).asc())
                          .first())
            last_item = (NormalizedItem
                         .select()
                         .where(NormalizedItem.cluster_id == cluster.cluster_id)
                         .order_by(fn.COALESCE(NormalizedItem.published_at, NormalizedItem.collected_at).desc())
                         .first())

            # Parse timestamps if they're strings
            def parse_timestamp(ts):
                if not ts:
                    return None
                if isinstance(ts, str):
                    try:
                        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except ValueError:
                        return None
                return ts

            if first_item:
                timestamp = first_item.published_at or first_item.collected_at
                cluster.first_seen_at = parse_timestamp(timestamp)
            if last_item:
                timestamp = last_item.published_at or last_item.collected_at
                cluster.last_seen_at = parse_timestamp(timestamp)

            cluster.save()

        logger.info(f"Recalculated stats for {len(active_clusters)} clusters")