# shared/location_offline.py
import os
import re
import math
import sqlite3
import threading
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

# Add near top-level of this module (or inside LocationGetter if you prefer)
ADMIN_QUALIFIERS = {
    "province", "region", "district", "county", "state", "governorate",
    "prefecture", "oblast", "raion", "municipality", "department",
    "commune", "parish", "canton", "voivodeship", "shire"
}

def _simple_tokens(s: str) -> list[str]:
    return [t for t in re.findall(r"[A-Za-z0-9]+", (s or "").casefold()) if t]

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # stable + fast enough; no external deps
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

logger = logging.getLogger(__name__)

# -------------------------
# Cache: query -> (lat,lng,area)
# -------------------------

class LocationCache:
    """SQLite cache for geocoding results."""
    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = os.getenv("LOCATION_CACHE_DB", "/tmp/location_cache.db")
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db_path = db_path
        self._local = threading.local()
        self._create_table()

    def _get_conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
        return self._local.conn

    def _create_table(self):
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                query TEXT PRIMARY KEY,
                lat REAL,
                lng REAL,
                area REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    def get(self, query: str):
        conn = self._get_conn()
        cur = conn.execute(
            "SELECT lat, lng, area FROM locations WHERE query = ?",
            (query.casefold().strip(),)
        )
        row = cur.fetchone()
        return row if row else None

    def set(self, query: str, lat: float, lng: float, area: float):
        conn = self._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO locations(query, lat, lng, area) VALUES (?,?,?,?)",
            (query.casefold().strip(), float(lat), float(lng), float(area))
        )
        conn.commit()

    def get_stats(self):
        conn = self._get_conn()
        cur = conn.execute("SELECT COUNT(*) FROM locations")
        return {"cached_locations": cur.fetchone()[0]}


# -------------------------
# GeoNames DB builder
# -------------------------

GEONAMES_SCHEMA = """
CREATE TABLE IF NOT EXISTS geonames (
  geonameid INTEGER PRIMARY KEY,
  name TEXT,
  asciiname TEXT,
  latitude REAL,
  longitude REAL,
  feature_class TEXT,
  feature_code TEXT,
  country_code TEXT,
  admin1_code TEXT,
  admin2_code TEXT,
  population INTEGER
);

CREATE TABLE IF NOT EXISTS names (
  name TEXT,
  geonameid INTEGER,
  is_preferred INTEGER DEFAULT 0,
  is_short INTEGER DEFAULT 0,
  lang TEXT,
  FOREIGN KEY(geonameid) REFERENCES geonames(geonameid)
);

CREATE INDEX IF NOT EXISTS idx_names_name ON names(name);
CREATE INDEX IF NOT EXISTS idx_names_geonameid ON names(geonameid);
CREATE INDEX IF NOT EXISTS idx_geonames_country ON geonames(country_code);
CREATE INDEX IF NOT EXISTS idx_geonames_feat ON geonames(feature_class, feature_code);
"""

def _norm(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def build_geonames_db(
    geonames_txt_path: str,
    db_path: str,
    alternatenames_txt_path: Optional[str] = None,
    min_population: int = 0,
) -> None:
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(GEONAMES_SCHEMA)

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    inserted = 0
    with open(geonames_txt_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 15:
                continue

            geonameid = int(parts[0])
            name = parts[1]
            asciiname = parts[2]
            lat = float(parts[4])
            lng = float(parts[5])
            feature_class = parts[6]
            feature_code = parts[7]
            country_code = parts[8]
            admin1_code = parts[10] if len(parts) > 10 else ""
            admin2_code = parts[11] if len(parts) > 11 else ""
            population = int(parts[14]) if parts[14].isdigit() else 0

            if population < min_population:
                continue

            conn.execute(
                """
                INSERT INTO geonames(
                geonameid, name, asciiname, latitude, longitude,
                feature_class, feature_code, country_code,
                admin1_code, admin2_code, population
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    geonameid,
                    name,
                    asciiname,
                    lat,
                    lng,
                    feature_class,
                    feature_code,
                    country_code,
                    admin1_code,
                    admin2_code,
                    population,
                ),
            )

            for nm, pref in [(name, 1), (asciiname, 0)]:
                nm = _norm(nm)
                if nm:
                    conn.execute(
                        "INSERT INTO names(name, geonameid, is_preferred, lang) VALUES (?,?,?,?)",
                        (nm.casefold(), geonameid, pref, None),
                    )

            inserted += 1
            if inserted % 200000 == 0:
                conn.commit()
                logger.info("Inserted %d geonames rows...", inserted)

    conn.commit()
    logger.info("Inserted %d geonames rows (min_population=%d).", inserted, min_population)

    if alternatenames_txt_path:
        alt_inserted = 0
        with open(alternatenames_txt_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 6:
                    continue
                try:
                    geonameid = int(parts[1])
                except Exception:
                    continue

                lang = parts[2] or None
                alt_name = _norm(parts[3])
                is_pref = 1 if parts[4] == "1" else 0
                is_short = 1 if parts[5] == "1" else 0

                if len(alt_name) < 2:
                    continue

                conn.execute(
                    "INSERT INTO names(name, geonameid, is_preferred, is_short, lang) VALUES (?,?,?,?,?)",
                    (alt_name.casefold(), geonameid, is_pref, is_short, lang),
                )

                alt_inserted += 1
                if alt_inserted % 500000 == 0:
                    conn.commit()
                    logger.info("Inserted %d alternate names...", alt_inserted)

        conn.commit()
        logger.info("Inserted %d alternate names.", alt_inserted)

    conn.execute("PRAGMA optimize;")
    conn.commit()
    conn.close()


# -------------------------
# Offline location extraction + geocoding
# -------------------------

DEFAULT_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "in", "on", "at", "to", "for", "by",
    "from", "with", "without", "near", "around", "into", "over", "under",
    "this", "that", "these", "those", "is", "are", "was", "were", "be", "been",
}

FEATURE_AREA_HINT = {
    "PPLC": 0.02, "PPLA": 0.03, "PPLA2": 0.04, "PPLA3": 0.05, "PPLA4": 0.06,
    "PPL": 0.07, "PPLX": 0.08, "PPLL": 0.08, "PPLS": 0.10,
    "ADM1": 0.30, "ADM2": 0.40, "ADM3": 0.50, "ADM4": 0.60,
    "*": 0.20,
}

@dataclass(frozen=True)
class LocationResult:
    name: str
    lat: float
    lng: float
    area: float
    score: float
    geonameid: int
    country: str
    feature_code: str


class GeoNamesIndex:
    """Thread-local SQLite connections + lookups."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
        return self._local.conn

    def lookup_name(self, name_cf: str, limit: int = 50) -> List[Tuple]:
        """
        Return candidate rows for an exact name match in names table.

        Rows:
          (alt_name, geonameid, is_preferred, is_short, lang,
           latitude, longitude, feature_code, country_code, population,
           official_name)

        Notes:
          - Adds official_name (g.name) for "missing qualifier" penalties in scoring.
          - Adds ORDER BY to avoid pathological ordering; final decision is still made by Python scorer.
        """
        conn = self._conn()
        cur = conn.execute(
            """
            SELECT
                n.name,
                n.geonameid,
                n.is_preferred,
                n.is_short,
                n.lang,
                g.latitude,
                g.longitude,
                g.feature_code,
                g.country_code,
                g.population,
                g.name AS official_name
            FROM names n
            JOIN geonames g ON g.geonameid = n.geonameid
            WHERE n.name = ?
            ORDER BY
                -- Prefer empty/en language alternates first (you treat text as English)
                CASE WHEN n.lang IS NULL OR n.lang = '' OR n.lang = 'en' THEN 0 ELSE 1 END,
                -- Prefer non-short names
                CASE WHEN n.is_short = 1 THEN 1 ELSE 0 END,
                -- Prefer populated places slightly over admin areas (helps 1-token queries)
                CASE WHEN g.feature_code LIKE 'P%' THEN 0 WHEN g.feature_code LIKE 'ADM%' THEN 1 ELSE 2 END,
                -- Higher population first
                COALESCE(g.population, 0) DESC,
                -- Finally preferred-name as a weak tiebreaker, not the primary sorter
                CASE WHEN n.is_preferred = 1 THEN 0 ELSE 1 END
            LIMIT ?
            """,
            (name_cf, limit),
        )
        return cur.fetchall()


class LocationGetter:
    """
    Drop-in replacement (no spaCy, offline):
      - get_location(text) -> (lat,lng,area) for a query string
      - parse_location(text) -> (name, lat, lng, area, score) or None
      - parse_locations_batch(texts) -> list aligned to inputs
    """
    def __init__(
        self,
        geonames_db: Optional[str] = None,
        cache_db: Optional[str] = None,
        max_ngram: int = 4,
    ):
        self.geonames_db = geonames_db or os.getenv("GEONAMES_DB", "/app/shared/data/geonames.db")
        if not os.path.exists(self.geonames_db):
            raise FileNotFoundError(f"GeoNames DB not found at {self.geonames_db}. Build it during Docker build.")
        self.index = GeoNamesIndex(self.geonames_db)
        self.cache = LocationCache(cache_db)
        self.max_ngram = max_ngram

        self._cache_hits = 0
        self._cache_misses = 0

        # small in-memory memoization for name->candidates
        self._name_memo: Dict[str, List[Tuple]] = {}

    def get_cache_stats(self):
        total = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / total * 100.0) if total else 0.0
        return {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "hit_rate": f"{hit_rate:.1f}%",
            **self.cache.get_stats(),
        }

    # --- Query geocoding (offline) ---
    def get_location(self, text: str):
        cached = self.cache.get(text)
        if cached is not None:
            self._cache_hits += 1
            return cached

        self._cache_misses += 1
        best = self._best_candidate_for_name(text)
        if best is None:
            return None

        self.cache.set(text, best.lat, best.lng, best.area)
        return (best.lat, best.lng, best.area)

    # --- Parsing ---
    def parse_location(self, text: str):
        if not text or len(text.strip()) < 2:
            return None
        best = self._best_candidate_in_text(text)
        if not best:
            return None
        return (best.name, best.lat, best.lng, best.area, best.score)

    def parse_locations_batch(self, texts: List[str], batch_size: int = 50):
        # batch_size kept for compatibility; algorithm is already linear.
        results: List[Optional[Tuple]] = [None] * len(texts)
        for i, t in enumerate(texts):
            results[i] = self.parse_location(t)
        return results

    # -------------------------
    # Internals
    # -------------------------

    _token_re = re.compile(r"\w+", re.UNICODE)

    def _tokens(self, text: str) -> List[str]:
        return self._token_re.findall(text)

    def _generate_candidates(self, text: str) -> List[str]:
        """
        Generate name candidates from text using token n-grams.
        Keeps it fast by:
          - limiting ngram length
          - skipping very short tokens
          - trimming stopwords on edges
        """
        toks = self._tokens(text)
        # keep original surface, but we match via casefold
        candidates: List[str] = []

        # quick short-circuit: if text itself looks like a single query
        if len(toks) <= self.max_ngram:
            candidates.append(" ".join(toks))

        n = len(toks)
        for i in range(n):
            for j in range(i + 1, min(n, i + self.max_ngram) + 1):
                span = toks[i:j]
                # trim stopwords at edges
                while span and span[0].casefold() in DEFAULT_STOPWORDS:
                    span = span[1:]
                while span and span[-1].casefold() in DEFAULT_STOPWORDS:
                    span = span[:-1]
                if not span:
                    continue
                if any(len(x) == 1 for x in span):
                    continue
                cand = " ".join(span)
                if len(cand) < 2:
                    continue
                candidates.append(cand)

        # de-dup while preserving order, prefer longer spans
        candidates = sorted(set(candidates), key=lambda s: (-len(s), s))
        return candidates[:200]  # hard cap

    def _best_candidate_for_name(self, name: str) -> Optional[LocationResult]:
        name_cf = _norm(name).casefold()
        if not name_cf:
            return None

        rows = self._memo_lookup(name_cf)
        if not rows:
            return None

        return self._score_and_pick(name, rows)

    def _best_candidate_in_text(self, text: str) -> Optional[LocationResult]:
        """
        Two-stage selection:
          1) score best match per candidate string
          2) add a cluster bonus for candidates whose locations are geographically close
        """
        cands = self._generate_candidates(text)
        picked: List[LocationResult] = []

        for cand in cands:
            cand_cf = _norm(cand).casefold()
            if not cand_cf:
                continue
            rows = self._memo_lookup(cand_cf)
            if not rows:
                continue

            res = self._score_and_pick(cand, rows)
            if res:
                picked.append(res)

        if not picked:
            return None

        # If we only found one, nothing to cluster.
        if len(picked) == 1:
            return picked[0]

        # Cluster bonus: prefer locations that are close to other extracted locations.
        # Sigma controls how "local" the bonus is; 500km works well for country/region coherence.
        sigma_km = 500.0
        sigma2 = sigma_km * sigma_km

        # Normalize by max base score so bonus stays bounded.
        max_base = max(p.score for p in picked) or 1.0

        best: Optional[LocationResult] = None
        for i, a in enumerate(picked):
            cohesion = 0.0
            for j, b in enumerate(picked):
                if i == j:
                    continue
                d = _haversine_km(a.lat, a.lng, b.lat, b.lng)
                w = math.exp(-(d * d) / (2.0 * sigma2))
                cohesion += w * (b.score / max_base)

            # modest bonus; base scoring still dominates when there is no strong geographic agreement
            cluster_bonus = 0.6 * cohesion
            final_score = a.score + cluster_bonus

            if best is None or final_score > best.score:
                # store final_score into the returned object so caller sees the effective score
                best = LocationResult(
                    name=a.name,
                    lat=a.lat,
                    lng=a.lng,
                    area=a.area,
                    score=float(final_score),
                    geonameid=a.geonameid,
                    country=a.country,
                    feature_code=a.feature_code,
                )

        return best

    def _memo_lookup(self, name_cf: str) -> List[Tuple]:
        if name_cf in self._name_memo:
            return self._name_memo[name_cf]

        # bumped limit to 50 (and your index can still cap internally if desired)
        rows = self.index.lookup_name(name_cf, limit=50)
        self._name_memo[name_cf] = rows

        if len(self._name_memo) > 20000:
            self._name_memo.clear()
        return rows

    def _score_and_pick(self, surface_name: str, rows: List[Tuple]) -> Optional[LocationResult]:
        """
        Improved scoring (English-default, no lang reliance), fixes:
          - smaller pref boost
          - stronger short penalty
          - penalize "missing admin qualifier" matches (e.g., 'gaza' vs 'gaza province')
          - bias populated places for single-token queries
          - mild penalty for admin areas on single-token queries
        """
        best: Optional[LocationResult] = None
        surface_len = len(surface_name)
        surface_tokens = set(_simple_tokens(_norm(surface_name)))

        # used for feature bias
        surface_is_single_token = (len(surface_tokens) == 1)

        for row in rows:
            # Backward/forward compatible unpacking:
            # OLD: (n_name, geonameid, is_pref, is_short, lang, lat, lng, feature_code, country_code, population)
            # NEW (recommended): add official_name at end
            official_name = None
            if len(row) == 10:
                (n_name, geonameid, is_pref, is_short, lang,
                 lat, lng, feature_code, country_code, population) = row
            elif len(row) >= 11:
                (n_name, geonameid, is_pref, is_short, lang,
                 lat, lng, feature_code, country_code, population, official_name) = row[:11]
            else:
                # unexpected schema
                continue

            pop = int(population or 0)
            pop_score = math.log10(pop + 10)  # ~1..7

            feature_code = str(feature_code or "")
            country_code = str(country_code or "")

            area_hint = FEATURE_AREA_HINT.get(feature_code, FEATURE_AREA_HINT["*"])
            area = float(area_hint)

            # Treat as mostly English text:
            lang = (lang or "")
            lang_adjust = 0.25 if (lang in ("", "en")) else -0.15

            # Smaller preferred-name boost
            pref_boost = 0.35 if int(is_pref or 0) == 1 else 0.0

            # Stronger short penalty
            short_penalty = -0.6 if int(is_short or 0) == 1 else 0.0

            len_boost = min(surface_len / 20.0, 1.0) * 0.4

            # Penalize admin qualifiers when surface omits them (fixes "Gaza Province" from "gaza")
            # Only apply for admin-ish qualifiers; do NOT penalize "strip", "bay", etc.
            official_subset_penalty = 0.0
            if official_name:
                off_tokens = set(_simple_tokens(_norm(official_name)))
                missing = (off_tokens - surface_tokens)
                if missing and surface_tokens.issubset(off_tokens):
                    # if the only missing bits are admin qualifiers, penalize
                    if any(t in ADMIN_QUALIFIERS for t in missing):
                        official_subset_penalty = -0.75

            # Feature bias for single-token queries
            feature_bias = 0.0
            if surface_is_single_token:
                if feature_code.startswith("P"):     # populated place
                    feature_bias += 0.25
                elif feature_code.startswith("ADM"): # admin area
                    feature_bias -= 0.15

            # Final score
            score = (
                (pop_score * 1.0)
                + pref_boost
                + lang_adjust
                + short_penalty
                + len_boost
                + feature_bias
                + official_subset_penalty
                - (area_hint * 0.6)
            )

            res = LocationResult(
                name=surface_name,
                lat=float(lat),
                lng=float(lng),
                area=area,
                score=float(score),
                geonameid=int(geonameid),
                country=country_code,
                feature_code=feature_code,
            )
            if best is None or res.score > best.score:
                best = res

        return best


# -------------------------
# CLI (used by Docker build step)
# -------------------------

def _parse_args():
    import argparse
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build")
    b.add_argument("--geonames", required=True)
    b.add_argument("--alts", required=False, default=None)
    b.add_argument("--db", required=True)
    b.add_argument("--min-population", type=int, default=0)

    d = sub.add_parser("demo")
    d.add_argument("--text", default="Hospital in New York burning due to flames blazing")

    return p.parse_args()

def main():
    args = _parse_args()
    if args.cmd == "build":
        logging.basicConfig(level=logging.INFO)
        build_geonames_db(
            geonames_txt_path=args.geonames,
            alternatenames_txt_path=args.alts,
            db_path=args.db,
            min_population=args.min_population,
        )
        return

    if args.cmd == "demo":
        logging.basicConfig(level=logging.INFO)
        lg = LocationGetter()
        print(lg.parse_location(args.text))
        return

if __name__ == "__main__":
    main()