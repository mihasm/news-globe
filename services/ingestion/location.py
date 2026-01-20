# services/ingestion/location.py
import os
import re
import math
import logging
import requests
from typing import List, Optional, Tuple, Dict, Any, Set

logger = logging.getLogger(__name__)

_WORD_RE = re.compile(r"[A-Za-z0-9]+")
_FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")

# Minimal, high-signal country aliases (extend as needed)
_COUNTRY_ALIASES: Dict[str, str] = {
    "us": "US",
    "usa": "US",
    "u.s.": "US",
    "u.s": "US",
    "unitedstates": "US",
    "uk": "GB",
    "u.k.": "GB",
    "uae": "AE",
}

# Feature intent keywords -> (feature_class whitelist, feature_code prefix whitelist)
_FEATURE_INTENT: Dict[str, Tuple[Set[str], Set[str]]] = {
    "river": ({"H"}, {"STM", "STMI", "WAD"}),  # stream/river-ish; imperfect but helpful
    "lake": ({"H"}, {"LK", "LKI", "LKS"}),
    "mount": ({"T"}, set()),
    "mountain": ({"T"}, set()),
    "city": ({"P"}, set()),
    "town": ({"P"}, set()),
    "country": ({"A"}, {"PCL"}),              # PCL* family (PCLI, PCL, etc.)
}

def _tokens(s: str) -> List[str]:
    return [t.lower() for t in _WORD_RE.findall(s or "")]

def _norm_join(tokens: List[str]) -> str:
    return "".join(tokens)

def _safe_float(x: Any) -> Optional[float]:
    """
    GeoNames-derived services sometimes return lat/lon as strings with extra junk.
    Example observed: "8.0JS:8" or "-66.0JS:-66".
    Extract the first float substring.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    m = _FLOAT_RE.search(str(x))
    return float(m.group(0)) if m else None

def _log_pop(pop: int) -> float:
    if pop <= 0:
        return 0.0
    return math.log10(pop + 1)

def _name_match_score(query: str, cand_name: str) -> float:
    q = (query or "").strip().lower()
    n = (cand_name or "").strip().lower()
    if not q or not n:
        return 0.0

    if q == n:
        return 5.0
    if n.startswith(q):
        return 3.0

    qt = set(_tokens(q))
    nt = set(_tokens(n))
    if not qt:
        return 0.0

    overlap = len(qt & nt) / len(qt)
    return 2.0 * overlap

def _detect_country_bias(tokens: List[str]) -> Optional[str]:
    joined = _norm_join(tokens)
    if joined in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[joined]

    for t in tokens:
        if t in _COUNTRY_ALIASES:
            return _COUNTRY_ALIASES[t]
        if len(t) == 2 and t.isalpha():
            return t.upper()

    return None

def _detect_feature_intent(tokens: List[str]) -> Tuple[Optional[Set[str]], Optional[Set[str]]]:
    for t in tokens:
        if t in _FEATURE_INTENT:
            cls, prefixes = _FEATURE_INTENT[t]
            return (set(cls) if cls else None, set(prefixes) if prefixes else None)
    return (None, None)

def _feature_score(feature_class: str, feature_code: str,
                   want_classes: Optional[Set[str]],
                   want_prefixes: Optional[Set[str]]) -> float:
    fc = (feature_class or "").upper()
    fcode = (feature_code or "").upper()
    score = 0.0

    if want_classes is not None:
        score += 2.5 if fc in want_classes else -2.5

    if want_prefixes:
        score += 2.0 if any(fcode.startswith(p) for p in want_prefixes) else -1.0

    return score

def _country_score(candidate: Dict[str, Any], country_bias: Optional[str]) -> float:
    if not country_bias:
        return 0.0
    cc = (candidate.get("country_code") or candidate.get("country") or "").upper()
    return 2.5 if cc == country_bias else -1.5

def _is_country_candidate(c: Dict[str, Any]) -> bool:
    fc = (c.get("feature_class") or "").upper()
    fcode = (c.get("feature_code") or "").upper()
    return fc == "A" and fcode.startswith("PCL")

def _has_valid_latlon(c: Dict[str, Any]) -> bool:
    return _safe_float(c.get("lat")) is not None and _safe_float(c.get("lon")) is not None

def _pick_best_country_candidate(candidates: List[Dict[str, Any]], query_tokens: List[str]) -> Optional[Dict[str, Any]]:
    country_cands = [c for c in candidates if _is_country_candidate(c) and _has_valid_latlon(c)]
    if not country_cands:
        return None

    qt = set(query_tokens)

    def key(c: Dict[str, Any]) -> Tuple[int, int, float]:
        pop = int(c.get("population") or 0)
        nm = (c.get("name") or "").lower()
        overlap = len(set(_tokens(nm)) & qt)  # "Bolivarian Republic of Venezuela" overlaps on "venezuela"
        # slight preference for better name overlap, then population
        return (overlap, pop, _name_match_score(" ".join(query_tokens), c.get("name", "")))

    return max(country_cands, key=key)

class LocationGetter:
    """
    HTTP client for the location service.
    Compatible interface with the old LocationGetter.
    """
    def __init__(self, service_url: Optional[str] = None):
        self.service_url = service_url or os.getenv("LOCATION_SERVICE_URL", "http://location:8787")
        self._session = requests.Session()
        self._timeout = 10  # seconds

    def get_location(self, text: str) -> Optional[Tuple[float, float, float]]:
        result = self.parse_location(text)
        if result:
            return (result[1], result[2], result[3])  # lat, lng, area
        return None

    def parse_location(self, text: str) -> Optional[Tuple[str, float, float, float, float]]:
        if not text or len(text.strip()) < 2:
            return None

        query = text.strip()
        toks = _tokens(query)
        if not toks:
            return None

        country_bias = _detect_country_bias(toks)
        want_classes, want_prefixes = _detect_feature_intent(toks)

        try:
            params = {"key": query, "limit": 50}
            response = self._session.get(f"{self.service_url}/query", params=params, timeout=self._timeout)
            response.raise_for_status()

            data = response.json()
            candidates: List[Dict[str, Any]] = data.get("candidates", [])
            if not candidates:
                return None

            # NEW PREFERENCE:
            # For single-token queries with no explicit feature intent,
            # prefer a country-level entity (A.PCL*) over places with the exact same name.
            # This fixes cases like "Venezuela" returning a town named Venezuela.
            if len(toks) == 1 and want_classes is None and want_prefixes is None:
                best_country = _pick_best_country_candidate(candidates, toks)
                if best_country is not None:
                    name = best_country.get("name", "")
                    lat = _safe_float(best_country.get("lat"))
                    lon = _safe_float(best_country.get("lon"))
                    if lat is not None and lon is not None:
                        return (name, float(lat), float(lon), 0.5, 10.0)

            # Otherwise, compute a weighted score.
            best: Optional[Dict[str, Any]] = None
            best_score = -1e9

            for c in candidates:
                name = c.get("name", "")
                lat = _safe_float(c.get("lat"))
                lon = _safe_float(c.get("lon"))
                if lat is None or lon is None:
                    continue

                fc = c.get("feature_class", "")
                fcode = c.get("feature_code", "")
                pop = int(c.get("population") or 0)

                s = 0.0
                s += _name_match_score(query, name) * 3.0
                s += _feature_score(fc, fcode, want_classes, want_prefixes) * 3.0
                s += _country_score(c, country_bias) * 3.0
                s += _log_pop(pop) * 0.8  # weak prior

                # Mild general preference for country entities (helps where name isn't exact).
                if _is_country_candidate(c):
                    s += 4.0

                # If query explicitly says "country", strongly prefer PCL*
                if want_prefixes and any(p == "PCL" for p in want_prefixes) and _is_country_candidate(c):
                    s += 6.0

                if s > best_score:
                    best_score = s
                    best = c

            if not best:
                return None

            name = best.get("name", "")
            lat = _safe_float(best.get("lat"))
            lon = _safe_float(best.get("lon"))
            if lat is None or lon is None:
                return None

            fc = (best.get("feature_class") or "").upper()
            if fc == "P":
                area = 0.1
            elif fc == "A":
                area = 0.5
            else:
                area = 0.2

            score = max(1.0, min(10.0, 1.0 + best_score / 5.0))
            return (name, float(lat), float(lon), area, score)

        except Exception as e:
            logger.error(f"Error querying location service for '{text}': {e}")
            return None

    def parse_locations_batch(self, texts: List[str], batch_size: int = 50) -> List[Optional[Tuple]]:
        return [self.parse_location(t) for t in texts]