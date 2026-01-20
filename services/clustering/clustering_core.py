"""
Events Clustering Core - NER/signature + char-ngram semantic backbone (multilingual-friendly)

Drop-in replacement for your current matcher:
- No spaCy vectors required.
- Uses multilingual NER + structured extractors to build a signature per item/cluster.
- Adds character n-gram hashed cosine similarity as the primary semantic backbone (language-agnostic).
- Uses weighted overlap (weighted Jaccard) as a precision booster / guardrail.
- Fixes SEMANTIC representation (token set, not one big string).
- Fixes event indicator detection (works with semantic tokens).
- Removes GPE from key-gate (prevents "Iran merges everything").
- Adds ISO_DATE mismatch penalty (dates as boundary, not glue).
- Keeps optional lexical near-dup path (rapidfuzz).

Assumes:
- spaCy nlp has an entity recognizer (doc.ents). Vectors not required.
- cluster_data = List[(cluster_id, rep_text, last_seen_at)] as before.
"""

from __future__ import annotations

import logging
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Literal, Optional, Tuple

from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

ItemType = Literal["normalized"]

# ----------------------------
# Light canonicalization (only for near-dup lexical path)
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

_STOP = {
    "the", "a", "an", "and", "or", "but",
    "to", "of", "in", "on", "at", "for", "from", "with", "by", "as",
    "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those",
    "it", "its", "they", "them", "their", "we", "you",
    "said", "says", "say", "report", "reports", "reported", "according",
    "via", "new", "latest", "breaking", "news",
}

_UNIT = {"hour": "h", "day": "d", "week": "w", "month": "m", "year": "y"}


def _nums(raw: str) -> List[str]:
    out: List[str] = []
    for m in _RE_NUMBER.finditer(raw):
        s = m.group(0).replace(",", "")
        if 1 <= len(s) <= 10:
            out.append(s)
    return out


def _pcts(raw: str) -> List[str]:
    return [f"{m.group(1)}%" for m in _RE_PERCENT.finditer(raw)]


def _tws(raw: str) -> List[str]:
    out: List[str] = []
    for m in _RE_TIMEWINDOW.finditer(raw):
        val = m.group(1)
        unit = (m.group(2) or "").lower()
        if unit.endswith("s"):
            unit = unit[:-1]
        u = _UNIT.get(unit)
        if u:
            out.append(f"{val}{u}")
    return out


def canonicalize(raw: Optional[str]) -> Tuple[str, set[str]]:
    """
    Returns (canon, rare_tokens) for optional near-dup matching.
    canon = sorted unique tokens joined by spaces (words + small numbers + % + timewindows)
    """
    if not raw:
        return "", set()

    t = raw.strip()
    t = _RE_RT.sub("", t)
    t = _RE_URL.sub(" ", t)
    t = _RE_MENTION.sub(" ", t)
    t = _RE_HASHTAG.sub(" ", t)
    t = _RE_EMOJI.sub(" ", t)

    nums, pcts, tws = _nums(t), _pcts(t), _tws(t)

    t = _RE_PUNCT_KEEP_PCT.sub(" ", t)
    t = _RE_SPACES.sub(" ", t).strip().lower()

    words = [w for w in t.split() if len(w) >= 3 and w not in _STOP]
    tokens = sorted(set(words + nums + pcts + tws))
    canon = " ".join(tokens)

    rare: set[str] = set()
    for tok in tokens:
        if tok in _STOP:
            continue
        if any(c.isdigit() for c in tok) or "%" in tok or len(tok) >= 4:
            rare.add(tok)

    return canon, rare


# ----------------------------
# NER/signature extraction
# ----------------------------

_RE_DOMAIN = re.compile(r"\b([a-z0-9-]+\.)+([a-z]{2,})\b", re.IGNORECASE)
_RE_ISO_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2}|2100)\b")

_LABEL_MAP = {
    "GPE": "GPE",
    "LOC": "LOC",
    "FAC": "FAC",
    "ORG": "ORG",
    "NORP": "NORP",
    "PERSON": "PERSON",
    "PRODUCT": "PRODUCT",
    "EVENT": "EVENT",
    "LAW": "LAW",
    "WORK_OF_ART": "WORK",
    "DATE": "DATE",
    "TIME": "TIME",
    "MONEY": "MONEY",
    "PERCENT": "PERCENT",
    "QUANTITY": "QUANTITY",
    "ORDINAL": "ORDINAL",
    "CARDINAL": "CARDINAL",
    "MISC": "ORG",
}

# Higher weight => matters more for topic identity
_DEFAULT_WEIGHTS: Dict[str, float] = {
    "PERSON": 2.0,
    "ORG": 2.2,
    "GPE": 0.9,   # keep low in scoring; NOT used as key-gate
    "LOC": 1.6,
    "FAC": 1.4,
    "EVENT": 2.8,
    "LAW": 1.8,
    "PRODUCT": 1.2,
    "WORK": 1.0,
    "DATE": 1.2,      # reduced: dates are often glue; boundaries handled separately via mismatch penalty
    "TIME": 0.8,
    "MONEY": 0.8,
    "PERCENT": 0.6,
    "QUANTITY": 0.6,
    "ORDINAL": 0.4,
    "CARDINAL": 0.4,
    "NUM": 0.7,
    "TW": 0.7,
    "DOMAIN": 1.0,
    "URL": 0.4,
    "ISO_DATE": 0.7,  # reduced in positive overlap; boundary handled via mismatch penalty
    "YEAR": 0.6,
    "SEMANTIC": 1.3,  # semantic tokens now participate as set overlap
    "SCRIPT": 0.0,
}

_SCRIPT_BUCKETS = (
    ("LATIN", ("LATIN",)),
    ("CYRILLIC", ("CYRILLIC",)),
    ("ARABIC", ("ARABIC",)),
    ("HEBREW", ("HEBREW",)),
    ("GREEK", ("GREEK",)),
    ("DEVANAGARI", ("DEVANAGARI",)),
    ("HAN", ("CJK UNIFIED IDEOGRAPH", "IDEOGRAPH", "HAN")),
    ("HIRAGANA", ("HIRAGANA",)),
    ("KATAKANA", ("KATAKANA",)),
    ("HANGUL", ("HANGUL",)),
)


def _norm_text(s: str) -> str:
    s = s.strip()
    s = unicodedata.normalize("NFKC", s)
    s = _RE_SPACES.sub(" ", s)
    return s.casefold()


def _script_signature(text: str) -> str:
    counts: Dict[str, int] = {}
    for ch in text:
        if not ch.isalpha():
            continue
        name = unicodedata.name(ch, "")
        bucket = "OTHER"
        for b, keys in _SCRIPT_BUCKETS:
            if any(k in name for k in keys):
                bucket = b
                break
        counts[bucket] = counts.get(bucket, 0) + 1
    if not counts:
        return "OTHER"
    return max(counts.items(), key=lambda kv: kv[1])[0]


# ----------------------------
# Semantic tokens (FIXED: token set, not one big string)
# ----------------------------

# Keep these intentionally small and high-signal; extend later as needed.
_EVENT_TYPE_KEYWORDS = {
    "protest": {"protest", "protests", "demonstration", "demonstrations", "rally", "rallies", "vigil", "march"},
    "violence": {"violence", "violent", "riot", "riots", "clash", "clashes", "unrest", "uprising"},
    "death": {"death", "deaths", "toll", "killed", "killing", "executed", "executions", "casualties", "fatalities"},
    "internet": {"blackout", "shutdown", "censorship", "blocked", "disrupted", "interrupted", "internet"},
    "regime": {"regime", "government", "authorities", "security", "forces", "crackdown", "repression"},
    "sanctions": {"sanctions", "embargo", "export", "ban", "banned", "restrictions", "diplomatic"},
    "media": {"footage", "video", "videos", "images", "photos", "journalist", "journalists", "coverage"},
    "activist": {"activist", "activists", "dissident", "dissidents", "rights", "freedom"},
}

# Event indicator tokens (match BOTH raw words and type-prefixed semantic tokens)
_EVENT_INDICATORS = {
    # raw keywords
    "protest", "protests", "demonstration", "rally", "unrest", "uprising", "riot", "clash", "crackdown",
    "violence", "death", "deaths", "killed", "executed", "casualties", "fatalities",
    "blackout", "shutdown", "censorship", "blocked", "disrupted", "internet",
    "sanctions", "embargo", "crisis", "conflict", "war",
    "activist", "rights", "freedom",
    # type-prefixed patterns used below
    "primary_protest", "primary_violence", "primary_death", "primary_internet", "primary_regime",
}


def _extract_semantic_tokens(text: str) -> set[str]:
    """
    Extract semantic tokens for topic identity when NER is weak.
    Returns a set of canonical tokens.
    """
    t = text.lower()
    t = _RE_URL.sub(" ", t)
    t = _RE_MENTION.sub(" ", t)
    t = _RE_HASHTAG.sub(" ", t)
    t = _RE_EMOJI.sub(" ", t)
    t = _RE_PUNCT_KEEP_PCT.sub(" ", t)
    t = _RE_SPACES.sub(" ", t).strip()

    tokens: set[str] = set()
    type_hits: List[str] = []

    for w in t.split():
        if len(w) < 3 or w in _STOP:
            continue

        found_type = None
        for et, kws in _EVENT_TYPE_KEYWORDS.items():
            if w in kws:
                found_type = et
                break

        if found_type:
            tokens.add(f"{found_type}:{w}")
            type_hits.append(found_type)
            continue

        # keep longer content words (helps cross-lingual somewhat via loanwords / named terms)
        if any(c.isdigit() for c in w) or "%" in w:
            tokens.add(w)
        elif len(w) >= 6:
            tokens.add(w)

    if type_hits:
        dominant = max(set(type_hits), key=type_hits.count)
        tokens.add(f"primary_{dominant}")

    return tokens


def extract_signature(nlp, raw: Optional[str]) -> Tuple[Dict[str, set[str]], str]:
    """
    Returns (features_by_label, script_bucket).
    features_by_label: label -> set of normalized values.
    """
    feats: Dict[str, set[str]] = {}
    if not raw:
        return feats, "OTHER"

    t = raw.strip()
    t = _RE_RT.sub("", t)
    script = _script_signature(t)

    # URLs/domains
    for m in _RE_URL.finditer(t):
        feats.setdefault("URL", set()).add(_norm_text(m.group(0)))

    for m in _RE_DOMAIN.finditer(t):
        feats.setdefault("DOMAIN", set()).add(_norm_text(m.group(0)))

    # Structured numbers/time windows/percents
    for x in _nums(t):
        feats.setdefault("NUM", set()).add(x)
    for x in _pcts(t):
        feats.setdefault("PERCENT", set()).add(x)
    for x in _tws(t):
        feats.setdefault("TW", set()).add(x)

    for m in _RE_ISO_DATE.finditer(t):
        feats.setdefault("ISO_DATE", set()).add(m.group(0))
    for m in _RE_YEAR.finditer(t):
        feats.setdefault("YEAR", set()).add(m.group(0))

    # NER
    doc = nlp(t)
    for ent in getattr(doc, "ents", []):
        label = _LABEL_MAP.get(ent.label_, ent.label_)
        val = _norm_text(ent.text)
        if not val or len(val) <= 2:
            continue
        feats.setdefault(label, set()).add(val)

    # Semantic tokens (FIXED)
    sem = _extract_semantic_tokens(t)
    if sem:
        feats.setdefault("SEMANTIC", set()).update(sem)

    return feats, script


def _flatten_features(feats: Dict[str, set[str]]) -> set[str]:
    out: set[str] = set()
    for label, vals in feats.items():
        for v in vals:
            out.add(f"{label}={v}")
    return out


# ----------------------------
# Char n-gram hashed vectors (no sklearn dependency)
# ----------------------------

def _clean_for_ngrams(text: str) -> str:
    t = text.casefold()
    t = _RE_URL.sub(" ", t)
    t = _RE_MENTION.sub(" ", t)
    t = _RE_HASHTAG.sub(" ", t)
    t = _RE_EMOJI.sub(" ", t)
    # keep letters/numbers/spaces; punctuation -> space
    t = _RE_PUNCT_KEEP_PCT.sub(" ", t)
    t = _RE_SPACES.sub(" ", t).strip()
    return t


def _hashed_char_ngrams(text: str, n_min: int, n_max: int, dim: int) -> Dict[int, float]:
    """
    Sparse hashed char n-gram counts with log-scaling.
    Returns: {bucket_index: weight}
    """
    t = _clean_for_ngrams(text)
    if not t:
        return {}

    # Add boundary markers to make short strings behave better
    t = f" {t} "
    L = len(t)

    counts: Dict[int, int] = {}

    for n in range(n_min, n_max + 1):
        if L < n:
            continue
        for i in range(0, L - n + 1):
            ng = t[i : i + n]
            # Python hash is salted per process; stable enough within one run / index lifetime.
            # If you need stability across processes, replace with a fixed hash (e.g. mmh3).
            h = hash(ng) % dim
            counts[h] = counts.get(h, 0) + 1

    # log scaling for robustness
    out: Dict[int, float] = {}
    for k, c in counts.items():
        out[k] = 1.0 + math.log(1.0 + float(c))
    return out


def _cosine_sparse(a: Dict[int, float], b: Dict[int, float]) -> float:
    if not a or not b:
        return 0.0
    # iterate smaller
    if len(a) > len(b):
        a, b = b, a
    dot = 0.0
    for k, va in a.items():
        vb = b.get(k)
        if vb is not None:
            dot += va * vb
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    if na <= 1e-12 or nb <= 1e-12:
        return 0.0
    return float(dot / (na * nb))


# ----------------------------
# Data structures
# ----------------------------

@dataclass(frozen=True)
class Item:
    item_type: ItemType
    item_id: str
    text: str
    created_at: datetime
    url: Optional[str] = None


@dataclass
class IndexEntry:
    cluster_id: str
    rep_text: str
    rep_canon: str
    rep_sig: Dict[str, set[str]]
    rep_flat: set[str]
    rep_script: str
    rep_ng: Dict[int, float]           # hashed char n-gram sparse vector
    last_seen_at: Optional[datetime]


# ----------------------------
# Index
# ----------------------------

class ClusterIndex:
    def __init__(self, nlp):
        self.nlp = nlp
        self.entries: List[IndexEntry] = []
        self.last_refresh: Optional[datetime] = None

        # n-gram settings
        self.ngram_dim = 1 << 16  # 65536 buckets
        self.ngram_n_min = 3
        self.ngram_n_max = 5

    def refresh_from_data(self, cluster_data: List[Tuple[str, str, Optional[datetime]]]) -> None:
        metas: List[Tuple[str, str, str, Dict[str, set[str]], set[str], str, Dict[int, float], Optional[datetime]]] = []

        for cid, rep_text, last_seen_at in cluster_data:
            canon, _ = canonicalize(rep_text)
            sig, script = extract_signature(self.nlp, rep_text)
            flat = _flatten_features(sig)
            ng = _hashed_char_ngrams(rep_text, self.ngram_n_min, self.ngram_n_max, self.ngram_dim)
            metas.append((cid, rep_text, canon, sig, flat, script, ng, last_seen_at))

        self.entries = [
            IndexEntry(
                cluster_id=cid,
                rep_text=rep_text,
                rep_canon=canon,
                rep_sig=sig,
                rep_flat=flat,
                rep_script=script,
                rep_ng=ng,
                last_seen_at=ls,
            )
            for (cid, rep_text, canon, sig, flat, script, ng, ls) in metas
        ]
        self.last_refresh = datetime.now(timezone.utc)

    def get_cluster_ids(self) -> List[str]:
        return [e.cluster_id for e in self.entries]

    def add_or_update_from_data(self, cluster_id: str, rep_text: str, last_seen_at: Optional[datetime]) -> None:
        canon, _ = canonicalize(rep_text)
        sig, script = extract_signature(self.nlp, rep_text)
        flat = _flatten_features(sig)
        ng = _hashed_char_ngrams(rep_text, self.ngram_n_min, self.ngram_n_max, self.ngram_dim)

        self.entries = [e for e in self.entries if e.cluster_id != cluster_id]
        self.entries.insert(
            0,
            IndexEntry(
                cluster_id=cluster_id,
                rep_text=rep_text,
                rep_canon=canon,
                rep_sig=sig,
                rep_flat=flat,
                rep_script=script,
                rep_ng=ng,
                last_seen_at=last_seen_at,
            ),
        )


# ----------------------------
# Matcher
# ----------------------------

class ClusterMatcher:
    def __init__(self, nlp, index: ClusterIndex, refresh_callback=None):
        self.nlp = nlp
        self.index = index
        self.refresh_callback = refresh_callback

        # Lexical near-dup (optional)
        self.lex_token_set = 85
        self.lex_partial = 88
        self.enable_lexical_near_dup = True

        # NER/signature scoring as precision booster
        self.weights = dict(_DEFAULT_WEIGHTS)
        self.min_sig_score = 0.18           # signature overlap is not the backbone anymore
        self.sig_weight = 0.35              # contribution to final score

        # Char n-gram cosine as semantic backbone
        self.ng_weight = 0.55
        self.min_ng_score = 0.28            # accept only if semantic similarity is decent

        # Combined acceptance threshold
        self.min_final_score = 0.36

        # Key identity gate (FIXED: remove GPE)
        self.min_shared_key_items = 1
        self.key_labels = {"PERSON", "ORG", "EVENT", "LAW"}  # optionally add FAC/LOC if you want

        # Fuzzy entity matching (targeted)
        self.enable_fuzzy_entities = True
        self.fuzzy_threshold = 88
        self.fuzzy_max_checks_per_label = 30
        self.fuzzy_bonus_weight = 0.10

        # Script guard (optional; keep conservative)
        self.script_guard = False
        self.allow_cross_script_if_strong = True
        self.cross_script_strong_score = 0.72

        # Date boundary penalty (FIXED: dates are boundary, not glue)
        self.iso_date_mismatch_penalty = 0.08  # subtract from final score if both have ISO_DATE but no overlap

        # Time-aware filtering/ranking
        self.max_cluster_age_days = 21
        self.time_half_life_hours = 72.0
        self.time_weight = 0.10

        # Prefiltering
        self.prefilter_min_overlap = 1
        self.prefilter_max_candidates = 2500

        # Index refresh
        self.max_age_hours = 72
        self.refresh_s = 300

    def assign(self, item: Item) -> Tuple[Optional[str], Optional[float], str]:
        if self._needs_refresh():
            if self.refresh_callback:
                self.refresh_callback(hours=self.max_age_hours)

        if not self.index.entries:
            return None, None, ""

        now = item.created_at
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        # Prep item
        canon, _ = canonicalize(item.text)
        sig, script = extract_signature(self.nlp, item.text)
        flat = _flatten_features(sig)
        ng = _hashed_char_ngrams(item.text, self.index.ngram_n_min, self.index.ngram_n_max, self.index.ngram_dim)

        if not sig and not canon and not ng:
            return None, None, ""

        cands = self._prefilter(flat) if flat else []
        if not cands:
            cands = self.index.entries

        # 1) Lexical near-dup early win
        if self.enable_lexical_near_dup and canon:
            cid, sc = self._best_lex(canon, cands)
            if cid:
                return cid, sc, "near_dup_rapidfuzz"
            cid, sc = self._best_partial(canon, cands)
            if cid:
                return cid, sc, "near_dup_partial"

        # 2) Combined semantic (ngram) + signature (NER/structured)
        best_cid: Optional[str] = None
        best_final = -1.0
        best_raw = -1.0

        for e in cands:
            if self._too_old(now, e.last_seen_at):
                continue

            if self.script_guard and script != "OTHER" and e.rep_script != "OTHER" and script != e.rep_script:
                if not self.allow_cross_script_if_strong:
                    continue

            ng_sc = _cosine_sparse(ng, e.rep_ng)
            if ng_sc < self.min_ng_score:
                continue

            sig_sc = self._weighted_jaccard(sig, e.rep_sig)

            # optional fuzzy rescue (only on key labels; only if semantic already decent)
            if self.enable_fuzzy_entities and ng_sc >= (self.min_ng_score + 0.05):
                if sig_sc < self.min_sig_score and sig_sc > (self.min_sig_score * 0.75):
                    sig_sc = max(sig_sc, self._fuzzy_boost(sig, e.rep_sig, base=sig_sc))

            # key gate: require overlap on PERSON/ORG/EVENT/LAW OR strong semantic similarity
            if not self._passes_key_gate(sig, e.rep_sig, ng_sc):
                continue

            final = (self.ng_weight * ng_sc) + (self.sig_weight * sig_sc)

            # date mismatch penalty (boundary)
            final -= self._iso_date_penalty(sig, e.rep_sig)

            # cross-script override: only accept very strong combined scores
            if self.script_guard and script != "OTHER" and e.rep_script != "OTHER" and script != e.rep_script:
                if final < self.cross_script_strong_score:
                    continue

            # time adjustment
            if self.time_weight and e.last_seen_at is not None:
                age_h = self._age_hours(now, e.last_seen_at)
                decay = self._exp_decay(age_h, self.time_half_life_hours)
                final = final + self.time_weight * (decay - 1.0)

            if final < self.min_final_score:
                continue

            if final > best_final:
                best_final = final
                best_raw = final
                best_cid = e.cluster_id

        if not best_cid:
            return None, None, ""

        return best_cid, float(best_raw), "ngram+ner_signature"

    def _needs_refresh(self) -> bool:
        lr = self.index.last_refresh
        if lr is None:
            return True
        return (datetime.now(timezone.utc) - lr).total_seconds() > self.refresh_s

    def _prefilter(self, flat: set[str]) -> List[IndexEntry]:
        if self.prefilter_min_overlap <= 0 or not flat:
            return []
        out: List[IndexEntry] = []
        for e in self.index.entries:
            if not e.rep_flat:
                continue
            if len(flat.intersection(e.rep_flat)) >= self.prefilter_min_overlap:
                out.append(e)
                if len(out) >= self.prefilter_max_candidates:
                    break
        return out

    def _best_lex(self, canon: str, cands: List[IndexEntry]) -> Tuple[Optional[str], Optional[float]]:
        best_id, best = None, 0.0
        for e in cands:
            if not e.rep_canon:
                continue
            s = float(fuzz.token_set_ratio(canon, e.rep_canon))
            if s >= self.lex_token_set and s > best:
                best_id, best = e.cluster_id, s
        return best_id, (best / 100.0 if best_id else None)

    def _best_partial(self, canon: str, cands: List[IndexEntry]) -> Tuple[Optional[str], Optional[float]]:
        best_id, best = None, 0.0
        for e in cands:
            if not e.rep_canon:
                continue
            s = float(fuzz.partial_ratio(canon, e.rep_canon))
            if s >= self.lex_partial and s > best:
                best_id, best = e.cluster_id, s
        return best_id, (best / 100.0 if best_id else None)

    def _too_old(self, now: datetime, last_seen_at: Optional[datetime]) -> bool:
        if last_seen_at is None:
            return False
        ls = last_seen_at
        if ls.tzinfo is None:
            ls = ls.replace(tzinfo=timezone.utc)
        return (now - ls) > timedelta(days=self.max_cluster_age_days)

    @staticmethod
    def _age_hours(now: datetime, last_seen_at: datetime) -> float:
        ls = last_seen_at
        if ls.tzinfo is None:
            ls = ls.replace(tzinfo=timezone.utc)
        return max(0.0, (now - ls).total_seconds() / 3600.0)

    @staticmethod
    def _exp_decay(age_hours: float, half_life_hours: float) -> float:
        if half_life_hours <= 0:
            return 1.0
        return float(2.0 ** (-age_hours / half_life_hours))

    def _weighted_jaccard(self, a: Dict[str, set[str]], b: Dict[str, set[str]]) -> float:
        if not a or not b:
            return 0.0

        inter = 0.0
        uni = 0.0

        labels = set(a.keys()) | set(b.keys())
        for lab in labels:
            wa = a.get(lab, set())
            wb = b.get(lab, set())
            if not wa and not wb:
                continue
            w = float(self.weights.get(lab, 1.0))
            i = len(wa.intersection(wb))
            u = len(wa.union(wb))
            inter += w * i
            uni += w * u

        if uni <= 1e-9:
            return 0.0
        return float(inter / uni)

    def _passes_key_gate(self, a: Dict[str, set[str]], b: Dict[str, set[str]], ng_score: float) -> bool:
        # If semantic similarity is very strong, allow even with weak NER
        if ng_score >= 0.60:
            return True

        # Otherwise require at least one key label overlap (FIXED: no GPE)
        shared = 0
        for lab in self.key_labels:
            sa = a.get(lab)
            sb = b.get(lab)
            if not sa or not sb:
                continue
            if sa.intersection(sb):
                shared += 1
                if shared >= self.min_shared_key_items:
                    return True

        # If no exact key overlap, allow when SEMANTIC indicates event-like match
        # (useful when NER is weak but text is clearly in same event frame)
        if self._has_event_indicators(a) and self._has_event_indicators(b) and ng_score >= 0.45:
            return True

        # Optional fuzzy key overlap (tight, only if semantic is already decent)
        if self.enable_fuzzy_entities and ng_score >= 0.42:
            if self._has_potential_key_overlap(a, b):
                return True

        return False

    def _iso_date_penalty(self, a: Dict[str, set[str]], b: Dict[str, set[str]]) -> float:
        da = a.get("ISO_DATE")
        db = b.get("ISO_DATE")
        if not da or not db:
            return 0.0
        if da.intersection(db):
            return 0.0
        # both have ISO_DATE but none matches => boundary penalty
        return self.iso_date_mismatch_penalty

    def _has_potential_key_overlap(self, a: Dict[str, set[str]], b: Dict[str, set[str]]) -> bool:
        for lab in self.key_labels:
            sa = a.get(lab)
            sb = b.get(lab)
            if not sa or not sb:
                continue
            la = list(sa)[: self.fuzzy_max_checks_per_label]
            lb = list(sb)[: self.fuzzy_max_checks_per_label]
            for va in la:
                for vb in lb:
                    if fuzz.token_set_ratio(va, vb) >= self.fuzzy_threshold:
                        return True
        return False

    def _has_event_indicators(self, sig: Dict[str, set[str]]) -> bool:
        sem = sig.get("SEMANTIC")
        if not sem:
            return False
        # If any known indicator token appears, consider it event-like.
        if sem.intersection(_EVENT_INDICATORS):
            return True
        # Also accept type-prefixed indicators like "protest:protest", "death:killed"
        for tok in sem:
            # "type:word"
            if ":" in tok:
                t, w = tok.split(":", 1)
                if t in {"protest", "violence", "death", "internet", "regime", "sanctions"}:
                    return True
                if w in _EVENT_INDICATORS:
                    return True
        return False

    def _fuzzy_boost(self, a: Dict[str, set[str]], b: Dict[str, set[str]], base: float) -> float:
        bonus_hits = 0.0
        bonus_total = 0.0

        for lab in self.key_labels:
            vals_a = a.get(lab, set())
            vals_b = b.get(lab, set())
            if not vals_a or not vals_b:
                continue

            la = list(vals_a)[: self.fuzzy_max_checks_per_label]
            lb = list(vals_b)[: self.fuzzy_max_checks_per_label]
            w = float(self.weights.get(lab, 1.0))

            for va in la:
                best = 0.0
                for vb in lb:
                    s = float(fuzz.token_set_ratio(va, vb))
                    if s > best:
                        best = s
                        if best >= 100.0:
                            break
                if best >= self.fuzzy_threshold:
                    bonus_hits += w * (best / 100.0)
                bonus_total += w

        if bonus_total <= 1e-9:
            return base

        bump = self.fuzzy_bonus_weight * (bonus_hits / bonus_total)
        return min(1.0, base + bump)