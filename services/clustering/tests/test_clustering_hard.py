# test_ner_signature_clustering_hard50.py
#
# Hard synthetic dataset (50 texts):
# - 10 clusters (25 items total across clusters; varying counts)
# - 25 noise items (must NOT match any cluster)
#
# This plugs into the same NER/signature-based clustering code pattern:
# - Build index with 10 clusters using representative texts.
# - Run matcher.assign() on all 50 items.
# - Assert correct cluster assignment for cluster items and None for noise.
#
# Requirements:
# - spaCy installed
# - A spaCy model with NER (tries SPACY_MODEL env var, then en_core_web_sm, then xx_ent_wiki_sm)
#
# Run:
#   SPACY_MODEL=xx_ent_wiki_sm pytest -q
#
# IMPORTANT:
# - Update the import path for ClusterIndex/ClusterMatcher/Item below to your real module.

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pytest

try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False
    spacy = None

# Adjust this import to your project
# from services.events.clustering_core_ner import ClusterIndex, ClusterMatcher, Item
from services.clustering.clustering_core import ClusterIndex, ClusterMatcher, Item  # <-- CHANGE THIS

UTC = timezone.utc


def _now() -> datetime:
    return datetime.now(UTC)


# 10 clusters, 25 total clustered items (counts: 3,2,3,2,3,2,3,2,3,2)
CLUSTERS: Tuple[Tuple[str, int], ...] = (
    ("CL_1", 3),
    ("CL_2", 2),
    ("CL_3", 3),
    ("CL_4", 2),
    ("CL_5", 3),
    ("CL_6", 2),
    ("CL_7", 3),
    ("CL_8", 2),
    ("CL_9", 3),
    ("CL_10", 2),
)


def _mk_cluster_data(now: datetime) -> List[Tuple[str, str, Optional[datetime]]]:
    # Representative texts include strong anchors: (place/org/unique tokens/date/number)
    # Keep them distinct across clusters to avoid accidental cross-matches.
    rep_1 = (
        "Gulf of Finland: Tallinn–Helsinki ferry 'Baltic Star' collided with cargo vessel on 2026-01-14; "
        "Port of Tallinn confirms minor damage, no injuries reported."
    )
    rep_2 = (
        "UN Security Council passes Resolution 9999 on 2026-01-15 calling for humanitarian corridors in Darfur; "
        "UN spokesperson cites access for aid convoys."
    )
    rep_3 = (
        "US SEC approves 'Atlas Bitcoin ETF' on 2026-01-13; ticker ATBX begins trading; "
        "approval referenced under file No. 34-12345."
    )
    rep_4 = (
        "Paris Metro strike: RATP drivers walk out on 2026-01-12; Lines 1, 4, 14 disrupted; unions demand pay talks."
    )
    rep_5 = (
        "São Paulo reports dengue surge on 2026-01-11; municipal health authority confirms 18,000 suspected cases; "
        "vaccination campaign expanded."
    )
    rep_6 = (
        "NASA delays Artemis II launch to 2026-05 citing heat-shield inspection; "
        "Kennedy Space Center schedule updated on 2026-01-10."
    )
    rep_7 = (
        "Microsoft 365 incident: Teams message delivery degraded on 2026-01-16; "
        "service health page says a routing change caused the outage."
    )
    rep_8 = (
        "Mumbai monsoon-style flooding after cloudburst on 2026-01-17; BMC issues alerts; "
        "Western Railway suspends some local trains."
    )
    rep_9 = (
        "Kyoto: fire at Kiyomizu-dera auxiliary hall on 2026-01-18; Kyoto Police arrest suspect for arson; "
        "no casualties reported."
    )
    rep_10 = (
        "Iceland: volcanic eruption near Reykjanes / Grindavík on 2026-01-19; "
        "Icelandic Met Office raises aviation color code; evacuations ordered."
    )
    return [
        ("CL_1", rep_1, now),
        ("CL_2", rep_2, now),
        ("CL_3", rep_3, now),
        ("CL_4", rep_4, now),
        ("CL_5", rep_5, now),
        ("CL_6", rep_6, now),
        ("CL_7", rep_7, now),
        ("CL_8", rep_8, now),
        ("CL_9", rep_9, now),
        ("CL_10", rep_10, now),
    ]


def _texts() -> Tuple[Dict[str, List[str]], List[str]]:
    # Cluster items are intentionally harder:
    # - multilingual (Latin + Cyrillic/Greek/Arabic/Japanese sprinkled in)
    # - paraphrases, abbreviations, different order, missing some details
    # - still preserving enough unique anchors to match correct cluster

    clustered: Dict[str, List[str]] = {
        # CL_1 (3): Tallinn–Helsinki ferry collision
        "CL_1": [
            "Tallinn–Helsinki ferry 'Baltic Star' bumps a cargo ship in the Gulf of Finland (2026-01-14); Port of Tallinn says no injuries.",
            "Trajekt Tallinn–Helsinki 'Baltic Star' trčil v tovorno ladjo v Finskem zalivu 2026-01-14; škoda manjša, brez poškodovanih.",
            "Паром Tallinn–Helsinki «Baltic Star» столкнулся с грузовым судном в Финском заливе (2026-01-14); порт Таллина сообщает: пострадавших нет.",
        ],
        # CL_2 (2): UNSC resolution on Darfur humanitarian corridors
        "CL_2": [
            "UNSC adopts Resolution 9999 on 2026-01-15 urging humanitarian corridors in Darfur; aid convoys mentioned by UN spokesperson.",
            "Conseil de sécurité de l’ONU: résolution 9999 (2026-01-15) sur des corridors humanitaires au Darfour; accès des convois d’aide.",
        ],
        # CL_3 (3): SEC approves Atlas Bitcoin ETF (ATBX)
        "CL_3": [
            "SEC greenlights the 'Atlas Bitcoin ETF' (ATBX) on 2026-01-13 under file No. 34-12345; trading starts shortly after.",
            "La SEC approva l’ETF Bitcoin 'Atlas' (ticker ATBX) il 2026-01-13; riferimento al fascicolo 34-12345.",
            "Η SEC εγκρίνει το Atlas Bitcoin ETF (ATBX) στις 2026-01-13· αναφορά στο 34-12345.",
        ],
        # CL_4 (2): Paris Metro strike (RATP)
        "CL_4": [
            "Paris: RATP staff strike disrupts Métro lines 1, 4 and 14 on 2026-01-12; unions push for pay negotiations.",
            "Sciopero della metro di Parigi (RATP) il 2026-01-12: linee 1/4/14 con forti ritardi; sindacati chiedono aumenti.",
        ],
        # CL_5 (3): São Paulo dengue surge (18,000 suspected)
        "CL_5": [
            "São Paulo sees dengue spike (18,000 suspected cases) reported 2026-01-11; city expands vaccination drive.",
            "En São Paulo, hausse de la dengue: 18 000 cas suspects (2026-01-11); la mairie élargit la vaccination.",
            "São Paulo: aumento de dengue (18.000 suspeitos) em 2026-01-11; autoridade de saúde municipal reforça campanha.",
        ],
        # CL_6 (2): NASA delays Artemis II to 2026-05 (heat shield inspection)
        "CL_6": [
            "NASA postpones Artemis II to May 2026 after heat-shield inspection; Kennedy Space Center schedule updated (2026-01-10).",
            "NASA odloži Artemis II na 2026-05 zaradi pregleda toplotnega ščita; posodobitev urnika KSC z dne 2026-01-10.",
        ],
        # CL_7 (3): Microsoft 365 / Teams message delivery degraded (routing change)
        "CL_7": [
            "Microsoft 365 incident: Teams message delivery degraded on 2026-01-16; service health cites a routing change.",
            "Problemi v Microsoft Teams 2026-01-16: dostava sporočil motena; status strani omenja spremembo usmerjanja prometa.",
            "انقطاع في Microsoft Teams بتاريخ 2026-01-16: تأخر تسليم الرسائل؛ صفحة الحالة تشير إلى تغيير في التوجيه (routing).",
        ],
        # CL_8 (2): Mumbai flooding / cloudburst, BMC alerts, Western Railway disruptions
        "CL_8": [
            "Mumbai hit by sudden cloudburst flooding on 2026-01-17; BMC issues alerts and Western Railway suspends some locals.",
            "Mumbai: poplave po nalivu 2026-01-17; BMC opozorila, Western Railway odpove del primestnih vlakov.",
        ],
        # CL_9 (3): Kyoto arson fire at Kiyomizu-dera auxiliary hall, Kyoto Police arrest
        "CL_9": [
            "Kyoto Police arrest arson suspect after fire at Kiyomizu-dera auxiliary hall on 2026-01-18; no casualties.",
            "Incendio doloso a Kyoto: Kiyomizu-dera (struttura ausiliaria) 2026-01-18; la polizia di Kyoto arresta un sospetto.",
            "京都: 清水寺(補助堂)で火災 2026-01-18。Kyoto Police が放火容疑で逮捕、負傷者なし。",
        ],
        # CL_10 (2): Reykjanes / Grindavík eruption, IMO aviation code raised
        "CL_10": [
            "Eruption near Reykjanes / Grindavík on 2026-01-19; Icelandic Met Office raises aviation color code, evacuations ordered.",
            "Volcanic activity in Iceland (Reykjanes, Grindavík) 2026-01-19; IMO updates aviation code; residents evacuated.",
        ],
    }

    # 25 noise items: must not match CL_1..CL_10.
    # Avoid key anchors used above: Tallinn, Helsinki, Gulf of Finland, Port of Tallinn, Darfur, UNSC, Resolution 9999,
    # Atlas Bitcoin ETF, ATBX, 34-12345, Paris Metro/RATP, São Paulo dengue 18,000, Artemis II/KSC, Teams routing,
    # Mumbai/BMC/Western Railway, Kyoto/Kiyomizu-dera/Kyoto Police, Reykjanes/Grindavík/Icelandic Met Office/aviation code.
    noise = [
        "New York City introduces stricter parking rules in Manhattan; fines increase from February.",
        "A new museum opens in Vienna featuring modern sculpture and interactive exhibits.",
        "Researchers publish findings on battery degradation in cold climates using lab simulations.",
        "A football club in Porto announces a coaching change after a string of losses.",
        "Severe fog delays flights at Amsterdam Schiphol; airlines advise passengers to check updates.",
        "Buenos Aires hosts an international jazz festival with artists from 12 countries.",
        "A court in Toronto hears arguments in a high-profile antitrust case involving retail pricing.",
        "Wildlife officials in Kenya report elephant migration shifts linked to changing rainfall.",
        "A startup unveils a compact desalination device designed for small coastal villages.",
        "Stock indexes in London fall slightly as energy shares decline in afternoon trading.",
        "A bridge renovation project in Prague will close lanes for three weeks starting Monday.",
        "Heavy rain triggers landslides in northern Colombia; rescue teams search for survivors.",
        "A new vaccination clinic opens in Dublin to improve access for rural communities.",
        "Sydney experiences record heat for the month; authorities open cooling centers.",
        "A university in Zurich launches a scholarship program for renewable energy students.",
        "A wildfire near Valencia forces temporary road closures; smoke impacts nearby towns.",
        "The price of cocoa rises amid supply concerns reported by commodity traders.",
        "A marine biologist documents unusual plankton blooms off the coast of Chile.",
        "A major retailer recalls a batch of kitchen appliances due to overheating risk.",
        "Tel Aviv hosts a technology conference focused on privacy-preserving AI systems.",
        "A train derailment in rural Argentina causes cargo delays; no injuries reported.",
        "A new art installation in Brussels uses projected light to transform city squares.",
        "Farmers in Iowa report improved yields after adopting precision irrigation sensors.",
        "An airline launches a new route between Lisbon and Casablanca starting spring.",
        "A scientific panel debates standards for measuring microplastics in drinking water.",
    ]

    # Sanity checks
    clustered_total = sum(len(v) for v in clustered.values())
    assert clustered_total == 25, f"Expected 25 clustered items, got {clustered_total}"
    assert len(clustered) == 10, f"Expected 10 clusters, got {len(clustered)}"
    assert len(noise) == 25, f"Expected 25 noise items, got {len(noise)}"

    return clustered, noise


@pytest.fixture(scope="session")
def nlp():
    if not SPACY_AVAILABLE:
        pytest.fail("spaCy is required for clustering tests but is not available")

    models_to_try = [
        os.getenv("SPACY_MODEL", "en_core_web_lg"),  # Large English model first
        "en_core_web_lg",  # Large English model
        "en_core_web_md",  # Medium English model
        "xx_ent_wiki_sm",  # Multilingual fallback
        "en_core_web_sm",  # Small English model last
    ]

    for model in models_to_try:
        try:
            nlp = spacy.load(model)
            print(f"✓ Loaded spaCy model: {model}")
            return nlp
        except Exception as e:
            print(f"Failed to load spaCy model '{model}': {e}")
            continue

    pytest.fail(
        "No suitable spaCy model found. "
        f"Tried: {', '.join(models_to_try)}. "
        "Install one, e.g.: python3 -m spacy download en_core_web_sm"
    )


@pytest.fixture()
def matcher(nlp):
    now = _now()

    idx = ClusterIndex(nlp)
    idx.refresh_from_data(_mk_cluster_data(now))

    m = ClusterMatcher(nlp, idx)

    return m


def _assign_one(matcher: ClusterMatcher, text: str, i: int) -> Optional[str]:
    now = _now()
    item = Item(
        item_type="normalized",
        item_id=f"it_{i}",
        text=text,
        created_at=now,
        url=None,
    )
    cid, score, why = matcher.assign(item)
    return cid


def _assign_all(matcher: ClusterMatcher, texts: List[str], start_i: int = 0) -> List[Optional[str]]:
    out: List[Optional[str]] = []
    for j, t in enumerate(texts):
        out.append(_assign_one(matcher, t, start_i + j))
    return out


def test_each_cluster_collapses_hard_multilingual(matcher: ClusterMatcher):
    clustered, _ = _texts()

    for cid, texts in clustered.items():
        got = _assign_all(matcher, texts, start_i=0)
        assert all(x == cid for x in got), f"{cid} misassigned: {list(zip(texts, got))}"


def test_noise_does_not_match_any_cluster(matcher: ClusterMatcher):
    _, noise = _texts()
    got = _assign_all(matcher, noise, start_i=10_000)
    assert all(x is None for x in got), f"Noise incorrectly matched: {list(zip(noise, got))}"


def test_overall_counts_50(matcher: ClusterMatcher):
    clustered, noise = _texts()

    all_texts: List[str] = []
    expected: List[Optional[str]] = []

    for cid, texts in clustered.items():
        all_texts.extend(texts)
        expected.extend([cid] * len(texts))

    all_texts.extend(noise)
    expected.extend([None] * len(noise))

    got = _assign_all(matcher, all_texts, start_i=50_000)

    # Exact count assertions
    for cid, n in CLUSTERS:
        assert got.count(cid) == n, f"{cid} expected {n}, got {got.count(cid)}"
    assert got.count(None) == 25, f"Expected 25 noise (None), got {got.count(None)}"

    # Positional checks for easier debugging (optional but useful)
    for i, (exp, actual) in enumerate(zip(expected, got)):
        assert exp == actual, f"Index {i}: expected {exp}, got {actual} :: {all_texts[i]}"