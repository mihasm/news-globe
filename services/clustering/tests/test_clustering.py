# test_ner_signature_clustering.py
#
# Synthetic dataset (25 texts):
# - Topic A (10 items): "Port of Koper container ship fire" (multilingual) -> MUST collapse into 1 cluster
# - Topic B (5 items): "ECB cuts rates" (multilingual) -> MUST collapse into 1 cluster
# - Topic C (3 items): "Tokyo earthquake" (multilingual) -> MUST collapse into 1 cluster
# - Noise (7 items): unrelated -> MUST NOT match A/B/C
#
# This test plugs into the NER/signature-based clustering code:
# - It builds an index with 3 clusters (A, B, C) using representative texts.
# - It runs matcher.assign() on all 25 items.
# - It asserts correct cluster assignment for A/B/C items and None for noise.
#
# Requirements:
# - spaCy installed.
# - A spaCy model with NER. Default tries SPACY_MODEL env var else "xx_ent_wiki_sm".
#   If your environment has a better multilingual NER model, set SPACY_MODEL accordingly.
#
# Run:
#   SPACY_MODEL=xx_ent_wiki_sm pytest -q
#
# IMPORTANT:
# - Update the import path for ClusterIndex/ClusterMatcher/Item below to your real module.

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pytest

# Import spaCy with error handling
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


def _mk_cluster_data(now: datetime) -> List[Tuple[str, str, Optional[datetime]]]:
    # Representative texts intentionally include strong anchors (place/org/date/domain/numbers)
    rep_a = (
        "Port of Koper: container ship caught fire near the terminal on 2026-01-18; "
        "firefighters contained the blaze; Luka Koper reported no fatalities."
    )
    rep_b = (
        "ECB cuts interest rates by 25 basis points on 2026-01-16; "
        "Christine Lagarde cites slowing eurozone inflation."
    )
    rep_c = (
        "Tokyo hit by magnitude 6.2 earthquake on 2026-01-17; "
        "Japan Meteorological Agency issued warnings; no tsunami confirmed."
    )
    return [
        ("CL_A", rep_a, now),
        ("CL_B", rep_b, now),
        ("CL_C", rep_c, now),
    ]


def _texts() -> Tuple[List[str], List[str], List[str], List[str]]:
    # Topic A: Port of Koper ship fire (10)
    topic_a = [
        # EN
        "Fire breaks out on a container ship at the Port of Koper on 2026-01-18; Luka Koper says no injuries.",
        # SL
        "V pristanišču Koper je 2026-01-18 zagorela kontejnerska ladja; Luka Koper sporoča, da ni poškodovanih.",
        # IT
        "Incendio su una nave portacontainer al porto di Capodistria (Koper) il 2026-01-18; nessun ferito.",
        # DE
        "Container-Schiff brennt im Hafen Koper am 2026-01-18; Betreiber Luka Koper meldet keine Verletzten.",
        # ES
        "Incendio en un buque portacontenedores en el puerto de Koper el 2026-01-18; sin heridos, afirma Luka Koper.",
        # HR
        "Požar na kontejnerskom brodu u luci Kopar 2026-01-18; Luka Koper navodi da nema ozlijeđenih.",
        # FR
        "Un incendie sur un porte-conteneurs au port de Koper le 2026-01-18; Luka Koper dit qu'il n'y a pas de blessés.",
        # EN (different phrasing)
        "Koper port container vessel blaze contained by firefighters; incident reported on 2026-01-18.",
        # SL (short)
        "Požar na ladji v Kopru (2026-01-18) – gasilci pogasili, brez žrtev.",
        # IT (short)
        "Koper/Capodistria: rogo su nave container (2026-01-18), intervento dei vigili del fuoco, nessuna vittima.",
    ]

    # Topic B: ECB cuts rates (5)
    topic_b = [
        "ECB cuts rates by 25 bps on 2026-01-16; Lagarde points to easing eurozone inflation.",
        "La BCE riduce i tassi di 25 punti base il 2026-01-16; Lagarde cita inflazione in calo nell'eurozona.",
        "ECB zniža obrestne mere za 25 bazičnih točk 2026-01-16; Lagarde omenja umirjanje inflacije.",
        "La BCE baja los tipos 25 puntos básicos el 2026-01-16; el euro reacciona tras el anuncio.",
        "EZB senkt Zinsen um 25 Basispunkte am 2026-01-16; Lagarde kommentiert den Inflationsrückgang.",
    ]

    # Topic C: Tokyo earthquake (3)
    topic_c = [
        "Magnitude 6.2 earthquake shakes Tokyo on 2026-01-17; JMA issues alerts; no tsunami confirmed.",
        "Potres magnitude 6,2 je stresel Tokio 2026-01-17; JMA izda opozorila; cunamija ni.",
        "Terremoto di magnitudo 6,2 a Tokyo il 2026-01-17; l'Agenzia meteorologica giapponese (JMA) emette avvisi.",
    ]

    # Noise/unrelated (7) - should not match any existing cluster A/B/C
    noise = [
        "Stock markets in New York close higher after tech earnings surprise analysts.",
        "Barcelona announces a new metro line extension; construction begins next month.",
        "Heavy snow closes mountain roads in Austria; motorists advised to use chains.",
        "WHO releases an updated vaccination guidance note for seasonal influenza.",
        "SpaceX launches a batch of satellites into low Earth orbit from Florida.",
        "A wildfire near Los Angeles forces evacuations; winds complicate firefighting efforts.",
        "New study suggests coffee consumption correlates with improved focus in adults.",
    ]

    assert len(topic_a) == 10
    assert len(topic_b) == 5
    assert len(topic_c) == 3
    assert len(noise) == 7
    return topic_a, topic_b, topic_c, noise


@pytest.fixture(scope="session")
def nlp():
    if not SPACY_AVAILABLE:
        pytest.fail("spaCy is required for clustering tests but is not available")

    # Try different models in order of preference - better multilingual models first
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

    # If no models work, fail with helpful message
    pytest.fail(
        f"No suitable spaCy model found. "
        f"Available models: {', '.join(models_to_try)}. "
        f"Install a spaCy model with NER support: "
        f"'python3.11 -m spacy download en_core_web_sm'"
    )


@pytest.fixture()
def matcher(nlp):
    now = _now()

    idx = ClusterIndex(nlp)
    idx.refresh_from_data(_mk_cluster_data(now))

    m = ClusterMatcher(nlp, idx)

    return m


def _assign_all(matcher: ClusterMatcher, texts: List[str]) -> List[Optional[str]]:
    now = _now()
    out: List[Optional[str]] = []
    for i, t in enumerate(texts):
        item = Item(
            item_type="normalized",
            item_id=f"it_{i}",
            text=t,
            created_at=now,
            url=None,
        )
        cid, score, why = matcher.assign(item)
        out.append(cid)
    return out


def test_topic_a_collapses_multilingual(matcher: ClusterMatcher):
    topic_a, _, _, _ = _texts()
    cids = _assign_all(matcher, topic_a)
    assert all(cid == "CL_A" for cid in cids), f"Topic A misassigned: {cids}"


def test_topic_b_collapses_multilingual(matcher: ClusterMatcher):
    _, topic_b, _, _ = _texts()
    cids = _assign_all(matcher, topic_b)
    assert all(cid == "CL_B" for cid in cids), f"Topic B misassigned: {cids}"


def test_topic_c_collapses_multilingual(matcher: ClusterMatcher):
    _, _, topic_c, _ = _texts()
    cids = _assign_all(matcher, topic_c)
    assert all(cid == "CL_C" for cid in cids), f"Topic C misassigned: {cids}"


def test_noise_does_not_match_any_cluster(matcher: ClusterMatcher):
    _, _, _, noise = _texts()
    cids = _assign_all(matcher, noise)
    assert all(cid is None for cid in cids), f"Noise incorrectly matched: {cids}"


def test_overall_counts(matcher: ClusterMatcher):
    topic_a, topic_b, topic_c, noise = _texts()
    all_texts = topic_a + topic_b + topic_c + noise
    cids = _assign_all(matcher, all_texts)

    assert cids.count("CL_A") == 10, cids
    assert cids.count("CL_B") == 5, cids
    assert cids.count("CL_C") == 3, cids
    assert cids.count(None) == 7, cids