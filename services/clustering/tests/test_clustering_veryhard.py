# test_ner_signature_clustering.py
#
# Dataset-driven clustering test for your NER/signature matcher.
#
# Dataset (DO NOT CHANGE):
# - 20 original items (cluster_id: 0..19) act as representatives.
# - synthetic_headlines add more items per cluster (same 0..19).
#
# What we assert:
# 1) Index builds exactly 20 clusters (0..19).
# 2) Every dataset text is assigned to the correct cluster.
# 3) No item is assigned None.
# 4) Cluster membership counts match exactly.
#
# IMPORTANT:
# - Update the import path for ClusterIndex/ClusterMatcher/Item below to your real module.

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

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


# ----------------------------
# Dataset (DO NOT CHANGE)
# ----------------------------
items = [
    ('È uscita "Digitalia #808 - Il bikini di vetro", in cui parliamo dell\'autopilota Tesla solo in abbonamento, di autocarri autonomi cinesi che sono diven...', 0),
    ("Seit mehr als einer Woche steht Matin Afzalnia täglich vor dem Kölner Dom und protestiert gegen das Regime in ihrer Heimat Iran.", 1),
    ("Digitale Unterdrückung: So schalten Staaten das #Internet aus", 2),
    ("— 🇺🇸 / 🇮🇷 NEW: The U.S. State Department-funded think tank 'Institute for the Study of War' (ISW) has recorded zero new protests in Iran from Janu...", 3),
    ("🇺🇸 🇮🇷 Trump doesn't run US foreign policy. He simply sells it. No one in his administration does either.", 4),
    ("🇺🇸 🇮🇷 Recent​ large-scale violence in Iran was likely planned as early as 2022 under the Biden administration when SpaceX and Elon Musk were appro...", 5),
    ("MY VLOG: Will Trump accept Iran's offer to talks on sanctions strike | Movement being hijacked?", 6),
    ("🎥 5 Videos from Iran that American Media will NEVER Show You. I Need You to See the Truth, OK?", 7),
    ("Während im Iran seit Wochen protestiert wird, antwortet das Regime mit tödlicher Gewalt und einem Internet-Blackout.", 8),
    ("Soon: An official statement from Iran's Atomic Energy Agency", 9),
    ("Western Media Plays Role to Sell Violent Terrorism Across Iran", 10),
    ("— 🇺🇸 / 🇮🇷 The White House claims that 800 executions were scheduled to take place in Iran yesterday, but they were canceled because of pressure from Trump", 11),
    ("— 🇮🇷 NEW: More and more footage is being released from the recent riots in Iran, showing the situation was far worse than just riots", 12),
    ("— 🇮🇷 NEW: Iran executed at least 5 people on January 14th – HRANA", 13),
    ("Three years ago, UFC fighter Beneil Dariush dedicated his fight to the people of Iran who were fighting for freedom.", 14),
    ("Iran Protest Death Toll Nears 4,000, Human Rights Group Says", 15),
    ("Hackers disrupt Iran state TV to broadcast pro-monarch, anti-crackdown message", 16),
    ("🇮🇷 🇮🇷 ❌ 🤔 President of the European Commission: We have proposed a ban on additional exports to Iran of sensitive technologies related to drones", 17),
    ("⚡ Qatari Prime Minister: Escalation with Iran will have consequences for the entire region, says he believes President Trump wants a deal", 18),
    ("BREAKING: Iran’s president warns that any attack on the Supreme Leader would be treated as an act of “all-out war.”", 19),
]

synthetic_headlines = [
    ("Digitalia: puntata su Tesla Autopilot in abbonamento e camion autonomi cinesi", 0),
    ("Tesla autopilot features move behind subscription paywall, sparking backlash", 0),
    ("Camions autonomes chinois: essor rapide des convois sans conducteur", 0),
    ("Protest am Kölner Dom: Aktivistin fordert ein Ende der Repression im Iran", 1),
    ("Cologne Cathedral vigil: daily protest highlights Iran crackdown", 1),
    ("Presidio davanti al Duomo di Colonia: protesta contro il regime iraniano", 1),
    ("Digitale Unterdrückung: Warum Regierungen das Internet abschalten", 2),
    ("How governments shut down the internet to crush dissent", 2),
    ("Répression numérique: les coupures d’Internet comme arme politique", 2),
    ("ISW: Nessuna nuova protesta registrata in Iran nel periodo più recente", 3),
    ("ISW report says it logged zero new Iran protests over the latest interval", 3),
    ("Commentary: US foreign policy criticized as transactional under Trump era politics", 4),
    ("Kritik: Außenpolitik als Geschäft—Vorwürfe gegen Trumps Umfeld", 4),
    ("Allegations surface claiming Iran unrest was planned as early as 2022; Musk/SpaceX mentioned", 5),
    ("Contro-narrazione: accuse di pianificazione delle violenze in Iran dal 2022", 5),
    ("Behauptung: Großgewalt im Iran sei seit 2022 vorbereitet gewesen, Musk wird genannt", 5),
    ("Vlog: Will Trump accept Iran talks offer on sanctions—movement ‘being hijacked’?", 6),
    ("Videoanalisi: trattative sulle sanzioni e timori di cooptazione del movimento iraniano", 6),
    ("Viral video thread claims US media ignores key footage from Iran unrest", 7),
    ("Série de vidéos: l’auteur affirme que les médias américains passent sous silence l’Iran", 7),
    ("Iran: Proteste e repressione—nuovo blackout di Internet dopo scontri mortali", 8),
    ("Iran protests met with lethal force as internet blackout expands, witnesses say", 8),
    ("Iran: Regime reagiert mit Gewalt und Internet-Blackout auf Proteste", 8),
    ("Iran’s Atomic Energy Organization to issue official statement soon", 9),
    ("Prossima dichiarazione ufficiale dell’Agenzia iraniana per l’energia atomica", 9),
    ("Narrative war: outlet claims Western media fuels violence narrative inside Iran", 10),
    ("Propaganda-Vorwurf: Westliche Medien würden Gewalt in Iran „verkaufen“", 10),
    ("White House claim: hundreds of Iran executions were planned but allegedly halted after pressure", 11),
    ("Casa Bianca: “800 esecuzioni previste in Iran” poi annullate—affermazione contestata online", 11),
    ("Behauptung aus Washington: Geplante Massenhinrichtungen im Iran angeblich gestoppt", 11),
    ("New footage from Iran unrest spreads online, suggesting broader violence than first reported", 12),
    ("Nuove immagini delle rivolte in Iran: la situazione “peggiore del previsto”, dicono post virali", 12),
    ("HRANA: Iran executed at least five people on January 14, report says", 13),
    ("HRANA riferisce: almeno cinque esecuzioni in Iran il 14 gennaio", 13),
    ("Flashback: Beneil Dariush dedicated UFC win to Iranians fighting for freedom", 14),
    ("Rückblick: UFC-Kämpfer Beneil Dariush widmete Sieg den Menschen im Iran", 14),
    ("Human rights group claims Iran protest death toll approaching 4,000", 15),
    ("ONG: il bilancio delle vittime delle proteste in Iran “verso 4.000”", 15),
    ("Menschenrechtsgruppe: Opferzahl bei Iran-Protesten nähere sich 4.000", 15),
    ("Hackers interrupt Iran state TV broadcast with pro-monarchy, anti-crackdown message", 16),
    ("Pirates informatiques: interruption de la TV d’État iranienne avec message pro-monarchie", 16),
    ("Hacker stören iranisches Staatsfernsehen mit pro-monarchischer Botschaft", 16),
    ("EU Commission proposes ban on additional exports of sensitive drone-related tech to Iran", 17),
    ("Von der Leyen/Commission: neues Exportverbot für drohnenrelevante Technologien nach Iran", 17),
    ("UE: proposta di divieto su ulteriori export di tecnologie sensibili legate ai droni verso l’Iran", 17),
    ("Qatar PM warns escalation with Iran would hit entire region; says Trump seeks a deal", 18),
    ("Premier qatariota: escalation con l’Iran avrebbe conseguenze regionali, “Trump vuole un accordo”", 18),
    ("Katars Premier: Eskalation mit Iran hätte Folgen für die ganze Region—Trump wolle Deal", 18),
    ("Iranian president warns attack on Supreme Leader would mean ‘all-out war’", 19),
    ("Il presidente iraniano: colpire la Guida Suprema sarebbe ‘guerra totale’", 19),
    ("Irans Präsident warnt: Angriff auf Obersten Führer würde „totalen Krieg“ auslösen", 19),
]


def _now() -> datetime:
    return datetime.now(UTC)


def _cid(n: int) -> str:
    # Cluster IDs in your index are strings; keep stable + explicit.
    return f"CL_{n}"


def _mk_cluster_data(now: datetime) -> List[Tuple[str, str, Optional[datetime]]]:
    # Use the original items as representatives (one per cluster 0..19).
    # (cluster_id_str, representative_text, created_at)
    return [(_cid(n), text, now) for (text, n) in items]


def _dataset_expected() -> List[Tuple[str, str]]:
    # Returns a flat list of (text, expected_cluster_id_str) for ALL dataset texts.
    out: List[Tuple[str, str]] = []
    for text, n in items:
        out.append((text, _cid(n)))
    for text, n in synthetic_headlines:
        out.append((text, _cid(n)))
    return out


def _expected_counts() -> Dict[str, int]:
    # Exact expected membership counts per cluster.
    counts: Dict[str, int] = {}
    for _, n in items:
        counts[_cid(n)] = counts.get(_cid(n), 0) + 1
    for _, n in synthetic_headlines:
        counts[_cid(n)] = counts.get(_cid(n), 0) + 1
    return counts


def _index_cluster_ids(idx: ClusterIndex) -> List[str]:
    """
    Best-effort introspection to assert "correct number of clusters built"
    without assuming your internal field names.

    Tries common patterns:
      - idx.clusters (dict keyed by cluster_id)
      - idx.by_cluster_id / idx.by_id (dict)
      - idx.cluster_ids() / idx.get_cluster_ids()
    """
    # Methods first
    for meth in ("cluster_ids", "get_cluster_ids", "all_cluster_ids"):
        fn = getattr(idx, meth, None)
        if callable(fn):
            try:
                ids = list(fn())
                if ids:
                    return ids
            except Exception:
                pass

    # Dict-like attributes
    for attr in ("clusters", "by_cluster_id", "by_id", "cluster_map", "cluster_by_id"):
        v = getattr(idx, attr, None)
        if isinstance(v, dict) and v:
            return list(v.keys())

    # Fallback: cannot introspect reliably
    return []


@pytest.fixture(scope="session")
def nlp():
    if not SPACY_AVAILABLE:
        pytest.fail("spaCy is required for clustering tests but is not available")

    # For this very hard test, prefer models that can handle multilingual content better
    models_to_try = [
        os.getenv("SPACY_MODEL", "en_core_web_sm"),  # Prefer English model that works better
        "en_core_web_sm",  # English model with good NER
        "en_core_web_md",
        "xx_ent_wiki_sm",  # Fallback to multilingual
        "en_core_web_lg",
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
        "Install one, e.g.: python3.11 -m spacy download en_core_web_sm"
    )


@pytest.fixture()
def matcher(nlp):
    now = _now()

    idx = ClusterIndex(nlp)
    idx.refresh_from_data(_mk_cluster_data(now))

    m = ClusterMatcher(nlp, idx)

    return m


def _assign(matcher: ClusterMatcher, text: str, i: int) -> Tuple[Optional[str], float, str]:
    now = _now()
    item = Item(
        item_type="normalized",
        item_id=f"it_{i}",
        text=text,
        created_at=now,
        url=None,
    )
    cid, score, why = matcher.assign(item)
    return cid, score, why


def test_index_builds_20_clusters(matcher: ClusterMatcher):
    # Access the index off matcher if your matcher exposes it; otherwise introspect via matcher.idx / matcher.index.
    idx = getattr(matcher, "idx", None) or getattr(matcher, "index", None) or getattr(matcher, "cluster_index", None)
    if idx is None:
        # If your matcher does not expose the index, we can still assert via assignments (below),
        # but this test requires index visibility. Make it explicit so it fails loudly.
        pytest.fail("ClusterMatcher does not expose its ClusterIndex (expected attribute: idx/index/cluster_index).")

    ids = _index_cluster_ids(idx)
    if not ids:
        pytest.fail(
            "Could not introspect ClusterIndex cluster IDs. "
            "Expose a cluster id list (method cluster_ids/get_cluster_ids) or a dict attribute (clusters/by_id)."
        )

    assert len(ids) == 20, f"Expected 20 clusters, got {len(ids)}: {sorted(ids)}"
    assert set(ids) == {_cid(n) for n in range(20)}, f"Cluster IDs mismatch: {sorted(ids)}"


def test_all_items_assign_to_correct_cluster(matcher: ClusterMatcher):
    expected = _dataset_expected()
    mis: List[str] = []

    for i, (text, exp_cid) in enumerate(expected):
        cid, score, why = _assign(matcher, text, i)
        if cid != exp_cid:
            mis.append(
                f"i={i} expected={exp_cid} got={cid} score={score} text={text!r} why={why}"
            )

    assert not mis, "Misassignments:\n" + "\n".join(mis)


def test_no_item_returns_none(matcher: ClusterMatcher):
    expected = _dataset_expected()
    bad: List[str] = []

    for i, (text, exp_cid) in enumerate(expected):
        cid, score, why = _assign(matcher, text, i)
        if cid is None:
            bad.append(f"i={i} expected={exp_cid} got=None score={score} text={text!r} why={why}")

    assert not bad, "Items returned None:\n" + "\n".join(bad)


def test_cluster_membership_counts_exact(matcher: ClusterMatcher):
    expected = _dataset_expected()
    want = _expected_counts()

    got: Dict[str, int] = {k: 0 for k in want.keys()}
    unexpected: List[str] = []

    for i, (text, exp_cid) in enumerate(expected):
        cid, score, why = _assign(matcher, text, i)
        if cid not in got:
            unexpected.append(f"i={i} expected={exp_cid} got={cid} score={score} text={text!r} why={why}")
        else:
            got[cid] += 1

    assert not unexpected, "Unexpected cluster IDs returned:\n" + "\n".join(unexpected)
    assert got == want, f"Cluster counts mismatch.\nExpected: {want}\nGot:      {got}"