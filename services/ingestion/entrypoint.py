# services/ingestion/entrypoint.py
import os
import sys
import shutil
import subprocess
from pathlib import Path

GEONAMES_DB = Path(os.getenv("GEONAMES_DB", "/app/shared/data/geonames.db"))
MIN_POP = os.getenv("GEONAMES_MIN_POP", "1000")

# spaCy model to install for multilingual NER (WikiNER)
SPACY_MODEL = os.getenv("SPACY_MODEL", "xx_ent_wiki_sm")

ALL_URL = "https://download.geonames.org/export/dump/allCountries.zip"
ALT_URL = "https://download.geonames.org/export/dump/alternateNamesV2.zip"


def run(cmd: list[str]) -> None:
    """Run a command and stream combined stdout/stderr live to Docker logs."""
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered
    )
    assert p.stdout is not None
    for line in p.stdout:
        print(line, end="", flush=True)
    rc = p.wait()
    if rc != 0:
        raise subprocess.CalledProcessError(rc, cmd)


def ensure_spacy_model() -> None:
    """
    Ensure the spaCy multilingual NER model is installed in the container.
    Uses `python -m spacy download ...` only if `spacy.load()` fails.
    """
    print(f"Checking spaCy model: {SPACY_MODEL}", flush=True)

    check_code = (
        "import spacy\n"
        f"spacy.load('{SPACY_MODEL}')\n"
        "print('OK')\n"
    )

    try:
        run(["python", "-c", check_code])
        print(f"spaCy model available: {SPACY_MODEL} (skip download)", flush=True)
        return
    except subprocess.CalledProcessError:
        pass

    print(f"Installing spaCy model: {SPACY_MODEL}", flush=True)
    run(["python", "-m", "spacy", "download", SPACY_MODEL])
    run(["python", "-c", check_code])
    print(f"spaCy model installed: {SPACY_MODEL}", flush=True)


def init_geonames() -> None:
    if GEONAMES_DB.exists():
        print(f"GeoNames DB exists: {GEONAMES_DB} (skip init)", flush=True)
        return

    print(f"Initializing GeoNames DB: {GEONAMES_DB}", flush=True)
    GEONAMES_DB.parent.mkdir(parents=True, exist_ok=True)

    tmp = Path("/tmp/geonames")
    if tmp.exists():
        shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)

    all_zip = tmp / "allCountries.zip"
    alt_zip = tmp / "alternateNamesV2.zip"

    print("Downloading GeoNames dumps...", flush=True)
    run(["curl", "--progress-bar", "-L", "--retry", "5", "--retry-delay", "2", "-o", str(all_zip), ALL_URL])
    run(["curl", "--progress-bar", "-L", "--retry", "5", "--retry-delay", "2", "-o", str(alt_zip), ALT_URL])

    print("Unzipping...", flush=True)
    run(["unzip", "-q", str(all_zip), "-d", str(tmp)])
    run(["unzip", "-q", str(alt_zip), "-d", str(tmp)])

    geonames_txt = tmp / "allCountries.txt"
    alts_txt = tmp / "alternateNamesV2.txt"
    if not geonames_txt.exists():
        raise RuntimeError("Missing allCountries.txt after unzip")
    if not alts_txt.exists():
        raise RuntimeError("Missing alternateNamesV2.txt after unzip")

    print(f"Building sqlite DB (min_population={MIN_POP})...", flush=True)
    run([
        "python", "/app/location.py", "build",
        "--geonames", str(geonames_txt),
        "--alts", str(alts_txt),
        "--db", str(GEONAMES_DB),
        "--min-population", str(MIN_POP),
    ])

    print("Cleaning temp files...", flush=True)
    shutil.rmtree(tmp, ignore_errors=True)
    print("GeoNames DB initialized.", flush=True)


def main() -> None:
    # Make python prints flush immediately even if not a TTY
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    ensure_spacy_model()
    init_geonames()
    run(["python", "ingestion.py"])


if __name__ == "__main__":
    main()