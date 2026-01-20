# services/ingestion/entrypoint.py
import os
import sys
import subprocess

# spaCy model to install for multilingual NER (WikiNER)
SPACY_MODEL = os.getenv("SPACY_MODEL", "xx_ent_wiki_sm")


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


def main() -> None:
    # Make python prints flush immediately even if not a TTY
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    ensure_spacy_model()
    run(["python", "ingestion.py"])


if __name__ == "__main__":
    main()