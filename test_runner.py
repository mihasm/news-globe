#!/usr/bin/env python3.11
"""
Test Runner for News Globe Services

Runs tests offline without requiring Docker services.
Verifies all dependencies are properly installed before running tests.
"""

import os
import sys
import subprocess
from pathlib import Path

def check_python_version():
    """Check if we're using Python 3.11."""
    version = sys.version_info
    if version.major == 3 and version.minor == 11:
        print(f"✓ Using Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"✗ Using Python {version.major}.{version.minor}.{version.micro}, need Python 3.11")
        return False

def verify_dependencies():
    """Verify all required dependencies are installed and working."""
    print("Verifying dependencies...")

    # Check pytest
    try:
        import pytest
        print("✓ pytest is available")
    except ImportError:
        print("✗ pytest is not installed")
        return False

    # Check rapidfuzz
    try:
        import rapidfuzz
        print("✓ rapidfuzz is available")
    except ImportError:
        print("✗ rapidfuzz is not installed")
        return False

    # Check spaCy and test it
    try:
        import spacy
        # Test basic functionality
        from spacy.lang.en import English
        nlp = English()
        doc = nlp("test")
        print("✓ spaCy is working")
    except Exception as e:
        print(f"✗ spaCy is not working: {e}")
        return False

    # Check if spaCy models are available
    models_to_check = ["en_core_web_sm", "xx_ent_wiki_sm"]
    available_models = []

    for model in models_to_check:
        try:
            spacy.load(model)
            available_models.append(model)
        except:
            pass

    if not available_models:
        print("✗ No spaCy models available. Install at least one: python -m spacy download en_core_web_sm")
        return False

    print(f"✓ Available spaCy models: {', '.join(available_models)}")
    return True

def ensure_spacy_models():
    """Ensure required spaCy models are available."""
    try:
        import spacy
        # Try to load the multilingual model used in tests
        try:
            spacy.load("xx_ent_wiki_sm")
            print("✓ xx_ent_wiki_sm model is available")
        except OSError:
            print("Installing xx_ent_wiki_sm model...")
            subprocess.run([
                sys.executable, "-m", "spacy", "download", "xx_ent_wiki_sm"
            ], check=True)
            print("✓ xx_ent_wiki_sm model installed")
    except ImportError:
        print("Warning: spaCy not available for model check")

# Removed host-based test functions - now using Docker container

def run_tests():
    """Run the test suite offline."""
    # Ensure we're in the project root
    project_root = Path(__file__).parent
    os.chdir(project_root)

    # Verify Python version
    if not check_python_version():
        print("Please run tests with Python 3.11")
        return False

    # Verify all dependencies
    if not verify_dependencies():
        print("Dependencies verification failed. Please install missing dependencies.")
        return False

    # Set Python path for imports
    env = os.environ.copy()
    env['PYTHONPATH'] = f"{project_root}/services/clustering:{project_root}/shared:{env.get('PYTHONPATH', '')}"

    # Set default spaCy model - prefer multilingual for better NER
    env['SPACY_MODEL'] = env.get('SPACY_MODEL', 'xx_ent_wiki_sm')

    # Run pytest
    cmd = [
        sys.executable, "-m", "pytest",
        "--tb=short",
        "--verbose",
        "services/clustering/tests/"
    ]

    print("Running tests offline...")
    print(f"Command: {' '.join(cmd)}")
    print(f"PYTHONPATH: {env['PYTHONPATH']}")
    print(f"SPACY_MODEL: {env['SPACY_MODEL']}")

    result = subprocess.run(cmd, env=env)
    return result.returncode == 0

def main():
    """Main entry point."""
    print("News Globe Test Runner (Offline)")
    print("=" * 40)

    # Run tests
    if run_tests():
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()