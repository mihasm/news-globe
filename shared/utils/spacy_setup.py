"""
Automatic spaCy model download utility.
Checks for required models and downloads them if missing.
"""
import subprocess
import sys
import logging

logger = logging.getLogger(__name__)

# Preferred models in order of preference
PREFERRED_MODELS = [
    "en_core_web_trf",  # Transformer model (best accuracy, larger)
    "en_core_web_md",   # Medium model (good balance)
    "en_core_web_sm",   # Small model (fastest, smallest)
]


def check_model_installed(model_name: str) -> bool:
    """
    Check if a spaCy model is installed.
    
    Args:
        model_name: Name of the model to check
        
    Returns:
        True if model is installed, False otherwise
    """
    try:
        import spacy
        nlp = spacy.load(model_name)
        logger.debug(f"Model {model_name} is already installed")
        return True
    except OSError:
        logger.debug(f"Model {model_name} is not installed")
        return False
    except Exception as e:
        logger.warning(f"Error checking model {model_name}: {e}")
        return False


def download_model(model_name: str) -> bool:
    """
    Download a spaCy model using python -m spacy download.
    
    Args:
        model_name: Name of the model to download
        
    Returns:
        True if download succeeded, False otherwise
    """
    logger.info(f"Downloading spaCy model: {model_name}...")
    logger.info("This may take a few minutes depending on your internet connection.")
    
    try:
        # Use subprocess to run: python -m spacy download <model_name>
        result = subprocess.run(
            [sys.executable, "-m", "spacy", "download", model_name],
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully downloaded {model_name}")
            return True
        else:
            logger.error(f"Failed to download {model_name}: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"Download of {model_name} timed out after 10 minutes")
        return False
    except Exception as e:
        logger.error(f"Error downloading {model_name}: {e}")
        return False


def ensure_spacy_models() -> bool:
    """
    Ensure at least one spaCy model is available.
    Tries preferred models in order and downloads the first available or first one if none are installed.
    
    Returns:
        True if at least one model is available, False otherwise
    """
    logger.info("Checking for spaCy models...")
    
    # Check if any preferred model is already installed
    for model in PREFERRED_MODELS:
        if check_model_installed(model):
            logger.info(f"Using spaCy model: {model}")
            return True
    
    # No models installed, download the preferred one
    logger.info("No spaCy models found. Downloading preferred model...")
    preferred_model = PREFERRED_MODELS[0]  # en_core_web_trf
    
    if download_model(preferred_model):
        logger.info(f"Successfully installed {preferred_model}")
        return True
    
    # If preferred model download failed, try fallback models
    logger.warning(f"Failed to download {preferred_model}, trying fallback models...")
    for model in PREFERRED_MODELS[1:]:
        if download_model(model):
            logger.info(f"Successfully installed fallback model: {model}")
            return True
    
    logger.error("Failed to install any spaCy model. Please install manually:")
    logger.error(f"  python -m spacy download en_core_web_trf")
    logger.error("Or:")
    logger.error(f"  python -m spacy download en_core_web_md")
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ensure_spacy_models()
