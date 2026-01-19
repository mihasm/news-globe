import asyncio
from multiprocessing import Process
import logging
logging.getLogger('aiohttp').setLevel(logging.CRITICAL)
import os
import sys
import webbrowser

from app import data_path

# Import services with proper handling for hyphenated directory names
_services_dir = os.path.join(os.path.dirname(__file__), 'services')

# Import from memory-store directory
sys.path.insert(0, os.path.join(_services_dir, 'memory-store'))
try:
    from server import MemoryServer
finally:
    sys.path.pop(0)

from services.proxy.server import ProxyServer
from shared.utils.spacy_setup import ensure_spacy_models

logging.basicConfig(level=logging.INFO)

def run_ingestion_service(stream_batch_size: int = 10, poll_interval: float = 1.0):
    """Wrapper function to create and run IngestionService inside the subprocess."""
    from services.ingestion.service import IngestionService
    service = IngestionService(stream_batch_size=stream_batch_size, poll_interval=poll_interval)
    service.autostart()


async def main():
    # Ensure spaCy models are installed before starting services
    logger = logging.getLogger(__name__)
    logger.info("Initializing application...")
    
    if not ensure_spacy_models():
        logger.error("Failed to ensure spaCy models are installed. Exiting.")
        return
    
    m = MemoryServer()
    await m.start_server()
    proxy_server = ProxyServer()
    await proxy_server.start_server()

    webbrowser.open("http://localhost")

    # Start Ingestion Service process FIRST (handles NLP/geocoding)
    # Processes in small batches of 10 for fast streaming to frontend
    ingestion_process = Process(target=run_ingestion_service, kwargs={'stream_batch_size': 10, 'poll_interval': 0.5})
    ingestion_process.start()

    while True:
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())