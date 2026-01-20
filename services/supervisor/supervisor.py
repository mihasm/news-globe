"""
Supervisor Service - Orchestrates connector execution and ingestion pipeline

Responsibilities:
1. Schedule and run connectors based on their intervals
2. Handle connector state persistence and error recovery
3. Route records from connectors to ingestion service
4. Provide monitoring and observability
5. Implement backoff and rate limiting
"""

import asyncio
import logging
import time
import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

import requests
from connectors import CONNECTORS
from shared.models.models import IngestionRecord

logger = logging.getLogger(__name__)


@dataclass
class ConnectorSchedule:
    """Configuration for a connector's execution schedule."""
    name: str
    interval_seconds: int
    enabled: bool = True
    config: Dict[str, Any] = None

    def __post_init__(self):
        if self.config is None:
            self.config = {}


@dataclass
class SupervisorStats:
    """Statistics for supervisor operation."""
    start_time: datetime
    connectors_scheduled: int = 0
    connectors_completed: int = 0
    records_processed: int = 0
    errors: int = 0
    last_heartbeat: Optional[datetime] = None


class SupervisorService:
    """
    Supervisor service that orchestrates the entire events ingestion pipeline.

    Manages connector execution, state persistence, and feeds records to ingestion.
    """

    def __init__(self, config_path: str = "supervisor_config.json"):
        self.config_path = config_path
        self.memory_store_url = os.getenv('MEMORY_STORE_URL', 'http://memory-store:6379')
        self.ingestion_service = None  # Placeholder for ingestion service reference

        # Default connector schedules
        self.connector_schedules = {
            'gdelt': ConnectorSchedule(
                name='gdelt',
                interval_seconds=300,  # 5 minutes
                config={
                    'query': '(protest OR riot OR earthquake OR flood OR cyclone OR breaking news OR news OR battle)',
                    'max_records': 50
                }
            ),
            'telegram': ConnectorSchedule(
                name='telegram',
                interval_seconds=60,  # 1 minute
                config={
                    'channels': [],  # Will be auto-discovered
                    'auto_discover': True,
                    'max_channels': 50
                }
            ),
            'mastodon': ConnectorSchedule(
                name='mastodon',
                interval_seconds=300,  # 5 minutes
                config={
                    'hashtags': ['news', 'breaking', 'earthquake', 'protest']
                }
            ),
            'rss': ConnectorSchedule(
                name='rss',
                interval_seconds=300,  # 5 minutes
                config={
                    'feeds_file': 'connectors/rss/rss_feeds.json',
                    'max_workers': 8,
                    'request_delay': 1.0
                }
            )
        }

        # Runtime state
        self.stats = SupervisorStats(start_time=datetime.now())
        self.connector_states = {}  # name -> state dict
        self.running = False
        self.tasks = {}  # name -> asyncio.Task

        # Load configuration and state
        self._load_config()
        self._load_state()

    def send_records_to_memory_store(self, records: List[IngestionRecord]) -> bool:
        """
        Send records to the memory store for ingestion service to consume.

        Args:
            records: List of IngestionRecord objects to send

        Returns:
            True if successful, False otherwise
        """
        if not records:
            return True

        try:
            # Convert records to dicts for JSON serialization
            records_data = [record.to_dict() for record in records]

            payload = {"key": "raw_items", "value": records_data}

            response = requests.post(
                f"{self.memory_store_url}/post",
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                logger.info(f"Successfully sent {len(records)} records to memory store: {result.get('queue_size', 0)} items in queue")
                return True
            else:
                logger.error(f"Failed to send records to memory store: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error sending records to memory store: {e}")
            return False

    def _load_config(self) -> None:
        """Load supervisor configuration."""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)

                # Update schedules from config
                for name, schedule_config in config.get('schedules', {}).items():
                    if name in self.connector_schedules:
                        schedule = self.connector_schedules[name]
                        schedule.interval_seconds = schedule_config.get('interval_seconds', schedule.interval_seconds)
                        schedule.enabled = schedule_config.get('enabled', schedule.enabled)
                        schedule.config.update(schedule_config.get('config', {}))

                logger.info(f"Loaded supervisor config from {self.config_path}")

        except Exception as e:
            logger.warning(f"Could not load supervisor config: {e}")

    def _save_config(self) -> None:
        """Save supervisor configuration."""
        try:
            config = {
                'schedules': {
                    name: {
                        'interval_seconds': schedule.interval_seconds,
                        'enabled': schedule.enabled,
                        'config': schedule.config
                    }
                    for name, schedule in self.connector_schedules.items()
                }
            }

            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=2)

        except Exception as e:
            logger.error(f"Could not save supervisor config: {e}")

    def _load_state(self) -> None:
        """Load connector states."""
        try:
            state_file = "supervisor_state.json"
            if os.path.exists(state_file):
                with open(state_file, 'r') as f:
                    self.connector_states = json.load(f)
                logger.info(f"Loaded supervisor state from {state_file}")
        except Exception as e:
            logger.warning(f"Could not load supervisor state: {e}")

    def _save_state(self) -> None:
        """Save connector states."""
        try:
            state_file = "supervisor_state.json"
            with open(state_file, 'w') as f:
                json.dump(self.connector_states, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save supervisor state: {e}")

    async def start(self) -> None:
        """Start the supervisor service."""
        logger.info("Starting Supervisor Service")
        self.running = True

        try:
            # Start all enabled connectors
            await self._start_all_connectors()

            # Main supervision loop
            while self.running:
                try:
                    await self._supervision_cycle()
                    await asyncio.sleep(10)  # Check every 10 seconds
                except Exception as e:
                    logger.error(f"Error in supervision cycle: {e}")
                    await asyncio.sleep(30)  # Wait before retry

        except Exception as e:
            logger.error(f"Supervisor failed: {e}")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the supervisor service."""
        logger.info("Stopping Supervisor Service")
        self.running = False

        # Cancel all connector tasks
        if hasattr(self, 'tasks') and self.tasks:
            for name, task in self.tasks.items():
                if not task.done():
                    task.cancel()

            # Wait for tasks to complete
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)

        # Save final state
        self._save_state()

        # Log final stats
        logger.info(f"Supervisor stopped. Final stats: {asdict(self.stats)}")

    async def _start_all_connectors(self) -> None:
        """Start all enabled connectors."""
        for name, schedule in self.connector_schedules.items():
            if schedule.enabled:
                await self._start_connector(name, schedule)

    async def _start_connector(self, name: str, schedule: ConnectorSchedule) -> None:
        """Start a single connector."""
        if hasattr(self, 'tasks') and name in self.tasks and not self.tasks[name].done():
            return  # Already running

        # Create connector instance
        try:
            connector_class = CONNECTORS.get(name)
            if not connector_class:
                logger.error(f"No connector class found for {name}")
                return

            logger.info(f"Starting connector {name} with config: {schedule.config}")
            connector = connector_class(schedule.config)
            self.tasks[name] = asyncio.create_task(
                self._run_connector_loop(name, connector, schedule)
            )
            logger.info(f"Started connector {name}")

        except Exception as e:
            logger.error(f"Failed to start connector {name}: {e}")

    async def _run_connector_loop(self, name: str, connector, schedule: ConnectorSchedule) -> None:
        """Run connector in a loop with scheduling."""
        logger.info(f"Starting connector loop for {name}")

        while self.running:
            try:
                start_time = time.time()

                # Run connector fetch
                records = list(connector.fetch())

                # Send records to memory store
                if records:
                    success = self.send_records_to_memory_store(records)
                    if not success:
                        logger.error(f"Failed to send {len(records)} records from {name} to memory store")

                # Update stats
                self.stats.connectors_completed += 1
                self.stats.records_processed += len(records)

                # Log success
                duration = time.time() - start_time
                logger.info(f"Connector {name} completed: {len(records)} records in {duration:.1f}s")

                # Wait for next interval
                await asyncio.sleep(schedule.interval_seconds)

            except Exception as e:
                logger.error(f"Error in connector {name}: {e}")
                self.stats.errors += 1

                # Wait before retry (with backoff)
                await asyncio.sleep(min(schedule.interval_seconds, 300))  # Max 5 minutes

    async def _supervision_cycle(self) -> None:
        """Perform supervision tasks."""
        # Update heartbeat
        self.stats.last_heartbeat = datetime.now()

        # Check for dead tasks and restart them
        for name, schedule in self.connector_schedules.items():
            if schedule.enabled and (not hasattr(self, 'tasks') or name not in self.tasks or self.tasks[name].done()):
                logger.warning(f"Connector {name} task is dead, restarting")
                await self._start_connector(name, schedule)

        # Save state periodically
        self._save_state()

        # Log stats periodically
        if self.stats.connectors_completed % 10 == 0:
            logger.info(f"Supervisor stats: {asdict(self.stats)}")

    def get_status(self) -> Dict[str, Any]:
        """Get supervisor status."""
        return {
            'running': self.running,
            'stats': asdict(self.stats),
            'connectors': {
                name: {
                    'enabled': schedule.enabled,
                    'interval': schedule.interval_seconds,
                    'running': hasattr(self, 'tasks') and name in self.tasks and not self.tasks[name].done(),
                    'state': self.connector_states.get(name, {})
                }
                for name, schedule in self.connector_schedules.items()
            },
            'ingestion_queue_size': self.ingestion_service.get_queue_size() if self.ingestion_service else 0
        }

    def enable_connector(self, name: str, enabled: bool = True) -> bool:
        """Enable or disable a connector."""
        if name not in self.connector_schedules:
            return False

        self.connector_schedules[name].enabled = enabled
        self._save_config()

        if enabled:
            # Start connector if supervisor is running
            if self.running:
                asyncio.create_task(self._start_connector(name, self.connector_schedules[name]))
        else:
            # Stop connector if running
            if hasattr(self, 'tasks') and name in self.tasks and not self.tasks[name].done():
                self.tasks[name].cancel()

        return True

    def update_connector_config(self, name: str, config: Dict[str, Any]) -> bool:
        """Update configuration for a connector."""
        if name not in self.connector_schedules:
            return False

        self.connector_schedules[name].config.update(config)
        self._save_config()
        return True


# Global instance
_supervisor_instance = None

def get_supervisor() -> SupervisorService:
    """Get the global supervisor instance."""
    global _supervisor_instance
    if _supervisor_instance is None:
        _supervisor_instance = SupervisorService()
    return _supervisor_instance

async def start_supervisor() -> None:
    """Start the supervisor service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    supervisor = get_supervisor()
    await supervisor.start()

if __name__ == "__main__":
    asyncio.run(start_supervisor())