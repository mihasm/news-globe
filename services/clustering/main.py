"""
Events Clustering Service Main - Processes normalized items into clusters

Runs periodic clustering of unassigned normalized items based on:
- Time proximity (15-minute windows)
- Geographic proximity (50km radius)
- Content similarity (lexical + semantic)
"""

import logging
import time
from datetime import datetime

from clustering_service import EventsClusteringService, ClusteringConfig

logger = logging.getLogger(__name__)


class EventsClusteringMain:
    """Main service for clustering events."""

    def __init__(self):
        self.config = ClusteringConfig()
        self.clustering_service = EventsClusteringService(self.config)
        self.running = False

        # Timing
        self.last_cluster = 0
        self.last_cleanup = 0
        self.last_stats = 0

    def start(self):
        """Start the clustering service."""
        logger.info("Starting Events Clustering Service")
        self.running = True

        try:
            while self.running:
                logger.info("Running clustering service loop")
                current_time = time.time()

                # Run clustering (process all unclustered items)
                self._run_clustering()
                self.last_cluster = current_time

                self._run_cleanup()
                self.last_cleanup = current_time

                self._log_stats()
                self.last_stats = current_time

                logger.info("Sleeping for 5 seconds")
                # Sleep
                time.sleep(5)  # Check every 5 seconds

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

    def stop(self):
        """Stop the clustering service."""
        logger.info("Stopping Events Clustering Service")
        self.running = False

    def _run_clustering(self):
        """Run clustering on all unclustered items."""
        try:
            logger.info("Starting clustering run")
            start_time = time.time()

            # Process all unclustered items
            stats = self.clustering_service.process_unassigned_items(batch_size=100)

            duration = time.time() - start_time
            logger.info(f"Clustering completed in {duration:.1f}s: {stats}")

        except Exception as e:
            logger.error(f"Error during clustering: {e}")

    def _run_cleanup(self):
        """Clean up old clusters."""
        try:
            logger.info("Starting cluster cleanup")
            removed = self.clustering_service.cleanup_old_clusters(max_age_days=7)
            logger.info(f"Removed {removed} old clusters")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def _log_stats(self):
        """Log current clustering statistics."""
        try:
            # Count current clusters and items
            from shared.models.database import database
            from shared.models.models import Cluster, NormalizedItem

            cluster_count = Cluster.select().count()
            unclustered_count = NormalizedItem.select().where(
                NormalizedItem.cluster_id.is_null()
            ).count()

            logger.info(f"Clustering stats: {cluster_count} clusters, {unclustered_count} unclustered items")

        except Exception as e:
            logger.error(f"Error logging stats: {e}")

    def trigger_clustering(self):
        """Manually trigger clustering and return stats."""
        self._run_clustering()
        return self.clustering_service.stats.copy()

    def get_status(self):
        """Get service status."""
        return {
            'running': self.running,
            'last_cluster': datetime.fromtimestamp(self.last_cluster).isoformat() if self.last_cluster else None,
            'last_cleanup': datetime.fromtimestamp(self.last_cleanup).isoformat() if self.last_cleanup else None,
            'config': self.config.__dict__,
        }


if __name__ == "__main__":
    import sys
    from shared.utils.spacy_setup import ensure_spacy_models

    # Ensure spaCy models are available
    if not ensure_spacy_models():
        logger.error("Failed to ensure spaCy models. Clustering requires spaCy.")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    service = EventsClusteringMain()
    service.start()