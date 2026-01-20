"""
Mastodon connector for public chatter and hashtag bursts.

Provides open microblog stream for early chatter and hashtag monitoring.
"""

import asyncio
import json
import time
import aiohttp
from typing import Iterator, Dict, Any, Optional, List
from datetime import datetime
from urllib.parse import urljoin

from ..base import BaseConnector
from shared.models.models import IngestionRecord


class MastodonConnector(BaseConnector):
    """
    Mastodon connector for public timeline monitoring.

    Monitors multiple Mastodon instances for public posts and hashtags.
    """

    DEFAULT_INSTANCES = [
        "https://mastodon.social",
        "https://fosstodon.org",
        "https://mastodon.world",
        "https://hachyderm.io",
        "https://mstdn.social",
        "https://infosec.exchange",
        "https://newsie.social",
        "https://mastodon.online",
        "https://mstdn.party",
        "https://mastodon.uno",
        "https://mastodon.cloud",
        "https://mastodon.art",
        "https://mastodon.scot",
        "https://mastodon.nz",
        "https://mastodon.ie",
        "https://mastodon.gamedev.place",
        "https://mastodon.green",
        "https://mastodon.sdf.org",
        "https://mastodon.opencloud.lu",
        "https://mastodon.me.uk",
        "https://mastodon.boston",
        "https://mastodon.tokyo",
        "https://mastodon.frl",
        "https://mastodon.indie.host",
        "https://mastodon.coffee",
        "https://mastodon.cc",
        "https://mastodon.nl",
        "https://mastodon.sk",
        "https://mastodon.de",
    ]

    def __init__(self, config: Dict[str, Any]):
        super().__init__('mastodon', config)

        # Configuration
        self.instances = config.get('instances', self.DEFAULT_INSTANCES)
        self.hashtags = config.get('hashtags', [])
        self.poll_interval = config.get('poll_interval', 300)  # 5 minutes
        self.timeout = config.get('timeout', 10)  # seconds per instance

    async def _fetch_instance_timeline(self, instance_url: str, stream_type: str = 'public:local') -> List[Dict[str, Any]]:
        """
        Fetch timeline from a single Mastodon instance.

        Returns list of status dictionaries.
        """
        statuses = []

        try:
            # Build API URL
            if stream_type == 'public:local':
                api_url = urljoin(instance_url, '/api/v1/timelines/public?local=true&limit=40')
            elif stream_type.startswith('tag:'):
                tag = stream_type.split(':', 1)[1]
                api_url = urljoin(instance_url, f'/api/v1/timelines/tag/{tag}?limit=40')
            else:
                api_url = urljoin(instance_url, '/api/v1/timelines/public?limit=40')

            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, list):
                            statuses = data
                    else:
                        self.logger.warning(f"HTTP {response.status} from {instance_url}")

        except Exception as e:
            self.logger.warning(f"Error fetching from {instance_url}: {e}")

        return statuses

    def _extract_text_from_html(self, html_content: str) -> str:
        """Extract plain text from HTML content."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup.get_text(separator=' ', strip=True)
        except:
            # Fallback: return as-is if BeautifulSoup not available
            return html_content

    def status_to_record(self, status: Dict[str, Any], instance: str, stream: str) -> IngestionRecord:
        """
        Convert Mastodon status to unified IngestionRecord format.
        """
        status_id = status.get('id')
        account = status.get('account')
        created_at = status.get('created_at')
        content = status.get('content')
        url = status.get('url')
        language = status.get('language')
        reblog = status.get('reblog') is not None

        # Extract text from HTML
        text = self._extract_text_from_html(content)

        # Account info
        acct = account.get('acct')
        display_name = account.get('display_name')

        # Create title from account and preview of text
        title = text[:100]

        # Use URL as source_id for deduping (URLs are globally unique)
        # Fall back to instance/status_id if URL is not available
        source_id = url

        return self.create_record(
            source_id=source_id,
            title=title,
            text=text,
            url=url,
            published_at=created_at,
            author=display_name,
            entities={
                'instance': instance,
                'stream': stream,
                'status_id': status_id,
                'instance_status_id': f"{instance}/{status_id}",
                'account_acct': acct,
                'account_display_name': display_name,
                'language': language,
                'reblog': reblog,
                'replies_count': status.get('replies_count'),
                'reblogs_count': status.get('reblogs_count'),
                'favourites_count': status.get('favourites_count'),
            },
            raw=status,
        )

    async def _fetch_all_timelines(self) -> List[IngestionRecord]:
        """Fetch timelines from all configured instances and hashtags."""
        records = []

        # Build list of (instance, stream_type) pairs to fetch
        fetches = []

        for instance in self.instances:
            # Public local timeline
            fetches.append((instance, 'public:local'))

            # Hashtag timelines
            for hashtag in self.hashtags:
                fetches.append((instance, f'tag:{hashtag}'))

        # Fetch all concurrently
        tasks = []
        for instance, stream in fetches:
            task = self._fetch_instance_timeline(instance, stream)
            tasks.append((instance, stream, task))

        # Run all tasks concurrently
        results = await asyncio.gather(
            *[task for _, _, task in tasks],
            return_exceptions=True
        )

        # Process results
        for (instance, stream, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                self.logger.warning(f"Failed to fetch {instance} {stream}: {result}")
                continue

            statuses = result
            for status in statuses:
                try:
                    record = self.status_to_record(status, instance, stream)
                    records.append(record)
                except Exception as e:
                    self.logger.warning(f"Error converting status from {instance}: {e}")

        return records

    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Fetch posts from Mastodon instances.
        """
        try:
            # Run async fetch
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If loop is already running, we need to create a new thread with its own loop
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(self._run_fetch_in_new_loop)
                        records = future.result()
                else:
                    records = loop.run_until_complete(self._fetch_all_timelines())
            except RuntimeError:
                # No event loop exists, create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    records = loop.run_until_complete(self._fetch_all_timelines())
                finally:
                    loop.close()

            for record in records:
                # Validate record
                from shared.models.models import validate_record
                errors = validate_record(record)
                if errors:
                    self.logger.warning(f"Invalid Mastodon record {record.source_id}: {errors}")
                    continue

                yield record

            self.logger.info(f"Fetched posts from Mastodon")

        except Exception as e:
            self.logger.error(f"Error fetching from Mastodon: {e}")
            raise

    def _run_fetch_in_new_loop(self):
        """Helper to run fetch in a new event loop from a thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._fetch_all_timelines())
        finally:
            loop.close()