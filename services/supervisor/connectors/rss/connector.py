"""
RSS connector for news feeds aggregation.

Fetches articles from configured RSS feeds and converts them to unified format.
Multithreaded with 8 workers (feed fetch + item->record conversion).

Safety timeout mechanism:
- Per-feed HTTP connect/read timeouts (requests), so a single slow/broken feed cannot hang a worker indefinitely.
- Optional per-feed total deadline (wall clock) guarding parsing work.
- Optional global fetch() deadline; cancels pending work and returns what is ready.
"""

import json
import os
import time
import threading
from typing import Iterator, Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import logging

import feedparser
import requests
from bs4 import BeautifulSoup

from ..base import BaseConnector
from shared.models.models import IngestionRecord, validate_record

logger = logging.getLogger(__name__)


class RSSConnector(BaseConnector):
    """
    RSS connector for aggregating news from RSS feeds.

    Fetches articles from configured RSS feeds and converts them to unified IngestionRecord format.
    """

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, config: Dict[str, Any]):
        super().__init__("rss", config)

        # Configuration
        self.feeds_file = config.get("feeds_file", "rss_feeds.json")

        # Force 8 workers as requested
        self.max_workers = 8

        # Optional: keep delay for politeness; applied per-host across threads.
        self.request_delay = float(config.get("request_delay", 1.0))

        # --- Safety timeouts (hard timeouts where possible) ---
        # Per-feed network timeouts: connect/read
        self.http_connect_timeout_s = float(config.get("http_connect_timeout_s", 5.0))
        self.http_read_timeout_s = float(config.get("http_read_timeout_s", 10.0))

        # Per-feed wall-clock deadline (covers http + parse); best-effort (cannot kill CPU parse mid-flight).
        self.feed_total_timeout_s = float(config.get("feed_total_timeout_s", 20.0))

        # Global fetch() wall-clock deadline; when exceeded, return what is already produced.
        # Set <= 0 to disable.
        self.fetch_total_timeout_s = float(config.get("fetch_total_timeout_s", 0.0))

        # Cap items per feed (prevents pathological feeds from ballooning work)
        self.max_items_per_feed = int(config.get("max_items_per_feed", 200))

        # Load feeds
        self.feeds = self._load_feeds()
        logger.info(f"Loaded {len(self.feeds)} feeds from {self.feeds_file}")

        # Configure feedparser
        feedparser.USER_AGENT = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        # Per-host rate limiting state (to avoid 8 threads sleeping independently).
        self._host_lock = threading.Lock()
        self._host_next_allowed: Dict[str, float] = {}

        # Requests session for connection reuse
        self._http = requests.Session()
        self._http.headers.update(
            {
                "User-Agent": feedparser.USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/html;q=0.9, */*;q=0.8",
            }
        )

    def _load_feeds(self) -> List[str]:
        """Load RSS feed URLs from config file."""
        import os

        self.logger.info(f"Loading feeds from: {self.feeds_file}")
        self.logger.info(f"Current working directory: {os.getcwd()}")
        self.logger.info(f"Absolute path: {os.path.abspath(self.feeds_file)}")
        with open(self.feeds_file, "r") as f:
            feeds = json.load(f)
        return feeds

    @staticmethod
    def _lowercase_url(url: str) -> str:
        """Convert URL scheme and netloc to lowercase (kept for callers who want consistent URL form)."""
        parsed = urlparse(url)
        return parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower()).geturl()

    def _throttle_host(self, feed_url: str) -> None:
        """
        Apply request_delay per host (shared across threads) so 8 workers don't all sleep uncoordinated.
        If request_delay <= 0, no throttling.
        """
        if self.request_delay <= 0:
            return

        host = urlparse(feed_url).netloc.lower()
        now = time.time()

        with self._host_lock:
            next_allowed = self._host_next_allowed.get(host, 0.0)
            wait_s = max(0.0, next_allowed - now)
            # Reserve next slot
            self._host_next_allowed[host] = max(next_allowed, now) + self.request_delay

        if wait_s > 0:
            time.sleep(wait_s)

    def _http_get_feed(self, feed_url: str) -> Tuple[Optional[bytes], Optional[int], Optional[str]]:
        """
        Fetch raw feed bytes with hard timeouts.
        Returns (content_bytes, status_code, final_url).
        """
        try:
            resp = self._http.get(
                feed_url,
                timeout=(self.http_connect_timeout_s, self.http_read_timeout_s),
                allow_redirects=True,
            )
            status = resp.status_code
            final_url = resp.url
            if status != 200:
                return None, status, final_url
            return resp.content, status, final_url
        except requests.Timeout:
            self.logger.warning(
                "Timeout fetching feed %s (connect=%.1fs read=%.1fs)",
                feed_url,
                self.http_connect_timeout_s,
                self.http_read_timeout_s,
            )
            return None, None, None
        except requests.RequestException as e:
            self.logger.warning("HTTP error fetching feed %s: %s", feed_url, e)
            return None, None, None

    def _fetch_single_feed(self, feed_url: str) -> List[Dict[str, Any]]:
        """
        Fetch articles from a single RSS feed with safety timeouts.
        """
        articles: List[Dict[str, Any]] = []
        self.logger.info("Fetching articles from %s", feed_url)

        start = time.monotonic()
        deadline = start + self.feed_total_timeout_s if self.feed_total_timeout_s > 0 else None

        try:
            self._throttle_host(feed_url)

            # Network stage (hard timeout via requests)
            content, status, final_url = self._http_get_feed(feed_url)

            if status is not None and status != 200:
                if status == 301:
                    self.logger.warning("Feed %s has moved (301) -> %s", feed_url, final_url or "")
                else:
                    self.logger.warning("Feed %s returned status %s", feed_url, status)

            if not content:
                return articles

            # Best-effort wall-clock guard before parsing (cannot preempt parser mid-run)
            if deadline is not None and time.monotonic() > deadline:
                self.logger.warning("Feed %s exceeded total timeout before parse", feed_url)
                return articles

            # Parse bytes (no network inside feedparser now)
            feed = feedparser.parse(content)

            # Best-effort wall-clock guard after parse
            if deadline is not None and time.monotonic() > deadline:
                self.logger.warning("Feed %s exceeded total timeout during parse", feed_url)
                return articles

            if "items" in feed:
                # Cap items to avoid a single feed creating huge fan-out
                for item in feed["items"][: self.max_items_per_feed]:
                    article = self._parse_feed_item(item, feed, feed_url)
                    if article:
                        articles.append(article)

            self.logger.debug("Fetched %d articles from %s", len(articles), feed_url)

        except Exception as e:
            self.logger.error("Error fetching feed %s: %s", feed_url, e)

        return articles

    def _parse_feed_item(self, item: Dict, feed: Dict, feed_url: str) -> Optional[Dict[str, Any]]:
        """Parse a single feed item into an article dictionary."""
        try:
            published_at = None
            if "published_parsed" in item and item["published_parsed"]:
                published_at = time.strftime(self.TIME_FORMAT, item["published_parsed"])
            if not published_at:
                published_at = time.strftime(self.TIME_FORMAT, time.gmtime())

            title = item.get("title", "")
            title = BeautifulSoup(title, "lxml").get_text() if title else ""

            description = item.get("summary", "")
            description = BeautifulSoup(description, "lxml").get_text() if description else ""

            if "worldaffairsjournal" in feed_url:
                description = ""

            author = feed.get("channel", {}).get("title", "Unknown")
            if hasattr(feed, "feed") and "title" in feed.feed:
                author = feed.feed.title

            link = item.get("link", "")
            if not link or not title:
                return None

            return {
                "author": author,
                "title": title,
                "description": description,
                "url": link,
                "publishedAt": published_at,
                "source": "rss",
            }

        except Exception as e:
            self.logger.error("Error parsing feed item: %s", e)
            return None

    def article_to_record(self, article: Dict[str, Any]) -> IngestionRecord:
        """Convert RSS article to unified IngestionRecord format."""
        text = article.get("description", "")
        if article.get("title") and text:
            text = f"{article['title']}. {text}"
        elif article.get("title"):
            text = article["title"]

        return self.create_record(
            source_id=article["url"],  # URL is the item id from upstream; dedupe happens later
            title=article.get("title"),
            text=text,
            url=article.get("url"),
            published_at=article.get("publishedAt"),
            author=article.get("author"),
            raw=article,
        )

    def _article_to_valid_record(self, article: Dict[str, Any]) -> Optional[IngestionRecord]:
        """
        Convert + validate in worker thread.
        Returns None if invalid / conversion fails.
        """
        try:
            record = self.article_to_record(article)
            errors = validate_record(record)
            if errors:
                self.logger.warning("Invalid record for %s: %s", article.get("url"), errors)
                return None
            return record
        except Exception as e:
            self.logger.error("Error converting article to record: %s", e)
            return None

    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Multithreaded pipeline with 8 workers:
        - Stage A: fetch feeds in parallel (fan-out)
        - Stage B: convert+validate articles in parallel (fan-out)
        Safety:
        - Optional global deadline; returns what is ready when exceeded.
        """
        if not self.feeds:
            return iter(())

        global_deadline = None
        if self.fetch_total_timeout_s and self.fetch_total_timeout_s > 0:
            global_deadline = time.monotonic() + self.fetch_total_timeout_s

        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        try:
            feed_futures = {executor.submit(self._fetch_single_feed, url): url for url in self.feeds}
            record_futures = set()

            # Helper: remaining time for waits
            def _remaining() -> Optional[float]:
                if global_deadline is None:
                    return None
                return max(0.0, global_deadline - time.monotonic())

            # Stage A: collect feeds with optional global timeout
            pending_feeds = set(feed_futures.keys())
            while pending_feeds:
                rem = _remaining()
                if rem is not None and rem <= 0:
                    self.logger.warning(
                        "Global fetch timeout reached (%.1fs); cancelling %d pending feed tasks",
                        self.fetch_total_timeout_s,
                        len(pending_feeds),
                    )
                    for pf in pending_feeds:
                        pf.cancel()
                    break

                done, pending_feeds = wait(
                    pending_feeds,
                    timeout=rem if rem is not None else None,
                    return_when=FIRST_COMPLETED,
                )

                if not done:
                    # timed out waiting, loop will cancel next iteration
                    continue

                for f in done:
                    feed_url = feed_futures.get(f, "<unknown>")
                    try:
                        articles = f.result()
                    except Exception as e:
                        self.logger.error("Feed %s generated an exception: %s", feed_url, e)
                        continue

                    for article in articles:
                        record_futures.add(executor.submit(self._article_to_valid_record, article))

            # Stage B: yield records with optional global timeout
            produced = 0
            pending_records = set(record_futures)
            while pending_records:
                rem = _remaining()
                if rem is not None and rem <= 0:
                    self.logger.warning(
                        "Global fetch timeout reached (%.1fs); cancelling %d pending record tasks",
                        self.fetch_total_timeout_s,
                        len(pending_records),
                    )
                    for pr in pending_records:
                        pr.cancel()
                    break

                done, pending_records = wait(
                    pending_records,
                    timeout=rem if rem is not None else None,
                    return_when=FIRST_COMPLETED,
                )

                if not done:
                    continue

                for rf in done:
                    try:
                        rec = rf.result()
                    except Exception as e:
                        self.logger.error("Record future exception: %s", e)
                        continue

                    if rec is not None:
                        produced += 1
                        yield rec

            self.logger.info(
                "Successfully processed %d valid RSS records from %d feeds using %d threads",
                produced,
                len(self.feeds),
                self.max_workers,
            )

        except Exception as e:
            self.logger.error("Error in RSS connector fetch: %s", e)
            raise
        finally:
            executor.shutdown(wait=True)