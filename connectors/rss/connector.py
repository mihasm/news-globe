"""
RSS connector for news feeds aggregation.

Fetches articles from configured RSS feeds and converts them to unified format.
"""

import json
import os
import re
import time
from typing import Iterator, Dict, Any, Optional, List
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
from bs4 import BeautifulSoup

from ..base import BaseConnector
from shared.models.models import IngestionRecord


class RSSConnector(BaseConnector):
    """
    RSS connector for aggregating news from RSS feeds.

    Fetches articles from configured RSS feeds and converts them to unified IngestionRecord format.
    """

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, config: Dict[str, Any]):
        super().__init__('rss', config)

        # Configuration
        self.feeds_file = config.get('feeds_file', 'shared/config/rss_feeds.json')
        self.max_workers = config.get('max_workers', 8)
        self.request_delay = config.get('request_delay', 1.0)

        # State for deduplication
        self.seen_urls = set()

        # Load feeds
        self.feeds = self._load_feeds()

        # Configure feedparser
        feedparser.USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

    def _load_feeds(self) -> List[str]:
        """Load RSS feed URLs from config file."""
        try:
            # Try multiple possible locations for the feeds file
            possible_paths = [
                self.feeds_file,
                os.path.join(os.path.dirname(__file__), '..', '..', '..', self.feeds_file),
                os.path.join(os.path.dirname(__file__), '..', '..', '..', 'shared', 'config', 'rss_feeds.json'),
                '/app/shared/config/rss_feeds.json',  # Docker path
            ]

            feeds_file = None
            for path in possible_paths:
                if os.path.exists(path):
                    feeds_file = path
                    break

            if not feeds_file:
                self.logger.error(f"RSS feeds file not found. Tried: {possible_paths}")
                return []

            with open(feeds_file, 'r') as f:
                feeds = json.load(f)
                self.logger.info(f"Loaded {len(feeds)} RSS feeds from {feeds_file}")
                return feeds

        except FileNotFoundError:
            self.logger.error(f"RSS feeds file not found: {self.feeds_file}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing RSS feeds file: {e}")
            return []

    @staticmethod
    def _lowercase_url(url: str) -> str:
        """Convert URL scheme and netloc to lowercase for deduplication."""
        parsed = urlparse(url)
        return parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower()
        ).geturl()

    def _fetch_single_feed(self, feed_url: str) -> List[Dict[str, Any]]:
        """
        Fetch articles from a single RSS feed.

        Args:
            feed_url: URL of the RSS feed

        Returns:
            List of article dictionaries
        """
        articles = []

        try:
            # Add delay between requests to avoid rate limiting
            if self.request_delay > 0:
                time.sleep(self.request_delay)

            feed = feedparser.parse(feed_url)

            # Check feed status
            if hasattr(feed, 'status'):
                if feed.status != 200:
                    if feed.status == 301:
                        self.logger.warning(f"Feed {feed_url} has moved (301)")
                    else:
                        self.logger.warning(f"Feed {feed_url} returned status {feed.status}")

            # Process feed items
            if 'items' in feed:
                for item in feed['items']:
                    article = self._parse_feed_item(item, feed, feed_url)
                    if article:
                        articles.append(article)

            self.logger.debug(f"Fetched {len(articles)} articles from {feed_url}")

        except Exception as e:
            self.logger.error(f"Error fetching feed {feed_url}: {e}")

        return articles

    def _parse_feed_item(self, item: Dict, feed: Dict, feed_url: str) -> Optional[Dict[str, Any]]:
        """
        Parse a single feed item into an article dictionary.

        Args:
            item: Feed item from feedparser
            feed: The parent feed object
            feed_url: URL of the feed

        Returns:
            Article dictionary or None if parsing fails
        """
        try:
            # Get published date
            published_at = None
            if 'published_parsed' in item and item['published_parsed']:
                published_at = time.strftime(self.TIME_FORMAT, item['published_parsed'])

            if not published_at:
                published_at = time.strftime(self.TIME_FORMAT, time.gmtime())

            # Get and clean title
            title = item.get('title', '')
            title = BeautifulSoup(title, "lxml").get_text() if title else ''

            # Get and clean description
            description = item.get('summary', '')
            description = BeautifulSoup(description, "lxml").get_text() if description else ''

            # Some feeds have problematic descriptions
            if "worldaffairsjournal" in feed_url:
                description = ""

            # Get author/source
            author = feed.get('channel', {}).get('title', 'Unknown')
            if hasattr(feed, 'feed') and 'title' in feed.feed:
                author = feed.feed.title

            # Get link
            link = item.get('link', '')

            if not link or not title:
                return None

            return {
                'author': author,
                'title': title,
                'description': description,
                'url': link,
                'publishedAt': published_at,
                'source': 'rss'
            }

        except Exception as e:
            self.logger.error(f"Error parsing feed item: {e}")
            return None

    def fetch_all_feeds(self) -> List[Dict[str, Any]]:
        """
        Fetch articles from all configured RSS feeds in parallel.

        Returns:
            List of all fetched articles
        """
        all_articles = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(self._fetch_single_feed, url): url
                for url in self.feeds
            }

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    articles = future.result()
                    all_articles.extend(articles)
                except Exception as e:
                    self.logger.error(f"Feed {url} generated an exception: {e}")

        self.logger.info(f"Fetched total of {len(all_articles)} articles from {len(self.feeds)} feeds")
        return all_articles

    def article_to_record(self, article: Dict[str, Any]) -> IngestionRecord:
        """
        Convert RSS article to unified IngestionRecord format.
        """
        # Combine title and description for text content
        text = article.get('description', '')
        if article.get('title') and text:
            text = f"{article['title']}. {text}"
        elif article.get('title'):
            text = article['title']

        return self.create_record(
            source_id=article['url'],  # URL is unique identifier
            title=article.get('title'),
            text=text,
            url=article.get('url'),
            published_at=article.get('publishedAt'),
            author=article.get('author'),
            raw=article,  # Keep original for debugging
        )

    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Fetch articles from RSS feeds and yield IngestionRecord objects.

        This method maintains state for deduplication across calls.
        """
        try:
            # Fetch all feeds
            articles = self.fetch_all_feeds()

            # Filter new articles (not seen before)
            new_articles = []
            for article in articles:
                url = self._lowercase_url(article['url'])
                if url not in self.seen_urls:
                    self.seen_urls.add(url)
                    new_articles.append(article)

            self.logger.info(f"New articles: {len(new_articles)} (skipped {len(articles) - len(new_articles)} duplicates)")

            # Convert to IngestionRecord objects
            for article in new_articles:
                try:
                    record = self.article_to_record(article)

                    # Validate record
                    from shared.models.models import validate_record
                    errors = validate_record(record)
                    if errors:
                        self.logger.warning(f"Invalid record for {article.get('url')}: {errors}")
                        continue

                    yield record

                except Exception as e:
                    self.logger.error(f"Error converting article to record: {e}")
                    continue

            self.logger.info(f"Successfully processed {len(new_articles)} RSS articles")

        except Exception as e:
            self.logger.error(f"Error in RSS connector fetch: {e}")
            raise