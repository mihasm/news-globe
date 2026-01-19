"""
Telegram connector for raw media leakage / on-the-ground clips.

Monitors public Telegram channels for new posts and extracts text and media URLs.

Notes:
- No API keys/accounts: scrapes public web previews at https://t.me/s/<channel>
- Channels are loaded from a JSON watchlist file (always).
"""

import asyncio
import json
import os
import re
from typing import Iterator, Dict, Any, Optional, List, Set

import aiohttp
from bs4 import BeautifulSoup

from ..base import BaseConnector
from shared.models.models import IngestionRecord


class TelegramConnector(BaseConnector):
    TG_PUBLIC_BASE = "https://t.me/s"

    def __init__(self, config: Dict[str, Any]):
        super().__init__("telegram", config)

        # Orchestrator-controlled cadence; kept for config consistency
        self.poll_interval = int(config.get("poll_interval", 60))

        # Always use the watchlist JSON file
        self.watchlist_file = config.get("watchlist_file", "tg_watchlist_200.json")

        # Max concurrent HTTP requests
        self.concurrency = int(config.get("concurrency", 10))

        # Regex patterns
        self._re_handle = re.compile(r"@([A-Za-z0-9_]{5,})")

    def _watchlist_path(self) -> str:
        return os.path.join(os.path.dirname(__file__), self.watchlist_file)

    def _load_channels(self) -> List[str]:
        """
        Load channel handles from the watchlist JSON file.

        The JSON can be:
        - ["channel1", "@channel2", ...]
        - or an object with a top-level "channels" list
        """
        path = self._watchlist_path()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict) and isinstance(data.get("channels"), list):
                raw = data["channels"]
            elif isinstance(data, list):
                raw = data
            else:
                self.logger.error(f"Watchlist JSON must be a list or {{'channels': [...]}}: {path}")
                return []

            out: List[str] = []
            seen: Set[str] = set()
            for item in raw:
                if not isinstance(item, str):
                    continue
                ch = item.strip()
                if not ch:
                    continue
                # accept "@handle" or plain handle; also extract "@handle" if embedded
                ch = ch.lstrip("@")
                if not ch:
                    continue
                if ch not in seen:
                    seen.add(ch)
                    out.append(ch)

            return out

        except FileNotFoundError:
            self.logger.error(f"Watchlist file not found: {path}")
            return []
        except Exception as e:
            self.logger.error(f"Error loading watchlist file {path}: {e}")
            return []

    async def _scrape_channel_posts(self, session: aiohttp.ClientSession, channel: str) -> List[Dict[str, Any]]:
        """
        Scrape posts from a single channel's public page.

        Returns list of post dictionaries.
        """
        url = f"{self.TG_PUBLIC_BASE}/{channel}"
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return []

                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                posts: List[Dict[str, Any]] = []
                for post_div in soup.select("div.tgme_widget_message[data-post]"):
                    post_data = self._extract_post_data(post_div, channel)
                    if post_data:
                        posts.append(post_data)
                return posts

        except Exception as e:
            self.logger.error(f"Error scraping channel {channel}: {e}")
            return []

    def _extract_post_data(self, post_div, channel: str) -> Optional[Dict[str, Any]]:
        """Extract post data from a BeautifulSoup post div."""
        try:
            data_post = post_div.get("data-post") or ""
            if "/" not in data_post:
                return None

            _, mid_s = data_post.split("/", 1)
            try:
                message_id = int(mid_s)
            except ValueError:
                return None

            # Timestamp (ISO string from Telegram HTML)
            t = post_div.select("time")
            date_iso = None
            if t:
                date_iso = t[-1].get("datetime")

            # Text content
            text_elem = post_div.select_one("div.tgme_widget_message_text")
            text = text_elem.get_text(" ", strip=True) if text_elem else ""

            media_urls: Set[str] = set()

            # Photo wraps: background-image:url('https://...')
            for a in post_div.select("a.tgme_widget_message_photo_wrap"):
                style = a.get("style") or ""
                m = re.search(r"url\(['\"]?(https?://[^'\")]+)", style)
                if m:
                    media_urls.add(m.group(1))

            # Videos: <video src="...">
            for v in post_div.select("video"):
                src = v.get("src")
                if src and src.startswith("http"):
                    media_urls.add(src)

            # External links (also includes Telegram-internal; filtered later)
            for a in post_div.select("a[href]"):
                href = a.get("href") or ""
                if href.startswith("http"):
                    media_urls.add(href)

            post_url = f"https://t.me/{channel}/{message_id}"

            return {
                "channel": channel,
                "message_id": message_id,
                "date_iso": date_iso,
                "text": text,
                "media_urls": sorted(media_urls),
                "post_url": post_url,
            }

        except Exception as e:
            self.logger.error(f"Error extracting post data: {e}")
            return None

    @staticmethod
    def _make_title(text: str, limit: int = 100) -> Optional[str]:
        t = (text or "").strip()
        if not t:
            return None
        if len(t) <= limit:
            return t
        return t[:limit] + "..."

    def post_to_record(self, post: Dict[str, Any]) -> IngestionRecord:
        """Convert Telegram post to unified IngestionRecord format."""
        channel = post["channel"]
        message_id = post["message_id"]

        source_id = f"{channel}/{message_id}"

        # Media URLs - dedupe and exclude Telegram-internal links
        media_urls: List[str] = []
        seen_urls: Set[str] = set()
        for url in post.get("media_urls", []) or []:
            if not isinstance(url, str):
                continue
            if url.startswith("https://t.me/"):
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            media_urls.append(url)

        text = post.get("text") or ""
        title = self._make_title(text, limit=100)

        return self.create_record(
            source_id=source_id,
            title=title,
            text=text if text else None,
            url=post.get("post_url"),
            published_at=post.get("date_iso"),
            author=channel,  # FIX 1: use channel name as author
            media_urls=media_urls if media_urls else None,
            entities={
                "channel": channel,
                "message_id": message_id,
            },
            raw=post,
        )

    async def _fetch_async(self) -> List[IngestionRecord]:
        channels = self._load_channels()
        if not channels:
            self.logger.warning("No Telegram channels loaded from watchlist")
            return []

        self.logger.info(f"Monitoring {len(channels)} Telegram channels (watchlist)")

        timeout = aiohttp.ClientTimeout(total=25)
        headers = {"User-Agent": "Mozilla/5.0"}
        sem = asyncio.Semaphore(self.concurrency)

        records: List[IngestionRecord] = []

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:

            async def _scrape_one(channel: str) -> List[IngestionRecord]:
                async with sem:
                    posts = await self._scrape_channel_posts(session, channel)
                    out: List[IngestionRecord] = []
                    for post in posts:
                        record = self.post_to_record(post)

                        from shared.models.models import validate_record

                        errors = validate_record(record)
                        if errors:
                            self.logger.warning(f"Invalid Telegram record: {errors}")
                            continue
                        out.append(record)
                    return out

            tasks = [_scrape_one(ch) for ch in channels]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, list):
                records.extend(r)
            else:
                self.logger.error(f"Telegram channel scrape task error: {r}")

        self.logger.info("Fetched Telegram posts")
        return records

    def fetch(self) -> Iterator[IngestionRecord]:
        """
        Fetch new posts from monitored Telegram channels.

        Uses async scraping with deduplication based on message IDs (enforced downstream by source_id).
        """
        try:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(self._run_fetch_in_new_loop)
                        records = future.result()
                else:
                    records = loop.run_until_complete(self._fetch_async())
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    records = loop.run_until_complete(self._fetch_async())
                finally:
                    loop.close()

            yield from records

        except Exception as e:
            self.logger.error(f"Error in Telegram fetch: {e}")
            raise

    def _run_fetch_in_new_loop(self) -> List[IngestionRecord]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._fetch_async())
        finally:
            loop.close()
