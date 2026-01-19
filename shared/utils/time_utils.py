"""Time formatting utilities"""
import datetime
import logging

logger = logging.getLogger(__name__)


def format_time_to_ljubljana(tweet_time_str: str) -> str:
    """
    Convert Twitter timestamp string to Ljubljana timezone format.

    Args:
        tweet_time_str: Twitter timestamp in format '%a %b %d %H:%M:%S %z %Y'

    Returns:
        Formatted timestamp string in 'YYYY-MM-DD HH:MM:SS' format (Ljubljana timezone)
    """
    import pytz
    dt = datetime.datetime.strptime(tweet_time_str, '%a %b %d %H:%M:%S %z %Y')
    timestamp_datetime = dt.replace(tzinfo=datetime.timezone.utc)
    ljubljana = pytz.timezone('Europe/Ljubljana')
    ljubljana_time = timestamp_datetime.astimezone(ljubljana)
    return ljubljana_time.strftime('%Y-%m-%d %H:%M:%S')


def format_time_to_iso(tweet_time_str: str) -> str:
    """
    Convert Twitter timestamp string to ISO 8601 Zulu format.

    Args:
        tweet_time_str: Twitter timestamp in format '%a %b %d %H:%M:%S %z %Y'

    Returns:
        Formatted timestamp string in ISO 8601 format (e.g., '2024-01-05T14:30:00Z')
    """
    dt = datetime.datetime.strptime(tweet_time_str, '%a %b %d %H:%M:%S %z %Y')
    # Convert to UTC
    utc_dt = dt.astimezone(datetime.timezone.utc)
    return utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_iso_datetime(dt_string: str) -> datetime.datetime:
    """
    Parse ISO datetime string to timezone-aware datetime.

    Handles various formats:
    - With 'Z' suffix: 2026-01-06T00:03:43.123Z
    - With timezone offset: 2026-01-06T00:03:43.123+00:00
    - Without timezone (assumes UTC): 2026-01-06T00:03:43.123

    Returns:
        Timezone-aware datetime in UTC, or None if parsing fails
    """
    if not dt_string:
        return None

    # Replace 'Z' with '+00:00' for fromisoformat compatibility
    dt_string = dt_string.replace('Z', '+00:00')

    try:
        dt = datetime.datetime.fromisoformat(dt_string)
        # If naive (no timezone), assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except ValueError as e:
        logger.warning(f"Failed to parse datetime '{dt_string}': {e}")
        return None
