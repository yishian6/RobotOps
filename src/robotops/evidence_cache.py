"""Optional Redis cache for evidence pages already verified against their source."""

import json
from pathlib import Path
from time import monotonic

_PAGE_TTL_SECONDS = 300
_MAX_CACHE_BYTES = 64 * 1024


def evidence_file_stamp(path: Path) -> tuple[int, ...] | None:
    """Identify unchanged evidence without loading the file on a cache hit."""
    try:
        stat = path.stat()
    except OSError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


class RedisEvidencePageCache:
    def __init__(self, url: str):
        try:
            from redis import Redis, RedisError
            from redis.backoff import NoBackoff
            from redis.retry import Retry
        except ImportError:
            raise ValueError("Install the Redis extra with uv sync --extra redis") from None
        try:
            self._client = Redis.from_url(
                url, socket_connect_timeout=0.1, socket_timeout=0.1,
                retry=Retry(NoBackoff(), 0), protocol=2,
            )
        except ValueError:
            raise ValueError("Invalid ROBOTOPS_REDIS_CACHE_URL") from None
        self._redis_error = RedisError
        self._retry_at = 0.0

    def get(self, key: str) -> tuple[dict, dict] | None:
        if monotonic() < self._retry_at:
            return None
        try:
            raw = self._client.get(key)
        except self._redis_error:
            self._retry_at = monotonic() + 5
            return None
        if not raw or len(raw) > _MAX_CACHE_BYTES:
            return None
        try:
            page, observation = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if (
            isinstance(page, dict) and isinstance(observation, dict)
            and isinstance(page.get("page"), dict) and isinstance(page.get("raw"), dict)
            and isinstance(observation.get("raw"), dict)
        ):
            return page, observation
        return None

    def put(self, key: str, page: dict, observation: dict) -> None:
        if monotonic() < self._retry_at:
            return
        data = json.dumps([page, observation], ensure_ascii=False, allow_nan=False).encode()
        if len(data) > _MAX_CACHE_BYTES:
            return
        try:
            self._client.set(key, data, ex=_PAGE_TTL_SECONDS)
        except self._redis_error:
            self._retry_at = monotonic() + 5
