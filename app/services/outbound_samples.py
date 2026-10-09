"""Recent outbound records kept for the portal's jq transformation editor.

One list per (integration, output type), newest first, holding up to
OUTBOUND_SAMPLES_MAX entries `{"captured_at": <ISO-8601 UTC>, "record": ...}`
and expiring OUTBOUND_SAMPLES_TTL_SECONDS after the last capture.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, List, Optional

import redis.asyncio as redis

from app import settings

logger = logging.getLogger(__name__)

KEY_PREFIX = "outbound_samples"


def samples_key(integration_id, output_type) -> str:
    return f"{KEY_PREFIX}.{integration_id}.{output_type}"


class OutboundSamples:

    def __init__(self, db_client: Optional[redis.Redis] = None):
        self.db_client = db_client or redis.Redis(
            host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_STATE_DB,
        )

    async def capture(self, integration_id, output_type, records: List[Any]) -> int:
        """Keep the newest of `records` (oldest first, JSON-safe); returns how many were stored.

        Only the last OUTBOUND_SAMPLES_MAX entries that fit OUTBOUND_SAMPLE_MAX_BYTES
        are written: the list is trimmed to that many anyway, so pushing the rest
        of a large bundle would only move data to Redis to discard it.
        """
        captured_at = datetime.now(timezone.utc).isoformat()
        entries = []
        for record in reversed(records):
            entry = json.dumps({"captured_at": captured_at, "record": record})
            size = len(entry.encode())
            if size > settings.OUTBOUND_SAMPLE_MAX_BYTES:
                logger.debug(
                    f"Not capturing a {size}-byte {output_type} sample for integration '{integration_id}': "
                    f"over OUTBOUND_SAMPLE_MAX_BYTES ({settings.OUTBOUND_SAMPLE_MAX_BYTES})."
                )
                continue
            entries.append(entry)
            if len(entries) == settings.OUTBOUND_SAMPLES_MAX:
                break
        if not entries:
            return 0
        key = samples_key(integration_id, output_type)
        async with self.db_client.pipeline(transaction=True) as pipe:
            # LPUSH puts its last argument at the head, so the newest goes last.
            pipe.lpush(key, *reversed(entries))
            pipe.ltrim(key, 0, settings.OUTBOUND_SAMPLES_MAX - 1)
            pipe.expire(key, settings.OUTBOUND_SAMPLES_TTL_SECONDS)
            await pipe.execute()
        return len(entries)

    async def clear(self, integration_id, *output_types):
        await self.db_client.delete(*(samples_key(integration_id, t) for t in output_types))

    async def read(self, integration_id, output_type) -> List[dict]:
        """Stored samples, newest first."""
        key = samples_key(integration_id, output_type)
        samples = []
        for entry in await self.db_client.lrange(key, 0, settings.OUTBOUND_SAMPLES_MAX - 1):
            try:
                sample = json.loads(entry)
                samples.append({"captured_at": sample["captured_at"], "record": sample["record"]})
            except (ValueError, KeyError, TypeError):
                logger.warning(f"Skipping an unreadable entry in '{key}'.")
        return samples
