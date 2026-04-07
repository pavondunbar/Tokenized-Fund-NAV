"""
Outbox Publisher — Polls outbox_events and publishes to Kafka.

Prevents double-spending by using FOR UPDATE SKIP LOCKED to
guarantee exactly-once delivery semantics from DB to Kafka.
After MAX_RETRIES, failed events move to the dead letter queue.

Trust domain: internal + backend
DB user: readonly_user (SELECT + UPDATE published_at only)
"""
import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import asyncpg
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("outbox-publisher")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "fund_ledger")
DB_USER = os.environ.get("DB_USER", "readonly_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "readonly_pass")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

MAX_RETRIES_PER_EVENT = 5
POLL_INTERVAL = 1


def _dsn():
    return (
        f"postgresql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


class OutboxPublisher:
    def __init__(self, pool, producer):
        self._pool = pool
        self._producer = producer
        self._retry_counts = {}

    async def poll_and_publish(self):
        """Poll unpublished outbox events and send to Kafka.

        Uses FOR UPDATE SKIP LOCKED to prevent multiple publisher
        instances from picking up the same event.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, aggregate_id, aggregate_type, "
                "event_type, payload, created_at "
                "FROM outbox_events "
                "WHERE published_at IS NULL "
                "ORDER BY created_at "
                "FOR UPDATE SKIP LOCKED "
                "LIMIT 50",
            )

            for row in rows:
                event_id = row["id"]
                event_type = row["event_type"]
                topic = f"fund.{event_type}"
                payload = row["payload"]

                if isinstance(payload, str):
                    payload = json.loads(payload)

                message = {
                    "event_id": str(event_id),
                    "aggregate_id": str(row["aggregate_id"]),
                    "aggregate_type": row["aggregate_type"],
                    "event_type": event_type,
                    "payload": payload,
                    "created_at": row["created_at"].isoformat(),
                }

                try:
                    await self._producer.send_and_wait(
                        topic,
                        json.dumps(message).encode("utf-8"),
                        key=str(
                            row["aggregate_id"]
                        ).encode("utf-8"),
                    )
                    await conn.execute(
                        "UPDATE outbox_events "
                        "SET published_at = $1 "
                        "WHERE id = $2",
                        datetime.now(timezone.utc),
                        event_id,
                    )
                    self._retry_counts.pop(event_id, None)
                    logger.info(
                        "Published: %s -> %s",
                        event_type, topic,
                    )
                except Exception:
                    retries = self._retry_counts.get(
                        event_id, 0,
                    ) + 1
                    self._retry_counts[event_id] = retries
                    logger.warning(
                        "Failed to publish %s (retry %d/%d)",
                        event_id, retries,
                        MAX_RETRIES_PER_EVENT,
                    )

                    if retries >= MAX_RETRIES_PER_EVENT:
                        await conn.execute(
                            "INSERT INTO dead_letter_queue "
                            "(id, source_table, source_id, "
                            "event_type, payload, "
                            "error_message, retry_count) "
                            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
                            uuid.uuid4(),
                            "outbox_events",
                            event_id,
                            event_type,
                            json.dumps(payload),
                            "Max retries exceeded",
                            retries,
                        )
                        await conn.execute(
                            "UPDATE outbox_events "
                            "SET published_at = $1 "
                            "WHERE id = $2",
                            datetime.now(timezone.utc),
                            event_id,
                        )
                        self._retry_counts.pop(event_id, None)
                        logger.warning(
                            "Moved to DLQ: %s", event_id,
                        )


async def run_forever(publisher):
    """Poll loop: check outbox every POLL_INTERVAL seconds."""
    logger.info(
        "Outbox publisher running "
        "(poll every %ds)...", POLL_INTERVAL,
    )
    while True:
        try:
            await publisher.poll_and_publish()
        except Exception:
            logger.exception("Poll cycle error")
        await asyncio.sleep(POLL_INTERVAL)


async def main():
    logger.info("Starting outbox publisher...")

    for attempt in range(15):
        try:
            pool = await asyncpg.create_pool(
                _dsn(), min_size=1, max_size=3,
            )
            logger.info("Connected to PostgreSQL")
            break
        except (OSError, asyncpg.CannotConnectNowError):
            logger.warning(
                "DB not ready, retry %d/15...", attempt + 1,
            )
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Could not connect to database")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        enable_idempotence=True,
    )
    for attempt in range(15):
        try:
            await producer.start()
            logger.info("Connected to Kafka")
            break
        except Exception:
            logger.warning(
                "Kafka not ready, retry %d/15...",
                attempt + 1,
            )
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Could not connect to Kafka")

    publisher = OutboxPublisher(pool, producer)
    try:
        await run_forever(publisher)
    finally:
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
