"""
Event Consumer — Reads Kafka events and routes to downstream.

Subscribes to fund.* topics and:
  - Writes audit trail entries for every event
  - Routes NAV-related events to the NAV engine
  - Routes settlement events to the transfer agent

Trust domain: internal + backend
DB user: readonly_user (SELECT + INSERT audit_events only)
"""
import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import aiohttp
import asyncpg
from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("event-consumer")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "fund_ledger")
DB_USER = os.environ.get("DB_USER", "readonly_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "readonly_pass")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
NAV_ENGINE_URL = os.environ.get(
    "NAV_ENGINE_URL", "http://nav-engine:8001",
)
TRANSFER_AGENT_URL = os.environ.get(
    "TRANSFER_AGENT_URL", "http://transfer-agent:8002",
)

TOPICS = [
    "fund.fund.created",
    "fund.investor.registered",
    "fund.nav.published",
    "fund.order.created",
    "fund.order.compliance_passed",
    "fund.order.approved",
    "fund.order.nav_applied",
    "fund.order.settled",
    "fund.order.redeemed",
    "fund.order.transferred",
    "fund.settlement.created",
    "fund.settlement.approved",
    "fund.settlement.signature_collected",
    "fund.settlement.signed",
    "fund.settlement.broadcasted",
    "fund.settlement.confirmed",
]


def _dsn():
    return (
        f"postgresql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


async def write_audit(pool, event):
    """Write an audit event for every consumed Kafka message."""
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO audit_events "
            "(id, trace_id, actor, actor_role, "
            "action, resource_type, resource_id, details) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            uuid.uuid4(),
            uuid.uuid4(),
            "SYSTEM/event-consumer",
            "SYSTEM",
            f"consumed.{event.get('event_type', 'unknown')}",
            event.get("aggregate_type", "UNKNOWN"),
            (
                uuid.UUID(event["aggregate_id"])
                if "aggregate_id" in event
                else None
            ),
            json.dumps(event.get("payload", {})),
        )


async def check_already_processed(pool, event_id):
    """Check if an event has already been processed."""
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            "SELECT 1 FROM processed_events "
            "WHERE event_id = $1",
            event_id,
        )
        return row is not None


async def mark_processed(pool, event_id, event_type):
    """Mark an event as processed for idempotency."""
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO processed_events "
            "(id, event_id, event_type) "
            "VALUES ($1, $2, $3)",
            uuid.uuid4(), event_id, event_type,
        )


async def handle_nav_published(session, event, event_id):
    """Forward NAV publication to NAV engine for validation."""
    headers = {"Idempotency-Key": str(event_id)}
    try:
        async with session.post(
            f"{NAV_ENGINE_URL}/validate",
            json=event.get("payload", {}),
            headers=headers,
        ) as resp:
            result = await resp.json()
            logger.info(
                "NAV engine validation: %s",
                result.get("status", "unknown"),
            )
    except Exception:
        logger.warning(
            "NAV engine unreachable, event logged",
        )


async def handle_order_settled(session, event, event_id):
    """Notify transfer agent of settlement."""
    headers = {"Idempotency-Key": str(event_id)}
    try:
        async with session.post(
            f"{TRANSFER_AGENT_URL}/notify",
            json=event.get("payload", {}),
            headers=headers,
        ) as resp:
            result = await resp.json()
            logger.info(
                "Transfer agent notified: %s",
                result.get("status", "unknown"),
            )
    except Exception:
        logger.warning(
            "Transfer agent unreachable, event logged",
        )


async def main():
    logger.info("Starting event consumer...")

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

    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="fund-event-consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8"),
        ),
    )

    for attempt in range(15):
        try:
            await consumer.start()
            logger.info("Connected to Kafka, consuming...")
            break
        except Exception:
            logger.warning(
                "Kafka not ready, retry %d/15...",
                attempt + 1,
            )
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Could not connect to Kafka")

    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                event = msg.value
                event_type = event.get("event_type", "unknown")
                event_id_str = event.get("id")
                event_id = (
                    uuid.UUID(event_id_str)
                    if event_id_str
                    else uuid.uuid4()
                )

                if await check_already_processed(
                    pool, event_id,
                ):
                    logger.info(
                        "Skipping duplicate: %s (%s)",
                        event_id, event_type,
                    )
                    continue

                logger.info(
                    "Consumed: %s (topic=%s)",
                    event_type, msg.topic,
                )
                await write_audit(pool, event)

                if event_type == "nav.published":
                    await handle_nav_published(
                        session, event, event_id,
                    )
                elif event_type in (
                    "order.settled",
                    "order.redeemed",
                    "order.transferred",
                ):
                    await handle_order_settled(
                        session, event, event_id,
                    )

                await mark_processed(
                    pool, event_id, event_type,
                )

        finally:
            await consumer.stop()
            await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
