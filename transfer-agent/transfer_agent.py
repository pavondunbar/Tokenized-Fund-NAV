"""
Transfer Agent — Share registry and settlement notifications.

Receives settlement notifications from the event consumer.
In production, this would interface with the official share
registrar (e.g., Computershare, EQ Shareowner Services)
and maintain the book-entry ownership records.

Trust domain: backend
No direct database access.
"""
import json
import logging
import os
from datetime import datetime, timezone

from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("transfer-agent")

PORT = int(os.environ.get("TRANSFER_AGENT_PORT", "8002"))

# In-memory registry for demo purposes
_registry = []


async def handle_notify(request):
    """Receive settlement notification and update registry.

    In production, this would:
    1. Update the official share register
    2. Issue transfer confirmations to both parties
    3. Report to regulators (SEC Form 13F, etc.)
    4. Update beneficial ownership records
    """
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"status": "error", "reason": "Invalid JSON"},
            status=400,
        )

    order_id = data.get("order_id", "unknown")
    shares = data.get("shares", "0")
    investor_id = data.get("investor_id")

    entry = {
        "order_id": order_id,
        "shares": shares,
        "investor_id": investor_id,
        "recorded_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "payload": data,
    }
    _registry.append(entry)

    logger.info(
        "Registry updated: order=%s shares=%s investor=%s",
        order_id, shares, investor_id,
    )

    return web.json_response({
        "status": "RECORDED",
        "order_id": order_id,
        "registry_count": len(_registry),
        "recorded_at": entry["recorded_at"],
    })


async def handle_registry(request):
    """Return the current share registry state."""
    return web.json_response({
        "entries": _registry,
        "total": len(_registry),
    })


async def handle_health(request):
    return web.json_response({"status": "healthy"})


def main():
    app = web.Application()
    app.router.add_post("/notify", handle_notify)
    app.router.add_get("/registry", handle_registry)
    app.router.add_get("/health", handle_health)

    logger.info(
        "Transfer agent starting on port %d...", PORT,
    )
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
