"""
Compliance Gateway — KYC/AML/Sanctions screening service.

Provides compliance screening for investor onboarding and
order processing. In production, this would integrate with
providers like Refinitiv World-Check, Dow Jones Risk &
Compliance, LexisNexis, and OFAC SDN lists.

Trust domain: backend
No direct database access.
"""
import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone

from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("compliance-gateway")

PORT = int(os.environ.get("COMPLIANCE_PORT", "8000"))

# Sandbox blocked entities for demo
BLOCKED_ENTITIES = {
    "BLOCKED_ENTITY_001",
    "SANCTIONED_LEI_001",
}


async def handle_screen(request):
    """Screen an entity for KYC/AML compliance.

    In production, this would:
    1. Check OFAC SDN/SSI lists
    2. Screen against PEP databases
    3. Run adverse media checks
    4. Verify accredited investor status
    5. Check country sanctions lists
    """
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"status": "error", "reason": "Invalid JSON"},
            status=400,
        )

    entity_id = data.get("entity_id", "unknown")
    screening_type = data.get("screening_type", "KYC")
    country = data.get("country", "US")

    screening_ref = (
        f"SCR-{screening_type}-"
        f"{hashlib.sha256(entity_id.encode()).hexdigest()[:12]}"
        f"-{uuid.uuid4().hex[:8]}"
    )

    if entity_id in BLOCKED_ENTITIES:
        logger.warning(
            "BLOCKED: entity=%s type=%s",
            entity_id, screening_type,
        )
        return web.json_response({
            "result": "FAIL",
            "entity_id": entity_id,
            "screening_type": screening_type,
            "screening_ref": screening_ref,
            "reason": "Entity appears on sanctions list",
            "screened_at": datetime.now(
                timezone.utc
            ).isoformat(),
        })

    logger.info(
        "PASS: entity=%s type=%s country=%s ref=%s",
        entity_id, screening_type, country, screening_ref,
    )

    return web.json_response({
        "result": "PASS",
        "entity_id": entity_id,
        "screening_type": screening_type,
        "screening_ref": screening_ref,
        "country": country,
        "screened_at": datetime.now(
            timezone.utc
        ).isoformat(),
    })


async def handle_batch_screen(request):
    """Screen multiple entities in a single request."""
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"status": "error", "reason": "Invalid JSON"},
            status=400,
        )

    entities = data.get("entities", [])
    results = []

    for entity in entities:
        entity_id = entity.get("entity_id", "unknown")
        screening_type = entity.get("screening_type", "KYC")

        entity_hash = hashlib.sha256(
            entity_id.encode(),
        ).hexdigest()[:12]
        screening_ref = (
            f"SCR-{screening_type}-{entity_hash}"
            f"-{uuid.uuid4().hex[:8]}"
        )

        result = "FAIL" if entity_id in BLOCKED_ENTITIES \
            else "PASS"

        results.append({
            "result": result,
            "entity_id": entity_id,
            "screening_type": screening_type,
            "screening_ref": screening_ref,
        })

    logger.info(
        "Batch screening: %d entities, %d passed",
        len(results),
        sum(1 for r in results if r["result"] == "PASS"),
    )

    return web.json_response({
        "results": results,
        "total": len(results),
        "screened_at": datetime.now(
            timezone.utc
        ).isoformat(),
    })


async def handle_health(request):
    return web.json_response({"status": "healthy"})


def main():
    app = web.Application()
    app.router.add_post("/screen", handle_screen)
    app.router.add_post("/batch-screen", handle_batch_screen)
    app.router.add_get("/health", handle_health)

    logger.info(
        "Compliance gateway starting on port %d...", PORT,
    )
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
