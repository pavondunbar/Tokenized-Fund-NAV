"""
NAV Engine — Isolated pricing and NAV calculation service.

Runs in the 'pricing' trust domain, isolated from the database.
Receives NAV validation requests from the event consumer.
In production, this would integrate with Bloomberg/Reuters
pricing feeds, custodian position reports, and accounting systems.

Trust domain: pricing + backend
No direct database access.
"""
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN

_PRECISION = Decimal("0.00000001")  # matches NUMERIC(28,8)

from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("nav-engine")

PORT = int(os.environ.get("NAV_ENGINE_PORT", "8001"))


async def handle_validate(request):
    """Validate a NAV publication.

    In production, this would:
    1. Pull independent pricing data from market data feeds
    2. Compare against custodian position reports
    3. Verify accounting methodology compliance
    4. Check for stale prices or missing securities
    5. Apply fair value adjustments
    """
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"status": "error", "reason": "Invalid JSON"},
            status=400,
        )

    nav_per_share = data.get("nav_per_share")
    net_asset_value = data.get("net_asset_value")

    validations = []

    # Validate NAV per share is positive
    try:
        nps = Decimal(str(nav_per_share))
        if nps <= 0:
            validations.append(
                "NAV per share must be positive"
            )
        elif nps > Decimal("1000000"):
            validations.append(
                "NAV per share exceeds sanity threshold"
            )
    except (InvalidOperation, TypeError):
        validations.append("Invalid NAV per share value")

    # Validate net asset value
    try:
        nav = Decimal(str(net_asset_value))
        if nav < 0:
            validations.append(
                "Net asset value cannot be negative"
            )
    except (InvalidOperation, TypeError):
        validations.append("Invalid net asset value")

    # Validate NAV date
    nav_date = data.get("nav_date")
    if nav_date:
        try:
            d = datetime.strptime(nav_date, "%Y-%m-%d").date()
            if d > datetime.now(timezone.utc).date():
                validations.append(
                    "NAV date cannot be in the future"
                )
        except ValueError:
            validations.append("Invalid NAV date format")

    if validations:
        logger.warning(
            "NAV validation FAILED: %s", validations,
        )
        return web.json_response({
            "status": "FAILED",
            "validations": validations,
            "validated_at": datetime.now(
                timezone.utc
            ).isoformat(),
        })

    logger.info(
        "NAV validation PASSED: $%s per share",
        nav_per_share,
    )
    return web.json_response({
        "status": "PASSED",
        "nav_per_share": str(nav_per_share),
        "validated_at": datetime.now(timezone.utc).isoformat(),
    })


async def handle_calculate(request):
    """Calculate NAV from position data.

    In production, this would pull real market prices
    and compute NAV from first principles.
    """
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"status": "error", "reason": "Invalid JSON"},
            status=400,
        )

    total_assets = Decimal(
        str(data.get("total_assets", "0"))
    )
    total_liabilities = Decimal(
        str(data.get("total_liabilities", "0"))
    )
    shares_outstanding = Decimal(
        str(data.get("shares_outstanding", "1"))
    )

    net_asset_value = total_assets - total_liabilities
    nav_per_share = (
        (net_asset_value / shares_outstanding).quantize(
            _PRECISION, rounding=ROUND_HALF_EVEN,
        )
        if shares_outstanding > 0
        else Decimal("0")
    )

    logger.info(
        "NAV calculated: $%s per share "
        "(assets=$%s liabilities=$%s shares=%s)",
        nav_per_share, total_assets,
        total_liabilities, shares_outstanding,
    )

    return web.json_response({
        "status": "CALCULATED",
        "net_asset_value": str(net_asset_value),
        "nav_per_share": str(nav_per_share),
        "total_assets": str(total_assets),
        "total_liabilities": str(total_liabilities),
        "shares_outstanding": str(shares_outstanding),
        "calculated_at": datetime.now(
            timezone.utc
        ).isoformat(),
    })


async def handle_health(request):
    return web.json_response({"status": "healthy"})


def main():
    app = web.Application()
    app.router.add_post("/validate", handle_validate)
    app.router.add_post("/calculate", handle_calculate)
    app.router.add_get("/health", handle_health)

    logger.info("NAV engine starting on port %d...", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
