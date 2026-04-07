"""
Tokenized Fund Service — Core fund operations.

This is the ONLY service with write access to the ledger.
All state changes produce outbox events for Kafka publishing.
Balances are never stored — they are derived from ledger replay.

Trust domain: internal + backend
DB user: ledger_user (full write)
"""
import asyncio
import hashlib
import json
import logging
import os
import random
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN

import asyncpg

from rbac import (
    check_permission,
    insert_audit_event,
    validate_order_transition,
    validate_settlement_transition,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("fund-service")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "fund_ledger")
DB_USER = os.environ.get("DB_USER", "ledger_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "ledger_pass")


def _dsn():
    return (
        f"postgresql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


_PRECISION = Decimal("0.00000001")  # 8 dp, matches NUMERIC(28,8)
_CASH_PRECISION = Decimal("0.01")   # 2 dp, for USD cash amounts


def _q(value):
    """Quantize a Decimal to 8 decimal places (NUMERIC(28,8))."""
    return value.quantize(_PRECISION, rounding=ROUND_HALF_EVEN)


def _cash(value):
    """Quantize a Decimal to 2 decimal places (USD cents)."""
    return value.quantize(_CASH_PRECISION, rounding=ROUND_HALF_EVEN)


def _new_id():
    return uuid.uuid4()


def _now():
    return datetime.now(timezone.utc)


class AuditContext:
    """Carries request/trace/actor metadata through operations."""

    def __init__(self, request_id, trace_id, actor, actor_role):
        self.request_id = request_id
        self.trace_id = trace_id
        self.actor = actor
        self.actor_role = actor_role

    @classmethod
    def system(cls, operation):
        return cls(
            request_id=_new_id(),
            trace_id=_new_id(),
            actor=f"SYSTEM/{operation}",
            actor_role="SYSTEM",
        )

    def as_sql_args(self):
        return (
            self.request_id, self.trace_id,
            self.actor, self.actor_role,
        )


class AsyncpgDB:
    """Thin async wrapper around an asyncpg connection pool."""

    def __init__(self, pool):
        self._pool = pool

    async def execute(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    def acquire(self):
        return self._pool.acquire()

    def transaction(self):
        return _TransactionContext(self._pool)


class _TransactionContext:
    def __init__(self, pool):
        self._pool = pool
        self._conn = None
        self._tx = None

    async def __aenter__(self):
        self._conn = await self._pool.acquire()
        self._tx = self._conn.transaction()
        await self._tx.start()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self._tx.rollback()
        else:
            await self._tx.commit()
        await self._pool.release(self._conn)


async def _write_outbox(
    conn, aggregate_id, aggregate_type,
    event_type, payload, ctx,
):
    """Write an event to the transactional outbox.

    MUST be called within the same transaction as the business
    state change to guarantee atomicity.
    """
    await conn.execute(
        "INSERT INTO outbox_events "
        "(id, aggregate_id, aggregate_type, event_type, "
        "payload, request_id, trace_id, actor, actor_role) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        _new_id(), aggregate_id, aggregate_type,
        event_type, json.dumps(payload),
        *ctx.as_sql_args(),
    )


async def connect_db():
    """Connect to Postgres with retry logic."""
    for attempt in range(15):
        try:
            pool = await asyncpg.create_pool(
                _dsn(), min_size=2, max_size=5,
            )
            logger.info("Connected to PostgreSQL")
            return pool
        except (OSError, asyncpg.CannotConnectNowError):
            logger.warning(
                "DB not ready, retry %d/15...", attempt + 1,
            )
            await asyncio.sleep(3)
    raise RuntimeError("Could not connect to database")


# =========================================================================
# FUND CREATION
# =========================================================================
async def create_fund(db, ctx):
    """Create the demo tokenized fund."""
    fund_id = _new_id()
    idem_key = "FUND-DEMO-BLACKROCK-BUIDL-001"

    existing = await db.fetchval(
        "SELECT id FROM funds "
        "WHERE idempotency_key = $1",
        idem_key,
    )
    if existing:
        logger.info("Fund already exists: %s", existing)
        return existing

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO funds "
            "(id, fund_name, fund_ticker, fund_type, "
            "base_currency, inception_date, domicile, "
            "isin, cusip, management_fee_bps, status, "
            "idempotency_key, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,"
            "$10,$11,$12,$13,$14,$15,$16)",
            fund_id,
            "BlackRock USD Institutional Digital "
            "Liquidity Fund",
            "BUIDL", "OPEN_END", "USD",
            date(2024, 3, 20), "US",
            "US09261HAC01", "09261HAC0",
            50, "ACTIVE", idem_key,
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO fund_status_events "
            "(id, fund_id, status, reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), fund_id, "ACTIVE",
            "Fund inception",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, fund_id, "FUND",
            "fund.created",
            {
                "fund_id": str(fund_id),
                "ticker": "BUIDL",
                "type": "OPEN_END",
            },
            ctx,
        )

    logger.info("Fund created: %s (BUIDL)", fund_id)
    return fund_id


# =========================================================================
# INVESTOR REGISTRATION
# =========================================================================
async def register_investor(
    db, ctx, name, investor_type, lei, domicile, idem_key,
):
    """Register a KYC-verified investor."""
    existing = await db.fetchval(
        "SELECT id FROM investors "
        "WHERE idempotency_key = $1",
        idem_key,
    )
    if existing:
        logger.info("Investor already exists: %s", existing)
        return existing

    investor_id = _new_id()
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO investors "
            "(id, investor_name, investor_type, lei, "
            "domicile, kyc_status, aml_status, "
            "idempotency_key, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            investor_id, name, investor_type, lei,
            domicile, "APPROVED", "CLEARED",
            idem_key, *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO investor_compliance_events "
            "(id, investor_id, event_type, "
            "old_status, new_status, screening_ref, "
            "reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            _new_id(), investor_id, "KYC",
            "PENDING", "APPROVED",
            f"KYC-{idem_key}",
            "Pre-verified institutional investor",
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO investor_compliance_events "
            "(id, investor_id, event_type, "
            "old_status, new_status, screening_ref, "
            "reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            _new_id(), investor_id, "AML",
            "PENDING", "CLEARED",
            f"AML-{idem_key}",
            "AML screening passed",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, investor_id, "INVESTOR",
            "investor.registered",
            {
                "investor_id": str(investor_id),
                "name": name,
                "type": investor_type,
            },
            ctx,
        )

    logger.info("Investor registered: %s (%s)", name, investor_id)
    return investor_id


# =========================================================================
# NAV PUBLICATION
# =========================================================================
async def publish_nav(
    db, ctx, fund_id, nav_date, total_assets,
    total_liabilities, shares_outstanding,
):
    """Publish an official NAV snapshot."""
    nav_id = _new_id()
    net_asset_value = total_assets - total_liabilities
    nav_per_share = (
        _q(net_asset_value / shares_outstanding)
        if shares_outstanding > 0
        else Decimal("0")
    )
    idem_key = f"NAV-{fund_id}-{nav_date}"

    existing = await db.fetchval(
        "SELECT id FROM nav_publications "
        "WHERE idempotency_key = $1",
        idem_key,
    )
    if existing:
        logger.info("NAV already published: %s", existing)
        return existing, nav_per_share

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO nav_publications "
            "(id, fund_id, nav_date, total_assets, "
            "total_liabilities, net_asset_value, "
            "shares_outstanding, nav_per_share, "
            "pricing_source, is_official, "
            "idempotency_key, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,"
            "$9,$10,$11,$12,$13,$14,$15)",
            nav_id, fund_id, nav_date,
            total_assets, total_liabilities,
            net_asset_value, shares_outstanding,
            nav_per_share,
            "INTERNAL_PRICING_ENGINE", True,
            idem_key, *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, fund_id, "FUND",
            "nav.published",
            {
                "fund_id": str(fund_id),
                "nav_date": str(nav_date),
                "nav_per_share": str(nav_per_share),
                "net_asset_value": str(net_asset_value),
            },
            ctx,
        )

    logger.info(
        "NAV published: %s per share (date=%s)",
        nav_per_share, nav_date,
    )
    return nav_id, nav_per_share


# =========================================================================
# SUBSCRIPTION ORDER
# =========================================================================
async def process_subscription(
    db, ctx, fund_id, investor_id,
    investment_amount, nav_per_share,
):
    """Process a subscription: compliance -> NAV apply -> settle."""
    order_id = _new_id()
    idem_key = f"SUB-{investor_id}-{fund_id}-{_now().isoformat()}"
    shares = _q(investment_amount / nav_per_share)
    journal_id = _new_id()

    check_permission(ctx.actor_role, "order.process")

    async with db.transaction() as conn:
        # 1. Create order
        await conn.execute(
            "INSERT INTO orders "
            "(id, fund_id, investor_id, order_type, "
            "shares, amount, currency, nav_per_share, "
            "idempotency_key, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,"
            "$10,$11,$12,$13)",
            order_id, fund_id, investor_id,
            "SUBSCRIPTION", shares, investment_amount,
            "USD", nav_per_share, idem_key,
            *ctx.as_sql_args(),
        )
        # 2. PENDING event
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "PENDING",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.created",
            {
                "order_id": str(order_id),
                "type": "SUBSCRIPTION",
                "amount": str(investment_amount),
            },
            ctx,
        )

    logger.info(
        "  Order created: %s (SUBSCRIPTION $%s)",
        order_id, investment_amount,
    )

    # 3. Compliance check
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "COMPLIANCE_CHECK",
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO compliance_screenings "
            "(id, order_id, investor_id, "
            "screening_type, result, provider, "
            "screening_ref, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            _new_id(), order_id, investor_id,
            "KYC", "PASS", "SANDBOX_COMPLIANCE",
            f"SCR-{order_id}",
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO compliance_screenings "
            "(id, order_id, investor_id, "
            "screening_type, result, provider, "
            "screening_ref, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            _new_id(), order_id, investor_id,
            "AML", "PASS", "SANDBOX_COMPLIANCE",
            f"AML-{order_id}",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.compliance_passed",
            {
                "order_id": str(order_id),
                "investor_id": str(investor_id),
            },
            ctx,
        )
    logger.info("  Compliance screening: PASS")

    # 4. Approve
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "APPROVED",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.approved",
            {"order_id": str(order_id)},
            ctx,
        )
    logger.info("  Order approved")

    # 5. Apply NAV
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), order_id, "NAV_APPLIED",
            f"NAV={nav_per_share} shares={shares}",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.nav_applied",
            {
                "order_id": str(order_id),
                "nav_per_share": str(nav_per_share),
                "shares": str(shares),
            },
            ctx,
        )
    logger.info(
        "  NAV applied: %s shares @ $%s",
        shares, nav_per_share,
    )

    # 6. Blockchain settlement pipeline
    settle_id, block_num = await settle_via_blockchain(
        db, ctx, order_id, fund_id, "SUBSCRIPTION",
    )
    logger.info(
        "  Blockchain settled: settlement=%s block=%d",
        settle_id, block_num,
    )

    # 7. Settle — atomic double-entry ledger writes
    async with db.transaction() as conn:
        # Share ledger: DEBIT from fund treasury, CREDIT to investor
        await conn.execute(
            "INSERT INTO share_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, shares, nav_per_share, amount, "
            "currency, reason, order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,"
            "$12,$13,$14,$15)",
            _new_id(), fund_id, investor_id, journal_id,
            "CREDIT", shares, nav_per_share,
            investment_amount, "USD", "SUBSCRIPTION",
            order_id, *ctx.as_sql_args(),
        )
        # Treasury side (use fund_id as investor for treasury)
        treasury_id = await _get_or_create_treasury(
            conn, fund_id, ctx,
        )
        await conn.execute(
            "INSERT INTO share_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, shares, nav_per_share, amount, "
            "currency, reason, order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,"
            "$12,$13,$14,$15)",
            _new_id(), fund_id, treasury_id, journal_id,
            "DEBIT", shares, nav_per_share,
            investment_amount, "USD", "SUBSCRIPTION",
            order_id, *ctx.as_sql_args(),
        )

        # Cash ledger: investor pays, fund receives
        cash_journal_id = _new_id()
        await conn.execute(
            "INSERT INTO cash_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, amount, currency, reason, "
            "order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,"
            "$10,$11,$12,$13)",
            _new_id(), fund_id, investor_id,
            cash_journal_id,
            "DEBIT", investment_amount, "USD",
            "SUBSCRIPTION_PAYMENT", order_id,
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO cash_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, amount, currency, reason, "
            "order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,"
            "$10,$11,$12,$13)",
            _new_id(), fund_id, treasury_id,
            cash_journal_id,
            "CREDIT", investment_amount, "USD",
            "SUBSCRIPTION_PAYMENT", order_id,
            *ctx.as_sql_args(),
        )

        # Mark settled
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), order_id, "SETTLED",
            f"Settled {shares} shares",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.settled",
            {
                "order_id": str(order_id),
                "fund_id": str(fund_id),
                "investor_id": str(investor_id),
                "shares": str(shares),
                "amount": str(investment_amount),
                "nav_per_share": str(nav_per_share),
            },
            ctx,
        )

    logger.info("  Subscription SETTLED: %s shares", shares)
    return order_id, shares


# =========================================================================
# REDEMPTION ORDER
# =========================================================================
async def process_redemption(
    db, ctx, fund_id, investor_id,
    redeem_shares, nav_per_share,
):
    """Process a redemption: verify balance -> settle."""
    order_id = _new_id()
    idem_key = f"RED-{investor_id}-{fund_id}-{_now().isoformat()}"
    payout_amount = _cash(redeem_shares * nav_per_share)
    journal_id = _new_id()

    check_permission(ctx.actor_role, "order.process")

    # Verify investor has sufficient shares (derived balance)
    balance = await db.fetchval(
        "SELECT share_balance FROM investor_share_balances "
        "WHERE fund_id = $1 AND investor_id = $2",
        fund_id, investor_id,
    )
    if balance is None or balance < redeem_shares:
        raise ValueError(
            f"Insufficient shares: have={balance} "
            f"need={redeem_shares}"
        )

    # 1. Create order + PENDING event
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO orders "
            "(id, fund_id, investor_id, order_type, "
            "shares, amount, currency, nav_per_share, "
            "idempotency_key, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,"
            "$10,$11,$12,$13)",
            order_id, fund_id, investor_id,
            "REDEMPTION", redeem_shares, payout_amount,
            "USD", nav_per_share, idem_key,
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "PENDING",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.created",
            {
                "order_id": str(order_id),
                "type": "REDEMPTION",
                "shares": str(redeem_shares),
            },
            ctx,
        )
    logger.info(
        "  Order created: %s (REDEMPTION %s shares)",
        order_id, redeem_shares,
    )

    # 2. Compliance check
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "COMPLIANCE_CHECK",
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO compliance_screenings "
            "(id, order_id, investor_id, "
            "screening_type, result, provider, "
            "screening_ref, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            _new_id(), order_id, investor_id,
            "AML", "PASS", "SANDBOX_COMPLIANCE",
            f"RED-AML-{order_id}",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.compliance_passed",
            {
                "order_id": str(order_id),
                "investor_id": str(investor_id),
            },
            ctx,
        )
    logger.info("  Compliance screening: PASS")

    # 3. Approve
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "APPROVED",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.approved",
            {"order_id": str(order_id)},
            ctx,
        )
    logger.info("  Order approved")

    # 4. Apply NAV
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), order_id, "NAV_APPLIED",
            f"NAV={nav_per_share} shares={redeem_shares}",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.nav_applied",
            {
                "order_id": str(order_id),
                "nav_per_share": str(nav_per_share),
                "shares": str(redeem_shares),
            },
            ctx,
        )
    logger.info(
        "  NAV applied: %s shares @ $%s",
        redeem_shares, nav_per_share,
    )

    # 5. Blockchain settlement pipeline
    settle_id, block_num = await settle_via_blockchain(
        db, ctx, order_id, fund_id, "REDEMPTION",
    )
    logger.info(
        "  Blockchain settled: settlement=%s block=%d",
        settle_id, block_num,
    )

    # 6. Settle — atomic double-entry ledger writes
    async with db.transaction() as conn:
        # Share ledger: DEBIT from investor, CREDIT to treasury
        treasury_id = await _get_or_create_treasury(
            conn, fund_id, ctx,
        )
        await conn.execute(
            "INSERT INTO share_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, shares, nav_per_share, amount, "
            "currency, reason, order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,"
            "$12,$13,$14,$15)",
            _new_id(), fund_id, investor_id, journal_id,
            "DEBIT", redeem_shares, nav_per_share,
            payout_amount, "USD", "REDEMPTION",
            order_id, *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO share_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, shares, nav_per_share, amount, "
            "currency, reason, order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,"
            "$12,$13,$14,$15)",
            _new_id(), fund_id, treasury_id, journal_id,
            "CREDIT", redeem_shares, nav_per_share,
            payout_amount, "USD", "REDEMPTION",
            order_id, *ctx.as_sql_args(),
        )

        # Cash ledger: fund pays out, investor receives
        cash_journal_id = _new_id()
        await conn.execute(
            "INSERT INTO cash_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, amount, currency, reason, "
            "order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,"
            "$10,$11,$12,$13)",
            _new_id(), fund_id, treasury_id,
            cash_journal_id,
            "DEBIT", payout_amount, "USD",
            "REDEMPTION_PAYOUT", order_id,
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO cash_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, amount, currency, reason, "
            "order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,"
            "$10,$11,$12,$13)",
            _new_id(), fund_id, investor_id,
            cash_journal_id,
            "CREDIT", payout_amount, "USD",
            "REDEMPTION_PAYOUT", order_id,
            *ctx.as_sql_args(),
        )

        # Mark settled
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), order_id, "SETTLED",
            f"Redeemed {redeem_shares} shares "
            f"for ${payout_amount}",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.redeemed",
            {
                "order_id": str(order_id),
                "fund_id": str(fund_id),
                "investor_id": str(investor_id),
                "shares": str(redeem_shares),
                "payout": str(payout_amount),
            },
            ctx,
        )

    logger.info(
        "  Redemption SETTLED: %s shares -> $%s",
        redeem_shares, payout_amount,
    )
    return order_id


# =========================================================================
# TRANSFER (investor-to-investor)
# =========================================================================
async def process_transfer(
    db, ctx, fund_id,
    from_investor_id, to_investor_id,
    transfer_shares, nav_per_share,
):
    """Transfer shares between investors."""
    order_id = _new_id()
    idem_key = (
        f"XFR-{from_investor_id}-{to_investor_id}"
        f"-{_now().isoformat()}"
    )
    amount = _cash(transfer_shares * nav_per_share)
    journal_id = _new_id()

    # Verify sender balance
    balance = await db.fetchval(
        "SELECT share_balance FROM investor_share_balances "
        "WHERE fund_id = $1 AND investor_id = $2",
        fund_id, from_investor_id,
    )
    if balance is None or balance < transfer_shares:
        raise ValueError(
            f"Insufficient shares for transfer: "
            f"have={balance} need={transfer_shares}"
        )

    # 1. Create order + PENDING event
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO orders "
            "(id, fund_id, investor_id, order_type, "
            "shares, amount, currency, nav_per_share, "
            "counterparty_investor_id, "
            "idempotency_key, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,"
            "$11,$12,$13,$14)",
            order_id, fund_id, from_investor_id,
            "TRANSFER", transfer_shares, amount,
            "USD", nav_per_share, to_investor_id,
            idem_key, *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "PENDING",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.created",
            {
                "order_id": str(order_id),
                "type": "TRANSFER",
                "shares": str(transfer_shares),
            },
            ctx,
        )
    logger.info(
        "  Order created: %s (TRANSFER %s shares)",
        order_id, transfer_shares,
    )

    # 2. Compliance check
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "COMPLIANCE_CHECK",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.compliance_passed",
            {
                "order_id": str(order_id),
                "from": str(from_investor_id),
                "to": str(to_investor_id),
            },
            ctx,
        )
    logger.info("  Compliance screening: PASS")

    # 3. Approve
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), order_id, "APPROVED",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.approved",
            {"order_id": str(order_id)},
            ctx,
        )
    logger.info("  Order approved")

    # 4. Apply NAV
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), order_id, "NAV_APPLIED",
            f"NAV={nav_per_share} shares={transfer_shares}",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.nav_applied",
            {
                "order_id": str(order_id),
                "nav_per_share": str(nav_per_share),
                "shares": str(transfer_shares),
            },
            ctx,
        )
    logger.info(
        "  NAV applied: %s shares @ $%s",
        transfer_shares, nav_per_share,
    )

    # 5. Blockchain settlement pipeline
    settle_id, block_num = await settle_via_blockchain(
        db, ctx, order_id, fund_id, "TRANSFER",
    )
    logger.info(
        "  Blockchain settled: settlement=%s block=%d",
        settle_id, block_num,
    )

    # 6. Settle — atomic double-entry ledger writes
    async with db.transaction() as conn:
        # Share ledger: debit sender, credit receiver
        await conn.execute(
            "INSERT INTO share_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, shares, nav_per_share, amount, "
            "currency, reason, order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,"
            "$12,$13,$14,$15)",
            _new_id(), fund_id, from_investor_id,
            journal_id,
            "DEBIT", transfer_shares, nav_per_share,
            amount, "USD", "TRANSFER_OUT",
            order_id, *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO share_ledger "
            "(id, fund_id, investor_id, journal_id, "
            "entry_type, shares, nav_per_share, amount, "
            "currency, reason, order_id, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,"
            "$12,$13,$14,$15)",
            _new_id(), fund_id, to_investor_id,
            journal_id,
            "CREDIT", transfer_shares, nav_per_share,
            amount, "USD", "TRANSFER_IN",
            order_id, *ctx.as_sql_args(),
        )

        # Mark settled
        await conn.execute(
            "INSERT INTO order_events "
            "(id, order_id, status, reason, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), order_id, "SETTLED",
            f"Transferred {transfer_shares} shares",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, order_id, "ORDER",
            "order.transferred",
            {
                "order_id": str(order_id),
                "from": str(from_investor_id),
                "to": str(to_investor_id),
                "shares": str(transfer_shares),
            },
            ctx,
        )

    logger.info(
        "  Transfer SETTLED: %s shares from %s -> %s",
        transfer_shares, from_investor_id, to_investor_id,
    )
    return order_id


# =========================================================================
# HELPERS
# =========================================================================
async def _get_or_create_treasury(conn, fund_id, ctx):
    """Get or create the fund treasury investor account.

    The treasury is a synthetic investor that represents
    the fund's own share issuance pool.
    """
    idem_key = f"TREASURY-{fund_id}"
    existing = await conn.fetchval(
        "SELECT id FROM investors "
        "WHERE idempotency_key = $1",
        idem_key,
    )
    if existing:
        return existing

    treasury_id = _new_id()
    await conn.execute(
        "INSERT INTO investors "
        "(id, investor_name, investor_type, "
        "domicile, kyc_status, aml_status, "
        "idempotency_key, "
        "request_id, trace_id, actor, actor_role) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        treasury_id,
        f"TREASURY/{fund_id}",
        "INSTITUTIONAL", "US",
        "APPROVED", "CLEARED",
        idem_key, *ctx.as_sql_args(),
    )
    return treasury_id


# =========================================================================
# SETTLEMENT PIPELINE — Blockchain settlement lifecycle
# =========================================================================
async def create_settlement(
    db, ctx, order_id, fund_id, settlement_type,
):
    """Create a settlement record and emit PENDING event."""
    settlement_id = _new_id()
    idem_key = f"SETTLE-{order_id}"

    existing = await db.fetchval(
        "SELECT id FROM settlements "
        "WHERE idempotency_key = $1",
        idem_key,
    )
    if existing:
        logger.info("Settlement already exists: %s", existing)
        return existing

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO settlements "
            "(id, order_id, fund_id, settlement_type, "
            "idempotency_key, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            settlement_id, order_id, fund_id,
            settlement_type, idem_key,
            *ctx.as_sql_args(),
        )
        await conn.execute(
            "INSERT INTO settlement_events "
            "(id, settlement_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), settlement_id, "PENDING",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, settlement_id, "SETTLEMENT",
            "settlement.created",
            {
                "settlement_id": str(settlement_id),
                "order_id": str(order_id),
                "type": settlement_type,
            },
            ctx,
        )

    logger.info(
        "    Settlement created: %s [PENDING]",
        settlement_id,
    )
    return settlement_id


async def approve_settlement(db, ctx, settlement_id):
    """Approve a settlement for MPC signing."""
    check_permission(ctx.actor_role, "settlement.approve")

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO settlement_events "
            "(id, settlement_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), settlement_id, "APPROVED",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, settlement_id, "SETTLEMENT",
            "settlement.approved",
            {"settlement_id": str(settlement_id)},
            ctx,
        )

    logger.info("    Settlement approved: %s", settlement_id)


async def collect_mpc_signature(
    db, ctx, settlement_id, signer_id,
):
    """Collect one MPC signature share from a signer."""
    check_permission(ctx.actor_role, "settlement.sign")

    sig_data = f"{settlement_id}:{signer_id}:{_now().isoformat()}"
    signature_share = hashlib.sha256(
        sig_data.encode(),
    ).hexdigest()

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO mpc_signatures "
            "(id, settlement_id, signer_id, "
            "signature_share, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), settlement_id, signer_id,
            signature_share,
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, settlement_id, "SETTLEMENT",
            "settlement.signature_collected",
            {
                "settlement_id": str(settlement_id),
                "signer_id": signer_id,
            },
            ctx,
        )

    logger.info(
        "    MPC signature collected: %s from %s",
        settlement_id, signer_id,
    )


async def check_quorum_and_sign(db, ctx, settlement_id):
    """Check if quorum (2-of-3) is reached; if so, mark SIGNED."""
    sig_count = await db.fetchval(
        "SELECT COUNT(*) FROM mpc_signatures "
        "WHERE settlement_id = $1",
        settlement_id,
    )

    if sig_count < 2:
        logger.info(
            "    Quorum not reached: %d/2 signatures",
            sig_count,
        )
        return False

    order_row = await db.fetchrow(
        "SELECT s.order_id, o.amount "
        "FROM settlements s "
        "JOIN orders o ON o.id = s.order_id "
        "WHERE s.id = $1",
        settlement_id,
    )
    tx_data = (
        f"{settlement_id}:{order_row['order_id']}:"
        f"{order_row['amount']}:{_now().isoformat()}"
    )
    tx_hash = "0x" + hashlib.sha256(
        tx_data.encode(),
    ).hexdigest()

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO settlement_events "
            "(id, settlement_id, status, tx_hash, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), settlement_id, "SIGNED",
            tx_hash, *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, settlement_id, "SETTLEMENT",
            "settlement.signed",
            {
                "settlement_id": str(settlement_id),
                "tx_hash": tx_hash,
            },
            ctx,
        )

    logger.info(
        "    Settlement SIGNED: %s tx=%s",
        settlement_id, tx_hash,
    )
    return True


async def broadcast_settlement(db, ctx, settlement_id):
    """Broadcast the signed transaction to the network."""
    check_permission(ctx.actor_role, "settlement.broadcast")

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO settlement_events "
            "(id, settlement_id, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            _new_id(), settlement_id, "BROADCASTED",
            *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, settlement_id, "SETTLEMENT",
            "settlement.broadcasted",
            {"settlement_id": str(settlement_id)},
            ctx,
        )

    logger.info(
        "    Settlement BROADCASTED: %s", settlement_id,
    )


async def confirm_settlement(db, ctx, settlement_id):
    """Confirm settlement with a simulated block number."""
    block_number = random.randint(19_000_000, 20_000_000)

    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO settlement_events "
            "(id, settlement_id, status, block_number, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _new_id(), settlement_id, "CONFIRMED",
            block_number, *ctx.as_sql_args(),
        )
        await _write_outbox(
            conn, settlement_id, "SETTLEMENT",
            "settlement.confirmed",
            {
                "settlement_id": str(settlement_id),
                "block_number": block_number,
            },
            ctx,
        )

    logger.info(
        "    Settlement CONFIRMED: %s block=%d",
        settlement_id, block_number,
    )
    return block_number


async def settle_via_blockchain(
    db, ctx, order_id, fund_id, settlement_type,
):
    """Run the full settlement pipeline for an order.

    create -> approve -> 2 MPC signatures -> quorum check
    -> broadcast -> confirm
    """
    settlement_id = await create_settlement(
        db, ctx, order_id, fund_id, settlement_type,
    )
    await approve_settlement(db, ctx, settlement_id)

    signer_ctx_1 = AuditContext(
        request_id=ctx.request_id,
        trace_id=ctx.trace_id,
        actor="SYSTEM/signer-1",
        actor_role="SIGNER",
    )
    await collect_mpc_signature(
        db, signer_ctx_1, settlement_id, "SIGNER_1",
    )

    signer_ctx_2 = AuditContext(
        request_id=ctx.request_id,
        trace_id=ctx.trace_id,
        actor="SYSTEM/signer-2",
        actor_role="SIGNER",
    )
    await collect_mpc_signature(
        db, signer_ctx_2, settlement_id, "SIGNER_2",
    )

    signed = await check_quorum_and_sign(
        db, ctx, settlement_id,
    )
    if not signed:
        raise RuntimeError(
            f"Quorum not reached for settlement "
            f"{settlement_id}"
        )

    await broadcast_settlement(db, ctx, settlement_id)
    block_number = await confirm_settlement(
        db, ctx, settlement_id,
    )

    return settlement_id, block_number


# =========================================================================
# STATE REBUILD — Deterministic replay and verification
# =========================================================================
async def rebuild_state(db):
    """Replay all event tables and compare against views.

    Returns a structured report with PASS/FAIL per section.
    """
    report = {}

    # 1. Share balances: replay share_ledger
    rows = await db.fetch(
        "SELECT fund_id, investor_id, entry_type, shares "
        "FROM share_ledger ORDER BY created_at",
    )
    replayed_shares = defaultdict(Decimal)
    for row in rows:
        key = (row["fund_id"], row["investor_id"])
        if row["entry_type"] == "CREDIT":
            replayed_shares[key] += row["shares"]
        else:
            replayed_shares[key] -= row["shares"]

    view_rows = await db.fetch(
        "SELECT fund_id, investor_id, share_balance "
        "FROM investor_share_balances",
    )
    view_shares = {
        (r["fund_id"], r["investor_id"]): r["share_balance"]
        for r in view_rows
    }
    share_mismatches = []
    for key in set(replayed_shares) | set(view_shares):
        expected = replayed_shares.get(key, Decimal("0"))
        actual = view_shares.get(key, Decimal("0"))
        if expected != actual:
            share_mismatches.append({
                "fund_id": str(key[0]),
                "investor_id": str(key[1]),
                "expected": str(expected),
                "actual": str(actual),
            })
    report["share_balances"] = {
        "status": "PASS" if not share_mismatches else "FAIL",
        "mismatches": share_mismatches,
    }

    # 2. Cash journals: verify balanced
    cash_rows = await db.fetch(
        "SELECT journal_id, "
        "SUM(CASE WHEN entry_type = 'DEBIT' "
        "    THEN amount ELSE 0 END) AS d, "
        "SUM(CASE WHEN entry_type = 'CREDIT' "
        "    THEN amount ELSE 0 END) AS c "
        "FROM cash_ledger GROUP BY journal_id",
    )
    cash_mismatches = []
    for row in cash_rows:
        if row["d"] != row["c"]:
            cash_mismatches.append({
                "journal_id": str(row["journal_id"]),
                "debit": str(row["d"]),
                "credit": str(row["c"]),
            })
    report["cash_journals"] = {
        "status": "PASS" if not cash_mismatches else "FAIL",
        "mismatches": cash_mismatches,
    }

    # 3. Order state machine: replay order_events
    order_ids = await db.fetch(
        "SELECT DISTINCT order_id FROM order_events",
    )
    order_mismatches = []
    valid_order_tx = {
        None: {"PENDING"},
        "PENDING": {
            "COMPLIANCE_CHECK", "REJECTED", "CANCELLED",
        },
        "COMPLIANCE_CHECK": {"APPROVED", "REJECTED"},
        "APPROVED": {"NAV_APPLIED", "FAILED", "CANCELLED"},
        "NAV_APPLIED": {"SETTLED", "FAILED"},
        "SETTLED": set(),
        "FAILED": set(),
        "CANCELLED": set(),
        "REJECTED": set(),
    }
    for oid_row in order_ids:
        oid = oid_row["order_id"]
        events = await db.fetch(
            "SELECT status FROM order_events "
            "WHERE order_id = $1 ORDER BY created_at",
            oid,
        )
        prev = None
        for ev in events:
            cur = ev["status"]
            if cur not in valid_order_tx.get(prev, set()):
                order_mismatches.append({
                    "order_id": str(oid),
                    "from": prev,
                    "to": cur,
                })
            prev = cur
    report["order_state_machine"] = {
        "status": "PASS" if not order_mismatches else "FAIL",
        "mismatches": order_mismatches,
    }

    # 4. Settlement state machine: replay settlement_events
    settlement_ids = await db.fetch(
        "SELECT DISTINCT settlement_id "
        "FROM settlement_events",
    )
    settlement_mismatches = []
    valid_settle_tx = {
        None: {"PENDING"},
        "PENDING": {"APPROVED", "FAILED"},
        "APPROVED": {"SIGNED", "FAILED"},
        "SIGNED": {"BROADCASTED", "FAILED"},
        "BROADCASTED": {"CONFIRMED", "FAILED"},
        "CONFIRMED": set(),
        "FAILED": set(),
    }
    for sid_row in settlement_ids:
        sid = sid_row["settlement_id"]
        events = await db.fetch(
            "SELECT status FROM settlement_events "
            "WHERE settlement_id = $1 "
            "ORDER BY created_at",
            sid,
        )
        prev = None
        for ev in events:
            cur = ev["status"]
            if cur not in valid_settle_tx.get(prev, set()):
                settlement_mismatches.append({
                    "settlement_id": str(sid),
                    "from": prev,
                    "to": cur,
                })
            prev = cur
    report["settlement_state_machine"] = {
        "status": (
            "PASS" if not settlement_mismatches else "FAIL"
        ),
        "mismatches": settlement_mismatches,
    }

    return report


# =========================================================================
# DEMO ORCHESTRATOR
# =========================================================================
async def main():
    logger.info("=" * 60)
    logger.info(
        "Tokenized Fund / NAV Service — "
        "Containerized Sandbox Demo"
    )
    logger.info("=" * 60)

    pool = await connect_db()
    db = AsyncpgDB(pool)
    ctx = AuditContext.system("fund-service")

    # Step 1: Create fund
    logger.info("[1/8] Creating tokenized fund...")
    fund_id = await create_fund(db, ctx)

    # Step 2: Register investors
    logger.info("[2/8] Registering investors...")
    investor_a = await register_investor(
        db, ctx,
        "Fidelity Investments",
        "INSTITUTIONAL",
        "549300FIDELITY0001",
        "US",
        "INV-FIDELITY-001",
    )
    investor_b = await register_investor(
        db, ctx,
        "Vanguard Group",
        "INSTITUTIONAL",
        "549300VANGUARD0001",
        "US",
        "INV-VANGUARD-001",
    )
    investor_c = await register_investor(
        db, ctx,
        "State Street Global Advisors",
        "INSTITUTIONAL",
        "549300SSGA00000001",
        "US",
        "INV-SSGA-001",
    )

    # Step 3: Publish initial NAV
    logger.info("[3/8] Publishing initial NAV...")
    nav_id, nav_per_share = await publish_nav(
        db, ctx, fund_id,
        nav_date=date(2026, 4, 7),
        total_assets=Decimal("100000000.00"),
        total_liabilities=Decimal("500000.00"),
        shares_outstanding=Decimal("1000000.00"),
    )

    # Step 4: Process subscriptions
    logger.info("[4/8] Processing subscriptions...")
    logger.info("  Fidelity subscribing $25,000,000...")
    await process_subscription(
        db, ctx, fund_id, investor_a,
        Decimal("25000000.00"), nav_per_share,
    )
    logger.info("  Vanguard subscribing $50,000,000...")
    await process_subscription(
        db, ctx, fund_id, investor_b,
        Decimal("50000000.00"), nav_per_share,
    )
    logger.info("  SSGA subscribing $15,000,000...")
    await process_subscription(
        db, ctx, fund_id, investor_c,
        Decimal("15000000.00"), nav_per_share,
    )

    # Step 5: Partial redemption
    logger.info("[5/8] Processing redemption...")
    fidelity_balance = await db.fetchval(
        "SELECT share_balance FROM investor_share_balances "
        "WHERE fund_id = $1 AND investor_id = $2",
        fund_id, investor_a,
    )
    redeem_shares = _q(fidelity_balance / Decimal("5"))
    logger.info(
        "  Fidelity redeeming %s shares (20%% of %s)...",
        redeem_shares, fidelity_balance,
    )
    await process_redemption(
        db, ctx, fund_id, investor_a,
        redeem_shares, nav_per_share,
    )

    # Step 6: Transfer shares
    logger.info("[6/8] Processing transfer...")
    transfer_shares = Decimal("50000.00")
    logger.info(
        "  Vanguard transferring %s shares to SSGA...",
        transfer_shares,
    )
    await process_transfer(
        db, ctx, fund_id,
        investor_b, investor_c,
        transfer_shares, nav_per_share,
    )

    # Step 7: Final state summary
    logger.info("[7/8] Final state summary...")
    logger.info("=" * 60)
    logger.info("FUND SUMMARY")
    logger.info("=" * 60)

    fund = await db.fetchrow(
        "SELECT * FROM funds WHERE id = $1", fund_id,
    )
    logger.info(
        "  Fund: %s (%s)", fund["fund_name"], fund["fund_ticker"],
    )
    logger.info("  Type: %s", fund["fund_type"])
    logger.info("  Status: %s", fund["status"])

    nav = await db.fetchrow(
        "SELECT * FROM fund_latest_nav WHERE fund_id = $1",
        fund_id,
    )
    if nav:
        logger.info(
            "  Latest NAV: $%s per share (date=%s)",
            nav["nav_per_share"], nav["nav_date"],
        )

    balances = await db.fetch(
        "SELECT isb.investor_id, i.investor_name, "
        "isb.share_balance, isb.cost_basis "
        "FROM investor_share_balances isb "
        "JOIN investors i ON i.id = isb.investor_id "
        "WHERE isb.fund_id = $1 "
        "AND i.investor_name NOT LIKE 'TREASURY/%%' "
        "ORDER BY isb.share_balance DESC",
        fund_id,
    )
    logger.info("  Investor Positions:")
    for b in balances:
        logger.info(
            "    %-35s %12s shares  (cost $%s)",
            b["investor_name"],
            b["share_balance"],
            b["cost_basis"],
        )

    total_shares = await db.fetchval(
        "SELECT total_shares FROM fund_shares_outstanding "
        "WHERE fund_id = $1",
        fund_id,
    )
    logger.info("  Total Shares Outstanding: %s", total_shares)

    # Show outbox events
    events = await db.fetch(
        "SELECT event_type, delivery_status "
        "FROM outbox_events_current "
        "ORDER BY created_at",
    )
    logger.info("Outbox events (%d):", len(events))
    for ev in events:
        logger.info(
            "  %-40s %s",
            ev["event_type"], ev["delivery_status"],
        )

    # Show order history
    orders = await db.fetch(
        "SELECT o.id, o.order_type, o.shares, o.amount, "
        "ocs.status "
        "FROM orders o "
        "JOIN order_current_status ocs ON ocs.order_id = o.id "
        "ORDER BY o.created_at",
    )
    logger.info("Orders (%d):", len(orders))
    for o in orders:
        logger.info(
            "  %-12s %12s shares  $%12s  [%s]",
            o["order_type"], o["shares"],
            o["amount"], o["status"],
        )

    # Settlement summary
    settlements = await db.fetch(
        "SELECT scs.settlement_id, scs.status, "
        "scs.tx_hash, scs.block_number, "
        "mqs.signature_count, mqs.quorum_reached "
        "FROM settlement_current_status scs "
        "JOIN mpc_quorum_status mqs "
        "ON mqs.settlement_id = scs.settlement_id "
        "ORDER BY scs.created_at",
    )
    logger.info("Settlements (%d):", len(settlements))
    for s in settlements:
        logger.info(
            "  %s  [%s]  tx=%s  block=%s  "
            "sigs=%s  quorum=%s",
            s["settlement_id"], s["status"],
            s["tx_hash"], s["block_number"],
            s["signature_count"], s["quorum_reached"],
        )

    # Step 8: State rebuild
    logger.info("[8/8] Running deterministic state rebuild...")
    report = await rebuild_state(db)
    logger.info("=" * 60)
    logger.info("STATE REBUILD REPORT")
    logger.info("=" * 60)
    for section, result in report.items():
        status = result["status"]
        mismatch_count = len(result["mismatches"])
        logger.info(
            "  %-30s %s (%d mismatches)",
            section, status, mismatch_count,
        )
        for m in result["mismatches"]:
            logger.info("    %s", m)

    logger.info("=" * 60)
    logger.info("DEMO COMPLETE")
    logger.info("=" * 60)

    await pool.close()
    logger.info("Fund service completed. Pool closed.")


if __name__ == "__main__":
    asyncio.run(main())
