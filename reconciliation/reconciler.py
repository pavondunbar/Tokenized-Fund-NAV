"""
Reconciliation Engine — Independent balance verification.

Replays all share and cash ledger entries from scratch and
compares against the derived balance views. Also validates
that every order status transition followed the state machine.

This is the last line of defense: if the ledger has been
tampered with (which the triggers prevent), reconciliation
will detect it.

Trust domain: internal (only)
DB user: readonly_user
"""
import asyncio
import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("reconciliation")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "fund_ledger")
DB_USER = os.environ.get("DB_USER", "readonly_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "readonly_pass")
RECON_INTERVAL = int(os.environ.get("RECON_INTERVAL", "60"))

VALID_ORDER_TRANSITIONS = {
    None: {"PENDING"},
    "PENDING": {"COMPLIANCE_CHECK", "REJECTED", "CANCELLED"},
    "COMPLIANCE_CHECK": {"APPROVED", "REJECTED"},
    "APPROVED": {"NAV_APPLIED", "FAILED", "CANCELLED"},
    "NAV_APPLIED": {"SETTLED", "FAILED"},
    "SETTLED": set(),
    "FAILED": set(),
    "CANCELLED": set(),
    "REJECTED": set(),
}

VALID_SETTLEMENT_TRANSITIONS = {
    None: {"PENDING"},
    "PENDING": {"APPROVED", "FAILED"},
    "APPROVED": {"SIGNED", "FAILED"},
    "SIGNED": {"BROADCASTED", "FAILED"},
    "BROADCASTED": {"CONFIRMED", "FAILED"},
    "CONFIRMED": set(),
    "FAILED": set(),
}


def _dsn():
    return (
        f"postgresql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


async def run_share_balance_recon(conn, run_id):
    """Replay share ledger entries and compare to view."""
    mismatches = 0

    # Replay from raw ledger
    rows = await conn.fetch(
        "SELECT fund_id, investor_id, entry_type, shares "
        "FROM share_ledger "
        "ORDER BY created_at",
    )

    replayed = defaultdict(Decimal)
    for row in rows:
        key = (row["fund_id"], row["investor_id"])
        if row["entry_type"] == "CREDIT":
            replayed[key] += row["shares"]
        else:
            replayed[key] -= row["shares"]

    # Compare against view
    view_rows = await conn.fetch(
        "SELECT fund_id, investor_id, share_balance "
        "FROM investor_share_balances",
    )
    view_balances = {
        (r["fund_id"], r["investor_id"]): r["share_balance"]
        for r in view_rows
    }

    all_keys = set(replayed.keys()) | set(view_balances.keys())
    for key in all_keys:
        expected = replayed.get(key, Decimal("0"))
        actual = view_balances.get(key, Decimal("0"))
        if expected != actual:
            mismatches += 1
            await conn.execute(
                "INSERT INTO reconciliation_mismatches "
                "(id, run_id, mismatch_type, entity_type, "
                "entity_id, expected_value, actual_value, "
                "details) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                uuid.uuid4(), run_id,
                "SHARE_BALANCE_MISMATCH",
                "INVESTOR",
                key[1],
                expected, actual,
                json.dumps({
                    "fund_id": str(key[0]),
                    "investor_id": str(key[1]),
                }),
            )
            logger.warning(
                "MISMATCH: fund=%s investor=%s "
                "expected=%s actual=%s",
                key[0], key[1], expected, actual,
            )

    return mismatches


async def run_cash_balance_recon(conn, run_id):
    """Replay cash ledger entries and verify journal balance."""
    mismatches = 0

    # Check every journal_id has balanced entries
    rows = await conn.fetch(
        "SELECT journal_id, "
        "SUM(CASE WHEN entry_type = 'DEBIT' "
        "    THEN amount ELSE 0 END) AS total_debit, "
        "SUM(CASE WHEN entry_type = 'CREDIT' "
        "    THEN amount ELSE 0 END) AS total_credit "
        "FROM cash_ledger "
        "GROUP BY journal_id",
    )

    for row in rows:
        if row["total_debit"] != row["total_credit"]:
            mismatches += 1
            await conn.execute(
                "INSERT INTO reconciliation_mismatches "
                "(id, run_id, mismatch_type, entity_type, "
                "expected_value, actual_value, details) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7)",
                uuid.uuid4(), run_id,
                "CASH_JOURNAL_IMBALANCE",
                "CASH_JOURNAL",
                row["total_debit"],
                row["total_credit"],
                json.dumps({
                    "journal_id": str(row["journal_id"]),
                }),
            )
            logger.warning(
                "CASH IMBALANCE: journal=%s "
                "debit=%s credit=%s",
                row["journal_id"],
                row["total_debit"],
                row["total_credit"],
            )

    return mismatches


async def run_share_journal_recon(conn, run_id):
    """Verify every share journal is balanced."""
    mismatches = 0

    rows = await conn.fetch(
        "SELECT journal_id, "
        "SUM(CASE WHEN entry_type = 'DEBIT' "
        "    THEN shares ELSE 0 END) AS total_debit, "
        "SUM(CASE WHEN entry_type = 'CREDIT' "
        "    THEN shares ELSE 0 END) AS total_credit "
        "FROM share_ledger "
        "GROUP BY journal_id",
    )

    for row in rows:
        if row["total_debit"] != row["total_credit"]:
            mismatches += 1
            await conn.execute(
                "INSERT INTO reconciliation_mismatches "
                "(id, run_id, mismatch_type, entity_type, "
                "expected_value, actual_value, details) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7)",
                uuid.uuid4(), run_id,
                "SHARE_JOURNAL_IMBALANCE",
                "SHARE_JOURNAL",
                row["total_debit"],
                row["total_credit"],
                json.dumps({
                    "journal_id": str(row["journal_id"]),
                }),
            )

    return mismatches


async def run_order_state_machine_recon(conn, run_id):
    """Validate every order status transition."""
    mismatches = 0

    order_ids = await conn.fetch(
        "SELECT DISTINCT order_id FROM order_events",
    )

    for oid_row in order_ids:
        order_id = oid_row["order_id"]
        events = await conn.fetch(
            "SELECT status FROM order_events "
            "WHERE order_id = $1 "
            "ORDER BY created_at",
            order_id,
        )

        prev = None
        for event in events:
            current = event["status"]
            allowed = VALID_ORDER_TRANSITIONS.get(
                prev, set(),
            )
            if current not in allowed:
                mismatches += 1
                await conn.execute(
                    "INSERT INTO reconciliation_mismatches "
                    "(id, run_id, mismatch_type, "
                    "entity_type, entity_id, details) "
                    "VALUES ($1,$2,$3,$4,$5,$6)",
                    uuid.uuid4(), run_id,
                    "INVALID_ORDER_TRANSITION",
                    "ORDER", order_id,
                    json.dumps({
                        "from": prev,
                        "to": current,
                    }),
                )
                logger.warning(
                    "INVALID TRANSITION: order=%s %s -> %s",
                    order_id, prev, current,
                )
            prev = current

    return mismatches


async def run_settlement_state_machine_recon(conn, run_id):
    """Validate every settlement status transition."""
    mismatches = 0

    settlement_ids = await conn.fetch(
        "SELECT DISTINCT settlement_id "
        "FROM settlement_events",
    )

    for sid_row in settlement_ids:
        settlement_id = sid_row["settlement_id"]
        events = await conn.fetch(
            "SELECT status FROM settlement_events "
            "WHERE settlement_id = $1 "
            "ORDER BY created_at",
            settlement_id,
        )

        prev = None
        for event in events:
            current = event["status"]
            allowed = VALID_SETTLEMENT_TRANSITIONS.get(
                prev, set(),
            )
            if current not in allowed:
                mismatches += 1
                await conn.execute(
                    "INSERT INTO reconciliation_mismatches "
                    "(id, run_id, mismatch_type, "
                    "entity_type, entity_id, details) "
                    "VALUES ($1,$2,$3,$4,$5,$6)",
                    uuid.uuid4(), run_id,
                    "INVALID_SETTLEMENT_TRANSITION",
                    "SETTLEMENT", settlement_id,
                    json.dumps({
                        "from": prev,
                        "to": current,
                    }),
                )
                logger.warning(
                    "INVALID SETTLEMENT TRANSITION: "
                    "settlement=%s %s -> %s",
                    settlement_id, prev, current,
                )
            prev = current

    return mismatches


async def run_mpc_quorum_recon(conn, run_id):
    """Verify that SIGNED settlements have >= 2 signatures."""
    mismatches = 0

    signed_settlements = await conn.fetch(
        "SELECT DISTINCT se.settlement_id "
        "FROM settlement_events se "
        "WHERE se.status = 'SIGNED'",
    )

    for row in signed_settlements:
        settlement_id = row["settlement_id"]
        sig_count = await conn.fetchval(
            "SELECT COUNT(*) FROM mpc_signatures "
            "WHERE settlement_id = $1",
            settlement_id,
        )
        if sig_count < 2:
            mismatches += 1
            await conn.execute(
                "INSERT INTO reconciliation_mismatches "
                "(id, run_id, mismatch_type, "
                "entity_type, entity_id, details) "
                "VALUES ($1,$2,$3,$4,$5,$6)",
                uuid.uuid4(), run_id,
                "MPC_QUORUM_NOT_MET",
                "SETTLEMENT", settlement_id,
                json.dumps({
                    "signature_count": sig_count,
                    "required": 2,
                }),
            )
            logger.warning(
                "MPC QUORUM NOT MET: settlement=%s "
                "signatures=%d required=2",
                settlement_id, sig_count,
            )

    return mismatches


async def run_full_reconciliation(pool):
    """Execute all reconciliation checks."""
    run_id = uuid.uuid4()
    trace_id = uuid.uuid4()

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO reconciliation_runs "
            "(id, run_type, status, "
            "request_id, trace_id, actor, actor_role) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            run_id, "FULL", "RUNNING",
            uuid.uuid4(), trace_id,
            "SYSTEM/reconciliation", "SYSTEM",
        )

        total_checked = 0
        total_mismatches = 0

        # Share balance reconciliation
        m = await run_share_balance_recon(conn, run_id)
        total_mismatches += m
        total_checked += 1

        # Cash journal reconciliation
        m = await run_cash_balance_recon(conn, run_id)
        total_mismatches += m
        total_checked += 1

        # Share journal reconciliation
        m = await run_share_journal_recon(conn, run_id)
        total_mismatches += m
        total_checked += 1

        # Order state machine reconciliation
        m = await run_order_state_machine_recon(conn, run_id)
        total_mismatches += m
        total_checked += 1

        # Settlement state machine reconciliation
        m = await run_settlement_state_machine_recon(
            conn, run_id,
        )
        total_mismatches += m
        total_checked += 1

        # MPC quorum reconciliation
        m = await run_mpc_quorum_recon(conn, run_id)
        total_mismatches += m
        total_checked += 1

        status = "PASSED" if total_mismatches == 0 else "FAILED"
        await conn.execute(
            "UPDATE reconciliation_runs "
            "SET status = $1, total_checked = $2, "
            "mismatches = $3, completed_at = $4 "
            "WHERE id = $5",
            status, total_checked, total_mismatches,
            datetime.now(timezone.utc), run_id,
        )

        logger.info(
            "Reconciliation %s: checks=%d mismatches=%d",
            status, total_checked, total_mismatches,
        )

    return total_mismatches == 0


async def main():
    logger.info("Starting reconciliation engine...")

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

    # Wait for initial data to be populated
    logger.info(
        "Waiting %ds before first reconciliation...",
        RECON_INTERVAL,
    )
    await asyncio.sleep(RECON_INTERVAL)

    while True:
        try:
            await run_full_reconciliation(pool)
        except Exception:
            logger.exception("Reconciliation error")
        await asyncio.sleep(RECON_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
