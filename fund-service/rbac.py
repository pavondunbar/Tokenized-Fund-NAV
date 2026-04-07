"""
RBAC and audit utilities for the Tokenized Fund system.

Defense-in-depth: Python-side permission and state machine
checks mirror the database trigger enforcement.
"""
import uuid
from datetime import datetime, timezone

ROLE_PERMISSIONS = {
    "ADMIN": {"*"},
    "FUND_MANAGER": {
        "fund.create", "fund.manage",
        "nav.publish", "order.approve",
        "settlement.approve",
    },
    "TRANSFER_AGENT": {
        "order.process", "transfer.execute",
        "settlement.finalize",
    },
    "COMPLIANCE_OFFICER": {
        "compliance.screen", "investor.approve",
    },
    "SYSTEM": {
        "fund.create", "fund.manage",
        "nav.publish", "nav.calculate",
        "order.process", "order.approve",
        "compliance.screen", "investor.approve",
        "transfer.execute", "settlement.finalize",
        "reconciliation.run",
        "settlement.sign", "settlement.approve",
        "settlement.broadcast",
    },
    "NAV_CALCULATOR": {"nav.calculate", "nav.publish"},
    "INVESTOR": {"order.submit"},
    "SIGNER": {"settlement.sign"},
}

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


def check_permission(role, permission):
    """Raise PermissionError if role lacks the permission."""
    allowed = ROLE_PERMISSIONS.get(role, set())
    if "*" not in allowed and permission not in allowed:
        raise PermissionError(
            f"Role {role} lacks permission {permission}"
        )


def validate_order_transition(current, target):
    """Raise ValueError on invalid order status transition."""
    allowed = VALID_ORDER_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise ValueError(
            f"Invalid order transition: {current} -> {target}"
        )


def validate_settlement_transition(current, target):
    """Raise ValueError on invalid settlement status transition."""
    allowed = VALID_SETTLEMENT_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise ValueError(
            f"Invalid settlement transition: "
            f"{current} -> {target}"
        )


async def insert_audit_event(
    db, trace_id, actor, actor_role,
    action, resource_type, resource_id=None,
    details=None,
):
    """Insert an append-only audit event."""
    await db.execute(
        "INSERT INTO audit_events "
        "(id, trace_id, actor, actor_role, "
        "action, resource_type, resource_id, details) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        uuid.uuid4(), trace_id, actor, actor_role,
        action, resource_type, resource_id,
        details,
    )
