-- =========================================================================
-- Read-Only Database User — Least-Privilege Access
-- =========================================================================
-- Only the fund-service writes to core tables via ledger_user.
-- All other services use readonly_user with targeted grants.
-- =========================================================================

CREATE USER readonly_user WITH PASSWORD 'readonly_pass';

-- SELECT on outbox (for polling unpublished events)
GRANT SELECT ON outbox_events TO readonly_user;
-- Column-level UPDATE: only published_at may be set
GRANT UPDATE (published_at) ON outbox_events TO readonly_user;

-- SELECT on all core tables and views
GRANT SELECT ON
    funds,
    fund_status_events,
    investors,
    investor_compliance_events,
    nav_publications,
    share_ledger,
    cash_ledger,
    orders,
    order_events,
    compliance_screenings,
    audit_events,
    rbac_roles,
    rbac_role_permissions,
    rbac_actor_roles,
    investor_share_balances,
    fund_shares_outstanding,
    order_current_status,
    outbox_events_current,
    fund_latest_nav
TO readonly_user;

-- INSERT on audit_events (all services can log)
GRANT INSERT ON audit_events TO readonly_user;

-- Reconciliation tables: full read + write
GRANT SELECT, INSERT, UPDATE
    ON reconciliation_runs TO readonly_user;
GRANT SELECT, INSERT
    ON reconciliation_mismatches TO readonly_user;

-- Dead letter queue: read + insert + limited update
GRANT SELECT, INSERT, UPDATE
    ON dead_letter_queue TO readonly_user;

-- Settlement tables and views
GRANT SELECT ON
    settlements,
    settlement_events,
    mpc_key_shares,
    mpc_signatures,
    settlement_current_status,
    mpc_quorum_status
TO readonly_user;

-- Processed events: consumer needs INSERT for idempotency
GRANT SELECT, INSERT
    ON processed_events TO readonly_user;
