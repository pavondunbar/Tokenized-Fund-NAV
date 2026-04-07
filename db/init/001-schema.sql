-- =========================================================================
-- Tokenized Fund / NAV System — Append-Only Ledger Schema
-- =========================================================================
-- Every table is INSERT-only. UPDATE and DELETE are denied by trigger.
-- Balances are DERIVED via views that replay ledger entries.
-- Double-spending is prevented by the transactional outbox pattern
-- combined with idempotency keys and FOR UPDATE SKIP LOCKED polling.
-- =========================================================================

-- 0. Immutability enforcement function
CREATE OR REPLACE FUNCTION deny_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'IMMUTABLE TABLE: % on %.% is denied',
        TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


-- =========================================================================
-- 1. FUNDS — Master fund registry
-- =========================================================================
CREATE TABLE funds (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_name       VARCHAR(256)    NOT NULL,
    fund_ticker     VARCHAR(20)     NOT NULL UNIQUE,
    fund_type       VARCHAR(50)     NOT NULL
                    CHECK (fund_type IN (
                        'OPEN_END', 'CLOSED_END', 'ETF',
                        'INTERVAL', 'MONEY_MARKET'
                    )),
    base_currency   VARCHAR(10)     NOT NULL DEFAULT 'USD',
    inception_date  DATE            NOT NULL,
    domicile        VARCHAR(10)     NOT NULL DEFAULT 'US',
    isin            VARCHAR(12),
    cusip           VARCHAR(9),
    management_fee_bps  INTEGER     NOT NULL DEFAULT 0,
    status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN (
                        'ACTIVE', 'SUSPENDED', 'LIQUIDATING', 'TERMINATED'
                    )),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(256)    NOT NULL UNIQUE,

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE TRIGGER trg_deny_update_funds
    BEFORE UPDATE ON funds FOR EACH ROW EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_funds
    BEFORE DELETE ON funds FOR EACH ROW EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 2. FUND STATUS EVENTS — Append-only status history
-- =========================================================================
CREATE TABLE fund_status_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id         UUID            NOT NULL REFERENCES funds(id),
    status          VARCHAR(20)     NOT NULL
                    CHECK (status IN (
                        'ACTIVE', 'SUSPENDED', 'LIQUIDATING', 'TERMINATED'
                    )),
    reason          TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_fund_status_events_fund ON fund_status_events (fund_id);

CREATE TRIGGER trg_deny_update_fund_status_events
    BEFORE UPDATE ON fund_status_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_fund_status_events
    BEFORE DELETE ON fund_status_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 3. INVESTORS — KYC-verified investor registry
-- =========================================================================
CREATE TABLE investors (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    investor_name   VARCHAR(256)    NOT NULL,
    investor_type   VARCHAR(50)     NOT NULL
                    CHECK (investor_type IN (
                        'INDIVIDUAL', 'INSTITUTIONAL',
                        'QUALIFIED_PURCHASER', 'ACCREDITED'
                    )),
    lei             VARCHAR(20),
    tax_id_hash     VARCHAR(64),
    domicile        VARCHAR(10)     NOT NULL,
    kyc_status      VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
                    CHECK (kyc_status IN (
                        'PENDING', 'APPROVED', 'REJECTED', 'EXPIRED'
                    )),
    aml_status      VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
                    CHECK (aml_status IN (
                        'PENDING', 'CLEARED', 'FLAGGED', 'BLOCKED'
                    )),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(256)    NOT NULL UNIQUE,

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE TRIGGER trg_deny_update_investors
    BEFORE UPDATE ON investors FOR EACH ROW EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_investors
    BEFORE DELETE ON investors FOR EACH ROW EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 4. INVESTOR COMPLIANCE EVENTS — KYC/AML status changes
-- =========================================================================
CREATE TABLE investor_compliance_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    investor_id     UUID            NOT NULL REFERENCES investors(id),
    event_type      VARCHAR(20)     NOT NULL
                    CHECK (event_type IN ('KYC', 'AML')),
    old_status      VARCHAR(20)     NOT NULL,
    new_status      VARCHAR(20)     NOT NULL,
    screening_ref   VARCHAR(256),
    reason          TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_investor_compliance_investor
    ON investor_compliance_events (investor_id);

CREATE TRIGGER trg_deny_update_investor_compliance
    BEFORE UPDATE ON investor_compliance_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_investor_compliance
    BEFORE DELETE ON investor_compliance_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 5. NAV PUBLICATIONS — Immutable NAV snapshots
-- =========================================================================
CREATE TABLE nav_publications (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id         UUID            NOT NULL REFERENCES funds(id),
    nav_date        DATE            NOT NULL,
    total_assets    NUMERIC(28,8)   NOT NULL,
    total_liabilities NUMERIC(28,8) NOT NULL DEFAULT 0,
    net_asset_value NUMERIC(28,8)   NOT NULL,
    shares_outstanding NUMERIC(28,8) NOT NULL,
    nav_per_share   NUMERIC(28,8)   NOT NULL,
    pricing_source  VARCHAR(100)    NOT NULL,
    is_official     BOOLEAN         NOT NULL DEFAULT FALSE,
    published_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(256)    NOT NULL UNIQUE,

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_nav_pub_fund_date
    ON nav_publications (fund_id, nav_date);

CREATE TRIGGER trg_deny_update_nav_publications
    BEFORE UPDATE ON nav_publications FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_nav_publications
    BEFORE DELETE ON nav_publications FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 6. SHARE LEDGER — Append-only share movements (double-entry)
-- =========================================================================
-- Every subscription, redemption, or transfer produces TWO entries:
--   one DEBIT (source) and one CREDIT (destination).
-- Balances are derived by SUM(credit) - SUM(debit) per investor+fund.
-- =========================================================================
CREATE TABLE share_ledger (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id         UUID            NOT NULL REFERENCES funds(id),
    investor_id     UUID            NOT NULL REFERENCES investors(id),
    journal_id      UUID            NOT NULL,
    entry_type      VARCHAR(10)     NOT NULL
                    CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    shares          NUMERIC(28,8)   NOT NULL CHECK (shares > 0),
    nav_per_share   NUMERIC(28,8)   NOT NULL,
    amount          NUMERIC(28,8)   NOT NULL,
    currency        VARCHAR(10)     NOT NULL DEFAULT 'USD',
    reason          VARCHAR(50)     NOT NULL
                    CHECK (reason IN (
                        'SUBSCRIPTION', 'REDEMPTION',
                        'TRANSFER_IN', 'TRANSFER_OUT',
                        'DIVIDEND_REINVEST', 'FEE_DEDUCTION',
                        'INITIAL_SEED'
                    )),
    order_id        UUID,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_share_ledger_fund_investor
    ON share_ledger (fund_id, investor_id);
CREATE INDEX idx_share_ledger_journal
    ON share_ledger (journal_id);

CREATE TRIGGER trg_deny_update_share_ledger
    BEFORE UPDATE ON share_ledger FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_share_ledger
    BEFORE DELETE ON share_ledger FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 6a. DEFERRED CONSTRAINT: Journal balance check
-- =========================================================================
-- At COMMIT, verify each journal_id has balanced debits = credits.
-- =========================================================================
CREATE OR REPLACE FUNCTION check_share_journal_balance()
RETURNS TRIGGER AS $$
DECLARE
    _debit  NUMERIC;
    _credit NUMERIC;
BEGIN
    SELECT
        COALESCE(SUM(CASE WHEN entry_type = 'DEBIT'
                          THEN shares ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN entry_type = 'CREDIT'
                          THEN shares ELSE 0 END), 0)
    INTO _debit, _credit
    FROM share_ledger
    WHERE journal_id = NEW.journal_id;

    IF _debit <> _credit THEN
        RAISE EXCEPTION
            'UNBALANCED JOURNAL %: debit=% credit=%',
            NEW.journal_id, _debit, _credit;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_check_share_journal_balance
    AFTER INSERT ON share_ledger
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION check_share_journal_balance();


-- =========================================================================
-- 7. CASH LEDGER — Append-only cash movements
-- =========================================================================
CREATE TABLE cash_ledger (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id         UUID            NOT NULL REFERENCES funds(id),
    investor_id     UUID            NOT NULL REFERENCES investors(id),
    journal_id      UUID            NOT NULL,
    entry_type      VARCHAR(10)     NOT NULL
                    CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount          NUMERIC(28,8)   NOT NULL CHECK (amount > 0),
    currency        VARCHAR(10)     NOT NULL DEFAULT 'USD',
    reason          VARCHAR(50)     NOT NULL
                    CHECK (reason IN (
                        'SUBSCRIPTION_PAYMENT',
                        'REDEMPTION_PAYOUT',
                        'DIVIDEND_PAYMENT',
                        'FEE_COLLECTION',
                        'TRANSFER_SETTLEMENT',
                        'INITIAL_SEED'
                    )),
    order_id        UUID,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_cash_ledger_fund_investor
    ON cash_ledger (fund_id, investor_id);

CREATE TRIGGER trg_deny_update_cash_ledger
    BEFORE UPDATE ON cash_ledger FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_cash_ledger
    BEFORE DELETE ON cash_ledger FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 7a. DEFERRED CONSTRAINT: Cash journal balance check
-- =========================================================================
CREATE OR REPLACE FUNCTION check_cash_journal_balance()
RETURNS TRIGGER AS $$
DECLARE
    _debit  NUMERIC;
    _credit NUMERIC;
BEGIN
    SELECT
        COALESCE(SUM(CASE WHEN entry_type = 'DEBIT'
                          THEN amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN entry_type = 'CREDIT'
                          THEN amount ELSE 0 END), 0)
    INTO _debit, _credit
    FROM cash_ledger
    WHERE journal_id = NEW.journal_id;

    IF _debit <> _credit THEN
        RAISE EXCEPTION
            'UNBALANCED CASH JOURNAL %: debit=% credit=%',
            NEW.journal_id, _debit, _credit;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_check_cash_journal_balance
    AFTER INSERT ON cash_ledger
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION check_cash_journal_balance();


-- =========================================================================
-- 8. ORDERS — Subscription / Redemption / Transfer orders
-- =========================================================================
CREATE TABLE orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id         UUID            NOT NULL REFERENCES funds(id),
    investor_id     UUID            NOT NULL REFERENCES investors(id),
    order_type      VARCHAR(20)     NOT NULL
                    CHECK (order_type IN (
                        'SUBSCRIPTION', 'REDEMPTION', 'TRANSFER'
                    )),
    shares          NUMERIC(28,8),
    amount          NUMERIC(28,8),
    currency        VARCHAR(10)     NOT NULL DEFAULT 'USD',
    nav_per_share   NUMERIC(28,8),
    counterparty_investor_id UUID
                    REFERENCES investors(id),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(256)    NOT NULL UNIQUE,

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_orders_fund ON orders (fund_id);
CREATE INDEX idx_orders_investor ON orders (investor_id);

CREATE TRIGGER trg_deny_update_orders
    BEFORE UPDATE ON orders FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_orders
    BEFORE DELETE ON orders FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 9. ORDER EVENTS — Status progression for orders
-- =========================================================================
CREATE TABLE order_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID            NOT NULL REFERENCES orders(id),
    status          VARCHAR(30)     NOT NULL
                    CHECK (status IN (
                        'PENDING', 'COMPLIANCE_CHECK',
                        'APPROVED', 'REJECTED',
                        'NAV_APPLIED', 'SETTLED', 'FAILED',
                        'CANCELLED'
                    )),
    reason          TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_order_events_order ON order_events (order_id);

CREATE TRIGGER trg_deny_update_order_events
    BEFORE UPDATE ON order_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_order_events
    BEFORE DELETE ON order_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 10. ORDER STATUS STATE MACHINE (trigger-enforced)
-- =========================================================================
CREATE OR REPLACE FUNCTION enforce_order_status_transition()
RETURNS TRIGGER AS $$
DECLARE
    _current VARCHAR(30);
BEGIN
    SELECT status INTO _current
    FROM order_events
    WHERE order_id = NEW.order_id
    ORDER BY created_at DESC
    LIMIT 1;

    -- First event must be PENDING
    IF _current IS NULL THEN
        IF NEW.status <> 'PENDING' THEN
            RAISE EXCEPTION
                'First order event must be PENDING, got %',
                NEW.status;
        END IF;
        RETURN NEW;
    END IF;

    -- Terminal states block further transitions
    IF _current IN ('SETTLED', 'FAILED', 'CANCELLED',
                     'REJECTED') THEN
        RAISE EXCEPTION
            'Order % is in terminal state %',
            NEW.order_id, _current;
    END IF;

    -- Valid transitions
    IF NOT (
        (_current = 'PENDING'
            AND NEW.status IN (
                'COMPLIANCE_CHECK', 'REJECTED', 'CANCELLED'
            ))
        OR (_current = 'COMPLIANCE_CHECK'
            AND NEW.status IN (
                'APPROVED', 'REJECTED'
            ))
        OR (_current = 'APPROVED'
            AND NEW.status IN (
                'NAV_APPLIED', 'FAILED', 'CANCELLED'
            ))
        OR (_current = 'NAV_APPLIED'
            AND NEW.status IN (
                'SETTLED', 'FAILED'
            ))
    ) THEN
        RAISE EXCEPTION
            'Invalid order transition: % -> %',
            _current, NEW.status;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_enforce_order_status
    BEFORE INSERT ON order_events
    FOR EACH ROW
    EXECUTE FUNCTION enforce_order_status_transition();


-- =========================================================================
-- 11. COMPLIANCE SCREENINGS
-- =========================================================================
CREATE TABLE compliance_screenings (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID            NOT NULL REFERENCES orders(id),
    investor_id     UUID            NOT NULL REFERENCES investors(id),
    screening_type  VARCHAR(20)     NOT NULL
                    CHECK (screening_type IN (
                        'KYC', 'AML', 'ACCREDITATION',
                        'SANCTIONS', 'PEP'
                    )),
    result          VARCHAR(20)     NOT NULL
                    CHECK (result IN (
                        'PASS', 'FAIL', 'REVIEW_REQUIRED'
                    )),
    provider        VARCHAR(100)    NOT NULL,
    screening_ref   VARCHAR(256),
    details         JSONB,
    screened_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_compliance_order
    ON compliance_screenings (order_id);

CREATE TRIGGER trg_deny_update_compliance_screenings
    BEFORE UPDATE ON compliance_screenings FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_compliance_screenings
    BEFORE DELETE ON compliance_screenings FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 11a. SETTLEMENTS — Blockchain settlement lifecycle
-- =========================================================================
CREATE TABLE settlements (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID            NOT NULL REFERENCES orders(id),
    fund_id         UUID            NOT NULL REFERENCES funds(id),
    settlement_type VARCHAR(20)     NOT NULL
                    CHECK (settlement_type IN (
                        'SUBSCRIPTION', 'REDEMPTION', 'TRANSFER'
                    )),
    tx_hash         VARCHAR(66),
    block_number    BIGINT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(256)    NOT NULL UNIQUE,

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_settlements_order ON settlements (order_id);
CREATE INDEX idx_settlements_fund ON settlements (fund_id);

CREATE TRIGGER trg_deny_update_settlements
    BEFORE UPDATE ON settlements FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_settlements
    BEFORE DELETE ON settlements FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 11b. SETTLEMENT EVENTS — Event-sourced settlement status
-- =========================================================================
CREATE TABLE settlement_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    settlement_id   UUID            NOT NULL REFERENCES settlements(id),
    status          VARCHAR(20)     NOT NULL
                    CHECK (status IN (
                        'PENDING', 'APPROVED', 'SIGNED',
                        'BROADCASTED', 'CONFIRMED', 'FAILED'
                    )),
    reason          TEXT,
    tx_hash         VARCHAR(66),
    block_number    BIGINT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_settlement_events_settlement
    ON settlement_events (settlement_id);

CREATE TRIGGER trg_deny_update_settlement_events
    BEFORE UPDATE ON settlement_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_settlement_events
    BEFORE DELETE ON settlement_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 11c. SETTLEMENT STATUS STATE MACHINE (trigger-enforced)
-- =========================================================================
CREATE OR REPLACE FUNCTION enforce_settlement_status_transition()
RETURNS TRIGGER AS $$
DECLARE
    _current VARCHAR(20);
BEGIN
    SELECT status INTO _current
    FROM settlement_events
    WHERE settlement_id = NEW.settlement_id
    ORDER BY created_at DESC
    LIMIT 1;

    -- First event must be PENDING
    IF _current IS NULL THEN
        IF NEW.status <> 'PENDING' THEN
            RAISE EXCEPTION
                'First settlement event must be PENDING, got %',
                NEW.status;
        END IF;
        RETURN NEW;
    END IF;

    -- Terminal states block further transitions
    IF _current IN ('CONFIRMED', 'FAILED') THEN
        RAISE EXCEPTION
            'Settlement % is in terminal state %',
            NEW.settlement_id, _current;
    END IF;

    -- Valid transitions
    IF NOT (
        (_current = 'PENDING'
            AND NEW.status IN ('APPROVED', 'FAILED'))
        OR (_current = 'APPROVED'
            AND NEW.status IN ('SIGNED', 'FAILED'))
        OR (_current = 'SIGNED'
            AND NEW.status IN ('BROADCASTED', 'FAILED'))
        OR (_current = 'BROADCASTED'
            AND NEW.status IN ('CONFIRMED', 'FAILED'))
    ) THEN
        RAISE EXCEPTION
            'Invalid settlement transition: % -> %',
            _current, NEW.status;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_enforce_settlement_status
    BEFORE INSERT ON settlement_events
    FOR EACH ROW
    EXECUTE FUNCTION enforce_settlement_status_transition();


-- =========================================================================
-- 11d. MPC KEY SHARES — Signer registry
-- =========================================================================
CREATE TABLE mpc_key_shares (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signer_id       VARCHAR(50)     NOT NULL UNIQUE,
    signer_name     VARCHAR(256)    NOT NULL,
    public_key      VARCHAR(256)    NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'REVOKED')),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_deny_update_mpc_key_shares
    BEFORE UPDATE ON mpc_key_shares FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_mpc_key_shares
    BEFORE DELETE ON mpc_key_shares FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 11e. MPC SIGNATURES — Individual signer approvals
-- =========================================================================
CREATE TABLE mpc_signatures (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    settlement_id   UUID            NOT NULL REFERENCES settlements(id),
    signer_id       VARCHAR(50)     NOT NULL
                    REFERENCES mpc_key_shares(signer_id),
    signature_share VARCHAR(256)    NOT NULL,
    signed_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL,

    UNIQUE (settlement_id, signer_id)
);

CREATE INDEX idx_mpc_signatures_settlement
    ON mpc_signatures (settlement_id);

CREATE TRIGGER trg_deny_update_mpc_signatures
    BEFORE UPDATE ON mpc_signatures FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_mpc_signatures
    BEFORE DELETE ON mpc_signatures FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 11f. PROCESSED EVENTS — Consumer idempotency deduplication
-- =========================================================================
CREATE TABLE processed_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID            NOT NULL UNIQUE,
    event_type      VARCHAR(100)    NOT NULL,
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_deny_update_processed_events
    BEFORE UPDATE ON processed_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_processed_events
    BEFORE DELETE ON processed_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 12. OUTBOX EVENTS — Transactional outbox for Kafka publishing
-- =========================================================================
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id    UUID            NOT NULL,
    aggregate_type  VARCHAR(50)     NOT NULL,
    event_type      VARCHAR(100)    NOT NULL,
    payload         JSONB           NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_outbox_unpublished
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

-- Outbox is special: published_at is the ONLY mutable column.
-- We use a targeted trigger that allows UPDATE only on published_at.
CREATE OR REPLACE FUNCTION deny_outbox_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'IMMUTABLE TABLE: DELETE on outbox_events denied';
    END IF;
    -- Allow UPDATE only if it touches published_at alone
    IF TG_OP = 'UPDATE' THEN
        IF NEW.id              IS DISTINCT FROM OLD.id
        OR NEW.aggregate_id    IS DISTINCT FROM OLD.aggregate_id
        OR NEW.aggregate_type  IS DISTINCT FROM OLD.aggregate_type
        OR NEW.event_type      IS DISTINCT FROM OLD.event_type
        OR NEW.payload         IS DISTINCT FROM OLD.payload
        OR NEW.created_at      IS DISTINCT FROM OLD.created_at
        OR NEW.request_id      IS DISTINCT FROM OLD.request_id
        OR NEW.trace_id        IS DISTINCT FROM OLD.trace_id
        OR NEW.actor           IS DISTINCT FROM OLD.actor
        OR NEW.actor_role      IS DISTINCT FROM OLD.actor_role
        THEN
            RAISE EXCEPTION
                'outbox_events: only published_at may be updated';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_deny_outbox_mutation
    BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION deny_outbox_mutation();


-- =========================================================================
-- 13. AUDIT EVENTS — Append-only audit log
-- =========================================================================
CREATE TABLE audit_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL,
    action          VARCHAR(100)    NOT NULL,
    resource_type   VARCHAR(50)     NOT NULL,
    resource_id     UUID,
    details         JSONB,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_trace ON audit_events (trace_id);
CREATE INDEX idx_audit_resource
    ON audit_events (resource_type, resource_id);

CREATE TRIGGER trg_deny_update_audit_events
    BEFORE UPDATE ON audit_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_audit_events
    BEFORE DELETE ON audit_events FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 14. RECONCILIATION TABLES
-- =========================================================================
CREATE TABLE reconciliation_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type        VARCHAR(30)     NOT NULL
                    CHECK (run_type IN (
                        'SHARE_BALANCE', 'CASH_BALANCE',
                        'NAV_CONSISTENCY', 'FULL'
                    )),
    status          VARCHAR(20)     NOT NULL DEFAULT 'RUNNING'
                    CHECK (status IN (
                        'RUNNING', 'PASSED', 'FAILED'
                    )),
    total_checked   INTEGER         NOT NULL DEFAULT 0,
    mismatches      INTEGER         NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,

    request_id      UUID            NOT NULL,
    trace_id        UUID            NOT NULL,
    actor           VARCHAR(256)    NOT NULL,
    actor_role      VARCHAR(50)     NOT NULL
);

-- Reconciliation runs are special: status and completed_at can
-- transition from RUNNING to a terminal state exactly once.
CREATE OR REPLACE FUNCTION guard_recon_run_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status <> 'RUNNING' THEN
        RAISE EXCEPTION
            'Reconciliation run % already terminal: %',
            OLD.id, OLD.status;
    END IF;
    IF NEW.status = 'RUNNING' THEN
        RAISE EXCEPTION
            'Cannot transition recon run to RUNNING';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_guard_recon_run_update
    BEFORE UPDATE ON reconciliation_runs
    FOR EACH ROW EXECUTE FUNCTION guard_recon_run_update();

CREATE TRIGGER trg_deny_delete_recon_runs
    BEFORE DELETE ON reconciliation_runs FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();

CREATE TABLE reconciliation_mismatches (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID            NOT NULL
                    REFERENCES reconciliation_runs(id),
    mismatch_type   VARCHAR(50)     NOT NULL,
    entity_type     VARCHAR(50)     NOT NULL,
    entity_id       UUID,
    expected_value  NUMERIC(28,8),
    actual_value    NUMERIC(28,8),
    details         JSONB,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_deny_update_recon_mismatches
    BEFORE UPDATE ON reconciliation_mismatches FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_recon_mismatches
    BEFORE DELETE ON reconciliation_mismatches FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 15. DEAD LETTER QUEUE
-- =========================================================================
CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_table    VARCHAR(50)     NOT NULL,
    source_id       UUID            NOT NULL,
    event_type      VARCHAR(100)    NOT NULL,
    payload         JSONB           NOT NULL,
    error_message   TEXT,
    retry_count     INTEGER         NOT NULL DEFAULT 0,
    last_retry_at   TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- DLQ allows limited updates (retry tracking only)
CREATE OR REPLACE FUNCTION guard_dlq_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.id             IS DISTINCT FROM OLD.id
    OR NEW.source_table   IS DISTINCT FROM OLD.source_table
    OR NEW.source_id      IS DISTINCT FROM OLD.source_id
    OR NEW.event_type     IS DISTINCT FROM OLD.event_type
    OR NEW.payload        IS DISTINCT FROM OLD.payload
    OR NEW.created_at     IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION
            'dead_letter_queue: only retry/resolved fields '
            'may be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_guard_dlq_update
    BEFORE UPDATE ON dead_letter_queue
    FOR EACH ROW EXECUTE FUNCTION guard_dlq_update();

CREATE TRIGGER trg_deny_delete_dlq
    BEFORE DELETE ON dead_letter_queue FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 16. RBAC TABLES
-- =========================================================================
CREATE TABLE rbac_roles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    role_name   VARCHAR(50)     NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_deny_update_rbac_roles
    BEFORE UPDATE ON rbac_roles FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_rbac_roles
    BEFORE DELETE ON rbac_roles FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();

CREATE TABLE rbac_role_permissions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    role_id     UUID            NOT NULL REFERENCES rbac_roles(id),
    permission  VARCHAR(100)    NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (role_id, permission)
);

CREATE INDEX idx_rbac_perms_role
    ON rbac_role_permissions (role_id);

CREATE TRIGGER trg_deny_update_rbac_perms
    BEFORE UPDATE ON rbac_role_permissions FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_rbac_perms
    BEFORE DELETE ON rbac_role_permissions FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();

CREATE TABLE rbac_actor_roles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor       VARCHAR(256)    NOT NULL,
    role_id     UUID            NOT NULL REFERENCES rbac_roles(id),
    granted_by  VARCHAR(256),
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (actor, role_id)
);

CREATE INDEX idx_rbac_actor_roles_actor
    ON rbac_actor_roles (actor);

CREATE TRIGGER trg_deny_update_rbac_actor_roles
    BEFORE UPDATE ON rbac_actor_roles FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();
CREATE TRIGGER trg_deny_delete_rbac_actor_roles
    BEFORE DELETE ON rbac_actor_roles FOR EACH ROW
    EXECUTE FUNCTION deny_mutation();


-- =========================================================================
-- 17. DERIVED VIEWS — Balances computed from ledger replay
-- =========================================================================

-- 17a. Share balances per investor per fund
CREATE VIEW investor_share_balances AS
SELECT
    fund_id,
    investor_id,
    SUM(CASE WHEN entry_type = 'CREDIT'
             THEN shares ELSE 0 END)
    - SUM(CASE WHEN entry_type = 'DEBIT'
               THEN shares ELSE 0 END)
        AS share_balance,
    SUM(CASE WHEN entry_type = 'CREDIT'
             THEN amount ELSE 0 END)
    - SUM(CASE WHEN entry_type = 'DEBIT'
               THEN amount ELSE 0 END)
        AS cost_basis
FROM share_ledger
GROUP BY fund_id, investor_id;

-- 17b. Total shares outstanding per fund
-- Excludes treasury accounts: in double-entry, treasury entries
-- offset real investor entries, netting to zero if included.
-- Shares outstanding = shares held by real investors only.
CREATE VIEW fund_shares_outstanding AS
SELECT
    sl.fund_id,
    SUM(CASE WHEN sl.entry_type = 'CREDIT'
             THEN sl.shares ELSE 0 END)
    - SUM(CASE WHEN sl.entry_type = 'DEBIT'
               THEN sl.shares ELSE 0 END)
        AS total_shares
FROM share_ledger sl
JOIN investors i ON i.id = sl.investor_id
WHERE i.investor_name NOT LIKE 'TREASURY/%'
GROUP BY sl.fund_id;

-- 17c. Current order status
CREATE VIEW order_current_status AS
SELECT DISTINCT ON (order_id)
    order_id, status, reason, created_at
FROM order_events
ORDER BY order_id, created_at DESC;

-- 17d. Current outbox delivery status
CREATE VIEW outbox_events_current AS
SELECT
    id, aggregate_id, aggregate_type,
    event_type, payload, created_at, published_at,
    CASE
        WHEN published_at IS NOT NULL THEN 'DELIVERED'
        ELSE 'PENDING'
    END AS delivery_status
FROM outbox_events;

-- 17e. Latest NAV per fund
CREATE VIEW fund_latest_nav AS
SELECT DISTINCT ON (fund_id)
    fund_id, nav_date, nav_per_share,
    total_assets, total_liabilities,
    net_asset_value, shares_outstanding,
    is_official, published_at
FROM nav_publications
WHERE is_official = TRUE
ORDER BY fund_id, nav_date DESC;

-- 17f. Current settlement status
CREATE VIEW settlement_current_status AS
SELECT DISTINCT ON (settlement_id)
    settlement_id, status, reason, tx_hash,
    block_number, created_at
FROM settlement_events
ORDER BY settlement_id, created_at DESC;

-- 17g. MPC quorum status per settlement
CREATE VIEW mpc_quorum_status AS
SELECT
    s.id AS settlement_id,
    s.order_id,
    COUNT(ms.id) AS signature_count,
    COUNT(ms.id) >= 2 AS quorum_reached
FROM settlements s
LEFT JOIN mpc_signatures ms ON ms.settlement_id = s.id
GROUP BY s.id, s.order_id;


-- =========================================================================
-- 18. RBAC SEED DATA
-- =========================================================================

-- Roles
INSERT INTO rbac_roles (id, role_name, description) VALUES
    ('00000000-0000-0000-0000-000000000001', 'ADMIN',
     'Full system access'),
    ('00000000-0000-0000-0000-000000000002', 'FUND_MANAGER',
     'Can create funds, publish NAV, manage orders'),
    ('00000000-0000-0000-0000-000000000003', 'TRANSFER_AGENT',
     'Can process subscriptions, redemptions, transfers'),
    ('00000000-0000-0000-0000-000000000004', 'COMPLIANCE_OFFICER',
     'Can run KYC/AML screenings, approve investors'),
    ('00000000-0000-0000-0000-000000000005', 'SYSTEM',
     'Internal system operations'),
    ('00000000-0000-0000-0000-000000000006', 'NAV_CALCULATOR',
     'Can compute and publish NAV'),
    ('00000000-0000-0000-0000-000000000007', 'INVESTOR',
     'Can submit subscription/redemption orders'),
    ('00000000-0000-0000-0000-000000000008', 'SIGNER',
     'MPC key share holder for settlement signing');

-- Permissions
INSERT INTO rbac_role_permissions (role_id, permission) VALUES
    ('00000000-0000-0000-0000-000000000001', '*'),
    ('00000000-0000-0000-0000-000000000002', 'fund.create'),
    ('00000000-0000-0000-0000-000000000002', 'fund.manage'),
    ('00000000-0000-0000-0000-000000000002', 'nav.publish'),
    ('00000000-0000-0000-0000-000000000002', 'order.approve'),
    ('00000000-0000-0000-0000-000000000003', 'order.process'),
    ('00000000-0000-0000-0000-000000000003', 'transfer.execute'),
    ('00000000-0000-0000-0000-000000000003', 'settlement.finalize'),
    ('00000000-0000-0000-0000-000000000004', 'compliance.screen'),
    ('00000000-0000-0000-0000-000000000004', 'investor.approve'),
    ('00000000-0000-0000-0000-000000000005', 'fund.create'),
    ('00000000-0000-0000-0000-000000000005', 'fund.manage'),
    ('00000000-0000-0000-0000-000000000005', 'nav.publish'),
    ('00000000-0000-0000-0000-000000000005', 'order.process'),
    ('00000000-0000-0000-0000-000000000005', 'order.approve'),
    ('00000000-0000-0000-0000-000000000005', 'compliance.screen'),
    ('00000000-0000-0000-0000-000000000005', 'investor.approve'),
    ('00000000-0000-0000-0000-000000000005', 'transfer.execute'),
    ('00000000-0000-0000-0000-000000000005', 'settlement.finalize'),
    ('00000000-0000-0000-0000-000000000005', 'reconciliation.run'),
    ('00000000-0000-0000-0000-000000000006', 'nav.calculate'),
    ('00000000-0000-0000-0000-000000000006', 'nav.publish'),
    ('00000000-0000-0000-0000-000000000007', 'order.submit'),
    ('00000000-0000-0000-0000-000000000005', 'settlement.sign'),
    ('00000000-0000-0000-0000-000000000005', 'settlement.approve'),
    ('00000000-0000-0000-0000-000000000005', 'settlement.broadcast'),
    ('00000000-0000-0000-0000-000000000002', 'settlement.approve'),
    ('00000000-0000-0000-0000-000000000008', 'settlement.sign');

-- Actor-role assignments
INSERT INTO rbac_actor_roles (actor, role_id, granted_by) VALUES
    ('SYSTEM/seed',
     '00000000-0000-0000-0000-000000000005', 'BOOTSTRAP'),
    ('SYSTEM/fund-service',
     '00000000-0000-0000-0000-000000000005', 'BOOTSTRAP'),
    ('SYSTEM/nav-engine',
     '00000000-0000-0000-0000-000000000006', 'BOOTSTRAP'),
    ('SYSTEM/transfer-agent',
     '00000000-0000-0000-0000-000000000003', 'BOOTSTRAP'),
    ('SYSTEM/compliance',
     '00000000-0000-0000-0000-000000000004', 'BOOTSTRAP'),
    ('SYSTEM/reconciliation',
     '00000000-0000-0000-0000-000000000005', 'BOOTSTRAP'),
    ('SYSTEM/signer-1',
     '00000000-0000-0000-0000-000000000008', 'BOOTSTRAP'),
    ('SYSTEM/signer-2',
     '00000000-0000-0000-0000-000000000008', 'BOOTSTRAP'),
    ('SYSTEM/signer-3',
     '00000000-0000-0000-0000-000000000008', 'BOOTSTRAP');

-- MPC Key Shares — 3 signers for 2-of-3 quorum
INSERT INTO mpc_key_shares
    (signer_id, signer_name, public_key, status)
VALUES
    ('SIGNER_1', 'Primary HSM Signer',
     'pk_sim_signer1_a1b2c3d4e5f6', 'ACTIVE'),
    ('SIGNER_2', 'Secondary HSM Signer',
     'pk_sim_signer2_f6e5d4c3b2a1', 'ACTIVE'),
    ('SIGNER_3', 'Disaster Recovery Signer',
     'pk_sim_signer3_1a2b3c4d5e6f', 'ACTIVE');
