# Tokenized Fund / NAV System (Python)

> **SANDBOX / EDUCATIONAL USE ONLY — NOT FOR PRODUCTION**
> This codebase is a reference implementation designed for learning, prototyping, and architectural exploration. It is **not audited, not legally reviewed, and must not be used to manage real fund shares, calculate official NAVs, or process real investor subscriptions, redemptions, or transfers.** See the [Production Warning](#production-warning) section for full details.

---

## Table of Contents

- [Overview](#overview)
- [What is a Tokenized Fund / NAV System?](#what-is-a-tokenized-fund--nav-system)
- [Architecture](#architecture)
- [Core Services](#core-services)
- [Key Features & Design Patterns](#key-features--design-patterns)
- [Database Schema](#database-schema)
- [State Machines](#state-machines)
- [Real-World Example: BlackRock BUIDL](#real-world-example-blackrock-buidl)
- [Running in a Sandbox Environment](#running-in-a-sandbox-environment)
- [Verifying the System](#verifying-the-system)
- [Project Structure](#project-structure)
- [Production Warning](#production-warning)
- [License](#license)

---

## Overview

The **Tokenized Fund / NAV System** is a Python-based reference implementation that models the full lifecycle of an institutional tokenized fund — from fund creation and investor onboarding through NAV publication, subscription processing, partial redemption, investor-to-investor share transfer, MPC-signed blockchain settlement, and independent reconciliation. Every operation is protected by **role-based access control (RBAC)** and recorded in an **append-only double-entry ledger** with full request/trace lineage.

The system is modeled closely on how institutional tokenized fund platforms operate at **BlackRock BUIDL**, **Franklin OnChain U.S. Government Money Fund**, and **Ondo Finance**. It demonstrates how traditional fund administration infrastructure (NAV calculation, transfer agency, KYC/AML screening, custodian reporting) integrates with blockchain technology (MPC threshold signing, on-chain settlement, tokenized share registries) to achieve T+0 subscription/redemption finality while maintaining full regulatory auditability.

| Component | File | Responsibility |
|---|---|---|
| Core Services | `fund-service/fund_service.py` | Only ledger writer — subscriptions, redemptions, transfers, settlements |
| RBAC & State Machines | `fund-service/rbac.py` | Permission enforcement + order/settlement transition validation |
| Database Schema | `db/init/001-schema.sql` | 23 append-only tables, 7 views, 14 triggers, 9 functions |
| Readonly User | `db/init/002-readonly-user.sql` | Least-privilege grants for non-writer services |
| Outbox Publisher | `outbox/outbox_publisher.py` | Async DB outbox to Kafka with dead letter queue |
| Event Consumer | `event-consumer/consumer.py` | Routes Kafka events to NAV engine and transfer agent |
| NAV Engine | `nav-engine/nav_engine.py` | Isolated NAV calculation and validation |
| Transfer Agent | `transfer-agent/transfer_agent.py` | Share registry notifications |
| Compliance Gateway | `compliance-gateway/compliance.py` | KYC/AML/sanctions screening |
| Reconciliation | `reconciliation/reconciler.py` | 6-check independent ledger verification |
| Docker Compose | `docker-compose.yaml` | 8-service orchestration with trust domain network isolation |

---

## What is a Tokenized Fund / NAV System?

A **tokenized fund** represents ownership shares in an investment fund as digital tokens on a blockchain. Unlike traditional fund shares that exist only as entries in a transfer agent's books, tokenized shares can settle in seconds (T+0) rather than days (T+2 to T+4), while maintaining the same legal protections and regulatory compliance requirements.

**Net Asset Value (NAV)** is the per-share value of a fund, calculated as:

```
NAV per share = (Total Assets - Total Liabilities) / Shares Outstanding
```

NAV is the price at which investors subscribe to (buy) and redeem (sell) fund shares. For tokenized funds, NAV is published on-chain, and subscription/redemption orders are settled atomically against the published NAV — eliminating the multi-day settlement cycle and counterparty risk of traditional fund administration.

Tokenized fund / NAV systems are the backbone of:
- **Tokenized money market funds** (BlackRock BUIDL — $2.9B AUM, Franklin OnChain)
- **Tokenized Treasury funds** where each token = $1.00 of NAV
- **Institutional subscription/redemption platforms** with T+0 settlement
- **On-chain share registries** replacing Computershare / SS&C / BNY Mellon transfer agents
- **Real-time NAV publication** replacing end-of-day pricing feeds

This system implements the full institutional stack — handling NAV calculation and validation, KYC/AML screening, double-entry share and cash ledgers, MPC-signed blockchain settlement, RBAC-enforced access control with separation of duties, and independent reconciliation that halts on any mismatch.

---

## Architecture

```
                    ┌──────────────────────────────────────────────────────────┐
                    │            TOKENIZED FUND LIFECYCLE                      │
                    └──────────────────────────────────────────────────────────┘

  Off-Chain / Application Layer                         On-Chain / Settlement
  ────────────────────────────────────────────────────────────────────────────

  1. FundService                  Create fund
     (create_fund) ──────────────────────────────────► PostgreSQL
                                                        + Outbox Event

  2. FundService                  Register investors (KYC pre-approved)
     (register_investor) ────────────────────────────► investors table
                                                        + compliance events
                                                        + Outbox Event

  3. FundService                  Publish official NAV
     (publish_nav) ──────────────────────────────────► nav_publications
                                  $99.5M / 1M shares    (immutable snapshot)
                                  = $99.50/share         + Outbox Event

  4. FundService                  Process subscription
     (process_subscription) ──── Compliance ─────────► order + order_events
                                  Gateway (KYC/AML)     (PENDING → SETTLED)
                                       │
                                       ▼
                                  settle_via_blockchain()
                                  ├── Create settlement (PENDING)
                                  ├── Approve (APPROVED)
                                  ├── Collect 2 MPC signatures
                                  ├── Sign (SIGNED + tx_hash)
                                  ├── Broadcast (BROADCASTED)
                                  └── Confirm (CONFIRMED + block_number)
                                       │
                                       ▼
                                  Share ledger: CREDIT investor, DEBIT treasury
                                  Cash ledger:  DEBIT investor, CREDIT treasury
                                  (balanced journal, verified at COMMIT)

  5. FundService                  Process redemption (inverse of subscription)
     (process_redemption) ───────────────────────────► Share: DEBIT investor
                                                        Cash: CREDIT investor

  6. FundService                  Investor-to-investor transfer
     (process_transfer) ─────────────────────────────► Share: DEBIT sender,
                                                              CREDIT receiver

  7. ReconciliationEngine         6-source verification
     (run_full_reconciliation)
        ├── Share balance replay vs view
        ├── Cash journal balance check
        ├── Share journal balance check
        ├── Order state machine replay
        ├── Settlement state machine replay
        └── MPC quorum verification
                                  ↓
                                  PASSED or FAILED (with mismatches)
```

### Trust Domains (Docker Networks)

| Network | Type | Services | Purpose |
|---|---|---|---|
| `internal` | Private (no egress) | postgres, fund-service, outbox-publisher, event-consumer, reconciliation | Database access zone — no internet |
| `backend` | Bridge | fund-service, outbox-publisher, event-consumer, compliance-gateway, transfer-agent, nav-engine, kafka, zookeeper | Inter-service communication + Kafka |
| `pricing` | Private (no egress) | nav-engine | Isolated NAV calculation — no database access |

PostgreSQL has no host ports exposed — only services on the `internal` network can reach it. The NAV engine is isolated in its own network with no database access and no internet — it receives pricing data via HTTP from the event consumer and returns validation results.

---

## Core Services

### `fund-service` (Only Ledger Writer)

The single point of truth for all ledger mutations. Every share and cash ledger entry in the system is written by this service — no other service has write access to the core ledger tables. Coordinates the full lifecycle: fund creation, investor registration, NAV publication, subscription processing (8-step flow including compliance screening, state machine transitions, MPC settlement, and balanced double-entry journal creation), partial redemption, and investor-to-investor transfer. Every call requires an `AuditContext` (request_id, trace_id, actor, actor_role) and passes through RBAC permission checks before execution. Runs a complete demo lifecycle on startup, then verifies all state via deterministic replay.

### `outbox-publisher` (Kafka Delivery)

Standalone async process that polls `outbox_events` with `FOR UPDATE SKIP LOCKED` for safe multi-replica deployment. Delivers events to Kafka topics prefixed with `fund.*` using idempotent producer configuration (`acks=all`, `enable_idempotence=True`). Processes batches of 50 events per cycle with 1-second polling interval. After 5 consecutive delivery failures, events are moved to the `dead_letter_queue` and marked as handled. Runs under the restricted `readonly_user` database role.

### `event-consumer` (Kafka Event Router)

Subscribes to 14 Kafka topics covering the full event taxonomy (fund creation, investor registration, NAV publication, order lifecycle, settlement lifecycle). Routes events to downstream services: `nav.published` events are forwarded to the NAV engine for validation, and `order.settled/redeemed/transferred` events are forwarded to the transfer agent for registry notification. Every consumed event is deduplicated via the `processed_events` table and recorded in the `audit_events` table with full request/trace context.

### `nav-engine` (Isolated NAV Calculation)

Stateless HTTP service running in the isolated `pricing` network with no database access. Exposes two endpoints: `/validate` (checks NAV per share > 0 and < 1M, net asset value >= 0, date not in future) and `/calculate` (computes NAV = total assets - total liabilities, NAV/share = NAV / shares outstanding). In production, this would integrate Bloomberg/Reuters pricing feeds, custodian AUM reports, and fair value adjustment models.

### `transfer-agent` (Share Registry)

Receives settlement notifications for completed subscriptions, redemptions, and transfers. Maintains an in-memory registry of all share movements. In production, this would update the official registrar (Computershare, SS&C, BNY Mellon), issue transfer confirmations, file SEC/FINRA reports, and update the on-chain token registry.

### `compliance-gateway` (KYC/AML Screening)

Screens entities against a blocked-entity list for KYC, AML, accreditation, sanctions, and PEP checks. Supports both single-entity and batch screening. In production, this would integrate OFAC SDN/SSI lists, Securitize for accreditation verification, ComplyAdvantage for adverse media, and country-specific sanctions lists.

### `reconciliation` (Independent Auditor)

Runs six independent verification checks on a 60-second cycle:

| Check | What It Verifies |
|---|---|
| Share balance replay | Replays share_ledger entries, compares against `investor_share_balances` view |
| Cash journal balance | Every cash `journal_id` has SUM(debit) = SUM(credit) |
| Share journal balance | Every share `journal_id` has SUM(debit) = SUM(credit) |
| Order state machine | Replays `order_events`, validates every transition against the state machine |
| Settlement state machine | Replays `settlement_events`, validates every transition |
| MPC quorum | Every SIGNED settlement has >= 2 MPC signatures |

Any mismatch is recorded in `reconciliation_mismatches` and the run is marked FAILED. The reconciliation engine operates under the `readonly_user` database role — it can verify but never modify the ledger.

---

## Key Features & Design Patterns

### Append-Only Ledger (Database-Enforced Immutability)

Every core table has `BEFORE UPDATE` and `BEFORE DELETE` triggers that raise exceptions — no user at any privilege level can UPDATE or DELETE rows. The `outbox_events` table allows UPDATE only on `published_at` (verified by trigger). The `dead_letter_queue` allows UPDATE only on retry/resolved fields. This is enforced at the PostgreSQL level, not just the application level.

### Double-Entry Accounting (Share + Cash Ledgers)

Share and cash ledgers use paired DEBIT/CREDIT entries with a shared `journal_id`. Deferred constraint triggers verify `SUM(debit) = SUM(credit)` at transaction COMMIT time. If a journal is unbalanced, the entire transaction aborts — it is impossible to create a one-sided entry. Subscriptions debit the fund treasury and credit the investor (shares), while simultaneously debiting the investor and crediting the treasury (cash).

### Derived Balances (No Stored Balance Columns)

No balance column exists anywhere in the schema. All balances are computed via views that replay ledger entries: `SUM(credit) - SUM(debit)`. This makes balance manipulation impossible without corrupting the append-only ledger — any discrepancy is immediately detected by the reconciliation engine.

### Transactional Outbox Pattern (Double-Spend Prevention)

Every service writes its outbox event in the **same database transaction** as the business record. The publisher uses `FOR UPDATE SKIP LOCKED` to prevent duplicate processing across multiple publisher instances. Kafka producer is configured with `acks=all` and `enable_idempotence=True`. Failed events move to the dead letter queue after 5 retries.

### Role-Based Access Control (RBAC)

Eight pre-seeded roles enforce separation of duties across all operations:

| Role | Permitted Operations |
|---|---|
| `ADMIN` | Full access (wildcard) |
| `FUND_MANAGER` | fund.create, fund.manage, nav.publish, order.approve, settlement.approve |
| `TRANSFER_AGENT` | order.process, transfer.execute, settlement.finalize |
| `COMPLIANCE_OFFICER` | compliance.screen, investor.approve |
| `SYSTEM` | All permissions (used by automated services) |
| `NAV_CALCULATOR` | nav.calculate, nav.publish |
| `INVESTOR` | order.submit |
| `SIGNER` | settlement.sign |

The `check_permission()` function raises `PermissionError` before any side effect if the actor's role lacks the required permission. RBAC tables (roles, permissions, actor assignments) are append-only with mutation triggers.

### MPC Threshold Signing (2-of-3 Quorum)

Settlement execution requires cryptographic approval from at least 2 of 3 registered signers. Each signer produces a SHA-256 signature share, stored in the immutable `mpc_signatures` table with a `UNIQUE(settlement_id, signer_id)` constraint preventing double-signing. The settlement is marked SIGNED only after `check_quorum_and_sign()` confirms >= 2 signatures, at which point a transaction hash is generated. The reconciliation engine independently verifies quorum compliance.

### Order State Machine (Trigger-Enforced)

Valid transitions are enforced by both Python code (`rbac.validate_order_transition()`) and a database trigger (`enforce_order_status_transition()`). Terminal states (SETTLED, FAILED, CANCELLED, REJECTED) block further transitions at the database level — even a direct SQL INSERT is rejected.

### Settlement State Machine (Trigger-Enforced)

Settlement status transitions (PENDING → APPROVED → SIGNED → BROADCASTED → CONFIRMED) are enforced by both Python code and database trigger. Failed settlements are terminal — the settlement pipeline must be restarted from scratch.

### Idempotency at Every Layer

Every create operation checks for an existing `idempotency_key` before any side effect. This covers: fund creation, investor registration, NAV publication, order creation, and settlement creation. Re-submitted requests return the existing record without duplicate operations. Kafka consumers deduplicate via the `processed_events` table.

### End-to-End Audit Trail

Every database write across all services includes `request_id`, `trace_id`, `actor`, and `actor_role` columns. A dedicated `audit_events` table records every consumed Kafka event. The `trace_id` links all operations within a single subscription/redemption flow, while `request_id` identifies individual service calls — enabling full reconstruction of who did what, when, and why.

### Decimal Precision for Financial Arithmetic

All share quantities use 8-decimal-place precision (`NUMERIC(28,8)`) with banker's rounding (`ROUND_HALF_EVEN`). Cash amounts use 2-decimal-place precision (USD cents). Python's `Decimal` type is used throughout — no IEEE 754 floating-point arithmetic anywhere in the financial pipeline.

### Deterministic State Rebuild

The `rebuild_state()` function replays the entire ledger from scratch and compares against the current derived views. It verifies: (1) share balances match the `investor_share_balances` view, (2) every cash journal is balanced, (3) every order state transition is valid, and (4) every settlement state transition is valid. This proves the system can reconstruct its full state from the append-only log alone.

### Reconciliation as a Hard Stop

The reconciliation engine treats any discrepancy as a failure, not a warning. Mismatches are recorded in `reconciliation_mismatches` with full details (expected vs. actual values), and the run is immediately marked FAILED. No new operations should proceed until mismatches are investigated and resolved.

---

## Database Schema

### `funds`

Fund registry — one row per tokenized fund.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `fund_name` | VARCHAR(256) | Human-readable fund name |
| `fund_ticker` | VARCHAR(20) UNIQUE | Trading ticker symbol |
| `fund_type` | ENUM | `OPEN_END`, `CLOSED_END`, `ETF`, `MONEY_MARKET`, `INTERVAL` |
| `base_currency` | VARCHAR(3) | Fund denomination currency |
| `isin` | VARCHAR(12) | ISO 6166 security identifier |
| `cusip` | VARCHAR(9) | CUSIP identifier |
| `management_fee_bps` | INTEGER | Annual management fee in basis points |
| `status` | VARCHAR(20) | `ACTIVE`, `SUSPENDED`, `LIQUIDATING`, `TERMINATED` |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate registration |

### `investors`

KYC-verified investor registry.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `investor_name` | VARCHAR(256) | Legal name |
| `investor_type` | ENUM | `INDIVIDUAL`, `INSTITUTIONAL`, `QUALIFIED_PURCHASER`, `ACCREDITED` |
| `lei` | VARCHAR(20) | ISO 17442 Legal Entity Identifier |
| `tax_id_hash` | VARCHAR(64) | Hashed tax ID (PII protection) |
| `kyc_status` | ENUM | `PENDING`, `APPROVED`, `REJECTED`, `EXPIRED` |
| `aml_status` | ENUM | `PENDING`, `CLEARED`, `FLAGGED`, `BLOCKED` |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate onboarding |

### `nav_publications`

Immutable NAV snapshots — one per fund per pricing date.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `fund_id` | UUID FK | References `funds(id)` |
| `nav_date` | DATE | Pricing date |
| `total_assets` | NUMERIC(28,8) | Fund total assets |
| `total_liabilities` | NUMERIC(28,8) | Fund total liabilities |
| `net_asset_value` | NUMERIC(28,8) | `total_assets - total_liabilities` |
| `shares_outstanding` | NUMERIC(28,8) | Total shares at time of calculation |
| `nav_per_share` | NUMERIC(28,8) | `net_asset_value / shares_outstanding` |
| `pricing_source` | VARCHAR(100) | Source of pricing data |
| `is_official` | BOOLEAN | Whether this is the official published NAV |
| `idempotency_key` | VARCHAR(256) UNIQUE | One NAV per fund per date |

### `share_ledger`

Double-entry share accounting journal — paired DEBIT/CREDIT entries per `journal_id`.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `fund_id` | UUID FK | References `funds(id)` |
| `investor_id` | UUID FK | References `investors(id)` |
| `journal_id` | UUID | Groups paired DEBIT/CREDIT entries |
| `entry_type` | ENUM | `DEBIT` or `CREDIT` |
| `shares` | NUMERIC(28,8) | Number of shares (must be > 0) |
| `nav_per_share` | NUMERIC(28,8) | NAV at time of entry |
| `amount` | NUMERIC(28,8) | USD equivalent (`shares * nav_per_share`) |
| `reason` | ENUM | `SUBSCRIPTION`, `REDEMPTION`, `TRANSFER_IN`, `TRANSFER_OUT`, `DIVIDEND_REINVEST`, `FEE_DEDUCTION`, `INITIAL_SEED` |
| `order_id` | UUID FK | References `orders(id)` |

> **Deferred constraint trigger**: `check_share_journal_balance()` verifies `SUM(debit shares) = SUM(credit shares)` per `journal_id` at COMMIT time. Unbalanced journals abort the transaction.

### `cash_ledger`

Double-entry cash accounting journal — paired DEBIT/CREDIT entries per `journal_id`.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `fund_id` | UUID FK | References `funds(id)` |
| `investor_id` | UUID FK | References `investors(id)` |
| `journal_id` | UUID | Groups paired DEBIT/CREDIT entries |
| `entry_type` | ENUM | `DEBIT` or `CREDIT` |
| `amount` | NUMERIC(28,8) | Cash amount (must be > 0) |
| `currency` | VARCHAR(3) | Currency code |
| `reason` | ENUM | `SUBSCRIPTION_PAYMENT`, `REDEMPTION_PAYOUT`, `DIVIDEND_PAYMENT`, `FEE_COLLECTION`, `TRANSFER_SETTLEMENT`, `INITIAL_SEED` |
| `order_id` | UUID FK | References `orders(id)` |

> **Deferred constraint trigger**: `check_cash_journal_balance()` verifies `SUM(debit amount) = SUM(credit amount)` per `journal_id` at COMMIT time.

### `orders`

Subscription, redemption, and transfer order records.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `fund_id` | UUID FK | References `funds(id)` |
| `investor_id` | UUID FK | References `investors(id)` |
| `order_type` | ENUM | `SUBSCRIPTION`, `REDEMPTION`, `TRANSFER` |
| `shares` | NUMERIC(28,8) | Number of shares |
| `amount` | NUMERIC(28,8) | Cash amount |
| `currency` | VARCHAR(3) | Currency code |
| `nav_per_share` | NUMERIC(28,8) | NAV used for pricing |
| `counterparty_investor_id` | UUID FK | Receiver for transfers |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate orders |

### `order_events`

Event-sourced order lifecycle — trigger-enforced state machine.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `order_id` | UUID FK | References `orders(id)` |
| `status` | ENUM | `PENDING`, `COMPLIANCE_CHECK`, `APPROVED`, `REJECTED`, `NAV_APPLIED`, `SETTLED`, `FAILED`, `CANCELLED` |
| `reason` | TEXT | Reason for status change |

### `compliance_screenings`

KYC/AML/sanctions screening results per order.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `order_id` | UUID FK | References `orders(id)` |
| `investor_id` | UUID FK | References `investors(id)` |
| `screening_type` | ENUM | `KYC`, `AML`, `ACCREDITATION`, `SANCTIONS`, `PEP` |
| `result` | ENUM | `PASS`, `FAIL`, `REVIEW_REQUIRED` |
| `provider` | VARCHAR(100) | Screening provider name |
| `screening_ref` | VARCHAR(256) | External reference ID |
| `details` | JSONB | Full screening response |

### `settlements`

Blockchain settlement records — one per order.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `order_id` | UUID FK | References `orders(id)` |
| `fund_id` | UUID FK | References `funds(id)` |
| `settlement_type` | ENUM | `SUBSCRIPTION`, `REDEMPTION`, `TRANSFER` |
| `tx_hash` | VARCHAR(256) | On-chain transaction hash |
| `block_number` | BIGINT | Blockchain block number |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate settlements |

### `settlement_events`

Event-sourced settlement lifecycle — trigger-enforced state machine.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `settlement_id` | UUID FK | References `settlements(id)` |
| `status` | ENUM | `PENDING`, `APPROVED`, `SIGNED`, `BROADCASTED`, `CONFIRMED`, `FAILED` |
| `reason` | TEXT | Reason for status change |
| `tx_hash` | VARCHAR(256) | Transaction hash (set on SIGNED/CONFIRMED) |
| `block_number` | BIGINT | Block number (set on CONFIRMED) |

### `mpc_key_shares`

MPC signer registry — 2-of-3 quorum required for settlement signing.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `signer_id` | VARCHAR(50) UNIQUE | Signer identifier (e.g., `SIGNER_1`) |
| `signer_name` | VARCHAR(256) | Human-readable name |
| `public_key` | VARCHAR(256) | Public key (simulated) |
| `status` | ENUM | `ACTIVE`, `REVOKED` |

### `mpc_signatures`

Individual signer approvals — one per `(settlement, signer)`.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `settlement_id` | UUID FK | References `settlements(id)` |
| `signer_id` | VARCHAR(50) FK | References `mpc_key_shares(signer_id)` |
| `signature_share` | VARCHAR(256) | SHA-256 signature hash |
| `signed_at` | TIMESTAMP | When the signature was produced |

> **Unique constraint**: `(settlement_id, signer_id)` — prevents double-signing.

### `outbox_events`

Reliable Kafka delivery buffer — shared across all services.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `aggregate_id` | VARCHAR(256) | ID of the entity that produced the event |
| `aggregate_type` | VARCHAR(100) | Entity type (fund, investor, order, settlement) |
| `event_type` | VARCHAR(256) | e.g. `fund.created`, `order.settled`, `settlement.confirmed` |
| `payload` | JSONB | Full event data |
| `published_at` | TIMESTAMP | NULL = pending Kafka delivery |
| `created_at` | TIMESTAMP | When the event was written |

> **Mutation trigger**: Only `published_at` can be updated. All other columns and DELETE are blocked.

### `audit_events`

Append-only audit log for all operations.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `trace_id` | UUID | Shared across all calls in one workflow |
| `actor` | VARCHAR(256) | Who performed the operation |
| `actor_role` | VARCHAR(50) | RBAC role of the actor |
| `action` | VARCHAR(100) | Operation name |
| `resource_type` | VARCHAR(100) | Affected entity type |
| `resource_id` | VARCHAR(256) | Affected entity ID |
| `details` | JSONB | Operation-specific payload |

### `reconciliation_runs`

Audit run history — tracks each reconciliation cycle.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `run_type` | ENUM | `SHARE_BALANCE`, `CASH_BALANCE`, `NAV_CONSISTENCY`, `FULL` |
| `status` | ENUM | `RUNNING`, `PASSED`, `FAILED` |
| `total_checked` | INTEGER | Number of items verified |
| `mismatches` | INTEGER | Number of discrepancies found |

> **Guard trigger**: Status can only transition from RUNNING to PASSED or FAILED, exactly once.

### `reconciliation_mismatches`

Detailed mismatch records from failed reconciliation checks.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `run_id` | UUID FK | References `reconciliation_runs(id)` |
| `mismatch_type` | VARCHAR(100) | Type of discrepancy |
| `entity_type` | VARCHAR(100) | Affected entity type |
| `entity_id` | UUID | Affected entity ID |
| `expected_value` | TEXT | What the ledger replay computed |
| `actual_value` | TEXT | What the view/current state shows |
| `details` | JSONB | Additional context |

### `dead_letter_queue`

Poison pill events that exceeded maximum retry count.

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Primary key |
| `source_table` | VARCHAR(100) | Originating table |
| `source_id` | UUID | Originating record ID |
| `event_type` | VARCHAR(256) | Event type that failed |
| `payload` | JSONB | Full event payload |
| `error_message` | TEXT | Reason for failure |
| `retry_count` | INTEGER | Total delivery attempts |
| `resolved_at` | TIMESTAMP | NULL until manually resolved |

> **Guard trigger**: Only `retry_count`, `last_retry_at`, and `resolved_at` can be updated. All other columns and DELETE are blocked.

### Database Views (Derived Balances)

| View | Purpose |
|---|---|
| `investor_share_balances` | `SUM(credit) - SUM(debit)` shares per investor per fund |
| `fund_shares_outstanding` | Total shares outstanding (excluding treasury accounts) |
| `order_current_status` | Latest status per order (`DISTINCT ON`) |
| `settlement_current_status` | Latest settlement status per settlement |
| `fund_latest_nav` | Latest official NAV per fund (`is_official = true`) |
| `mpc_quorum_status` | MPC signature count and quorum check (>= 2) per settlement |
| `outbox_events_current` | Outbox events with delivery status (PENDING/DELIVERED) |

---

## State Machines

### Order Status

```
                      ┌──────────────────┐
  process_*() ───────►│     PENDING      │
                      └────────┬─────────┘
                               │
                      ┌────────┴─────────┐
                      │                  │
                      ▼                  ▼
             ┌──────────────┐   ┌──────────────┐
             │ COMPLIANCE   │   │  CANCELLED   │  (terminal)
             │    CHECK     │   └──────────────┘
             └──────┬───────┘
                    │
           ┌────────┴─────────┐
           │                  │
           ▼                  ▼
  ┌──────────────┐   ┌──────────────┐
  │   APPROVED   │   │   REJECTED   │  (terminal)
  └──────┬───────┘   └──────────────┘
         │
         ▼
  ┌──────────────┐
  │  NAV_APPLIED │
  └──────┬───────┘
         │
    ┌────┴─────┐
    │          │
    ▼          ▼
┌────────┐ ┌────────┐
│SETTLED │ │ FAILED │  (both terminal)
└────────┘ └────────┘
```

### Settlement Status

```
  create_settlement() ──► PENDING ──► APPROVED ──► SIGNED ──► BROADCASTED ──► CONFIRMED
                            │           │           │            │
                            ▼           ▼           ▼            ▼
                          FAILED      FAILED      FAILED       FAILED
                          (all terminal — no further transitions allowed)
```

### Investor Compliance Status

```
  KYC:  PENDING ──► APPROVED | REJECTED | EXPIRED
  AML:  PENDING ──► CLEARED  | FLAGGED  | BLOCKED
```

### Fund Status

```
  ACTIVE ──► SUSPENDED ──► LIQUIDATING ──► TERMINATED
```

---

## Real-World Example: BlackRock BUIDL

The demo in `fund-service/fund_service.py` is modeled on the **BlackRock USD Institutional Digital Liquidity Fund (BUIDL)**, the largest tokenized money market fund with over **$2.9B AUM**. The sandbox creates a fund with the following parameters:

| Attribute | Value |
|---|---|
| Fund Name | BlackRock USD Institutional Digital Liquidity Fund |
| Ticker | BUIDL |
| Fund Type | OPEN_END |
| Base Currency | USD |
| Management Fee | 50 bps (0.50% annual) |
| Initial Total Assets | $100,000,000 |
| Initial Total Liabilities | $500,000 |
| Net Asset Value | $99,500,000 |
| Shares Outstanding | 1,000,000 |
| NAV per Share | **$99.50** |

Three institutional investors subscribe:

| Investor | Type | LEI | Subscription | Shares Received |
|---|---|---|---|---|
| Fidelity Investments | INSTITUTIONAL | 549300FIDELITY0001 | $25,000,000 | ~251,256.28 |
| Vanguard Group | INSTITUTIONAL | 549300VANGUARD0001 | $50,000,000 | ~502,512.56 |
| State Street Global Advisors | INSTITUTIONAL | 549300SSGA00000001 | $15,000,000 | ~150,753.77 |
| **Total** | | | **$90,000,000** | **~904,522.61** |

After subscriptions, two additional operations execute:

1. **Partial redemption**: Fidelity redeems 20% of its position (~50,251.26 shares for ~$4,999,950.47)
2. **Investor transfer**: Vanguard transfers 50,000 shares to State Street Global Advisors

**Final positions (after all operations):**

| Investor | Shares | Approximate Value |
|---|---|---|
| Fidelity Investments | ~201,005.03 | ~$20,000,000 |
| Vanguard Group | ~452,512.56 | ~$45,025,000 |
| State Street Global Advisors | ~200,753.77 | ~$19,975,000 |

Every operation is settled via the full MPC pipeline (2-of-3 threshold signing), recorded in balanced double-entry journals, and verified by deterministic state rebuild.

---

## Running in a Sandbox Environment

### Option A: Docker Compose (Recommended)

The fastest way to run the entire system. Docker Compose orchestrates all 8 services with trust domain network isolation.

**Prerequisites:** Docker and Docker Compose.

```bash
docker compose up --build
```

This starts:

| Service | Description | Network(s) |
|---|---|---|
| `postgres` | PostgreSQL 15 — schema auto-initialized, no host ports | internal |
| `fund-service` | Runs full demo lifecycle (register, fund, settle) | backend, internal |
| `outbox-publisher` | Polls outbox, delivers to Kafka (`readonly_user`) | backend, internal |
| `event-consumer` | Routes Kafka events to NAV engine and transfer agent | backend, internal |
| `nav-engine` | Isolated NAV calculation and validation | backend, pricing |
| `transfer-agent` | Share registry notifications | backend |
| `compliance-gateway` | KYC/AML/sanctions screening | backend |
| `reconciliation` | Independent 6-check ledger verification | backend, internal |
| `zookeeper` | Kafka coordination | backend |
| `kafka` | Event streaming | backend |

The `fund-service` runs a complete demo lifecycle on startup:

1. Creates a tokenized fund (BlackRock BUIDL-style)
2. Registers three institutional investors (Fidelity, Vanguard, SSGA)
3. Publishes an official NAV ($99.50 per share)
4. Processes three subscriptions ($25M, $50M, $15M) — each with compliance screening, MPC settlement, and balanced double-entry journals
5. Processes a partial redemption (20% of Fidelity's position)
6. Executes an investor-to-investor share transfer (Vanguard → SSGA)
7. Prints final positions, outbox event summary, order history, and settlement summary
8. Runs deterministic state rebuild verification

The reconciliation engine independently verifies all balances and state transitions on a 60-second cycle.

**View logs:**

```bash
docker compose logs -f fund-service        # Settlement lifecycle
docker compose logs -f outbox-publisher     # Kafka delivery
docker compose logs -f event-consumer       # Event routing
docker compose logs -f reconciliation       # Audit verification
docker compose logs -f nav-engine           # NAV validation
```

**Tear down:**

```bash
docker compose down -v    # Remove containers and volumes
```

### Option B: Local Development (Manual Setup)

For running directly on your machine without Docker.

#### Prerequisites

- Python 3.13+
- PostgreSQL 15+ (running locally — the service auto-creates the database)
- Kafka (local or Docker — optional, the outbox publisher falls back gracefully)

#### 1. Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install asyncpg aiokafka aiohttp
```

#### 2. Start Kafka (Optional)

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  apache/kafka:latest
```

#### 3. Run the Fund Service

```bash
cd fund-service
python fund_service.py
```

The script automatically:
- Connects to PostgreSQL with retry logic (15 attempts, 3-second backoff)
- Creates the database and loads the append-only schema from `db/init/`
- Seeds RBAC roles, permissions, actor assignments, and MPC key shares
- Runs the full demo lifecycle with RBAC enforcement, MPC signing, and audit trails
- Prints final state summary and deterministic state rebuild verification

---

## Verifying the System

After running the demo, you can inspect every layer of the system.

### Inspecting the Database

Connect to the database using `docker exec`:

```bash
docker exec -it tokenized-fund-nav-postgres-1 psql -U ledger_user -d tokenized_fund
```

#### Fund Overview

```sql
SELECT f.fund_name, f.fund_ticker, f.fund_type, f.status,
       n.nav_per_share, n.net_asset_value, n.shares_outstanding
FROM funds f
JOIN fund_latest_nav n ON n.fund_id = f.id;
```

#### Investor Share Balances (Derived from Ledger)

```sql
SELECT * FROM investor_share_balances;
```

#### Fund Shares Outstanding

```sql
SELECT * FROM fund_shares_outstanding;
```

#### Order History

```sql
SELECT o.order_type, o.shares, o.amount, o.currency, ocs.current_status
FROM orders o
JOIN order_current_status ocs ON ocs.order_id = o.id
ORDER BY o.created_at;
```

#### Settlement Summary

```sql
SELECT s.settlement_type, scs.current_status, s.tx_hash, s.block_number
FROM settlements s
JOIN settlement_current_status scs ON scs.settlement_id = s.id
ORDER BY s.created_at;
```

#### MPC Quorum Verification

```sql
SELECT * FROM mpc_quorum_status;
```

#### Double-Entry Share Ledger

```sql
SELECT fund_id, investor_id, journal_id, entry_type, shares, reason
FROM share_ledger
ORDER BY created_at;
```

#### Outbox Event Delivery Status

```sql
SELECT event_type, aggregate_type, created_at,
       CASE WHEN published_at IS NOT NULL THEN 'DELIVERED' ELSE 'PENDING' END AS delivery_status
FROM outbox_events
ORDER BY created_at;
```

#### Reconciliation Results

```sql
SELECT run_type, status, total_checked, mismatches, completed_at
FROM reconciliation_runs
ORDER BY started_at;
```

#### Append-Only Verification

```sql
-- This will fail with: "Mutations are not allowed on this table"
UPDATE funds SET fund_name = 'test';
```

---

## Project Structure

```
Tokenized-Fund-NAV/
│
├── docker-compose.yaml              # 8-service orchestration with 3 trust domains
│
├── db/
│   └── init/
│       ├── 001-schema.sql           # 23 tables, 7 views, 14 triggers, 9 functions
│       └── 002-readonly-user.sql    # Least-privilege grants for non-writer services
│
├── fund-service/                    # ONLY ledger writer
│   ├── Dockerfile
│   ├── fund_service.py              # Core business logic — subscriptions,
│   │                                #   redemptions, transfers, MPC settlement,
│   │                                #   double-entry journals, state rebuild
│   └── rbac.py                      # RBAC permission checks + state machine
│                                    #   validation (Python mirrors of DB triggers)
│
├── outbox/                          # Kafka publisher
│   ├── Dockerfile
│   └── outbox_publisher.py          # FOR UPDATE SKIP LOCKED, dead letter queue,
│                                    #   idempotent Kafka producer (acks=all)
│
├── event-consumer/                  # Kafka event router
│   ├── Dockerfile
│   └── consumer.py                  # Routes to NAV engine + transfer agent,
│                                    #   deduplication via processed_events
│
├── nav-engine/                      # Isolated NAV calculation
│   ├── Dockerfile
│   └── nav_engine.py                # Stateless — no DB access, pricing network only
│
├── transfer-agent/                  # Share registry
│   ├── Dockerfile
│   └── transfer_agent.py            # Settlement notifications, in-memory registry
│
├── compliance-gateway/              # KYC/AML screening
│   ├── Dockerfile
│   └── compliance.py                # Entity screening, batch support
│
├── reconciliation/                  # Independent auditor
│   ├── Dockerfile
│   └── reconciler.py               # 6 verification checks on 60-second cycle
│
├── README.md
└── LICENSE
```

---

## Production Warning

**This project is explicitly NOT suitable for production use.** Tokenized fund administration is among the most regulated, operationally complex, and legally sensitive activities in financial services. The following critical components are absent or stubbed:

| Missing Component | Risk if Absent |
|---|---|
| Real fund administrator integration (SS&C, BNY Mellon, State Street) | No official NAV calculation or share registry |
| Licensed transfer agent (Computershare, Securitize) | Cannot legally maintain the shareholder registry |
| Real custodian API integration | Cannot verify actual fund assets under management |
| Real KYC/AML provider (Securitize, ComplyAdvantage, Onfido) | No actual identity or accreditation verification |
| Real sanctions screening (OFAC SDN API) | Potential sanctions violations and regulatory exposure |
| Smart contract audit (ERC-1400 / ERC-3643) | Token contract may have exploitable vulnerabilities |
| HSM / MPC key management (Thales, Fireblocks) | Private keys exposed in software |
| Securities law compliance (SEC, FINRA, MAS) | Fund may constitute an unregistered securities offering |
| Real pricing feeds (Bloomberg, Reuters, ICE) | NAV calculations based on stub data |
| Production authentication (OAuth / mTLS / API keys) | RBAC is enforced but actors are not authenticated against an identity provider |
| TLS / mTLS for service-to-service communication | Plaintext internal traffic |
| Rate limiting and position limits | No controls on subscription/redemption size |
| Comprehensive test suite with mutation testing | Untested edge cases in fund handling |
| Dead-letter queue manual replay tooling | Failed events require developer intervention |
| Disaster recovery procedures | No tested failover for settlement outages |
| Regulatory reporting (SEC Form N-PORT, Form PF, MiFID II) | Post-trade reporting violations |
| Multi-jurisdiction tax withholding | No tax computation on redemptions or distributions |

> Tokenized fund administration at institutional scale requires: SEC-registered investment adviser status, licensed transfer agent, qualified custodian, FINRA membership (for distribution), compliance with the Investment Company Act of 1940 (or applicable exemption), and legal agreements with all counterparties. **Do not use this code to create, manage, or administer any real fund, process real investor subscriptions or redemptions, or calculate official NAVs.**

---

## License

This project is provided as-is for educational and reference purposes under the MIT License.

---

*Built with ♥️ by Pavon Dunbar — Modeled on BlackRock BUIDL, Franklin OnChain U.S. Government Money Fund, and Ondo Finance*
