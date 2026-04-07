# Tokenized Fund / NAV System

Institutional-grade tokenized fund management prototype with append-only ledger, double-entry accounting, and trust domain isolation.

## Architecture

```
                    +-----------+
                    |  backend  |
                    +-----+-----+
                          |
     +--------------------+--------------------+
     |          |         |         |           |
fund-service  outbox   event-    compliance  transfer-
     |       publisher consumer  gateway     agent
     |          |         |
     +----+-----+---------+
          |                        +-----------+
    +-----+-----+                  |  pricing  |
    |  internal  |                 |  (private) |
    |  (private) |                 +-----+-----+
    +-----+-----+                        |
          |                          nav-engine
      postgres
      reconciliation
```

### Trust Domains (Docker Networks)

| Network | Type | Services | Purpose |
|---------|------|----------|---------|
| `internal` | Private (no egress) | postgres, fund-service, outbox-publisher, event-consumer, reconciliation | Database access zone |
| `backend` | Bridge | fund-service, outbox, consumer, compliance, transfer-agent, nav-engine | Inter-service communication + Kafka |
| `pricing` | Private (no egress) | nav-engine | Isolated NAV calculation |

### Services

| Service | DB Access | Role |
|---------|-----------|------|
| `fund-service` | `ledger_user` (full write) | Only writer to the ledger. Processes subscriptions, redemptions, transfers |
| `outbox-publisher` | `readonly_user` (SELECT + UPDATE published_at) | Polls outbox, publishes to Kafka. Uses `FOR UPDATE SKIP LOCKED` |
| `event-consumer` | `readonly_user` (SELECT + INSERT audit) | Routes Kafka events to NAV engine and transfer agent |
| `nav-engine` | None | Isolated NAV calculation and validation |
| `transfer-agent` | None | Share registry notifications |
| `compliance-gateway` | None | KYC/AML/sanctions screening |
| `reconciliation` | `readonly_user` | Independent balance verification via ledger replay |

## Security Properties

### Append-Only Database
- Every table has `BEFORE UPDATE` and `BEFORE DELETE` triggers that raise exceptions
- No user at any privilege level can UPDATE or DELETE rows from core tables
- The `outbox_events` table allows UPDATE only on `published_at` (verified by trigger)
- The `dead_letter_queue` allows UPDATE only on retry/resolved fields

### Double-Entry Accounting
- Share and cash ledgers use paired DEBIT/CREDIT entries with a shared `journal_id`
- Deferred constraint triggers verify `SUM(debit) = SUM(credit)` at commit time
- Unbalanced journals abort the entire transaction

### Derived Balances
- No balance column exists anywhere in the schema
- All balances are computed via views that replay ledger entries: `SUM(credit) - SUM(debit)`
- This makes balance manipulation impossible without corrupting the append-only ledger

### Outbox Pattern (Double-Spend Prevention)
- Business state changes and outbox events are written in the same transaction
- Publisher uses `FOR UPDATE SKIP LOCKED` to prevent duplicate processing
- Kafka producer configured with `acks=all` and `enable_idempotence=True`
- Failed events move to dead letter queue after max retries

### Order State Machine
- Valid transitions enforced by both Python code and database trigger
- Terminal states (SETTLED, FAILED, CANCELLED, REJECTED) block further transitions

## Quick Start

```bash
docker compose up --build
```

The fund-service runs a complete demo lifecycle:
1. Creates a tokenized fund (BlackRock BUIDL-style)
2. Registers three institutional investors (Fidelity, Vanguard, SSGA)
3. Publishes an official NAV
4. Processes three subscriptions ($25M, $50M, $15M)
5. Processes a partial redemption (20% of Fidelity's position)
6. Executes an investor-to-investor share transfer
7. Prints final positions and outbox event summary

The reconciliation engine independently verifies all balances and state transitions.
