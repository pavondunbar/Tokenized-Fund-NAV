# =========================================================================
# Tokenized Fund / NAV System — Makefile
# =========================================================================
# Run `make help` to see all available commands.
# =========================================================================

.PHONY: help up down demo logs build restart status clean \
        db-balances db-ledger db-shell \
        health integrity topics kafka-tail shell-kafka \
        test

.DEFAULT_GOAL := help

# -------------------------------------------------------------------
# Core commands
# -------------------------------------------------------------------

help: ## Show all available commands
	@echo "Tokenized Fund / NAV System"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## Build and start all services
	docker compose up --build -d
	@echo ""
	@echo "All services starting. Run 'make logs' to follow output."

down: ## Stop and remove all containers and volumes
	docker compose down -v --remove-orphans

demo: ## Rebuild and run the full demo lifecycle
	docker compose down -v --remove-orphans
	docker compose up --build --abort-on-container-exit \
		fund-service outbox-publisher event-consumer

logs: ## Tail all container logs
	docker compose logs -f

build: ## Build all images without starting containers
	docker compose build

restart: ## Restart all running containers
	docker compose restart

# -------------------------------------------------------------------
# Database inspection
# -------------------------------------------------------------------

DB_CONTAINER := fund-ledger-db
DB_CMD := docker exec -t $(DB_CONTAINER) psql -U ledger_user -d fund_ledger

db-balances: ## Query derived investor balances from the ledger
	@$(DB_CMD) -c "\echo '--- Investor Share Balances ---'" \
		-c "SELECT i.investor_name, isb.share_balance, isb.cost_basis \
		    FROM investor_share_balances isb \
		    JOIN investors i ON i.id = isb.investor_id \
		    WHERE i.investor_name NOT LIKE 'TREASURY/%' \
		    ORDER BY isb.share_balance DESC;" \
		-c "\echo ''" \
		-c "\echo '--- Fund Shares Outstanding ---'" \
		-c "SELECT f.fund_name, fso.total_shares \
		    FROM fund_shares_outstanding fso \
		    JOIN funds f ON f.id = fso.fund_id;" \
		-c "\echo ''" \
		-c "\echo '--- Cash Journal Totals ---'" \
		-c "SELECT journal_id, \
		           SUM(debit_amount) AS total_debit, \
		           SUM(credit_amount) AS total_credit \
		    FROM cash_ledger \
		    GROUP BY journal_id \
		    ORDER BY journal_id \
		    LIMIT 10;"

db-ledger: ## Query share and cash ledger entries
	@$(DB_CMD) -c "\echo '--- Share Ledger (last 20 entries) ---'" \
		-c "SELECT sl.journal_id, i.investor_name, sl.entry_type, \
		           sl.shares, sl.reason, sl.created_at \
		    FROM share_ledger sl \
		    JOIN investors i ON i.id = sl.investor_id \
		    ORDER BY sl.created_at DESC \
		    LIMIT 20;" \
		-c "\echo ''" \
		-c "\echo '--- Cash Ledger (last 20 entries) ---'" \
		-c "SELECT cl.journal_id, cl.counterparty, cl.entry_type, \
		           cl.debit_amount, cl.credit_amount, cl.currency, \
		           cl.created_at \
		    FROM cash_ledger cl \
		    ORDER BY cl.created_at DESC \
		    LIMIT 20;"

db-shell: ## Interactive PostgreSQL shell
	docker exec -it $(DB_CONTAINER) psql -U ledger_user -d fund_ledger

# -------------------------------------------------------------------
# Health & integrity
# -------------------------------------------------------------------

health: ## Show container status and health
	@docker compose ps --format "table {{.Name}}\t{{.Service}}\t{{.Status}}"

integrity: ## Show latest reconciliation results
	@$(DB_CMD) -c "\echo '--- Reconciliation Runs (last 5) ---'" \
		-c "SELECT run_type, status, total_checked, mismatches, \
		           started_at, completed_at \
		    FROM reconciliation_runs \
		    ORDER BY started_at DESC \
		    LIMIT 5;" \
		-c "\echo ''" \
		-c "\echo '--- Recent Mismatches ---'" \
		-c "SELECT rm.mismatch_type, rm.entity_type, \
		           rm.expected_value, rm.actual_value, \
		           rm.created_at \
		    FROM reconciliation_mismatches rm \
		    ORDER BY rm.created_at DESC \
		    LIMIT 10;"

# -------------------------------------------------------------------
# Kafka inspection
# -------------------------------------------------------------------

KAFKA_CONTAINER := fund-kafka

topics: ## List all Kafka topics
	@docker exec -t $(KAFKA_CONTAINER) \
		kafka-topics --list --bootstrap-server localhost:9092

kafka-tail: ## Stream recent Kafka events (Ctrl-C to stop)
	docker exec -t $(KAFKA_CONTAINER) \
		kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--include 'fund\..*' \
		--from-beginning --max-messages 50 \
		--timeout-ms 10000

shell-kafka: ## Interactive shell inside the Kafka container
	docker exec -it $(KAFKA_CONTAINER) bash

# -------------------------------------------------------------------
# Tests (run locally, no Docker required)
# -------------------------------------------------------------------

VENV := .venv

$(VENV):
	uv venv $(VENV) --python 3.13
	uv pip install pytest --python $(VENV)/bin/python

test: $(VENV) ## Run unit tests (no Docker required)
	$(VENV)/bin/python -m pytest tests/ -q

# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------

status: ## Show Docker Compose service status
	docker compose ps

clean: down ## Stop services and prune volumes
	docker volume prune -f
