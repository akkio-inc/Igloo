# Everything runs in containers; only Docker is required.

GO_IMAGE := golang:1.27-bookworm
COMPOSE  := docker compose --project-directory deploy/single-vm
VERSION  ?= dev

.PHONY: help test lint e2e helm-e2e build env up down reset logs psql load-tpch tpch

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

test: ## Run the Go tests (starts a throwaway Postgres)
	scripts/test.sh

lint: ## Check formatting and run go vet
	docker run --rm -v "$(CURDIR)":/src -v igloo-go-mod:/go/pkg/mod -w /src $(GO_IMAGE) \
		sh -c 'test -z "$$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }; go vet -tags duckdb_arrow ./...'

e2e: ## Run the end-to-end test against an isolated copy of the stack
	scripts/e2e.sh

helm-e2e: ## Test the Helm chart on a throwaway kind cluster
	deploy/helm/e2e.sh

build: ## Build the igloo:dev image (VERSION=1.2.3 to stamp a version)
	docker build --build-arg VERSION=$(VERSION) -t igloo:dev .

env: ## Create deploy/single-vm/.env with random secrets
	deploy/single-vm/init-env.sh

up: env ## Start the single-machine stack
	$(COMPOSE) up -d --build --wait

down: ## Stop the stack, keeping data
	$(COMPOSE) down

reset: ## Stop the stack and delete all data
	$(COMPOSE) --profile local-storage --profile tools down -v

logs: ## Follow Igloo's logs
	$(COMPOSE) logs -f igloo

psql: ## Open a SQL shell as the admin
	$(COMPOSE) run --rm psql

load-tpch: ## Load TPC-H data (SF=1 by default; make load-tpch SF=10)
	$(COMPOSE) run --rm -T psql -v ON_ERROR_STOP=1 -v sf=$(or $(SF),1) -f /sql/load-tpch.sql

tpch: ## Time the 22 TPC-H queries
	@{ printf '%s\n' '\timing on' '\o /dev/null'; \
	  for q in $$(seq 1 22); do printf '%s\n' "\\echo Q$$q" "PRAGMA tpch($$q);"; done; } \
	  | $(COMPOSE) run --rm -T psql -X -v ON_ERROR_STOP=1 2>/dev/null \
	  | grep -v '^Timing is' | paste - - | awk '{printf "%-4s %9s ms\n", $$1, $$3}'
