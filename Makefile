# Everything runs in containers; only Docker is required.

GO_IMAGE := golang:1.27-bookworm
# The website's build tools (Astro, Pagefind) run in this image.
NODE_IMAGE := node:24.19.0-trixie-slim@sha256:ab3eebe934147fee049b5eb83c570f68c849a13c930bdfa482de99fcdfa3b3de
COMPOSE  := docker compose --project-directory deploy/single-vm
VERSION  ?= dev
# The Python the end-to-end tests' clients and the migration report run on.
PYTHON_IMAGE := python:3.12-slim

.PHONY: help test lint migrate-test e2e e2e-distributed bi-e2e engines-e2e sharing-e2e helm-e2e build env up down reset logs psql load-tpch tpch site site-dev

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

test: ## Run the Go tests (starts a throwaway Postgres)
	scripts/test.sh

lint: ## Check formatting and run go vet
	docker run --rm -v "$(CURDIR)":/src -v igloo-go-mod:/go/pkg/mod -w /src $(GO_IMAGE) \
		sh -c 'test -z "$$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }; go vet -tags duckdb_arrow ./...'

migrate-test: ## Run the migration report's unit tests (migrate/; no warehouse needed)
	docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -v "$(CURDIR)":/src:ro -w /src $(PYTHON_IMAGE) sh -c \
		'pip install -q --disable-pip-version-check --root-user-action=ignore "$$(grep ^sqlglot== migrate/requirements.txt)" \
		&& python -W error::ResourceWarning -m unittest discover -s migrate/tests -t .'

e2e: ## Run the end-to-end test against an isolated copy of the stack
	scripts/e2e.sh

e2e-distributed: ## Test distributed execution against a copy of the stack with workers
	scripts/e2e-distributed.sh

bi-e2e: ## Test Metabase, Superset and Npgsql (Power BI) against an isolated stack
	scripts/bi-e2e.sh

engines-e2e: ## Test Spark and Trino on the Iceberg REST catalog (pulls ~3 GB)
	scripts/engines-e2e.sh
sharing-e2e: ## Test Delta Sharing with the official Python client and the reference server
	scripts/sharing-e2e.sh

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

# The website runs as you, so site/node_modules and site/dist are yours; npm's
# download cache stays in site/.npm between runs. SITE_PUBLIC and SITE_URL are
# passed through (see CONTRIBUTING.md).
SITE_RUN = docker run --rm --user "$$(id -u):$$(id -g)" -e HOME=/tmp -e npm_config_cache=/src/site/.npm \
	-e ASTRO_TELEMETRY_DISABLED=1 -e SITE_PUBLIC="$(SITE_PUBLIC)" -e SITE_URL="$(SITE_URL)" -v "$(CURDIR)":/src -w /src/site

site: ## Build the website and docs into site/dist, and check its links
	$(SITE_RUN) $(NODE_IMAGE) sh -c 'npm ci --no-audit --no-fund && npm run build'

site-dev: ## Serve the website at http://localhost:4321, updating as docs change
	$(SITE_RUN) -it -p 127.0.0.1:4321:4321 $(NODE_IMAGE) \
		sh -c 'npm ci --no-audit --no-fund && npm run dev -- --host 0.0.0.0'
