# Agent Constraints

**Read this file before making any changes.** These constraints apply to all AI-assisted work in this repository.

## Project Identity

**Bittensor Emissions Tracker** is a tax-accounting tool for Bittensor emissions. It supports three workflows—**Smart Contract** (validator staking and contract income), **Mining** (miner hotkey rewards), and **Payment** (third-party TAO payments received and transferred to brokerage)—each writing to its own Google Sheets sub-ledger. The codebase is a batch-style Python application (CLI entrypoints), not a web service.

## Stack (Non-Negotiable)

- **Language**: Python 3.12+ (see `pyproject.toml` upper bound)
- **Package manager**: Poetry
- **Core**: `pydantic-settings`, `bittensor`, `gspread`, `oauth2client`, `substrate-interface`, `backoff`
- **Linting**: bandit, black, flake8, isort, mypy
- **Testing**: pytest, pytest-cov
- **Formatting / cleanup**: autoflake (via `make reformat`)
- **Optional CI-style runs**: Docker Compose (`make lint`, `make test`) per `docker/` and `Makefile`

Do not introduce alternate stacks (e.g., a web framework, ORM, or different sheet backend) without explicit discussion.

## Hard Rules

- **Never install a new dependency** without discussion first
- **Never modify or add a database schema** without a migration plan (this project has no DB today; the rule applies if one is introduced)
- **Application code** lives under `emissions_tracker/`; **tests** under `tests/`
- **Docker** under `docker/`; **shell helpers** under `scripts/` where applicable
- **Environment variables** via `pydantic-settings` and `local/.env` (see README)—never hardcode API keys, credential paths, or sensitive SS58 values
- **90% test coverage floor** (`make test-local` uses `--fail-under=90`)—add or extend tests when you change logic
- **Run `make lint-local` and `make reformat`** before committing
- **Accounting semantics**: respect [SANITIZATION_RULES.md](SANITIZATION_RULES.md), [TRANSACTIONS.md](TRANSACTIONS.md), and [MINING_GUIDE.md](MINING_GUIDE.md) when touching ledger or journal behavior
- **Releases**: the root [VERSION](VERSION) file tracks release version; bump when releasing

## Patterns

- **Config**: central settings in `emissions_tracker/config.py` (pydantic-settings)
- **Models**: `emissions_tracker/models.py`—avoid ad-hoc untyped dicts for domain data
- **Exceptions**: `emissions_tracker/exceptions.py`
- **Layout**: `clients/` (Taostats, wallet, price, chain), `trackers/` (contract, mining, shared Bittensor logic), `entrypoints/` (CLI), `journal.py` for journal generation—match style of neighboring modules
- **Tests**: `tests/unit`, `tests/integration`, fixtures in `tests/fixtures`, static JSON in `tests/data/`
- **One concern per module**; split large files rather than growing god-modules
- **Follow existing naming** in the package you are editing

This is not an async HTTP API: do not default to “async-first service” patterns unless a specific call site already uses async.

## Architecture Overview

| Area | Purpose |
|------|---------|
| **`emissions_tracker/clients/`** | External integrations (TaoStats API, wallet/substrate, pricing) |
| **`emissions_tracker/trackers/`** | Contract tracker, mining tracker, payment tracker, shared Bittensor tracking |
| **`emissions_tracker/entrypoints/`** | CLI runners (`contract`, `mining`, `payment`) |
| **`emissions_tracker/journal.py`** | Wave journal entry generation |
| **`emissions_tracker/utils.py`** | Shared helpers |
| **`tests/`** | Unit and integration tests, fixtures, sample API JSON |
| **`docker/`**, **`Makefile`** | Containerized lint/test and local dev targets |

## CLI Entrypoints

After `poetry install`, Poetry exposes:

- **`track-contract`** → `emissions_tracker.entrypoints.contract:run`
- **`track-mining`** → `emissions_tracker.entrypoints.mining:run`
- **`track-payment`** → `emissions_tracker.entrypoints.payment:run`

Equivalent without console scripts:

- `python -m emissions_tracker.entrypoints.contract`
- `python -m emissions_tracker.entrypoints.mining`
- `python -m emissions_tracker.entrypoints.payment`

## Development Commands

```bash
make help        # List targets
make venv        # poetry install
make lint-local  # bandit, black, flake8, isort, mypy (Poetry on host)
make reformat    # autoflake + isort + black
make test-local  # pytest with coverage (fail under 90%)
make lint        # same linters via Docker Compose
make test        # tests via Docker Compose
make build       # build Docker images
make ci          # clean, build, infrastructure, lint, test, clean
```
