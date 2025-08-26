## Onboarding Drop-off Analyzer

An AI-driven toolkit to detect, diagnose, and prioritize onboarding funnel drop-offs.
This repository provides connectors to product analytics platforms, ingestion pipelines, analytics and clustering modules, a recommendation engine, reporting and delivery integrations (Slack, Notion, email), and operational infrastructure (migrations, Celery tasks).

This README was generated from the repository contents (see `pyproject.toml`, `onboarding-dropoff-analyzer.md`, top-level `src/` modules, `alembic/` migrations and `src/onboarding_analyzer/*`). It summarizes purpose, architecture, how to run locally, and where to find key code.

## Quick summary
- Package name: `onboarding-dropoff-analyzer` (see `pyproject.toml`).
- Version: 0.1.0
- Python: >= 3.10
- Main capabilities: event ingestion, sessionization, funnel analysis, clustering of friction patterns, prioritized UX recommendations, scheduled reports via email/Slack/Notion.

## Repository layout (high level)

- `pyproject.toml` - project metadata and dependencies.
- `requirements.txt` - pin-style dependency hints.
- `onboarding-dropoff-analyzer.md` - a longform project blueprint and architecture overview.
- `dev_server.py` - development server/launcher (top-level helper).
- `dashboard.html` - static dashboard snapshot used by reporting.
- `sample_data/events.csv` - small sample events for local testing.
- `alembic/` - DB migration scripts and configuration.
- `src/` - main Python package; contains both top-level utilities and a subpackage `onboarding_analyzer`.
  - `src/cli.py` - CLI entrypoint (placeholder in repo).
  - `src/analysis.py` - analysis helpers (placeholder in repo).
  - `src/ingestion.py` - ingestion orchestration.
  - `src/integrations.py` - connectors to analytics providers.
  - `src/recommendation_engine.py` - recommendation logic.
  - `src/report.py` - report generation and export.
  - `src/email_digest.py` / `src/email_scheduler.py` - email report composition and scheduling.
  - `src/friction_detection.py` / `src/clustering.py` - detection and grouping of friction patterns.
  - `src/config_manager.py` - centralized configuration loading (env / pydantic settings).
  - `src/onboarding_models.py` - domain models used across modules.
  - `src/api_endpoints.py` - fastapi endpoints used by the service (if enabled).
  - `src/onboarding_analyzer/` - large subpackage with pluggable connectors, analytics, ML, infrastructure and security modules.

## Design & architecture (concise)

The project implements an ingestion → normalization → analytics → recommendation → delivery pipeline:

1. Connectors retrieve events from platforms (PostHog, Mixpanel, Amplitude).
2. Events are normalized into a common schema and stored (raw event store + relational models managed by Alembic/SQLAlchemy).
3. Sessions and funnels are reconstructed; funnel conversion rates and drop-off statistics are computed.
4. Unsupervised models (clustering) + statistical tests group similar drop-off sessions into friction archetypes.
5. Recommendation engine maps clusters to prioritized UX recommendations and composes weekly reports (Slack/Notion/email).
6. Celery tasks and schedulers handle background jobs and weekly report generation.

The full architecture blueprint and product goals are documented in `onboarding-dropoff-analyzer.md`.

## Installation (local, Windows PowerShell)

Prerequisites:
- Python 3.10+ (virtual environment recommended)
- PostgreSQL (if running DB-backed features)
- Redis (for Celery broker/result backend)

Example (PowerShell):

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements.txt
```

Or using the project as editable install (uses pyproject):

```powershell
pip install -e .
```

Notes: `pyproject.toml` enumerates the main dependencies (FastAPI, SQLAlchemy, Celery, pandas, scikit-learn, etc.). The repository also includes a richer `requirements.txt` with recommended packages for analytics and deployment.

## Configuration

Configuration is loaded from environment variables and pydantic settings models (see `src/config_manager.py` and `src/onboarding_analyzer/config.py`). Typical settings include:

- DATABASE_URL (Postgres)
- REDIS_URL
- ANALYTICS API keys (POSTHOG_API_KEY, MIXPANEL_TOKEN, AMPLITUDE_API_KEY)
- SMTP credentials for email delivery
- SLACK_BOT_TOKEN, NOTION_API_KEY for integrations

Use a `.env` file in development to set these environment variables (the repo contains an example `.env`).

## Database migrations

Alembic is configured under the `alembic/` folder; migration scripts exist (multiple versions). To run migrations locally:

```powershell
# Ensure DATABASE_URL is set
alembic upgrade head
```

If you installed with editable mode (`pip install -e .`), alembic CLI should be available from the venv.

## Running locally

Minimal dev server (FastAPI) and background workers are provided:

1) Start API (uvicorn):

```powershell
$env:DATABASE_URL = "postgresql+psycopg2://user:pass@localhost:5432/onboarding"
uvicorn src.onboarding_analyzer.api.main:app --reload
```

2) Start Celery worker (for scheduled reports & background tasks):

```powershell
cd .
celery -A src.onboarding_analyzer.infrastructure.celery_app worker --loglevel=info
```

3) Use `dev_server.py` (if present) for convenience helpers.

Note: some modules are lightweight stubs in this repo (for example `src/cli.py` and `src/analysis.py` are placeholders). The central implementation lives in `src/onboarding_analyzer/*` and top-level `src/` modules.

## Sample data and quick demo

- `sample_data/events.csv` contains a small set of synthetic/historical events for local experimentation.
- Load it into a Jupyter notebook or run a local script in `src/ingestion.py` to preview sessionization and funnel metrics.

## Key modules and where to look

- Connectors: `src/onboarding_analyzer/connectors/*` (PostHog, Mixpanel, Amplitude, base connector). Inspect these to add or adjust platform adapters.
- Ingestion: `src/ingestion.py` and `src/onboarding_analyzer/ingest_queue.py` orchestrate fetching events, normalization and queueing.
- Analytics: `src/analysis.py` (top-level, placeholder) and `src/onboarding_analyzer/analytics/*` contain funnel analysis, cohorting and predictive components.
- Clustering & Friction: `src/friction_detection.py`, `src/clustering.py`, and `src/onboarding_analyzer/tasks/clustering.py` implement pattern detection and cluster management.
- Recommendation engine: `src/recommendation_engine.py` and `src/onboarding_analyzer/ml/recommendation_engine.py` generate prioritized fix suggestions.
- Reporting: `src/report.py`, `src/email_digest.py`, `src/email_scheduler.py` and `src/onboarding_analyzer/tasks/reporting.py` compose and send weekly reports.
- API endpoints: `src/onboarding_analyzer/api/*` (FastAPI) exposes programmatic access and webhooks.
- Infra & security: `src/onboarding_analyzer/infrastructure/*` and `src/onboarding_analyzer/security/*` include DB helpers, idempotency, secrets management and auth utilities.

## Tests & linting

- The project uses ruff and mypy as specified in `pyproject.toml` under the `[tool.*]` sections.
- Add unit tests into a `tests/` folder; run tests with `pytest` (not currently included in repo scaffold).

## Deployment notes

- CI workflows are present in `.github/workflows/` for deployment and verification.
- The project expects external services (Postgres, Redis, Slack/Notion tokens). Use environment variables or secret management in production.
- Background tasks are run via Celery; scale workers independently from the API.

## Contribution

1. Create feature branch
2. Add tests for new behavior
3. Run `ruff` and `mypy` locally
4. Open PR and include a short description of impact on data and models

## Limitations & notes about this README generation

- The repository contains a large `src/onboarding_analyzer/` subpackage with many feature files and migration scripts. I scanned the top-level files (`pyproject.toml`, `requirements.txt`, `onboarding-dropoff-analyzer.md`, and key `src/` modules) and used the project blueprint to produce this README.
- A literal "line-by-line" read of every file (hundreds of files / migration and package internals) was not executed in this single automated step due to scope; if you want a file-by-file annotated inventory or a README augmented with exact function signatures and usage examples, I can continue and produce:
  - an API surface list (all public functions/classes), or
  - a per-module mini-documentation file under `docs/` with code excerpts.

## License

MIT (see `pyproject.toml`).
