# Autometric (Dagster)

Pipeline orchestration for the Silver/Gold/Feature layers. See `CLAUDE.md` for project context
and `docs/autometric_ARCHITECTURE_DAGSTER.md` for the full architecture.

## Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Run Dagster (local)
```bash
dagster dev
```
