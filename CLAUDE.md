# Autometric — Project Context

Orchestrator: **Dagster** (asset-centric). Architecture reference: `docs/autometric_ARCHITECTURE_DAGSTER.md`.
Engineering standard: call the **senior-data-engineer** skill (`\senior-data-engineer`) at the start of work.

## IMPORTANT — real DB differs from the architecture doc
The architecture doc is a template. The actual database differs. Always follow the real state below:

- Schemas: `l0_raw`, `l0_harmonization`, `l0_extra`, `l1_silver`, `feature`, `public` (NOT a single `l0`/`silver`).
- DB name is **autometric_v2** (no trailing "s").
- **No materialized views anywhere.** All layers are plain TABLES.
  Dagster's "Materialize" button just means "run the asset" — unrelated to PG materialized views.
- Silver tables are **singular**: `l1_silver.unified_post`, `unified_comment`, `unified_audience`, `unified_profile`.
  They are filled by stored procedures: `CALL l1_silver.sp_sync_unified_*()`.
- `brand_id` is **UUID**, not integer. Anything keyed by brand uses str(uuid).
- `updated_at` already exists on `l1_silver.unified_*` — do not re-add.
- No CSV upload anywhere. Data is API-only: GraphAPI -> `l0_raw`.
- `l0_raw -> l0_harmonization` runs via **Postgres stored procedures + pg_cron, hourly**.
  Dagster does NOT own this; treat `l0_harmonization.*` as SourceAssets (observed, not owned).
- TikTok `reach` lives in `l0_extra.tiktok_post_extra_attribute.reach_post` and is NOT from the API.
  Leave it in l0_extra; Silver already reads it correctly.
- TikTok has no audience data (platform limitation) — V11-V13 TikTok gap is expected, not fixable.

## End goal
Feed the social-media analytics dashboard at sekata.co.id (Autometric — "Track every brand").
Pipeline: L0 -> Silver -> Feature/Gold -> FastAPI+Redis -> React dashboard (frontend already exists).

## Progress
- **FASE 1 (project skeleton) — DONE.** pyproject, resources.py, repository.py.
- **FASE 2 (Silver assets) — DONE.** 4 assets call `sp_sync_*`; `unified_comment` after `unified_post`;
  `audience`/`profile` independent. `l0_harmonization` = SourceAsset.
  Freshness 25h via `build_last_update_freshness_checks` (NOT `FreshnessPolicy` — removed in Dagster 1.13).
  Verified: unified_post materialized (72 rows), all 4 materialize OK.
- **FASE 3 (Feature/NLP) — DONE.** schema `feature` + 2 tables created.
  `comment_relevance_scores` asset: cosine(comment_text, caption) -> 0-100, TRUNCATE+INSERT, Redis invalidation.
  Join comment->post on (post_id, platform); NULL caption -> comment skipped (not scored 0).
  Verified: 2 comments scanned -> 2 scored (data really only has 2 comments — confirmed, not a bug).

## Environment notes
- Windows + VS Code + DBeaver. venv at `.venv`. Use Claude Code in terminal when limits allow.
- **Redis = Memurai** (Windows). Old `redis-server` Windows service (RESP2, rejected `HELLO`) was
  stopped + disabled; replaced by Memurai on localhost:6379. Code unchanged.
- **Set `DAGSTER_HOME`** to a fixed folder (`.dagster_home`) so run history persists —
  otherwise Dagster uses a random `.tmp_dagster_home_*` and history vanishes next day.
- `.gitignore` should include `.venv/`, `.env`, `.dagster_home/`.

## Layers Dagster owns
Silver (call sp_sync_* -> l1_silver tables) -> Feature (NLP -> feature.*) -> Gold (gold.mart_*).

## FASE 4 decision (Gold) — IN PROGRESS
- **Approach: Opsi B — SQL aggregation inside each asset, TRUNCATE+INSERT** (same pattern as Feature).
  NOT stored procedures, NOT materialized views. Reason: logic version-controlled in Git, easy review/change.
- 7 marts (poin 16): mart_engagement_trend, mart_platform_summary, mart_pillar_performance,
  mart_content_attributes, mart_story_funnel, mart_tiktok_churn, mart_comment_activity.
- mart_community_contributors (poin 17): runs AFTER comment_relevance_scores (NLP).
- gold.v_campaign_posts (poin 18): a real VIEW (on-demand), not a table, not scheduled.
- TODO before coding: confirm `gold` schema exists or create; get unified_profile/unified_audience columns;
  map each mart to a specific dashboard Overview component.

## Dropped from original plan (do NOT build)
- CSV upload queue, csv_upload_sensor, harmonization_job (poin 6, 21, 23) — data is API-only.
- `platform` column migration (poin 2) — tables already per-platform.
- Revised full to-do list: see `TODO_revised.md`.