"""
Dagster entry point. Registers all assets, jobs, schedules, sensors, resources.

Credentials dibaca dari environment variables — tidak pernah hard-coded.
Env vars (set di .env, JANGAN commit .env):
  AUTOMETRIC_DB_URL = postgresql://<user>:<password>@<host>:<port>/<db>?sslmode=require
                      (Tiger Cloud: port 39700, db tsdb, sslmode=require WAJIB)
  REDIS_HOST        = <redis host>   (opsional, default localhost)

Orkestrasi penuh di Dagster (TIDAK ada pg_cron):
  l0_raw (source) -> harmonization -> silver -> feature -> gold
Dipicu daily_pipeline_job pada 03:15 WIB (lihat jobs.py).
"""
from dotenv import load_dotenv
load_dotenv()

import os
from dagster import Definitions

from autometric.resources import (
    PostgresResource,
    SentenceTransformerResource,
    SentimentModelResource,
    RedisResource,
)
from autometric.assets.harmonization_assets import (
    harmonization_source_assets,
    harmonization_assets,
)
from autometric.assets.silver_assets import (
    silver_assets,
    silver_freshness_checks,
)
from autometric.assets.feature_assets import (
    feature_assets,
)
from autometric.assets.gold_assets import (
    gold_assets,
)
from autometric.assets.competitor_assets import (
    competitor_assets,
)
from autometric.jobs import (
    jobs,
    schedules,
)
from autometric.sensors import (
    sensors,
)

defs = Definitions(
    assets=[
        *harmonization_source_assets,   # l0_raw (SourceAsset)
        *harmonization_assets,          # raw -> harmonization (Dagster, bukan pg_cron)
        *silver_assets,                 # harmonization -> silver (Langkah 10-11)
        *feature_assets,                # silver -> feature/NLP (Langkah 14-15)
        *gold_assets,                   # silver/feature -> gold mart (Langkah 16-17)
        *competitor_assets,             # harmonization -> silver -> gold khusus kompetitor (Brand vs Competitor report)
    ],
    asset_checks=[*silver_freshness_checks],   # Langkah 11: freshness 25 jam
    jobs=jobs,             # Langkah 19: daily_pipeline_job
    schedules=schedules,   # Langkah 19: daily_pipeline_schedule (03:15 WIB)
    sensors=sensors,       # new_account_sensor (raw ada + gold kosong + initial_scrape_logs success -> trigger pipeline)
    resources={
        "postgres": PostgresResource(
            connection_string=os.environ["AUTOMETRIC_DB_URL"],
        ),
        "nlp_model": SentenceTransformerResource(),
        "sentiment_model": SentimentModelResource(),
        "redis": RedisResource(
            host=os.environ.get("REDIS_HOST", "localhost"),
        ),
    },
)