"""
Dagster jobs + schedule (Langkah 19).

daily_pipeline_job
  Materialize SELURUH lineage dalam satu run, urutan dijaga oleh deps antar-asset:
    l0_raw (source) -> harmonization -> silver -> feature -> gold
  Dagster otomatis menjalankan asset sesuai urutan dependency, jadi job ini cukup
  menyeleksi SEMUA asset (AssetSelection.all()) tanpa mendaftar manual satu per satu.

  Yang ikut ke-materialize:
    - 6 harmonization assets (raw -> l0_harmonization.*)
    - 6 silver assets        (l0_harmonization -> l1_silver.*)
    - 1 feature asset        (comment_relevance_scores + word_frequencies, REPLACE + invalidate Redis)
    - 8 gold mart assets     (l2_gold.* via sp_build_*)
  l0_raw adalah SourceAsset (diisi ingest API di luar Dagster) -> tidak dimaterialisasi,
  hanya jadi titik awal lineage.

daily_schedule
  Cron 02:00 setiap hari, timezone Asia/Jakarta (WIB).
  Alasan jam 02:00: memberi jarak setelah aktivitas ingest API malam hari, dan
  dijalankan saat trafik rendah. Seluruh transform (harmonization -> gold) kini
  dipicu Dagster di sini, BUKAN pg_cron.

CATATAN — campaign analysis (Langkah 20):
  Halaman "Campaign Analysis" bersifat on-demand (user memilih subset post lalu
  menekan "Run"), output level-post di-precompute oleh gold assets di daily run
  (gold_post_catalog / timeline / wordcloud bila/ketika ditambahkan), lalu agregasi
  subset terpilih dihitung real-time di layer API (Fase 6). Karena itu TIDAK dibuat
  scheduled-partitioned campaign_analysis_job di sini.
"""

from dagster import (
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
)


# --- Job: materialize seluruh lineage ------------------------------------
# AssetSelection.all() memilih semua asset yang terdaftar di Definitions.
# SourceAsset (l0_raw) otomatis dikecualikan dari materialisasi.
daily_pipeline_job = define_asset_job(
    name="daily_pipeline_job",
    selection=AssetSelection.all(),
    description=(
        "Materialize seluruh pipeline harian: "
        "harmonization -> silver -> feature -> gold. "
        "Urutan dijaga oleh dependency antar-asset."
    ),
)


# --- Schedule: 02:00 WIB tiap hari ---------------------------------------
daily_schedule = ScheduleDefinition(
    name="daily_pipeline_schedule",
    job=daily_pipeline_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Asia/Jakarta",
    description="Jalankan daily_pipeline_job tiap hari pukul 02:00 WIB.",
)


# Dikumpulkan untuk import di repository.py
jobs = [daily_pipeline_job]
schedules = [daily_schedule]