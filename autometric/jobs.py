"""
Dagster jobs + schedule (Langkah 19).

daily_pipeline_job
  Materialize SELURUH lineage dalam satu run, urutan dijaga oleh deps antar-asset:
    l0_raw (source) -> harmonization -> silver -> feature -> gold
  Dagster otomatis menjalankan asset sesuai urutan dependency, jadi job ini cukup
  menyeleksi SEMUA asset (AssetSelection.all()) tanpa mendaftar manual satu per satu.

  Yang ikut ke-materialize: seluruh asset terdaftar di repository.py
  (harmonization -> silver -> feature/NLP -> gold + competitor assets).
  Daftar tidak dihitung manual di sini agar tidak basi tiap ada asset baru.
  l0_raw adalah SourceAsset (diisi ingest API di luar Dagster) -> tidak dimaterialisasi,
  hanya jadi titik awal lineage.

daily_schedule
  Cron 03:15 setiap hari, timezone Asia/Jakarta (WIB).
  Alasan jam 03:15: scraper competitor (Apify) jalan 03:00 WIB dan datanya
  konsisten landing ~03:01-03:03 WIB. Buffer sampai 03:15 memastikan pipeline
  tidak makan data competitor yang masih setengah ter-ingest.
  (Main brand di-scrape 02:00 via Meta Graph -- sudah lama selesai saat 03:15.)
  Seluruh transform (harmonization -> gold) dipicu Dagster di sini, BUKAN pg_cron.

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


# --- Schedule: 03:15 WIB tiap hari ---------------------------------------
daily_schedule = ScheduleDefinition(
    name="daily_pipeline_schedule",
    job=daily_pipeline_job,
    cron_schedule="15 3 * * *",
    execution_timezone="Asia/Jakarta",
    description="Jalankan daily_pipeline_job tiap hari pukul 03:15 WIB (buffer setelah scrape competitor 03:00).",
)


# Dikumpulkan untuk import di repository.py
jobs = [daily_pipeline_job]
schedules = [daily_schedule]