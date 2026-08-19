"""
Competitor assets — isi tabel l1_silver.unified_competitor_* dan
l2_gold.competitor_* lewat stored procedure sp_sync_*/sp_build_*.

File ini masuk ke: autometric/assets/competitor_assets.py

Mengikuti pola persis silver_assets.py & harmonization_assets.py:
  - Helper _sync()/_build() manggil "CALL schema.proc()" + lapor row count
    lewat Output, sama seperti helper _sync() dan _harmonize() existing.
  - deps pakai OBJECT asset yang di-import langsung dari
    harmonization_assets.py (harmonized_post, harmonized_profile) — asset
    ini SUDAH menjalankan proc raw->harmonization utk brand utama & kompetitor
    sekaligus (sp_sync_facebook_post_from_raw dkk sudah handle keduanya),
    jadi tidak perlu asset harmonization terpisah utk kompetitor.

Dependency:
  unified_competitor_post          <- harmonized_post
  unified_competitor_profile_daily <- harmonized_profile
  competitor_post_metric           <- unified_competitor_post
  competitor_profile_metric_daily  <- unified_competitor_profile_daily
                                       DAN competitor_post_metric
                                       (proc gold-nya JOIN ke unified_competitor_post
                                       utk agregasi like/comment/share harian)

BELUM DIKONFIRMASI: nama group di gold_assets.py (aku pakai group_name="gold"
sebagai tebakan konsisten dgn group_name="silver"/"harmonization" yang sudah
ada — cek gold_assets.py kalau mau exact match).
"""

from dagster import asset, Output
from autometric.resources import PostgresResource
from autometric.assets.harmonization_assets import (
    harmonized_post,
    gapfilled_profile_dates,
)


# --- Helper: sama persis pola _sync() di silver_assets.py ------------------
def _sync(postgres: PostgresResource, proc: str, table: str) -> Output:
    postgres.call_procedure(f"CALL l1_silver.{proc}()")
    n = postgres.count_rows(f"l1_silver.{table}")
    return Output(n, metadata={"rows": n, "table": f"l1_silver.{table}"})


def _build(postgres: PostgresResource, proc: str, table: str) -> Output:
    postgres.call_procedure(f"CALL l2_gold.{proc}()")
    n = postgres.count_rows(f"l2_gold.{table}")
    return Output(n, metadata={"rows": n, "table": f"l2_gold.{table}"})


# --- L1 SILVER --------------------------------------------------------------

@asset(
    group_name="silver",
    deps=[harmonized_post],
    kinds={"postgres"},
    description="Sinkronkan l1_silver.unified_competitor_post via sp_sync_unified_competitor_post(). Filter baris kompetitor dari tabel harmonization post yang sama dipakai brand utama.",
)
def unified_competitor_post(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_competitor_post", "unified_competitor_post")


@asset(
    group_name="silver",
    deps=[gapfilled_profile_dates],
    kinds={"postgres"},
    description=(
        "Sinkronkan l1_silver.unified_competitor_profile_daily via "
        "sp_sync_unified_competitor_profile_daily(). Filter baris kompetitor "
        "dari tabel harmonization profile yang sama dipakai brand utama. "
        "Depend ke gapfilled_profile_dates (2026-08-19, bukan langsung "
        "harmonized_profile) -- kompetitor paling sering kena gap tanggal "
        "dari scraping yang gagal, jadi ini yang paling diuntungkan gap-fill. "
        "Independen dari unified_competitor_post."
    ),
)
def unified_competitor_profile_daily(postgres: PostgresResource) -> Output:
    return _sync(postgres, "sp_sync_unified_competitor_profile_daily", "unified_competitor_profile_daily")


# --- L2 GOLD ------------------------------------------------------------

@asset(
    group_name="gold",
    deps=[unified_competitor_post],
    kinds={"postgres"},
    description="Bangun l2_gold.competitor_post_metric via sp_build_competitor_post_metric(). Resolve brand_id client lewat brand_competitors.",
)
def competitor_post_metric(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_competitor_post_metric", "competitor_post_metric")


@asset(
    group_name="gold",
    # butuh competitor_post_metric duluan: sp_build_competitor_profile_metric_daily
    # JOIN ke unified_competitor_post utk agregasi like/comment/share harian.
    deps=[unified_competitor_profile_daily, competitor_post_metric],
    kinds={"postgres"},
    description="Bangun l2_gold.competitor_profile_metric_daily via sp_build_competitor_profile_metric_daily(). Siap dipakai report Brand vs Competitor.",
)
def competitor_profile_metric_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_competitor_profile_metric_daily", "competitor_profile_metric_daily")


# Dikumpulkan biar gampang di-import di repository.py
competitor_assets = [
    unified_competitor_post,
    unified_competitor_profile_daily,
    competitor_post_metric,
    competitor_profile_metric_daily,
]