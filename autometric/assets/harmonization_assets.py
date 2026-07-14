"""
Harmonization assets — isi tabel l0_harmonization.* lewat stored procedure
sp_sync_*_from_raw(). Dipindah dari pg_cron ke Dagster supaya SEMUA orkestrasi
(raw -> harmonization -> silver -> feature -> gold) berada di satu tempat.

Pola sama dengan silver_assets.py: tiap asset cuma memanggil
PostgresResource.call_procedure(), TIDAK punya output data Python.

Pengelompokan: 1 asset PER ENTITAS (post/comment/profile/audience/story),
tiap asset menjalankan procedure platform yang relevan untuk entitas itu.
Ini menjaga jumlah asset tetap wajar sambil memberi lineage bermakna.

Catatan platform (sesuai DB asli):
  - post     : facebook, instagram, tiktok
  - comment  : facebook, instagram   (TikTok comment via raw belum ada proc)
  - profile  : facebook, instagram, tiktok
  - audience : facebook, instagram   (TikTok tidak punya audience)
  - story    : instagram saja
  - tagged   : instagram saja        (opsional, tidak dipakai Silver inti)

l0_raw dideklarasikan sebagai SourceAsset: diisi oleh ingest fullstack dev
(API Meta/TikTok), Dagster hanya mengamati sebagai titik awal lineage.
"""

from dagster import (
    asset,
    AssetKey,
    SourceAsset,
    Output,
)
from autometric.resources import PostgresResource


# --- Source layer: l0_raw (diisi ingest API di luar Dagster) ---------------
l0_raw = SourceAsset(
    key=AssetKey(["l0_raw"]),
    description="Diisi proses ingest (API Meta/TikTok) di luar Dagster. Titik awal lineage.",
    group_name="l0",
)


# --- Helper: jalankan satu/lebih procedure harmonization -------------------
def _harmonize(postgres: PostgresResource, procs: list[str], label: str) -> Output:
    for p in procs:
        postgres.call_procedure(f"CALL l0_harmonization.{p}()")
    return Output(len(procs), metadata={"procedures_dijalankan": procs, "entitas": label})


# --- Harmonization assets (per entitas) -----------------------------------
@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.*_post (facebook, instagram, tiktok).",
)
def harmonized_post(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_facebook_post_from_raw",
        "sp_sync_instagram_post_from_raw",
        "sp_sync_tiktok_post_from_raw",
    ], "post")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.*_comment (facebook, instagram).",
)
def harmonized_comment(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_facebook_comment_from_raw",
        "sp_sync_instagram_comment_from_raw",
    ], "comment")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.*_profile (facebook, instagram, tiktok).",
)
def harmonized_profile(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_facebook_profile_from_raw",
        "sp_sync_instagram_profile_from_raw",
        "sp_sync_tiktok_profile_from_raw",
    ], "profile")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description=(
        "raw -> l0_harmonization.instagram_audience (instagram saja). "
        "CATATAN: facebook_audience_from_raw sudah deprecated (struktur raw FB "
        "audience berubah), jadi tidak dipanggil di sini."
    ),
)
def harmonized_audience(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_instagram_audience_from_raw",
    ], "audience")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description="raw -> l0_harmonization.instagram_story (instagram saja).",
)
def harmonized_story(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_instagram_story_from_raw",
    ], "story")


@asset(
    group_name="harmonization",
    deps=[l0_raw],
    kinds={"postgres"},
    description=(
        "raw -> l0_harmonization.instagram_tagged_post (instagram saja). "
        "CATATAN: tabel ini diisi dari raw, TAPI belum dibaca procedure Silver "
        "mana pun (belum tersambung ke hilir). Disertakan untuk kelengkapan; "
        "sambungkan ke Silver bila fitur 'tagged posts' dibangun."
    ),
)
def harmonized_tagged_post(postgres: PostgresResource) -> Output:
    return _harmonize(postgres, [
        "sp_sync_instagram_tagged_post_from_raw",
    ], "tagged_post")


# Dikumpulkan untuk import di repository.py
harmonization_source_assets = [l0_raw]
harmonization_assets = [
    harmonized_post,
    harmonized_comment,
    harmonized_profile,
    harmonized_audience,
    harmonized_story,
    harmonized_tagged_post,
]