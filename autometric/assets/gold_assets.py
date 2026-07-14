"""
Gold assets — isi tabel l2_gold.* (mart) lewat stored procedure sp_build_*.

Pola sama dengan silver/harmonization: asset cuma CALL procedure + lapor row count.
Mart Gold = tabel agregat harian (BUKAN hypertable; UPSERT/REPLACE di dalam procedure).

Dependency (dari analisis FROM/JOIN tiap procedure):
  mart_brand_metric_daily      <- unified_post, unified_comment, unified_profile
  mart_comment_activity        <- unified_comment, feature.comment_relevance_scores
  mart_community_contributors  <- unified_post, unified_comment, feature (Langkah 17)
  mart_content_attributes      <- unified_post
  mart_pillar_performance      <- unified_post, unified_story
  mart_story_funnel            <- unified_profile, unified_story
  mart_tiktok_churn            <- unified_profile
  ugc_tagged_posts             <- unified_tagged_post   (IG-only, UGC)
  audience_demographics_daily  <- unified_audience      (age+gender, IG-only)
  audience_geo_daily           <- unified_audience      (city+country unnest, IG-only)
  posting_time_heatmap         <- unified_post          (weekday x hour, WIB)
  dim_content_pillar           <- pillar_performance_daily  (dimensi/konfig pillar, umbrella brand)
  comment_relevance_distribution <- comment_relevance_scores (distribusi tier komentar, umbrella brand)
  post_comment_timeline        <- unified_comment, unified_post  (timeline komentar per post, WIB harian)

Dua mart (comment_activity, community_contributors) baca feature.comment_relevance_scores,
jadi depend pada asset comment_relevance_scores (Feature).

CATATAN: v_campaign_posts (Langkah 18) adalah VIEW, bukan asset (dibuat saat diminta,
tidak dijadwalkan). Tidak disertakan di sini.
"""

from dagster import asset, Output, AssetKey
from psycopg2.extras import execute_values

from autometric.resources import PostgresResource
from autometric.feature.comment_relevance_scorer import compute_wordcloud_per_post


# Asset keys Silver & Feature (untuk deps lintas-file tanpa import langsung)
_POST = AssetKey("unified_post")
_COMMENT = AssetKey("unified_comment")
_PROFILE = AssetKey("unified_profile")
_AUDIENCE = AssetKey("unified_audience")
_STORY = AssetKey("unified_story")
_TAGGED = AssetKey("unified_tagged_post")
_FEATURE = AssetKey("comment_relevance_scores")
_SENTIMENT_FEATURE = AssetKey("comment_sentiment_scores")


def _build(postgres: PostgresResource, proc: str, table: str) -> Output:
    postgres.call_procedure(f"CALL l2_gold.{proc}()")
    n = postgres.count_rows(f"l2_gold.{table}")
    return Output(n, metadata={"rows": n, "table": f"l2_gold.{table}"})


# unified_story kini SUDAH jadi asset Silver (lihat silver_assets.py),
# jadi mart yang butuh story depend padanya dengan benar.


@asset(
    group_name="gold",
    deps=[_POST, _COMMENT, _PROFILE],
    kinds={"postgres"},
    description="mart_engagement_trend (harian) via sp_build_brand_metric_daily().",
)
def mart_brand_metric_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_brand_metric_daily", "brand_metric_daily")


@asset(
    group_name="gold",
    deps=[_POST],
    kinds={"postgres"},
    description=(
        "l2_gold.post_metric (grain per-post) via sp_build_post_metric(). "
        "Proyeksi langsung dari unified_post: engagement owned/public + 4 ER per post. "
        "Developer query tabel ini langsung (Model 1)."
    ),
)
def post_metric(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_post_metric", "post_metric")


@asset(
    group_name="gold",
    deps=[_COMMENT, _FEATURE],
    kinds={"postgres"},
    description="mart_comment_activity via sp_build_comment_activity(). Baca feature.",
)
def mart_comment_activity(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_activity", "comment_activity_daily")


@asset(
    group_name="gold",
    deps=[_POST, _COMMENT, _FEATURE],   # Langkah 17: setelah NLP
    kinds={"postgres"},
    description="mart_community_contributors via sp_build_community_contributors(). Setelah NLP.",
)
def mart_community_contributors(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_community_contributors", "community_contributors")


@asset(
    group_name="gold",
    deps=[_POST],
    kinds={"postgres"},
    description="mart_content_attributes via sp_build_content_attribute_daily().",
)
def mart_content_attributes(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_content_attribute_daily", "content_attribute_daily")


@asset(
    group_name="gold",
    deps=[_POST, _STORY],
    kinds={"postgres"},
    description="mart_pillar_performance via sp_build_pillar_performance().",
)
def mart_pillar_performance(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_pillar_performance", "pillar_performance_daily")


@asset(
    group_name="gold",
    deps=[_PROFILE, _STORY],
    kinds={"postgres"},
    description="mart_story_funnel via sp_build_story_funnel() (IG-only).",
)
def mart_story_funnel(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_story_funnel", "story_metric_daily")


@asset(
    group_name="gold",
    deps=[_PROFILE],
    kinds={"postgres"},
    description="mart_tiktok_churn via sp_build_tiktok_churn() (TikTok-only).",
)
def mart_tiktok_churn(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_tiktok_churn", "tiktok_churn_daily")


@asset(
    group_name="gold",
    deps=[_TAGGED],
    kinds={"postgres"},
    description="mart ugc_tagged_posts (Tagged Posts, Audience Deep Dive) via sp_build_ugc_tagged_posts(). IG-only.",
)
def ugc_tagged_posts(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_ugc_tagged_posts", "ugc_tagged_posts")


# --- Audience marts (Gap #2 PETA_GOLD_DASHBOARD) --------------------------
# Dua mart baru dari l1_silver.unified_audience (IG-only saat ini).
# age/gender sudah kolom flat di Silver; city/country jsonb di-unnest di procedure.
# Melayani halaman AUDIENCE DEEP DIVE: Age Distribution, Gender Split, Top Audience Cities.
@asset(
    group_name="gold",
    deps=[_AUDIENCE],
    kinds={"postgres"},
    description=(
        "l2_gold.audience_demographics_daily (grain hari x tipe) via "
        "sp_build_audience_demographics_daily(). Age buckets + gender (count, bukan %). "
        "IG-only. Developer query tabel ini langsung (Model 1)."
    ),
)
def audience_demographics_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_audience_demographics_daily", "audience_demographics_daily")


@asset(
    group_name="gold",
    deps=[_AUDIENCE],
    kinds={"postgres"},
    description=(
        "l2_gold.audience_geo_daily (grain hari x tipe x level x key) via "
        "sp_build_audience_geo_daily(). Unnest city_breakdown & country_breakdown (jsonb). "
        "IG-only. Developer query tabel ini langsung (Model 1)."
    ),
)
def audience_geo_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_audience_geo_daily", "audience_geo_daily")


# --- Posting time heatmap (Gap #3 PETA_GOLD_DASHBOARD) --------------------
# Agregat weekday x hour (WIB) dari unified_post. post_date sudah WIB wall-clock,
# jadi EXTRACT(DOW/HOUR) langsung benar. Full rebuild (TRUNCATE+INSERT di procedure).
# Melayani OVERVIEW -> Best Posting Times.
@asset(
    group_name="gold",
    deps=[_POST],
    kinds={"postgres"},
    description=(
        "l2_gold.posting_time_heatmap (grain weekday x hour, WIB) via "
        "sp_build_posting_time_heatmap(). Best Posting Times (OVERVIEW). "
        "Komponen additive; full rebuild. Developer query tabel ini langsung (Model 1)."
    ),
)
def posting_time_heatmap(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_posting_time_heatmap", "posting_time_heatmap")


# --- Dimensi content pillar (Gap #4 PETA_GOLD_DASHBOARD) ------------------
# Tabel konfig/dimensi pillar (nama+warna+urutan). Di-seed dari pillar_performance_daily
# (BUKAN unified_post) supaya brand_id = umbrella -> JOIN FE align. SP pakai DO NOTHING
# jadi tidak menimpa warna/edit user. READ-WRITE untuk app. Melayani CONTENT PILLARS page.
# CATATAN dep: depend ke mart_pillar_performance (Gold->Gold), bukan Silver -- kalau jalan
# duluan sebelum mart terisi, seed-nya kosong.
@asset(
    group_name="gold",
    deps=[mart_pillar_performance],
    kinds={"postgres"},
    description=(
        "l2_gold.dim_content_pillar (dimensi/konfig pillar) via sp_build_dim_content_pillar(). "
        "Seed (brand_id umbrella, content_pillar) dari pillar_performance_daily -> JOIN FE align. "
        "DO NOTHING (tidak menimpa warna user). READ-WRITE untuk app. CONTENT PILLARS page."
    ),
)
def dim_content_pillar(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_dim_content_pillar", "dim_content_pillar")


# --- Comment relevance distribution (Gap #5 PETA_GOLD_DASHBOARD) ----------
# Distribusi komentar per tier relevance (High>75 / Mid 40-75 / Low<40) dari
# feature.comment_relevance_scores. brand_id per-akun -> umbrella via brand_social_accounts
# (pola sama community_contributors). Simpan count (FE hitung pct). AUDIENCE DEEP DIVE.
@asset(
    group_name="gold",
    deps=[_FEATURE],
    kinds={"postgres"},
    description=(
        "l2_gold.comment_relevance_distribution (brand umbrella x platform x tier) via "
        "sp_build_comment_relevance_distribution(). Distribusi tier komentar (High/Mid/Low). "
        "brand_id di-translate ke umbrella via brand_social_accounts. AUDIENCE DEEP DIVE."
    ),
)
def comment_relevance_distribution(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_relevance_distribution", "comment_relevance_distribution")


# --- Comment timeline per post (Campaign Analysis) ------------------------
# Timeline komentar harian per post (WIB). bucket_date = sumbu absolut;
# days_since_post = sumbu relatif (butuh JOIN ke unified_post untuk post_date).
# CATATAN: pakai comment_date (sudah WIB), BUKAN comment_time (UTC).
@asset(
    group_name="gold",
    deps=[_COMMENT, _POST],
    kinds={"postgres"},
    description=(
        "l2_gold.post_comment_timeline (platform x post_id x bucket_date harian, WIB) via "
        "sp_build_post_comment_timeline(). Timeline komentar per post; sumbu absolut (bucket_date) "
        "+ relatif (days_since_post). CAMPAIGN ANALYSIS. Developer query tabel ini langsung (Model 1)."
    ),
)
def post_comment_timeline(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_post_comment_timeline", "post_comment_timeline")


# --- Word cloud per post (Python asset, bukan CALL procedure) -------------
# Beda pola dari mart lain: mart lain memanggil sp_build_* (SQL murni).
# post_wordcloud harus tokenize teks komentar di Python (buang stopword/emoji),
# jadi ia baca Silver, hitung di Python (reuse _tokenize via compute_wordcloud_per_post),
# lalu REPLACE penuh ke l2_gold.post_wordcloud. Pola sama dengan feature_assets.py.
#
# Strategi isi: full REPLACE (TRUNCATE + INSERT). Konsisten dengan Feature layer,
# cocok untuk volume saat ini, dan otomatis membuang kata dari komentar yang sudah
# dihapus/dimoderasi. Bisa diubah ke incremental nanti bila volume besar.
def _fetch_post_comments(postgres: PostgresResource) -> list[dict]:
    """Ambil komentar (post_id, platform, comment_text) untuk hitung word cloud."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT post_id, platform, comment_text
                FROM l1_silver.unified_comment
                WHERE comment_text IS NOT NULL AND comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


@asset(
    group_name="gold",
    deps=[_COMMENT],
    kinds={"postgres", "python"},
    description=(
        "l2_gold.post_wordcloud — top-50 kata per (post_id, platform) dari komentar. "
        "Untuk word cloud Campaign Analysis. Baca Silver, tokenize di Python "
        "(reuse _tokenize), full REPLACE. Developer query tabel ini langsung (Model 1)."
    ),
)
def post_wordcloud(postgres: PostgresResource) -> Output:
    comments = _fetch_post_comments(postgres)
    rows = compute_wordcloud_per_post(comments, top_n=50)

    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE l2_gold.post_wordcloud")
            if rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO l2_gold.post_wordcloud
                        (post_id, platform, word, frequency)
                    VALUES %s
                    """,
                    [(r["post_id"], r["platform"], r["word"], r["frequency"]) for r in rows],
                )
        conn.commit()
    finally:
        conn.close()

    posts = {(r["post_id"], r["platform"]) for r in rows}
    return Output(
        len(rows),
        metadata={
            "rows": len(rows),
            "posts_dengan_wordcloud": len(posts),
            "comments_scanned": len(comments),
            "table": "l2_gold.post_wordcloud",
        },
    )


# --- Comment sentiment (breakdown harian & per post) ----------------------
# Baca feature.comment_sentiment_scores (hasil model IndoRoBERTa). brand_id
# per-akun -> umbrella via brand_social_accounts (pola sama comment_relevance_
# distribution & community_contributors). Simpan count per label (additive),
# bukan persentase -- FE hitung sendiri utk window custom.
@asset(
    group_name="gold",
    deps=[_SENTIMENT_FEATURE],
    kinds={"postgres"},
    description=(
        "l2_gold.comment_sentiment_daily (brand umbrella x platform x hari) via "
        "sp_build_comment_sentiment_daily(). Breakdown count positive/neutral/negative, "
        "additive -- untuk window custom: SUM per kategori, JANGAN AVG kolom ratio."
    ),
)
def comment_sentiment_daily(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_sentiment_daily", "comment_sentiment_daily")


@asset(
    group_name="gold",
    deps=[_SENTIMENT_FEATURE],
    kinds={"postgres"},
    description=(
        "l2_gold.comment_sentiment_post (grain per post) via "
        "sp_build_comment_sentiment_post(). Breakdown count + dominant_sentiment per post."
    ),
)
def comment_sentiment_post(postgres: PostgresResource) -> Output:
    return _build(postgres, "sp_build_comment_sentiment_post", "comment_sentiment_post")


gold_assets = [
    mart_brand_metric_daily,
    post_metric,
    mart_comment_activity,
    mart_community_contributors,
    mart_content_attributes,
    mart_pillar_performance,
    mart_story_funnel,
    mart_tiktok_churn,
    ugc_tagged_posts,
    audience_demographics_daily,
    audience_geo_daily,
    posting_time_heatmap,
    dim_content_pillar,
    comment_relevance_distribution,
    post_comment_timeline,
    post_wordcloud,
    comment_sentiment_daily,
    comment_sentiment_post,
]