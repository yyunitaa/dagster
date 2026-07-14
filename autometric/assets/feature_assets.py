"""
Feature assets — output NLP ditulis balik ke warehouse (schema feature).

Langkah 15:
  - comment_relevance_scores: cosine(comment_text, caption) -> 0-100, REPLACE penuh.
  - word_frequencies: top-50 kata per (brand, platform), REPLACE penuh.
  - Jalan SETELAH unified_comment & unified_post.
  - Setelah tulis, invalidate cache Redis tiap brand yang datanya berubah.

Strategi: REPLACE penuh (TRUNCATE + INSERT) sesuai keputusan — simpel, cocok untuk
volume saat ini. Mudah diubah ke incremental nanti.

Tambahan — comment_sentiment_scores:
  - Sentimen komentar (positive/neutral/negative) pakai model IndoRoBERTa
    (w11wo/indonesian-roberta-base-sentiment-classifier).
  - INCREMENTAL (UPSERT), BEDA dari comment_relevance_scores yang REPLACE penuh —
    inference transformer jauh lebih berat daripada cosine similarity, jadi
    re-score seluruh histori tiap run bakal boros compute & waktu.
  - Jalan SETELAH unified_comment saja (tidak butuh caption/unified_post).
"""

from __future__ import annotations

from dagster import asset, Output, AssetKey
from psycopg2.extras import execute_values

from autometric.resources import (
    PostgresResource,
    SentenceTransformerResource,
    RedisResource,
    SentimentModelResource,
)
from autometric.feature.comment_relevance_scorer import (
    compute_relevance_scores,
    compute_word_frequencies,
)
from autometric.feature.comment_sentiment_scorer import compute_sentiment_scores


def _fetch_comments(postgres: PostgresResource) -> list[dict]:
    """Ambil semua comment yang punya teks. brand_id di-cast ke str (UUID)."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT comment_id, platform, brand_id::text, post_id, comment_text
                FROM l1_silver.unified_comment
                WHERE comment_text IS NOT NULL AND comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_captions(postgres: PostgresResource) -> dict[tuple[str, str], str]:
    """Mapping (post_id, platform) -> caption. Hanya post yang punya caption."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT post_id, platform, caption
                FROM l1_silver.unified_post
                WHERE caption IS NOT NULL AND caption <> ''
            """)
            return {(r[0], r[1]): r[2] for r in cur.fetchall()}
    finally:
        conn.close()


def _fetch_comments_for_sentiment(postgres: PostgresResource) -> list[dict]:
    """Ambil comment yang BELUM punya skor sentimen (incremental, beda dari
    _fetch_comments di atas yang REPLACE penuh). Comment tanpa teks di-skip,
    sama seperti relevance scorer."""
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.comment_id, c.platform, c.brand_id::text, c.comment_text
                FROM l1_silver.unified_comment c
                LEFT JOIN feature.comment_sentiment_scores s
                    ON s.comment_id = c.comment_id AND s.platform = c.platform
                WHERE s.comment_id IS NULL
                  AND c.comment_text IS NOT NULL AND c.comment_text <> ''
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


@asset(
    group_name="feature",
    deps=[AssetKey("unified_comment"), AssetKey("unified_post")],  # Langkah 15: setelah keduanya
    kinds={"postgres", "python"},
    description="Skor relevansi comment vs caption (0-100) + word frequencies. REPLACE penuh, lalu invalidate Redis.",
)
def comment_relevance_scores(
    postgres: PostgresResource,
    nlp_model: SentenceTransformerResource,
    redis: RedisResource,
) -> Output:
    # 1. Ambil data dari Silver.
    comments = _fetch_comments(postgres)
    captions = _fetch_captions(postgres)

    # 2. Hitung (CPU/GPU-bound, model sudah dimuat sekali oleh resource).
    relevance_rows = compute_relevance_scores(comments, captions, nlp_model.model)
    word_rows = compute_word_frequencies(comments, top_n=50)

    # 3. Tulis REPLACE penuh dalam satu transaksi.
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE feature.comment_relevance_scores")
            if relevance_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO feature.comment_relevance_scores
                        (comment_id, platform, brand_id, relevance_score)
                    VALUES %s
                    ON CONFLICT (comment_id, platform) DO NOTHING
                    """,
                    [
                        (r["comment_id"], r["platform"], r["brand_id"], r["relevance_score"])
                        for r in relevance_rows
                    ],
                )

            cur.execute("TRUNCATE feature.word_frequencies")
            if word_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO feature.word_frequencies
                        (brand_id, platform, word, frequency)
                    VALUES %s
                    """,
                    [
                        (r["brand_id"], r["platform"], r["word"], r["frequency"])
                        for r in word_rows
                    ],
                )
        conn.commit()
    finally:
        conn.close()

    # 4. Invalidate Redis untuk tiap brand yang muncul di hasil.
    brands = {r["brand_id"] for r in relevance_rows} | {r["brand_id"] for r in word_rows}
    deleted = 0
    for b in brands:
        deleted += redis.invalidate_brand(b)

    return Output(
        len(relevance_rows),
        metadata={
            "relevance_rows": len(relevance_rows),
            "word_freq_rows": len(word_rows),
            "comments_scanned": len(comments),
            "captions_available": len(captions),
            "brands_invalidated": len(brands),
            "redis_keys_deleted": deleted,
        },
    )


@asset(
    group_name="feature",
    deps=[AssetKey("unified_comment")],
    kinds={"postgres", "python"},
    description=(
        "Skor sentimen komentar (positive/neutral/negative) pakai model "
        "w11wo/indonesian-roberta-base-sentiment-classifier. INCREMENTAL (UPSERT) -- "
        "beda dari comment_relevance_scores yang REPLACE penuh, karena inference "
        "transformer jauh lebih berat daripada cosine similarity. Comment tanpa teks "
        "di-skip (sama seperti comment_relevance_scores)."
    ),
)
def comment_sentiment_scores(
    postgres: PostgresResource,
    sentiment_model: SentimentModelResource,
) -> Output:
    # 1. Ambil comment baru yang belum discore.
    comments = _fetch_comments_for_sentiment(postgres)

    if not comments:
        return Output(0, metadata={"new_comments_scored": 0})

    # 2. Score batch (model dimuat sekali oleh resource lewat setup_for_execution,
    #    sama persis pola nlp_model.model di comment_relevance_scores).
    scored_rows = compute_sentiment_scores(comments, sentiment_model.pipeline)

    # 3. UPSERT (bukan TRUNCATE) -- cuma nambah/update baris yang baru discore.
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO feature.comment_sentiment_scores
                    (comment_id, platform, brand_id, sentiment_label, sentiment_score)
                VALUES %s
                ON CONFLICT (comment_id, platform) DO UPDATE SET
                    sentiment_label = EXCLUDED.sentiment_label,
                    sentiment_score = EXCLUDED.sentiment_score,
                    scored_at       = now()
                """,
                [
                    (r["comment_id"], r["platform"], r["brand_id"], r["sentiment_label"], r["sentiment_score"])
                    for r in scored_rows
                ],
            )
        conn.commit()
    finally:
        conn.close()

    label_counts: dict[str, int] = {}
    for r in scored_rows:
        label_counts[r["sentiment_label"]] = label_counts.get(r["sentiment_label"], 0) + 1

    return Output(
        len(scored_rows),
        metadata={
            "new_comments_scored": len(scored_rows),
            **{f"count_{k}": v for k, v in label_counts.items()},
        },
    )


feature_assets = [comment_relevance_scores, comment_sentiment_scores]