"""
comment_sentiment_scorer.py
============================
Sentiment scorer untuk komentar Autometric.

Model: w11wo/indonesian-roberta-base-sentiment-classifier (HuggingFace transformers)
       -> 3 label: 'positive' / 'neutral' / 'negative'

Gaya modul ini SENGAJA disamakan dengan comment_relevance_scorer.py:
  - Plain function, bukan class.
  - TIDAK meng-load model sendiri -- model di-load sekali oleh
    SentimentModelResource (lihat resources.py, method setup_for_execution,
    sama persis pola SentenceTransformerResource) lalu di-pass masuk sebagai
    parameter `pipe`. Modul ini murni logika, gampang di-unit-test tanpa
    perlu load model beneran (tinggal pass fake pipe/mock).

Diadaptasi dari sentiment_analysis.py (project referensi kamu, alva_report_generator),
disederhanakan + dibikin batch (bukan .apply() row-by-row -- jauh lebih cepat).
"""

from __future__ import annotations

import re

VALID_LABELS = {"positive", "neutral", "negative"}

_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE,
)


def clean_text(text: str) -> str:
    """Bersihkan teks komentar sebelum discore.

    Sama seperti bersihkan_teks() di kode referensi: buang URL, mention,
    digit, tanda baca, emoji, dan rapikan whitespace. Hashtag tetap disimpan.
    """
    if not isinstance(text, str):
        return ""

    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"[.,!?;:()\[\]{}\-_\"']", "", text)
    text = _EMOJI_PATTERN.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def compute_sentiment_scores(comments: list[dict], pipe, batch_size: int = 32) -> list[dict]:
    """Score sentimen untuk list comment.

    comments: list of dict, minimal berisi comment_id, platform, brand_id,
              comment_text (format sama seperti _fetch_comments_for_sentiment
              di feature_assets.py).
    pipe: HuggingFace `transformers.pipeline("sentiment-analysis", ...)` yang
          SUDAH di-load (dari SentimentModelResource.pipeline). Fungsi ini
          TIDAK load model sendiri.

    Return: list of dict {comment_id, platform, brand_id, sentiment_label,
            sentiment_score} -- siap di-INSERT/UPSERT ke
            feature.comment_sentiment_scores.

    Komentar kosong/whitespace -> ('neutral', 0.0) tanpa dikirim ke model
    (hemat compute, konsisten dengan kode referensi).
    """
    cleaned = [clean_text(c.get("comment_text")) for c in comments]

    non_empty_idx = [i for i, t in enumerate(cleaned) if t.strip()]
    labels = ["neutral"] * len(cleaned)
    scores = [0.0] * len(cleaned)

    if non_empty_idx:
        payload = [cleaned[i] for i in non_empty_idx]
        raw = pipe(payload, batch_size=batch_size, truncation=True)

        for pos, r in zip(non_empty_idx, raw):
            label = str(r["label"]).lower()
            if label not in VALID_LABELS:
                # Model varian lain kadang balikin 'LABEL_0'/'LABEL_1'/dst --
                # jangan diam-diam nebak mapping-nya, fallback ke neutral dan
                # biarkan kelihatan di data supaya ketauan pas verifikasi.
                label = "neutral"
            labels[pos] = label
            scores[pos] = float(r["score"])

    return [
        {
            "comment_id": c["comment_id"],
            "platform": c["platform"],
            "brand_id": c["brand_id"],
            "sentiment_label": labels[i],
            "sentiment_score": round(scores[i], 5),
        }
        for i, c in enumerate(comments)
    ]


def quick_check(model_name: str = "w11wo/indonesian-roberta-base-sentiment-classifier") -> None:
    """Jalankan manual sekali (BUKAN bagian dari asset/pipeline Dagster) buat
    verifikasi label output model sebelum dipakai production:

        python comment_sentiment_scorer.py

    Ini load model sendiri (beda dari compute_sentiment_scores yang terima
    pipe dari luar) -- khusus buat quick test standalone.
    """
    from transformers import pipeline

    samples = [
        {"comment_id": "1", "platform": "ig", "brand_id": "test", "comment_text": "Produknya bagus banget, aku suka!"},
        {"comment_id": "2", "platform": "ig", "brand_id": "test", "comment_text": "Pelayanannya lambat dan mengecewakan."},
        {"comment_id": "3", "platform": "ig", "brand_id": "test", "comment_text": "Oke standar aja sih, biasa aja."},
        {"comment_id": "4", "platform": "ig", "brand_id": "test", "comment_text": ""},
    ]

    print(f"Memuat model: {model_name} ...")
    pipe = pipeline("sentiment-analysis", model=model_name, tokenizer=model_name, truncation=True)
    print("Model berhasil dimuat!\n")

    results = compute_sentiment_scores(samples, pipe)
    for s, r in zip(samples, results):
        print(f"{r['sentiment_label']:10s} ({r['sentiment_score']:.3f})  <- {s['comment_text']!r}")


if __name__ == "__main__":
    quick_check()