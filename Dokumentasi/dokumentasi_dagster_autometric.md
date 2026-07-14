# Dokumentasi Dagster — Autometric

> Disusun berdasarkan source code aktual (`repository.py`, `jobs.py`, `sensors.py`,
> `resources.py`, `assets/*.py`) per 10 Juli 2026. Kalau ada perbedaan dengan versi
> terbaru, source code adalah kebenaran — dokumen ini cuma potret pada saat dibuat.

---

## 1. Kenapa Pakai Dagster?

Sebelum ada Dagster, orkestrasi pipeline (raw → harmonization → silver → feature →
gold) tersebar di **pg_cron** (jadwal jalan di dalam Postgres) dan panggilan manual.
Masalahnya:

- **Urutan tidak terjamin.** pg_cron cuma tahu "jalankan jam sekian", tidak tahu
  "tunggu tabel A selesai sebelum tabel B jalan". Kalau `unified_post` belum selesai
  tapi `sp_build_post_metric` sudah jalan duluan, hasilnya korup diam-diam (tidak
  error, cuma datanya kurang/salah).
- **Tidak ada lineage.** Kalau angka di dashboard aneh, susah menelusuri "ini datang
  dari tabel/procedure mana, terakhir kali di-refresh kapan". Harus cek manual satu-satu.
- **Tidak ada visibilitas kegagalan.** pg_cron gagal secara senyap — tidak ada UI buat
  lihat run mana yang gagal, retry, atau lihat log per-step.
- **Debugging susah.** Semua step "raw → gold" itu satu urutan panjang tanpa checkpoint
  yang bisa dilihat terpisah.

**Analoginya:** pg_cron itu kayak alarm yang bunyi jam 2 pagi bilang "waktunya masak",
tanpa peduli apakah bahan-bahannya sudah siap dipotong dulu. Dagster itu kayak resep
masak bertingkat — tiap langkah (asset) tahu persis bahan apa yang dia butuhkan
(dependency), dan kalau satu langkah gagal, kamu langsung tahu langkah mana yang
gagal dan kenapa, tanpa harus masak ulang dari nol.

Yang didapat dengan pindah ke Dagster:

1. **Dependency graph eksplisit** — Silver baru jalan setelah Harmonization-nya siap,
   Gold baru jalan setelah Silver/Feature yang relevan siap. Dagster yang jaga urutan,
   bukan asumsi manual.
2. **Lineage visual** — di Dagster UI kelihatan garis dari `l0_raw` sampai
   `l2_gold.*`, jadi kalau ada tabel aneh, tinggal lihat "asset ini depend ke apa aja".
3. **Observability** — tiap asset punya row count, waktu eksekusi, dan status
   (sukses/gagal) yang tercatat, jadi ketahuan begitu ada yang bermasalah.
4. **Materialize selektif** — bisa jalankan cuma satu asset (misal cuma
   `mart_tiktok_churn`) tanpa harus re-run seluruh pipeline dari raw.
5. **Satu tempat untuk semua orkestrasi** — raw→harmonization→silver→feature→gold
   semuanya di satu repo Dagster. Tidak ada lagi campuran pg_cron + manual + Dagster.

---

## 2. Konsep Dasar (versi singkat, khusus konteks project ini)

| Konsep | Di project ini artinya |
|---|---|
| **Asset** | Satu "hasil" data yang di-track Dagster — hampir semua asset di sini cuma pembungkus tipis yang manggil `CALL schema.sp_xxx()` di Postgres, lalu lapor row count. Tidak ada transformasi data yang jalan di Python (kecuali Feature layer & `post_wordcloud`). |
| **SourceAsset** | Titik awal lineage yang **bukan** dihasilkan Dagster — di sini cuma `l0_raw`, karena raw diisi proses ingest API (Meta/TikTok) yang terpisah dari Dagster. |
| **Resource** | Koneksi/dependency eksternal yang di-inject ke asset (bukan variabel global), supaya tiap asset gampang di-unit-test. Ada 4: `postgres`, `nlp_model`, `sentiment_model`, `redis`. |
| **Job** | Kumpulan asset yang dijalankan bareng dalam satu run. Cuma ada satu: `daily_pipeline_job` — isinya `AssetSelection.all()`, jadi otomatis mencakup semua asset yang terdaftar. |
| **Schedule** | Pemicu job berdasarkan waktu. Cuma satu: cron `0 2 * * *` (02:00 WIB) menjalankan `daily_pipeline_job`. |
| **Sensor** | Pemicu job berdasarkan event (bukan waktu). **Kosong** di project ini — lihat Bagian 6. |
| **Asset Check** | Validasi otomatis di luar isi data, misalnya "apakah asset ini di-update dalam 25 jam terakhir". Dipakai buat freshness check di Silver. |
| **group_name** | Label pengelompokan asset di UI Dagster (`l0`, `harmonization`, `silver`, `feature`, `gold`) — murni buat kerapian visual, tidak mempengaruhi eksekusi. |

**Pola yang konsisten di seluruh project:** hampir semua asset = `CALL schema.sp_xxx()` + `SELECT COUNT(*)` buat metadata. Dagster di sini **bukan** engine transformasi data —
transformasi tetap di stored procedure Postgres. Dagster cuma jadi **lapisan orkestrasi
dan observability** di atasnya. Pengecualian: Feature layer (scoring NLP) dan
`post_wordcloud` beneran menjalankan logika Python (tokenisasi, cosine similarity,
inference model), bukan cuma manggil SP.

---

## 3. Layer-Layer di Dagster & Fungsinya

Urutan lineage penuh: **`l0_raw` → `harmonization` → `silver` → `feature` → `gold`**

```
l0_raw (SourceAsset, diisi API ingest di luar Dagster)
   │
   ▼
harmonization (6 asset)  — raw mentah → bentuk seragam per entitas
   │
   ▼
silver (6 asset + 2 asset competitor)  — union lintas-platform, siap dipakai hilir
   │
   ├──▶ feature (2 asset)  — NLP: relevance, word freq, sentiment
   │         │
   ▼         ▼
gold (18 asset + 2 asset competitor)  — mart siap query langsung oleh dashboard
```

### 3.1 Layer `l0` — Source (bukan asset yang dieksekusi)

- **`l0_raw`** (`SourceAsset`): representasi data mentah dari Meta Graph API / TikTok
  API. Diisi oleh proses ingest yang **di luar Dagster** (tanggung jawab fullstack
  dev). Dagster cuma menjadikannya titik awal lineage — tidak pernah dimaterialize.

### 3.2 Layer `harmonization` — Raw → Bentuk Seragam per Entitas

**Tujuan:** menyeragamkan struktur data mentah dari 3 platform (Facebook, Instagram,
TikTok) yang formatnya beda-beda, jadi satu bentuk konsisten per entitas
(post/comment/profile/audience/story/tagged), sebelum di-union lintas-platform di
Silver. Ini dulunya dikerjakan pg_cron; sekarang jadi asset Dagster supaya masuk
lineage yang sama dengan layer lain.

Pengelompokan: **1 asset per entitas**, tiap asset manggil beberapa `sp_sync_*`
sekaligus (satu SP per platform) — bukan 1 asset per platform, supaya jumlah asset
tetap wajar tapi lineage tetap bermakna.

| Asset | Manggil procedure | Platform |
|---|---|---|
| `harmonized_post` | `sp_sync_facebook_post_from_raw`, `sp_sync_instagram_post_from_raw`, `sp_sync_tiktok_post_from_raw` | FB, IG, TikTok |
| `harmonized_comment` | `sp_sync_facebook_comment_from_raw`, `sp_sync_instagram_comment_from_raw` | FB, IG (TikTok comment belum ada proc dari raw) |
| `harmonized_profile` | `sp_sync_facebook_profile_from_raw`, `sp_sync_instagram_profile_from_raw`, `sp_sync_tiktok_profile_from_raw` | FB, IG, TikTok |
| `harmonized_audience` | `sp_sync_instagram_audience_from_raw` | IG saja (FB audience deprecated karena struktur raw berubah) |
| `harmonized_story` | `sp_sync_instagram_story_from_raw` | IG saja |
| `harmonized_tagged_post` | `sp_sync_instagram_tagged_post_from_raw` | IG saja (UGC — belum tersambung ke Silver mana pun, disertakan untuk kelengkapan) |

### 3.3 Layer `silver` — Union Lintas-Platform, Siap Dipakai Hilir

**Tujuan:** menyatukan data dari berbagai platform (yang sudah seragam di
harmonization) jadi satu tabel `unified_*` per entitas, yang jadi basis dipakai
Feature dan Gold. Ini adalah **tabel biasa** yang diisi stored procedure
(`CALL l1_silver.sp_sync_*()`), **bukan** materialized view.

| Asset | Depend pada | Catatan urutan |
|---|---|---|
| `unified_profile` | `harmonized_profile` | Independen, tapi harus sync **duluan** — dibutuhkan `unified_post` |
| `unified_post` | `harmonized_post`, `unified_profile` | Butuh `unified_profile` untuk kolom `followers_on_post_day` (carry-forward) |
| `unified_comment` | `unified_post`, `harmonized_comment` | Jalan **setelah** `unified_post` |
| `unified_audience` | `harmonized_audience` | Independen |
| `unified_story` | `harmonized_story` | Independen |
| `unified_tagged_post` | `harmonized_tagged_post` | Independen, IG-only, UGC |

**Freshness check (25 jam):** `unified_post`, `unified_comment`, `unified_audience`,
`unified_profile`, `unified_story` semuanya dicek — kalau tidak ter-update dalam 25
jam terakhir, check ini akan gagal (sinyal ada yang macet di pipeline). Sengaja
`unified_tagged_post` **tidak** diikutkan karena UGC sifatnya sporadis (tidak update
harian), jadi guard 25 jam akan sering false-alarm kalau dipasang.

### 3.4 Layer `feature` — NLP

**Tujuan:** hasil pemrosesan NLP atas komentar, ditulis balik ke warehouse (schema
`feature`) supaya bisa di-JOIN oleh Gold. Ini satu-satunya layer (bareng
`post_wordcloud` di Gold) yang beneran menjalankan Python — bukan cuma `CALL` SP.

| Asset | Depend pada | Isi | Strategi tulis |
|---|---|---|---|
| `comment_relevance_scores` | `unified_comment`, `unified_post` | Skor relevansi comment vs caption (0–100, cosine similarity) **+** `word_frequencies` (top-50 kata per brand/platform) | **REPLACE penuh** (TRUNCATE + INSERT), lalu invalidate cache Redis per brand yang datanya berubah |
| `comment_sentiment_scores` | `unified_comment` | Label sentimen komentar (positive/neutral/negative) pakai model IndoRoBERTa | **INCREMENTAL (UPSERT)** — beda dari asset di atas, karena inference transformer jauh lebih berat daripada cosine similarity; re-score seluruh histori tiap run bakal boros compute |

Kedua asset skip comment yang teksnya kosong/NULL.

### 3.5 Layer `gold` — Mart Siap Query Dashboard

**Tujuan:** tabel agregat/mart final yang di-query **langsung** oleh dashboard
frontend (Model 1 — tanpa API intermediary). Semuanya tabel biasa (bukan hypertable),
diisi via UPSERT/REPLACE di dalam stored procedure masing-masing.

18 asset gold (di luar 2 asset competitor, lihat Bagian 3.6):

| Asset | Depend pada | Fungsi / halaman dashboard |
|---|---|---|
| `mart_brand_metric_daily` | post, comment, profile | Tren engagement harian |
| `post_metric` | post | Metrik per-post (engagement owned/public + 4 jenis ER). Developer query langsung. |
| `mart_comment_activity` | comment, `comment_relevance_scores` | Aktivitas komentar harian |
| `mart_community_contributors` | post, comment, `comment_relevance_scores` | Kontributor komunitas (jalan setelah NLP) |
| `mart_content_attributes` | post | Atribut konten harian |
| `mart_pillar_performance` | post, story | Performa content pillar |
| `mart_story_funnel` | profile, story | Funnel story (IG-only) |
| `mart_tiktok_churn` | profile | Churn TikTok (TikTok-only) |
| `ugc_tagged_posts` | `unified_tagged_post` | Tagged Posts / Audience Deep Dive (IG-only, UGC) |
| `audience_demographics_daily` | audience | Age distribution + gender split (IG-only) |
| `audience_geo_daily` | audience | Top audience cities/country (unnest JSONB, IG-only) |
| `posting_time_heatmap` | post | Best Posting Times (weekday × hour, WIB) |
| `dim_content_pillar` | `mart_pillar_performance` (Gold→Gold!) | Dimensi/konfig pillar (nama, warna) untuk halaman Content Pillars. Pakai `DO NOTHING` supaya tidak menimpa edit user dari app. |
| `comment_relevance_distribution` | `comment_relevance_scores` | Distribusi tier komentar (High/Mid/Low) untuk Audience Deep Dive |
| `post_comment_timeline` | comment, post | Timeline komentar per post (WIB harian) untuk Campaign Analysis |
| `post_wordcloud` | comment | Word cloud per post — **satu-satunya Gold asset yang jalan Python** (tokenisasi), bukan cuma CALL SP |
| `comment_sentiment_daily` | `comment_sentiment_scores` | Breakdown sentimen harian per brand/platform |
| `comment_sentiment_post` | `comment_sentiment_scores` | Breakdown sentimen + dominant sentiment per post |

> Catatan: `v_campaign_posts` adalah VIEW biasa (dibuat saat diminta), **bukan** asset
> Dagster — tidak dijadwalkan.

### 3.6 Layer Competitor (lintas silver & gold)

File terpisah (`competitor_assets.py`), tapi asetnya masuk ke `group_name="silver"`
dan `"gold"` yang sama — bukan layer baru. Kompetitor di-filter dari tabel
harmonization yang **sama** dipakai brand utama (`sp_sync_facebook_post_from_raw` dkk
sudah handle keduanya sekaligus), jadi tidak perlu asset harmonization terpisah.

| Asset | Layer | Depend pada |
|---|---|---|
| `unified_competitor_post` | silver | `harmonized_post` |
| `unified_competitor_profile_daily` | silver | `harmonized_profile` |
| `competitor_post_metric` | gold | `unified_competitor_post` |
| `competitor_profile_metric_daily` | gold | `unified_competitor_profile_daily`, `competitor_post_metric` (butuh post_metric duluan untuk agregasi like/comment/share harian) |

### 3.7 Urutan Eksekusi Penuh (Wave)

Dagster **tidak** menjalankan asset satu-satu secara linear. Asset yang saling
independen (dependency-nya sudah terpenuhi di waktu yang sama) dijalankan **paralel**
dalam satu "wave" (gelombang). Wave berikutnya baru mulai begitu semua asset yang
dia depend pada di wave-wave sebelumnya selesai. Urutan ini dihitung otomatis oleh
Dagster dari graph `deps` — tabel di bawah adalah hasil topological sort dari
dependency yang sama seperti Bagian 3.2–3.6, cuma disusun jadi satu urutan run
penuh dari awal sampai akhir:

| Wave | Asset |
|---|---|
| **0** | `l0_raw` (source, tidak dimaterialize) |
| **1** | `harmonized_post`, `harmonized_comment`, `harmonized_profile`, `harmonized_audience`, `harmonized_story`, `harmonized_tagged_post` |
| **2** | `unified_profile`, `unified_audience`, `unified_story`, `unified_tagged_post`, `unified_competitor_post`, `unified_competitor_profile_daily` |
| **3** | `unified_post`, `mart_story_funnel`, `mart_tiktok_churn`, `ugc_tagged_posts`, `audience_demographics_daily`, `audience_geo_daily`, `competitor_post_metric` |
| **4** | `unified_comment`, `post_metric`, `mart_content_attributes`, `posting_time_heatmap`, `mart_pillar_performance`, `competitor_profile_metric_daily` |
| **5** | `comment_relevance_scores`, `comment_sentiment_scores`, `mart_brand_metric_daily`, `post_comment_timeline`, `post_wordcloud`, `dim_content_pillar` |
| **6** *(terakhir)* | `mart_comment_activity`, `mart_community_contributors`, `comment_relevance_distribution`, `comment_sentiment_daily`, `comment_sentiment_post` |

Poin yang tidak kelihatan dari tabel per-layer di Bagian 3.2–3.6:

- **`unified_post` baru jalan di wave 3**, bukan wave 2 — meskipun `harmonized_post`
  sudah siap dari wave 1, dia tetap nunggu `unified_profile` (wave 2) kelar duluan
  (untuk kolom `followers_on_post_day`).
- **`mart_pillar_performance` (wave 4) harus kelar sebelum `dim_content_pillar`
  (wave 5)** — satu-satunya dependency Gold→Gold di seluruh pipeline.
- **`mart_comment_activity` dan `mart_community_contributors` jadi yang paling
  terakhir (wave 6)** — nunggu `comment_relevance_scores` (hasil NLP, wave 5), yang
  sendirinya nunggu `unified_comment` (wave 4).
- **Rantai terpanjang** (critical path) di seluruh pipeline: `l0_raw` →
  `harmonized_profile` → `unified_profile` → `unified_post` → `unified_comment` →
  `comment_relevance_scores` → `mart_comment_activity` (6 langkah setelah raw). Ini
  yang menentukan berapa lama minimal `daily_pipeline_job` bisa selesai, meskipun
  banyak resource tersedia untuk paralelisasi.

---

## 4. Job & Schedule

```python
daily_pipeline_job = define_asset_job(
    name="daily_pipeline_job",
    selection=AssetSelection.all(),
)

daily_schedule = ScheduleDefinition(
    name="daily_pipeline_schedule",
    job=daily_pipeline_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Asia/Jakarta",
)
```

- Cuma **satu job**: `daily_pipeline_job`, isinya `AssetSelection.all()` — otomatis
  mencakup semua asset terdaftar (harmonization + silver + feature + gold +
  competitor), urutannya dijaga otomatis oleh dependency graph, jadi tidak perlu
  didaftar manual satu-satu.
- Jalan **02:00 WIB** setiap hari (cron `0 2 * * *`, timezone `Asia/Jakarta`).
  Alasan jam segini: kasih jarak setelah aktivitas ingest API malam hari, dan
  dijalankan saat trafik rendah.
- Campaign Analysis (halaman on-demand) **tidak** punya job/schedule terpisah —
  output level-post di-precompute oleh gold assets di run harian
  (`post_comment_timeline`, `post_wordcloud`), lalu agregasi subset yang dipilih user
  dihitung real-time di layer API, bukan di Dagster.

---

## 5. Resources

| Resource | Fungsi | Detail |
|---|---|---|
| `postgres` (`PostgresResource`) | Koneksi ke `autometric_v2`/`tsdb` via psycopg2 | `call_procedure()` jalankan `CALL sp_xxx()`; `count_rows()` untuk metadata row count. Connection string dari env var `AUTOMETRIC_DB_URL` (Tiger Cloud: port 39700, `sslmode=require` wajib). Kredensial **tidak pernah** hardcode. |
| `nlp_model` (`SentenceTransformerResource`) | Model `paraphrase-multilingual-MiniLM-L12-v2` untuk relevance scoring | Di-load sekali per run lewat `setup_for_execution`, dipakai bareng di beberapa asset dalam run yang sama — hindari reload berulang |
| `sentiment_model` (`SentimentModelResource`) | Model `w11wo/indonesian-roberta-base-sentiment-classifier` | Pola loading sama dengan `nlp_model`; load baru terjadi saat job yang butuh resource ini beneran dieksekusi, bukan saat Dagster daemon start — hindari nambah beban startup timeout |
| `redis` (`RedisResource`) | Invalidate cache dashboard per brand | `invalidate_brand(brand_id)` scan & hapus key `dash:{brand_id}:*`. Host dari env var `REDIS_HOST` (default `localhost`) |

---

## 6. Sensors — Sengaja Kosong

`sensors.py` sengaja tidak berisi sensor apa pun. Tiga sensor yang tadinya
direncanakan di blueprint gugur setelah dicocokkan ke kondisi nyata sistem:

1. **`csv_upload_sensor`** — gugur karena sistem ini API-only (Meta/TikTok GraphAPI
   langsung ke `l0_raw`), tidak ada upload file yang perlu di-watch.
2. **`replica_lag_sensor`** — gugur karena replica Tiger Cloud murni untuk High
   Availability/failover (async standby). Pipeline selalu baca-tulis ke **primary**,
   tidak pernah baca dari replica, jadi lag replica tidak memengaruhi kebenaran data.
3. **`harmonization_job`** (job/sensor terpisah untuk harmonization) — gugur karena
   harmonization sekarang jadi asset Dagster biasa yang otomatis masuk
   `daily_pipeline_job`, tidak perlu job terpisah.

Kalau nanti butuh trigger event-driven (misal run begitu `l0_raw` terdeteksi
berubah), tinggal tambah `@sensor` di `sensors.py` lalu daftarkan di `repository.py`.

---

## 7. Cara Pakai Sehari-Hari

- **Materialize semua (full pipeline):** jalankan `daily_pipeline_job` dari Dagster
  UI, atau tunggu schedule 02:00 WIB otomatis jalan.
- **Materialize selektif (asset tertentu saja):** pilih asset spesifik di Dagster UI
  lalu klik "Materialize" (bukan "Materialize all") — ini praktik yang sudah dipakai
  untuk menghindari trigger operasi NLP yang mahal secara tidak sengaja. Dagster
  otomatis akan menyertakan upstream yang stale kalau perlu.
- **Cek lineage:** buka Dagster UI → Asset Graph, filter per `group_name`
  (`l0`/`harmonization`/`silver`/`feature`/`gold`) untuk lihat garis dependency dari
  raw sampai mart tertentu.
- **Cek kegagalan freshness:** kalau `silver_freshness_checks` gagal (merah di UI),
  artinya salah satu dari `unified_post/comment/audience/profile/story` belum
  ter-update 25 jam terakhir — cek run terakhir yang gagal atau apakah schedule
  02:00 WIB benar-benar jalan.
- **Ubah jadwal:** edit `cron_schedule` di `jobs.py`, lalu reload code location di
  Dagster (tidak perlu restart Postgres/procedure apa pun, karena jadwal murni
  konsep Dagster, bukan pg_cron).

---

## 8. Hal yang Perlu Diingat

- **Dagster di sini adalah orkestrator, bukan engine transformasi.** Hampir semua
  asset cuma `CALL schema.sp_xxx()`. Kalau logika transformasi salah, benerinnya di
  stored procedure Postgres, bukan di kode Dagster. Pengecualian: Feature layer dan
  `post_wordcloud`.
- **Urutan Silver penting dan sudah di-encode di `deps`:** `unified_profile` harus
  duluan (dibutuhkan `unified_post` untuk `followers_on_post_day`), lalu
  `unified_comment` setelah `unified_post`. Jangan ubah urutan ini tanpa mengerti
  kenapa — bisa bikin data korup diam-diam kayak masalah lama sebelum ada Dagster.
  
- **`dim_content_pillar` itu Gold-depend-ke-Gold**, bukan Silver — sengaja depend ke
  `mart_pillar_performance` (bukan langsung ke `unified_post`) supaya seed-nya
  (brand_id umbrella, content_pillar) align dengan yang dibaca frontend. Kalau jalan
  duluan sebelum `mart_pillar_performance` terisi, seed-nya kosong.
- **Freshness check tidak mencakup `unified_tagged_post`** — jangan kaget kalau tidak
  ada alert meskipun data UGC lama tidak update, itu memang disengaja.
- **`comment_sentiment_scores` UPSERT, `comment_relevance_scores` REPLACE penuh** —
  kalau butuh re-score ulang seluruh histori sentimen (misal ganti model), harus
  manual TRUNCATE dulu karena asset-nya tidak didesain untuk full-refresh otomatis.
- **`l0_raw` tidak pernah dimaterialize** — kalau di Dagster UI kelihatan asset ini
  "belum pernah materialize", itu normal, karena memang SourceAsset yang diisi di
  luar Dagster.
