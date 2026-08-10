# Dokumentasi Dagster — Autometric

> Disusun berdasarkan source code aktual (`repository.py`, `jobs.py`, `sensors.py`,
> `resources.py`, `assets/*.py`) per 22 Juli 2026. Kalau ada perbedaan dengan versi
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
| **Schedule** | Pemicu job berdasarkan waktu. Cuma satu: cron `15 3 * * *` (03:15 WIB) menjalankan `daily_pipeline_job`. |
| **Sensor** | Pemicu job berdasarkan event/kondisi DB (bukan waktu). Ada satu: `new_account_sensor` — polling tiap 60 detik, trigger `daily_pipeline_job` begitu ada akun baru yang siap diolah. Lihat Bagian 6. |
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
    cron_schedule="15 3 * * *",
    execution_timezone="Asia/Jakarta",
)
```

- Cuma **satu job**: `daily_pipeline_job`, isinya `AssetSelection.all()` — otomatis
  mencakup semua asset terdaftar (harmonization + silver + feature + gold +
  competitor), urutannya dijaga otomatis oleh dependency graph, jadi tidak perlu
  didaftar manual satu-satu.
- Jalan **03:15 WIB** setiap hari (cron `15 3 * * *`, timezone `Asia/Jakarta`).
  Alasan jam segini: scraper competitor (Apify) jalan 03:00 WIB dan datanya
  konsisten landing ~03:01–03:03 WIB. Buffer sampai 03:15 memastikan pipeline
  tidak makan data competitor yang masih setengah ter-ingest. Main brand
  di-scrape 02:00 via Meta Graph — sudah lama selesai saat 03:15.
  (Sebelumnya jadwal ini 02:00 WIB; digeser karena bentrok dengan window scrape
  competitor.)
- `daily_pipeline_job` punya **dua pemicu**: schedule 03:15 WIB (rutin, semua brand)
  dan `new_account_sensor` (event-driven, akun baru — lihat Bagian 6). Job-nya
  sama persis, jadi tidak ada perbedaan hasil antara keduanya.
- **`QueuedRunCoordinator` dengan `max_concurrent_runs: 1`** — kalau sensor kepicu
  pas run lain masih jalan, run baru ngantri, bukan jalan barengan. Ini yang
  mencegah TRUNCATE tabrakan di asset gold yang pakai pola full rebuild.
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

## 6. Sensors — `new_account_sensor`

Sejak 19 Juli 2026 `sensors.py` **tidak lagi kosong**. Ada satu sensor aktif:
`new_account_sensor`.

### 6.1 Masalah yang Diselesaikan

Sebelum ada sensor, akun brand yang baru connect harus **nunggu run malam** untuk
muncul di dashboard — worst case ~24 jam kosong melompong setelah user
menyambungkan akunnya. Sensor ini bikin akun baru diolah dalam hitungan menit
setelah initial scrape-nya selesai, tanpa bikin job/pipeline baru: dia trigger
`daily_pipeline_job` yang **sama persis** dengan yang dipakai schedule malam.

**Analoginya:** schedule 03:15 itu satpam yang keliling sekali sehari. Sensor ini
satpam kedua yang keliling tiap 1 menit, tapi cuma bereaksi kalau ada tamu baru
yang sudah lengkap surat-suratnya.

### 6.2 Kondisi Trigger

Driver-nya `public.social_accounts WHERE connected = true` (tabel kecil, dan
`connected = false` secara natural memisahkan akun competitor — diverifikasi
2026-07-14). Untuk tiap akun, tiga syarat harus terpenuhi sekaligus:

| Syarat | Cek | Perannya |
|---|---|---|
| **Datanya beneran ada** | `EXISTS` di `l0_raw.fb_post_snapshots` / `ig_media_snapshots` / `tt_video_snapshots` | Pagar anti "log bohong" — log bilang sukses tapi raw kosong |
| **Scrape sudah kelar** | `EXISTS` row di `public.initial_scrape_logs` dengan `status = 'success'` | Gerbang anti data parsial — mencegah pipeline jalan di tengah initial scrape |
| **Belum pernah diolah** | `NOT EXISTS` di `l2_gold.post_metric` (`brand_id = social_accounts.id`) | Pembeda akun baru vs lama, **sekaligus tombol off otomatis** begitu run selesai |

Semua tabel `l0_raw` sudah punya index btree di `social_account_id`, jadi tiap
`EXISTS` cuma index lookup murah — query ini aman dijalankan tiap 60 detik.

> **Catatan grain:** `l2_gold.post_metric.brand_id` itu `social_accounts.id`
> (grain per-channel), **bukan** `brands.id`. Diverifikasi 2026-07-14: 20/20 match
> ke `social_accounts`, 0 match ke `brands`, plus ada FK eksplisit.

### 6.3 Tabel Gerbang: `public.initial_scrape_logs`

Tabel ini bukan bagian dari Dagster, tapi jadi **kontrak antara backend dan
sensor**. Fungsinya seperti buku absensi: backend menulis satu row **sekali, di
akhir**, setelah semua data initial scrape ter-commit ke `l0_raw`.

- Ada row `success` → jaminan raw utuh → sensor boleh jalan.
- Tidak ada row / row `failed` → sensor diam. Backstop tetap jadwal 03:15 WIB
  (full rebuild) yang bakal nyapu akun tersebut di run berikutnya.

Pendekatan ini menggantikan pendekatan debounce lama (nunggu N menit setelah data
pertama muncul) yang tidak deterministik — tidak ada cara tahu apakah scrape sudah
benar-benar selesai atau cuma lagi jeda antar-halaman API.

**Struktur aktual** (diverifikasi via `information_schema.columns` + `pg_constraint`
+ `pg_indexes`, 23 Juli 2026):

```sql
CREATE TABLE public.initial_scrape_logs (
    id                uuid        NOT NULL DEFAULT gen_random_uuid(),
    social_account_id uuid        NOT NULL,
    platform          text        NOT NULL,
    brand_id          uuid,
    org_id            uuid,
    status            text        NOT NULL,
    records_synced    integer,
    error_message     text,
    started_at        timestamptz NOT NULL,
    finished_at       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT initial_scrape_logs_pkey PRIMARY KEY (id),
    CONSTRAINT initial_scrape_logs_status_check
        CHECK (status = ANY (ARRAY['success'::text, 'failed'::text])),
    CONSTRAINT initial_scrape_logs_social_account_id_fkey
        FOREIGN KEY (social_account_id) REFERENCES social_accounts(id),
    CONSTRAINT initial_scrape_logs_brand_id_fkey
        FOREIGN KEY (brand_id) REFERENCES brands(id),
    CONSTRAINT initial_scrape_logs_org_id_fkey
        FOREIGN KEY (org_id) REFERENCES organizations(id)
);

CREATE INDEX idx_isl_account_status
    ON public.initial_scrape_logs USING btree (social_account_id, status);
```

| Kolom | Diisi oleh | Kenapa ada |
|---|---|---|
| `id` | DB (`gen_random_uuid()`) | PK teknis. Bukan identitas akun — lihat catatan multi-row di bawah |
| `social_account_id` | Backend | **Kolom yang dibaca sensor.** Grain per-channel, sama dengan `l2_gold.post_metric.brand_id` |
| `platform` | Backend | Audit/diagnostik. Sensor **tidak** memfilter kolom ini |
| `brand_id`, `org_id` | Backend (opsional, nullable) | Konteks umbrella brand & tenant untuk audit multi-tenant. Tidak dipakai sensor |
| `status` | Backend | Gerbangnya. Cuma `'success'` atau `'failed'` — dipagar CHECK constraint |
| `records_synced` | Backend (opsional) | Diagnostik: berapa row yang masuk. Berguna waktu investigasi "log success tapi gold kosong" |
| `error_message` | Backend (opsional) | Alasan gagal, diisi kalau `status = 'failed'` |
| `started_at` | Backend | **Tidak punya default** — backend wajib supply eksplisit |
| `finished_at` | DB (`now()`) atau backend | Default `now()`, cocok dengan pola "INSERT sekali di akhir" |

Tiga hal dari DDL ini yang mempengaruhi cara baca sensor:

1. **Tidak ada UNIQUE di `social_account_id`** — satu akun **boleh** punya banyak
   row log. Ini disengaja dan justru bikin retry aman: scrape gagal → row
   `failed`; scrape ulang berhasil → row `success` kedua. Sensor pakai
   `EXISTS (... status = 'success')`, bukan "row terakhir statusnya apa", jadi
   kehadiran row `failed` lama **tidak** memblokir apa pun. Konsekuensi lain: kalau
   backend menulis satu row per platform per akun, sensor tetap berperilaku benar
   karena `EXISTS` cuma butuh minimal satu yang sukses.
2. **`idx_isl_account_status (social_account_id, status)`** persis mencakup predikat
   sensor — jadi cek gerbang ini cuma index lookup, aman di-poll tiap 60 detik.
3. **FK ke `social_accounts` tanpa `ON DELETE`** (default `NO ACTION`) — menghapus
   row `social_accounts` akan **ditolak** selama masih ada row log yang menunjuk ke
   sana. Kalau perlu hapus akun (misal bersih-bersih data test), hapus row di
   `initial_scrape_logs` duluan.

### 6.4 Strategi `run_key` (Anti-Duplikat)

`run_key` = gabungan sorted account IDs: `"new_accounts:<id1>,<id2>"`. Kalau
kepanjangan (>200 char), di-hash SHA-1 — determinismenya tetap sama karena ID-nya
sudah di-sort duluan.

Konsekuensinya:

- Satu poll nemu N akun baru → tetap **satu run** (job-nya full pipeline, N run
  identik cuma numpuk antrian tanpa hasil tambahan).
- Run **gagal** dengan set akun yang sama → **tidak** auto-retry, karena Dagster
  men-dedupe `run_key` yang sama. Backstop: re-run manual dari UI, atau jadwal
  03:15 yang full rebuild.
- Kalau muncul akun baru **lain** sebelum yang gagal ke-handle → set berubah →
  `run_key` baru → run baru jalan. Karena gold pakai full rebuild
  (TRUNCATE + INSERT), akun yang tadi gagal ikut kebawa. Self-healing.

### 6.5 Sensor yang Gugur dari Blueprint Fase 5

Tiga sensor yang tadinya direncanakan tetap **tidak** diimplementasikan:

1. **`csv_upload_sensor`** — gugur karena sistem ini API-only (Meta/TikTok GraphAPI
   langsung ke `l0_raw`), tidak ada upload file yang perlu di-watch.
2. **`replica_lag_sensor`** — gugur karena replica Tiger Cloud murni untuk High
   Availability/failover (async standby). Pipeline selalu baca-tulis ke **primary**,
   tidak pernah baca dari replica, jadi lag replica tidak memengaruhi kebenaran data.
3. **`harmonization_job`** (job/sensor terpisah untuk harmonization) — gugur karena
   harmonization sekarang jadi asset Dagster biasa yang otomatis masuk
   `daily_pipeline_job`, tidak perlu job terpisah.

---

## 7. Cara Pakai Sehari-Hari

- **Materialize semua (full pipeline):** jalankan `daily_pipeline_job` dari Dagster
  UI, atau tunggu schedule 03:15 WIB otomatis jalan.
- **Akun brand baru:** tidak perlu tindakan manual. Begitu backend menulis row
  `success` di `public.initial_scrape_logs` dan raw-nya sudah masuk,
  `new_account_sensor` akan trigger `daily_pipeline_job` dalam ≤60 detik.
- **Cek kondisi sensor:** Dagster UI → Sensors → `new_account_sensor` → lihat tick
  history. Tick berwarna abu dengan `SkipReason` = normal (artinya tidak ada akun
  yang memenuhi syarat). Kalau akun baru tidak kunjung kepicu, cek berurutan:
  (1) `connected = true`? (2) ada row di `l0_raw.*_snapshots`? (3) ada row
  `success` di `initial_scrape_logs`? (4) apakah `l2_gold.post_metric` sudah
  terlanjur punya row untuk akun itu (berarti sudah diolah)?
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
  03:15 WIB benar-benar jalan.
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
- **`new_account_sensor` bergantung pada disiplin backend.** Sensor cuma seaman
  timing INSERT ke `initial_scrape_logs`. Kalau backend menulis `success` begitu
  API call selesai (bukan setelah semua data ter-commit ke `l0_raw`), sensor bisa
  jalan di atas data parsial dan hasil gold-nya kurang. Ini kontrak yang harus
  dipegang, bukan sesuatu yang bisa divalidasi dari sisi Dagster.
- **Sensor mati sendiri setelah sukses** — begitu `l2_gold.post_metric` punya row
  untuk akun tersebut, syarat `NOT EXISTS` gagal dan sensor berhenti memicu. Jadi
  kalau butuh re-trigger manual untuk suatu akun, hapus dulu row gold-nya (atau
  cukup re-run job dari UI, lebih aman).
- **`l0_raw` tidak pernah dimaterialize** — kalau di Dagster UI kelihatan asset ini
  "belum pernah materialize", itu normal, karena memang SourceAsset yang diisi di
  luar Dagster.