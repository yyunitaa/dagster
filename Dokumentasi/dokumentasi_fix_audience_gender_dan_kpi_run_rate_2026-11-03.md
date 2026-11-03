# Fix Audience Gender (TikTok) & KPI Run Rate — 2026-11-03

> Dibuat: 3 November 2026. Dua stored procedure di `autometric_v2` diubah hari ini
> (`l1_silver.sp_sync_unified_audience` dan `l2_gold.sp_calculate_kpi_achievement`),
> dipicu pertanyaan client soal angka gender TikTok yang kelihatan ganjil di
> dashboard, lalu lanjut ke validasi formula Run Rate KPI yang dikasih client.
> Semua perubahan sudah di-apply ke DB + `daily_pipeline_job` sudah dijalankan
> ulang (`RUN_SUCCESS`, materialize all) setiap kali ada perubahan.

---

## 1. Bug: Audience Gender TikTok ikut persen, bukan count

### 1.1 Gejala
Dashboard Audience menampilkan gender split Instagram vs TikTok dengan skala
yang kelihatan beda — IG nunjukin angka count asli (misal "Female: 28.926"),
sementara TikTok cuma nunjukin angka kecil (misal "Female: 52") yang sekilas
kelihatan seperti persentase.

### 1.2 Root cause
Dicek langsung ke raw tabel harmonization:
- `l0_harmonization.instagram_audience.value` = **count asli** dari Meta Graph
  API (contoh: `F=9473, M=5000, U=8050`, totalnya nyambung ke follower count).
- `l0_harmonization.tiktok_audience.value` = **persentase (0–100)** dari TikTok
  Business API (contoh: `Female=66`), karena TikTok memang cuma expose
  persentase, bukan count absolut. `gender_female + gender_male` TikTok selalu
  ≈100 terlepas dari jumlah follower — dicek di 10 brand, konsisten.

Ini **by design** di layer harmonization — ada proc
`l0_harmonization.sp_normalize_audience_percentage_rounding()` yang memang
sengaja membedakan grup "format persentase" (SUM≈100) vs "format absolut"
(count asli), tapi konversi persen→count **belum pernah dibuat** sebelum data
masuk ke `l1_silver.unified_audience`. Akibatnya kolom `gender_female` /
`gender_male` / dkk punya satuan berbeda tergantung platform, padahal nama
kolomnya sama-sama dibaca sebagai "count" oleh Gold & dashboard.

### 1.3 Fix
Diterapkan di `l1_silver.sp_sync_unified_audience` (bukan di harmonization,
biar raw tetap apa adanya sesuai desain `sp_normalize_audience_percentage_rounding`).
Untuk baris TikTok, `value` (persen) dikalikan `follower_count` TikTok
terdekat (carry-forward dari `unified_profile`, `profile_date <= audience_date`)
lalu dibagi 100 → jadi count perkiraan. Berlaku otomatis untuk semua dimensi
TikTok (age, gender, city, country) karena semuanya persen dari sumber yang
sama.

Bonus fix kecil: TikTok punya kategori gender `"Other"` yang sebelumnya **tidak
match** ke bucket manapun (`gender_unknown` cuma nerima `unknown`/`not_specified`/
`unspecified`) — jadi nilainya ke-drop diam-diam. Ditambahkan `'other'` ke
daftar mapping `gender_unknown`.

### 1.4 Verifikasi
```
brand c33167a5... (TikTok): female=111.904 + male=121.229 + unknown=0 = 233.133 = follower_count (233.133) ✓
brand 0eaacf26... (TikTok): female=26.658  + male=33.478  + unknown=1.860 = 61.996 = follower_count (61.996) ✓
```
Total gender TikTok sekarang pas = follower_count (selisih ±1–11 di tanggal
lama wajar, karena carry-forward pakai snapshot follower terdekat, bukan
snapshot persis hari yang sama).

### 1.5 Temuan terpisah (BUKAN bug, informational)
Instagram **TIDAK PERNAH** nutup 100% follower count di breakdown gender-nya
(gap konsisten ~25–35%, `gender_unknown` hampir selalu `0`/`NULL`). Ini
karakteristik asli Meta Graph API `follower_demographics` — sebagian follower
(akun privat/baru/tidak bisa diestimasi) memang tidak ikut breakdown gender
sama sekali. Tidak actionable dari sisi pipeline; kalau perlu, FE bisa kasih
keterangan tambahan ("data mencakup X% dari total follower") biar client tidak
salah tafsir — **belum diputuskan, masih open**.

---

## 2. KPI Run Rate — iterasi formula

### 2.1 Formula awal client (versi lisan) — matematis cacat
Client sempat kasih formula lisan: `Run Rate = CP / ((CP/12) × TimeGone)`.
Dicek aljabar: `CP` saling coret di pembilang & penyebut, hasilnya cuma
`12/TimeGone` — **sama sekali tidak tergantung performance**. Tidak jadi
diimplementasikan; client diminta klarifikasi ke timnya.

### 2.2 Formula resmi dari client (via gambar)
```
Run Rate = Actual Performance / ((Annual Target/12) × Months Elapsed) × 100%
```
Contoh client: Target=15.000, 8/12 bulan, Actual=16.497 → Target
seharusnya=10.000 → **Run Rate = 164,97%**.

### 2.3 Implementasi v1 — prorata berbasis HARI
Digeneralisasi dari formula bulanan client ke basis hari (`days_elapsed/total_days`),
supaya tetap benar untuk KPI custom period (bukan cuma tahunan 12 bulan persis).
```sql
v_prorated_target := v_kpi.value_target * v_days_elapsed / v_total_days;
v_run_rate         := ROUND(v_achieved / v_prorated_target * 100, 2);
```
Berlaku untuk **semua** metric (`metrics_target`) tanpa pengecualian.

### 2.4 Bug ditemukan client — metric rate/average ikut diprorata (SALAH)
Client cek: untuk metric yang sifatnya **rate/persentase** (`avg_er_reach`,
`er_reach`, dkk dari `l2_gold.post_metric`), `achievement_rate` dan `run_rate`
seharusnya **sama persis** — karena `achieved`-nya itu rata-rata, bukan
akumulasi yang menumpuk seiring waktu, jadi target-nya tidak boleh diprorata.

Dicek di DB (brand Fitbar, KPI `avg_er_reach` target 1,8%):
```
achieved = 13,14%
achievement_rate = 730,00%
run_rate (v1, basis hari) = 965,40%   ← SALAH, seharusnya 730,00%
```

### 2.5 Fix v2 — branch-aware (rate vs cumulative)
Kode sudah punya pemisahan branch dari awal (`post_metric` = rate/average vs
`brand_metric_daily` = SUM harian/kumulatif). Perlakuan `run_rate` dibedakan:
- **Branch rate** (`er_reach`, `avg_er_reach`, `er_views`, `avg_er_views`,
  `avg_engagement`, `avg_reach`, `avg_views`, `avg_likes`, `avg_comments`,
  `avg_shares`, `avg_saves`) → `run_rate = achievement_rate`, target **tidak**
  diprorata.
- **Branch kumulatif** (`followers_growth`, `engagement`, `reach`, dst dari
  `brand_metric_daily`) → tetap prorata seperti v1.

> ⚠️ Catatan: pemisahan ini digeneralisasi ke **semua** metric di branch "per
> post", bukan cuma yang literal persentase (`avg_er_reach`). Client baru
> eksplisit konfirmasi untuk `avg_er_reach` — belum dikonfirmasi eksplisit
> untuk `avg_likes`/`avg_comments`/dkk yang non-persentase. Masih masuk akal
> (semuanya rata-rata, bukan kumulatif), tapi worth di-review kalau ada
> keberatan dari client.

Verifikasi: `avg_er_reach` → `achievement_rate = run_rate = 730,00%` ✓.

### 2.6 Fix v3 — basis prorata diganti HARI → BULAN KALENDER
Live production (`app.kepiai.co`) dibandingkan ke contoh manual client
(cutoff 30 Sep 2026, Feb 2026–Jan 2027): basis hari kasih `165,88%`, basis
bulan (`Annual Target/12 × Months Elapsed`) kasih `164,97%` — client minta
exact match ke formula bulanan.
```sql
v_total_months   := (EXTRACT(YEAR FROM end_date)*12 + EXTRACT(MONTH FROM end_date))
                   - (EXTRACT(YEAR FROM start_date)*12 + EXTRACT(MONTH FROM start_date)) + 1;
v_months_elapsed := (EXTRACT(YEAR FROM period_end)*12 + EXTRACT(MONTH FROM period_end))
                   - (EXTRACT(YEAR FROM start_date)*12 + EXTRACT(MONTH FROM start_date)) + 1;
v_prorated_target := v_kpi.value_target * v_months_elapsed / v_total_months;
```
Pakai kalender asli (`year*12+month`), bukan estimasi 30 hari/bulan, supaya
akurat untuk bulan dengan panjang beda-beda (Feb 28/29 vs Jan 31 dst).

Verifikasi manual (angka client): `prorated_target = 15.000 × 8/12 = 10.000`,
`run_rate = 16.497/10.000 × 100 = 164,97%` ✓ persis sama.

### 2.7 State final `sp_calculate_kpi_achievement`
- `achievement_rate = achieved / value_target × 100` — **tidak berubah** dari
  awal, sudah sesuai permintaan client.
- `run_rate`:
  - Metric rate/average (`post_metric`) → `= achievement_rate` (no proration).
  - Metric kumulatif (`brand_metric_daily`) → `achieved / (value_target ×
    months_elapsed/total_months) × 100`, basis bulan kalender.

---

## 3. Temuan lain yang masih OPEN (belum dieksekusi, perlu keputusan)

### 3.1 `Avg. ER Followers` belum nyambung ke KPI engine
Formula resmi sudah terdokumentasi di `dokumentasi_silver_dan_gold_layer.md`
(§4.1, keputusan 03 Jul 2026): `Avg. ER Followers = SUM(er_followers per post)
/ TOTAL POST`, sumber `l2_gold.post_metric.er_followers` (sudah terisi). Tapi
`sp_calculate_kpi_achievement` **tidak** punya `er_followers`/`avg_er_followers`
di daftar `metrics_target` yang didukung — kalau dipakai bikin KPI, langsung
`RAISE EXCEPTION 'Unsupported metrics_target'`.

### 3.2 `er_followers` tidak ada sama sekali untuk kompetitor
`l2_gold.competitor_post_metric` cuma punya raw count
(`like_count, comment_count, share_count, view_count, save_count`), tidak ada
kolom `er_*` apapun. Bahannya tersedia (`competitor_profile_metric_daily.follower_count`
harian + engagement count), tapi belum dirakit jadi metric ER. `er_reach`
kompetitor sendiri memang tidak mungkin dihitung (reach bukan data publik,
kompetitor hanya discrape).

### 3.3 Lompatan follower TikTok Fitbar, 20→22 Okt 2026
`l1_silver.unified_profile` TikTok Fitbar: `6.914` (20–21 Okt) → `8.096` (22
Okt), lompatan +1.182 (~17%) dalam semalam, lalu lanjut naik stabil
~120–150/hari sampai `9.773` (3 Nov). Belum diinvestigasi apakah ini growth
asli atau data quality issue (mirip pola insiden duplikat IG yang pernah
terjadi — lihat `dokumentasi_insiden_duplikat_instagram_2026-10-14.md`).

### 3.4 Brand duplikat di `public.brands`
Ditemukan **2 `brand_id`** berbeda untuk nama "Fitbar" dan **2 `brand_id`**
berbeda untuk "Giv Body Wash" (sama-sama ke-link ke `social_accounts` yang
sama via `brand_social_accounts`). Belum dicek apakah ada data yang ke-split
antara dua `brand_id` ini — berisiko bikin angka di dashboard salah kalau FE
query `brand_id` yang beda-beda per halaman.

### 3.5 Platform X (Twitter) kosong total
`social_accounts` untuk X terdaftar di Emeron, Giv Body Wash, Hers Protex,
Jasjus, tapi `l1_silver.unified_profile` (dan turunannya) nggak punya data
sama sekali untuk platform ini. Belum jelas apakah belum pernah di-scrape,
atau X memang belum didukung penuh di pipeline ingest.

### 3.6 Giv Body Wash TikTok — data baru sejak 1 Sep 2026
Platform lain brand yang sama (FB/IG/dan TikTok brand lain) punya data sejak
Jun–Jul 2025, tapi TikTok Giv baru mulai 1 Sep 2026 — ~14 bulan lebih telat.
Belum dikonfirmasi ke client apakah akun ini memang baru di-connect belakangan.

---

## 4. File SQL terkait
Draft fix (sumber kebenaran ada di DB lewat `CREATE OR REPLACE PROCEDURE`,
file-file ini cuma arsip lokal, tidak di-commit ke repo karena project ini
tidak nyimpen source SQL stored procedure di git — lihat §"Dropped from
original plan" di `CLAUDE.md`):
- `fix_sp_sync_unified_audience.sql` — konversi persen→count TikTok audience.
- `fix_sp_calculate_kpi_achievement.sql` → `_v2.sql` → `_v3.sql` — evolusi
  formula Run Rate (lihat §2.3–2.6).
