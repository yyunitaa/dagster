# Glosarium Metrik Autometric — Tooltip Dashboard

> **Tujuan:** teks tooltip Bahasa Indonesia untuk setiap metrik di dashboard, keyed by kolom `l2_gold`.
> **Sumber kebenaran:** hasil introspeksi DB 23 Jul 2026 (`pg_attribute` + `pg_get_functiondef` semua `sp_sync_*` / `sp_build_*`). Bukan dari asumsi dokumentasi.
> **Status:** v1.0 — seluruh entri terverifikasi terhadap DB dan kode feature layer.

---

## 0. Catatan Integrasi untuk Fullstack Dev (bukan teks tooltip)

1. **Semua kolom `er_*` di Gold = fraksi 0..1.** FE tampilkan `× 100` dengan simbol `%`. `NULL` = penyebut nol / tidak diketahui → tampilkan "–", **bukan 0%**.
2. **Rentang tanggal custom (7D/30D/dst):** hitung `SUM(numerator) / SUM(denominator)` dari komponen additive. **Jangan** merata-rata kolom ratio harian (`er_reach_daily`, dll.) — ratio tidak additive.
3. **Facebook "Likes" = kolom `reactions`** (total semua reaksi: Like, Love, Haha, dst.). Berlaku konsisten di brand utama dan kompetitor.
4. **Grain `brand_id` per tabel:**
   - `brand_id` = **akun/channel** (`social_accounts.id`): `post_metric`, `comment_sentiment_post`, `posting_time_heatmap`, `audience_demographics_daily`, `audience_geo_daily`.
   - `brand_id` = **brand payung** (`brands.id`, via `brand_social_accounts`): `brand_metric_daily` (plus `account_id` = akun), `comment_activity_*`, `comment_sentiment_daily`, `comment_relevance_distribution`, `community_contributors`, `content_attribute_daily`, `pillar_performance_daily`, `story_*`, `tiktok_churn_daily`, `ugc_tagged_posts`, `dim_content_pillar`, dan kedua tabel `competitor_*` (brand klien pelacak).
   - Tanpa `brand_id`: `post_comment_timeline`, `post_wordcloud` — join lewat `(platform, post_id)`.
5. **Encoding hari tidak seragam:** `posting_time_heatmap.weekday` = `EXTRACT(DOW)` → **0=Minggu..6=Sabtu**. `unified_comment.comment_weekday` (kalau dipakai) = `ISODOW` → **1=Senin..7=Minggu**. Jangan pakai satu mapping untuk keduanya.
6. **Semua tanggal/jam sudah WIB** (`Asia/Jakarta`) — `post_date`, `comment_date`/`comment_hour`, heatmap `hour`, `post_date_wib` kompetitor.
7. **Data kompetitor: `NULL` ≠ 0.** `NULL` berarti metrik **tidak tersedia** dari scraping publik (mis. comments FB, shares/saves IG). Tampilkan "–" atau "N/A", jangan nol.
8. `post_metric.completion_rate` bertipe **text** (bukan numeric) — parse hati-hati di FE.
9. `brand_metric_daily.video_views_sum` = duplikat `views_sum` (formula sama: `SUM(views)`). Pakai salah satu saja.
10. `posting_time_heatmap` adalah snapshot **all-time** (tanpa filter tanggal, TRUNCATE tiap build) — bukan per-periode.
11. **Rumus formal semua metrik terhitung** (kontrak perhitungan untuk dev) ada di **§10** — teks tooltip di §1–8 hanya versi bahasa awamnya.

---

## 1. `l2_gold.post_metric` — metrik per konten

Grain: 1 baris per post. `brand_id` = akun/channel.

| Key | Label saran | Tooltip |
|---|---|---|
| `likes` | Suka | Jumlah suka yang diterima konten ini. Khusus Facebook, angka ini mencakup semua jenis reaksi (Like, Love, Haha, dan lainnya). |
| `comments` | Komentar | Jumlah komentar yang diterima konten ini. |
| `shares` | Dibagikan | Berapa kali konten ini dibagikan oleh pengguna. |
| `saves` | Disimpan | Berapa kali konten ini disimpan oleh pengguna. Hanya tersedia untuk Instagram dan TikTok. |
| `reposts` | Repost | Berapa kali konten ini di-repost. Hanya tersedia untuk Instagram. |
| `follows` | Follower dari Konten | Jumlah follower baru yang mulai mengikuti akun setelah melihat konten ini. Saat ini hanya tersedia untuk Instagram. |
| `reach` | Jangkauan | Jumlah akun unik yang melihat konten ini. |
| `views` | Tayangan | Berapa kali konten ini ditonton atau dilihat. |
| `impressions` | Impresi | Berapa kali konten ini muncul di layar pengguna, termasuk tayangan berulang oleh orang yang sama. Hanya tersedia untuk Facebook. |
| `followers_on_post_day` | Follower saat Tayang | Jumlah follower akun pada hari konten ini dipublikasikan. Dipakai sebagai pembanding performa antar konten. |
| `engagement_owned` | Total Interaksi | Total seluruh interaksi pada konten ini. Instagram: suka + komentar + bagikan + simpan + repost. TikTok: suka + komentar + bagikan + simpan. Facebook: reaksi + komentar + bagikan. |
| `engagement_public` | Interaksi Publik | Interaksi yang terlihat oleh publik. Instagram: suka + komentar. Facebook dan TikTok: sama dengan total interaksi. |
| `er_reach` | ER (Jangkauan) | Persentase orang yang berinteraksi dibanding jumlah akun yang melihat konten ini. Semakin tinggi, semakin efektif konten memancing interaksi. |
| `er_views` | ER (Tayangan) | Persentase interaksi dibanding jumlah tayangan konten ini. |
| `er_impressions` | ER (Impresi) | Persentase interaksi dibanding jumlah impresi konten ini. Hanya relevan untuk Facebook. |
| `er_followers` | ER (Follower) | Persentase interaksi dibanding jumlah follower akun pada hari konten tayang. |
| `avg_watch_time` | Rata-rata Durasi Tonton | Rata-rata lamanya penonton menonton konten video ini. Tersedia untuk Instagram Reels dan TikTok. **Perhatian satuan: Instagram dalam milidetik, TikTok dalam detik** — FE perlu konversi IG (÷1000) sebelum dibandingkan atau ditampilkan bersamaan dengan TikTok. |
| `duration_s` | Durasi | Panjang konten video dalam detik. |
| `completion_rate` | Tingkat Tonton Selesai | Persentase penonton yang menonton video ini sampai selesai. Hanya tersedia untuk TikTok. |
| `reels_skip_rate` | Reels Skip Rate | Belum tersedia — sumber data dari platform belum menyediakan metrik ini. Nilai selalu kosong; sebaiknya jangan ditampilkan dulu. |

**Dimensi (bukan metrik, tooltip opsional):** `post_type` (jenis konten), `content_pillar` / `content_pillar_id` (kategori pilar konten; otomatis dicocokkan dari hashtag), `format`, `is_campaign` (bagian dari campaign), `is_boosted` (konten beriklan), `hashtag_list`.

---

## 2. `l2_gold.brand_metric_daily` — rekap harian per channel

Grain: brand payung × akun × platform × hari. Kolom `*_sum` berasal dari konten yang tayang hari itu; kolom follower berasal dari snapshot profil hari itu.

| Key | Label saran | Tooltip |
|---|---|---|
| `post_count` | Jumlah Konten | Jumlah konten yang dipublikasikan pada hari ini. |
| `engagement_sum` | Total Interaksi | Total seluruh interaksi (suka, komentar, bagikan, simpan, repost) dari semua konten yang tayang hari ini. |
| `engagement_public_sum` | Interaksi Publik | Total interaksi yang terlihat publik dari semua konten yang tayang hari ini. |
| `likes_sum` | Suka | Total suka dari semua konten yang tayang hari ini. Facebook: mencakup semua jenis reaksi. |
| `comments_sum` | Komentar | Total komentar dari semua konten yang tayang hari ini. |
| `shares_sum` | Dibagikan | Total berapa kali konten hari ini dibagikan. |
| `saves_sum` | Disimpan | Total berapa kali konten hari ini disimpan. |
| `reposts_sum` | Repost | Total repost dari konten hari ini. Hanya Instagram. |
| `reach_sum` | Jangkauan | Total jangkauan dari semua konten yang tayang hari ini. Catatan: penjumlahan antar konten bisa menghitung orang yang sama lebih dari sekali. |
| `views_sum` | Tayangan | Total tayangan dari semua konten yang tayang hari ini. |
| `impressions_sum` | Impresi | Total impresi dari semua konten yang tayang hari ini. Hanya Facebook. |
| `video_views_sum` | — | (Duplikat `views_sum`, jangan dipakai dua-duanya di UI.) |
| `er_reach_daily` | ER Harian (Jangkauan) | Persentase interaksi dibanding jangkauan untuk konten yang tayang hari ini. Untuk rentang beberapa hari, angka dihitung ulang dari total — bukan rata-rata angka harian. |
| `er_views_daily` | ER Harian (Tayangan) | Persentase interaksi dibanding tayangan untuk konten yang tayang hari ini. |
| `er_impressions_daily` | ER Harian (Impresi) | Persentase interaksi dibanding impresi untuk konten yang tayang hari ini. Hanya Facebook. |
| `er_followers_daily` | ER Harian (Follower) | Persentase interaksi dibanding jumlah follower saat konten tayang. |
| `follower_count_eod` | Total Follower | Jumlah follower akun pada akhir hari ini. |
| `new_followers_sum` | Follower Baru | Jumlah follower baru yang didapat hari ini. Facebook dan TikTok; Instagram hanya menyediakan angka pertumbuhan bersih. |
| `lost_followers_sum` | Follower Hilang | Jumlah follower yang berhenti mengikuti hari ini. Hanya tersedia untuk TikTok. |
| `net_growth_sum` | Pertumbuhan Bersih | Selisih follower baru dan follower yang hilang pada hari ini. |
| `profile_visit_sum` | Kunjungan Profil | Berapa kali profil akun dikunjungi hari ini. |
| `profile_reach_sum` | Jangkauan Profil | Jumlah akun yang terjangkau di tingkat profil hari ini. Penjumlahan antar hari bisa menghitung orang yang sama lebih dari sekali. |
| `accounts_engaged_sum` | Akun Berinteraksi | Jumlah akun unik yang berinteraksi dengan akun ini hari ini. Hanya tersedia untuk Instagram. |

**Komponen internal (jangan ditampilkan sebagai metrik):** `er_denominator_sum`, `reach_denom_sum`, `views_denom_sum`, `impressions_denom_sum`, `followers_denom_sum` — penyebut untuk perhitungan ER pada rentang custom.

---

## 3. Komentar & Komunitas

### 3.1 `l2_gold.comment_activity_daily` / `comment_activity_hourly`

| Key | Label saran | Tooltip |
|---|---|---|
| `comment_count` | Jumlah Komentar | Jumlah komentar yang masuk pada hari (atau jam) ini di semua konten. |
| `likes_sum` (daily) | Suka pada Komentar | Total suka yang diterima komentar-komentar pada hari ini. |
| `replies_sum` (daily) | Balasan Komentar | Total balasan yang diterima komentar-komentar pada hari ini. |
| `hour_of_day` (hourly) | Jam | Jam masuknya komentar dalam waktu Indonesia bagian barat (WIB), 0–23. |

### 3.2 `l2_gold.comment_sentiment_daily` — sentimen komentar per hari

| Key | Label saran | Tooltip |
|---|---|---|
| `total_comments` | Komentar Dianalisis | Jumlah komentar yang dianalisis sentimennya pada hari ini. |
| `positive_count` | Positif | Jumlah komentar bernada positif menurut analisis AI berbahasa Indonesia. |
| `neutral_count` | Netral | Jumlah komentar bernada netral. |
| `negative_count` | Negatif | Jumlah komentar bernada negatif. |
| `avg_sentiment_score` | — | ⚠️ Rata-rata *confidence* model (0..1) terhadap label yang diprediksi, tercampur antar-label. **Bukan** skor polaritas positif–negatif dan tidak sebaiknya ditampilkan sebagai KPI. Untuk mengukur "seberapa positif" sentimen hari itu, pakai proporsi `positive_count / total_comments`. Komentar kosong dihitung sebagai neutral dengan score 0. |

### 3.3 `l2_gold.comment_sentiment_post` — sentimen per konten

Kolom sama dengan 3.2 (per konten), plus:

| Key | Label saran | Tooltip |
|---|---|---|
| `dominant_sentiment` | Sentimen Dominan | Nada komentar yang paling banyak muncul pada konten ini (positif, netral, atau negatif). Bila jumlahnya seri, positif diprioritaskan, lalu negatif. |

### 3.4 `l2_gold.comment_relevance_distribution` — sebaran relevansi komentar

| Key | Label saran | Tooltip |
|---|---|---|
| `tier` | Tingkat Relevansi | Pengelompokan komentar berdasarkan seberapa relevan isinya dengan konten: Tinggi (skor di atas 75), Sedang (40–75), Rendah (di bawah 40). Skor dihitung otomatis oleh AI, skala 0–100. |
| `comment_count` | Jumlah Komentar | Jumlah komentar dalam kelompok relevansi ini. |

### 3.5 `l2_gold.community_contributors` — kontributor teraktif

Grain: per pengguna, per platform, per jendela waktu (`window_days` = 7 / 30 / 90 hari terakhir). Snapshot, dibangun ulang setiap pipeline jalan.

| Key | Label saran | Tooltip |
|---|---|---|
| `comments_count` | Komentar | Jumlah komentar yang ditulis pengguna ini dalam periode terpilih. |
| `likes_received` | Suka Diterima | Total suka yang diterima komentar-komentar pengguna ini. |
| `replies_sum` | Balasan Diterima | Total balasan yang diterima komentar-komentar pengguna ini. |
| `avg_relevance` | Rata-rata Relevansi | Rata-rata skor relevansi komentar pengguna ini terhadap konten brand (0–100). |
| `composite_score` | Skor Kontributor | Skor gabungan 0–100: separuh dari tingkat keaktifan (dibandingkan komentator teraktif), separuh dari rata-rata relevansi komentarnya. |
| `tier` | Kategori | Kategori kontributor berdasarkan skor: Super Fan (70 ke atas), Aktif (40–69), Kasual (di bawah 40). |
| `rank_in_window` | Peringkat | Peringkat pengguna ini di antara seluruh kontributor pada periode terpilih. |

### 3.6 `l2_gold.post_comment_timeline` — linimasa komentar per konten

| Key | Label saran | Tooltip |
|---|---|---|
| `days_since_post` | Hari ke- | Jarak hari antara tanggal komentar dan tanggal konten dipublikasikan. Hari ke-0 berarti komentar masuk di hari yang sama dengan tayangnya konten. |
| `comment_count` | Jumlah Komentar | Jumlah komentar yang masuk pada hari tersebut. |

### 3.7 `l2_gold.post_wordcloud` — kata terpopuler per konten

| Key | Label saran | Tooltip |
|---|---|---|
| `word` / `frequency` | Kata Populer | Kata-kata yang paling sering muncul di **komentar** konten ini beserta jumlah kemunculannya. Teks dinormalisasi (lowercase, minimal 3 karakter, buang stopword Bahasa Indonesia + Inggris, buang emoji dan angka murni). Maksimum 50 kata teratas per konten. |

> **Catatan pipeline (bukan tooltip):** fungsi `compute_wordcloud_per_post` sudah tersedia di `comment_relevance_scorer.py` tapi belum di-wire sebagai step di `feature_assets.py` — asset yang di-wire baru `compute_word_frequencies` (grain brand+platform, ditulis ke `feature.word_frequencies`). Cek `SELECT COUNT(*) FROM l2_gold.post_wordcloud` — kalau 0, wiring perlu ditambahkan sebelum FE bisa memakai tabel ini.

---

## 4. Pilar Konten & Atribut

### 4.1 `l2_gold.pillar_performance_daily` — performa per pilar konten

| Key | Label saran | Tooltip |
|---|---|---|
| `post_count` | Jumlah Konten | Jumlah konten dengan pilar ini yang tayang pada hari ini. |
| `engagement_sum` | Total Interaksi | Total interaksi dari konten pilar ini yang tayang hari ini. |
| `reach_sum` | Jangkauan | Total jangkauan konten pilar ini hari ini. |
| `views_sum` | Tayangan | Total tayangan konten pilar ini hari ini. |
| `watch_time_sum` | — | ⚠️ Ini penjumlahan dari *rata-rata* durasi tonton tiap konten — **bukan** total waktu tonton sesungguhnya. Jangan diberi label "Total Watch Time"; kalau mau dipakai, bagi dengan `post_count` untuk mendapat rata-rata antar konten. |

### 4.2 `l2_gold.content_attribute_daily` — performa per atribut konten

| Key | Label saran | Tooltip |
|---|---|---|
| `content_tag` | Atribut Konten | Penanda jenis konten: beriklan (boosted), kolaborasi, campaign, event, aon, activity, repost, atau organik (tidak beriklan). Satu konten bisa memiliki lebih dari satu atribut. |
| `post_count` | Jumlah Konten | Jumlah konten dengan atribut ini yang tayang hari ini. Karena satu konten bisa punya beberapa atribut, angka antar-atribut tidak boleh dijumlahkan. |
| `engagement_sum` | Total Interaksi | Total interaksi dari konten dengan atribut ini pada hari ini. |

### 4.3 `l2_gold.dim_content_pillar` — master pilar konten (dimensi)

| Key | Tooltip |
|---|---|
| `content_pillar` | Nama pilar/kategori konten yang ditetapkan brand. |
| `hashtags` | Daftar hashtag pemicu — konten yang memakai salah satu hashtag ini otomatis dikategorikan ke pilar ini. |
| `color` / `display_order` / `description` / `is_active` | Pengaturan tampilan: warna chart (kosong = pakai palet default), urutan tampil, deskripsi, dan status aktif. |

---

## 5. Story (Instagram)

### 5.1 `l2_gold.story_metric_daily`

| Key | Label saran | Tooltip |
|---|---|---|
| `story_count` | Jumlah Story | Jumlah story yang tayang pada hari ini. |
| `reach_sum` | Jangkauan | Total akun unik yang melihat story hari ini. |
| `views_sum` | Tayangan | Total berapa kali story hari ini dilihat. |
| `replies_sum` | Balasan | Jumlah balasan (DM) yang diterima story hari ini. |
| `taps_fwd_sum` | Tap Maju | Berapa kali penonton mengetuk untuk melompat ke story berikutnya. Angka tinggi bisa berarti story kurang menahan perhatian. |
| `taps_back_sum` | Tap Mundur | Berapa kali penonton mengetuk untuk kembali menonton ulang story sebelumnya. Angka tinggi biasanya pertanda konten menarik. |
| `exits_sum` | Keluar | Berapa kali penonton keluar dari story sebelum selesai. |
| `swipe_up_sum` | Buka Tautan | Berapa kali penonton membuka tautan yang dipasang di story. |
| `follows_sum` | Follower dari Story | Jumlah follower baru yang datang setelah melihat story hari ini. |

### 5.2 `l2_gold.story_type_daily`

| Key | Label saran | Tooltip |
|---|---|---|
| `story_type` | Tipe Story | Jenis story (mis. foto, video). Nilai "unknown" berarti tipe tidak terdeteksi dari sumber data. |
| `story_count` / `reach_sum` / `replies_sum` | — | Jumlah story, jangkauan, dan balasan per tipe story pada hari ini. |

---

## 6. Follower & Audiens

### 6.1 `l2_gold.tiktok_churn_daily` — pergerakan follower TikTok

| Key | Label saran | Tooltip |
|---|---|---|
| `new_followers` | Follower Baru | Jumlah follower baru TikTok pada hari ini. |
| `lost_followers` | Follower Hilang | Jumlah follower TikTok yang berhenti mengikuti pada hari ini. |
| `net_growth` | Pertumbuhan Bersih | Selisih follower baru dan follower yang hilang pada hari ini. |
| `video_views_sum` | Tayangan Video | Total tayangan video akun TikTok pada hari ini (dari data tingkat profil). |

### 6.2 `l2_gold.audience_demographics_daily` — demografi audiens

| Key | Label saran | Tooltip |
|---|---|---|
| `age_13_17` … `age_65_plus` | Kelompok Usia | Jumlah audiens pada tiap kelompok usia. |
| `gender_female` / `gender_male` / `gender_unknown` | Gender | Jumlah audiens per gender. "Tidak diketahui" berarti platform tidak memiliki data gender pengguna tersebut. |
| `audience_type` | Tipe Audiens | Jenis audiens yang diukur. Dua nilai: `follower_demographics` (demografi seluruh follower akun) dan `engaged_audience_demographics` (demografi audiens yang berinteraksi dengan konten). |

### 6.3 `l2_gold.audience_geo_daily` — lokasi audiens

| Key | Label saran | Tooltip |
|---|---|---|
| `geo_level` / `geo_key` | Lokasi | Sebaran audiens per kota atau per negara. |
| `audience_count` | Jumlah Audiens | Jumlah audiens dari lokasi ini. |

---

## 7. Kompetitor (Brand vs Competitor)

Sumber: scraping publik — cakupan data lebih terbatas dari akun brand sendiri. **`NULL` berarti metrik tidak tersedia dari scraping, bukan nol.** Metrik jangkauan/impresi tidak tersedia sama sekali untuk kompetitor.

### 7.1 `l2_gold.competitor_post_metric` — per konten kompetitor

| Key | Label saran | Tooltip |
|---|---|---|
| `like_count` | Suka | Jumlah suka konten kompetitor. Facebook: mencakup semua jenis reaksi. |
| `comment_count` | Komentar | Jumlah komentar konten kompetitor. Tidak tersedia untuk Facebook. |
| `share_count` | Dibagikan | Berapa kali konten kompetitor dibagikan. Tidak tersedia untuk Instagram. |
| `view_count` | Tayangan | Berapa kali konten kompetitor ditonton. Tidak tersedia untuk Facebook. |
| `save_count` | Disimpan | Berapa kali konten kompetitor disimpan. Hanya tersedia untuk TikTok. |

### 7.2 `l2_gold.competitor_profile_metric_daily` — harian per akun kompetitor

| Key | Label saran | Tooltip |
|---|---|---|
| `follower_count` | Follower | Jumlah follower akun kompetitor pada hari ini. |
| `following_count` | Mengikuti | Jumlah akun yang diikuti kompetitor. Tidak tersedia untuk Facebook. |
| `followers_growth` | Pertumbuhan Follower | Perubahan jumlah follower kompetitor pada hari ini. |
| `post_count` | Konten Hari Ini | Jumlah konten yang dipublikasikan kompetitor pada hari ini. |
| `like_count` … `save_count` | Interaksi Harian | Total suka/komentar/bagikan/tayangan/simpan dari konten kompetitor yang tayang hari ini. Ketersediaan per platform sama dengan tabel per konten. |

### 7.3 `l2_gold.ugc_tagged_posts` — konten UGC yang menandai brand

| Key | Label saran | Tooltip |
|---|---|---|
| `username` | Pengguna | Akun publik yang membuat konten dan menandai brand ini. Saat ini hanya Instagram. |
| `like_count` / `comment_count` | Suka / Komentar | Interaksi yang diterima konten buatan pengguna tersebut. |
| `total_engagement` | Total Interaksi | Jumlah suka ditambah komentar pada konten tersebut. |

---

## 8. Waktu Posting

### `l2_gold.posting_time_heatmap`

Snapshot **sepanjang masa** (bukan per periode). `weekday`: 0=Minggu … 6=Sabtu. `hour`: jam WIB 0–23.

| Key | Label saran | Tooltip |
|---|---|---|
| `post_count` | Jumlah Konten | Jumlah konten yang pernah tayang pada kombinasi hari dan jam ini. |
| `engagement_sum` | Total Interaksi | Total interaksi dari konten yang tayang pada kombinasi hari dan jam ini — untuk melihat waktu posting yang paling efektif. |
| `reach_sum` / `views_sum` | Jangkauan / Tayangan | Total jangkauan dan tayangan konten pada slot waktu ini. |

---

## 9. Lampiran — Metrik Silver yang Belum Diekspos ke Gold

Ada di `l1_silver` tapi belum diproyeksikan ke tabel Gold mana pun (belum bisa tampil di dashboard tanpa perubahan pipeline):

| Kolom Silver | Keterangan |
|---|---|
| `unified_post.link_click` | Klik tautan pada post (Facebook). |
| `unified_post.profile_visits` | Kunjungan profil dari post (Instagram). |
| `unified_post.slide_count` | Jumlah slide carousel. |
| `unified_post.video_view_total_time` | Total waktu tonton Reels (Instagram). |
| `unified_post.title`, `brand_offering`, `is_collab/aon/activity/event/repost` | Atribut konten — flag sudah terwakili di `content_attribute_daily` bentuk agregat. |
| `unified_post.engagement_rate` | ER legacy (skala persen, basis beda per platform: FB/IG=reach, TikTok=views). Gantinya: `er_*` di `post_metric`. |
| `unified_profile.page_like_count` | Likes halaman Facebook. |
| `unified_profile.account_total_post_count` | Total konten sepanjang masa (IG/TikTok). |
| `unified_profile.content_views`, `link_clicks`, `profile_link_taps` | Metrik profil harian tambahan (`content_views` TikTok terpakai di `tiktok_churn_daily`). |
| `unified_profile.likes … total_interactions` | Interaksi tingkat channel harian (IG/TikTok). |
| `unified_story.shares`, `reposts`, `total_interactions`, `profile_activity`, `profile_visit` | Metrik story tambahan di luar funnel utama. |
| `unified_comment.comment_weekday` | Hari komentar (ISODOW: 1=Senin..7=Minggu — beda encoding dengan heatmap!). |

---

## 10. Rumus Formal Metrik Terhitung (Kontrak Perhitungan)

Referensi teknis untuk dev — diturunkan langsung dari definisi stored procedure (verifikasi 23 Jul 2026). Semua komponen `NULL` di penjumlahan diperlakukan 0 (`COALESCE`), kecuali disebut lain. Metrik yang tidak tercantum di sini = data mentah dari platform (tanpa perhitungan).

### 10.1 Per konten (`l1_silver.unified_post` → `l2_gold.post_metric`)

| Metrik | Rumus | Perilaku NULL / edge case |
|---|---|---|
| `engagement_owned` | FB: `reactions + comments + shares` · IG: `likes + comments + shares + saves + repost` · TikTok: `likes + comments + shares + saves` | Komponen NULL dianggap 0. |
| `engagement_public` | IG: `likes + comments` · FB & TikTok: `= engagement_owned` | — |
| `er_reach` | `engagement_owned / reach` | `NULL` bila `reach ≤ 0`. Fraksi 0..1. |
| `er_views` | `engagement_owned / views` | `NULL` bila `views ≤ 0`. Fraksi 0..1. |
| `er_impressions` | `engagement_owned / impressions` | `NULL` bila `impressions ≤ 0`. Praktis FB-only (IG/TikTok impressions = 0 → NULL). |
| `er_followers` | `engagement_owned / followers_on_post_day` | `NULL` bila follower tidak diketahui atau 0. |
| `followers_on_post_day` | `follower_count` dari snapshot `unified_profile` **terakhir** dengan `profile_date <= post_date::date` (carry-forward, per akun+platform) | `NULL` bila post lebih tua dari snapshot pertama. |
| `content_pillar_id` | Pillar pertama di `dim_content_pillar` (via semua brand payung akun) yang `hashtags && hashtag_list`, tie-break `id` terkecil | `NULL` bila tidak ada hashtag yang cocok. |
| `engagement_rate` (Silver, legacy) | FB & IG: `engagement/reach × 100` · TikTok: `engagement/views × 100` | **0** (bukan NULL) bila penyebut 0 — konvensi beda dari `er_*`. Tidak diekspos ke Gold; jangan dipakai. |

### 10.2 Harian channel (`l2_gold.brand_metric_daily`)

Grain agregasi: `(brand payung, akun, platform, hari WIB)`. Sumber post: konten dengan `post_date::date` = hari itu. Sumber profil: snapshot `profile_date` = hari itu.

| Metrik | Rumus | Catatan |
|---|---|---|
| `post_count` | `COUNT(post)` | — |
| `engagement_sum`, `likes_sum`, `comments_sum`, `shares_sum`, `saves_sum`, `reposts_sum`, `engagement_public_sum`, `reach_sum`, `views_sum`, `impressions_sum` | `SUM(kolom terkait di unified_post)` | `video_views_sum` = `SUM(views)` (duplikat `views_sum`). |
| `reach_denom_sum` / `views_denom_sum` / `impressions_denom_sum` / `followers_denom_sum` | `SUM(reach)` / `SUM(views)` / `SUM(impressions)` / `SUM(followers_on_post_day)` | Penyebut untuk ER window custom. |
| `er_denominator_sum` (legacy) | `SUM(CASE engagement_rate_base: 'reach'→reach, 'view'/'views'→views, else 0)` | Penyebut campuran ikut basis per post; pakai `*_denom_sum` yang eksplisit untuk visual baru. |
| `er_reach_daily` | `engagement_sum / reach_denom_sum` | `NULL` bila penyebut 0. Pola sama untuk `er_views_daily`, `er_impressions_daily`, `er_followers_daily`. |
| **ER window custom (N hari)** | `SUM(engagement_sum) / SUM(x_denom_sum)` lintas hari | **Kontrak wajib FE** — jangan `AVG(er_x_daily)`. |
| `follower_count_eod` | `MAX(follower_count)` snapshot profil hari itu | Profil normalnya 1 baris/hari; MAX hanya guard. |
| `new_followers_sum` / `lost_followers_sum` / `net_growth_sum` | `SUM` dari `unified_profile` | Sumber per platform: FB `new`=raw `new_followers`, `lost`=0 (tak tersedia), `net`=`follows_increase` · IG `new`/`lost`=0, `net`=`followers_growth` · TikTok: semua dari raw. |
| `profile_visit_sum` / `profile_reach_sum` / `accounts_engaged_sum` | `SUM` dari profil harian | `accounts_engaged` IG-only (FB/TikTok=0). SUM lintas hari over-count akun unik. |

### 10.3 Komentar & komunitas

| Metrik | Rumus | Catatan |
|---|---|---|
| `comment_activity_*.comment_count` | `COUNT(komentar)` per hari (daily) / per `comment_hour` WIB (hourly) | `likes_sum`/`replies_sum` = `SUM(likes_count)`/`SUM(replies_count)`. |
| `comment_sentiment_*.positive/neutral/negative_count` | `COUNT(*) FILTER (sentiment_label = x)` dari `feature.comment_sentiment_scores` | Hanya komentar yang punya skor sentimen. |
| `avg_sentiment_score` | `AVG(sentiment_score)` dari `feature.comment_sentiment_scores` | `sentiment_score` = confidence 0..1 dari model IndoRoBERTa (`w11wo/indonesian-roberta-base-sentiment-classifier`) terhadap `sentiment_label` yang diprediksi. Rata-rata lintas label tercampur → bukan skor polaritas. Komentar kosong: `('neutral', 0.0)` tanpa inference. |
| `avg_watch_time` (Silver → Gold, passthrough) | Langsung dari sumber: IG = `instagram_post.reel_avg_watch_time`, TikTok = `tiktok_post_extra_attribute.avg_watch_time` | **Satuan tidak seragam**: IG milidetik, TikTok detik. Tidak dinormalisasi di Silver. |
| `dominant_sentiment` | Label ber-count terbanyak; seri → prioritas `positive` > `negative` > `neutral` | `NULL` bila semua count 0. |
| `comment_relevance_distribution.tier` | `relevance_score > 75` → high · `40–75` → mid · `< 40` → low | Skor 0–100 dari `feature.comment_relevance_scores`; skor NULL di-exclude. |
| `community_contributors.avg_relevance` | `AVG(relevance_score)` per user per window | Komentar tanpa skor tetap dihitung di `comments_count`, tapi tak menyumbang AVG. |
| `community_contributors.composite_score` | `ROUND( (comments_count / max_comments × 100) × 0.5 + COALESCE(avg_relevance, 0) × 0.5, 2)` — `max_comments` = komentar terbanyak dalam `(window, brand, platform)` yang sama | Skala 0–100. Skor relatif terhadap komunitas, bukan absolut. |
| `community_contributors.tier` | `≥ 70` → super_fan · `≥ 40` → active · else → casual | — |
| `rank_in_window` | `ROW_NUMBER()` per `(brand, platform, window)` `ORDER BY composite_score DESC, comments_count DESC` | — |
| `post_comment_timeline.days_since_post` | `comment_date − post_date::date` (keduanya WIB) | Bisa negatif secara teori bila data anomali; `NULL` bila post tidak ditemukan di Silver. |

### 10.4 Pilar, atribut, story, heatmap, churn, UGC, kompetitor

| Metrik | Rumus | Catatan |
|---|---|---|
| `content_attribute_daily.*` | Un-pivot flag: tiap flag `TRUE` → 1 baris tag (`boosted/collab/campaign/event/aon/activity/repost`), plus `organic` = `NOT is_boosted`; lalu `COUNT`/`SUM(engagement)`/`SUM(er_denominator)` per tag | Satu post bisa masuk banyak tag → antar-tag **tidak additive**. |
| `pillar_performance_daily.*` | `COUNT`/`SUM` per `(brand, platform, hari, content_pillar)`; hanya post dengan pillar terisi | `watch_time_sum = SUM(avg_watch_time)` ⚠ jumlah dari rata-rata, bukan total waktu tonton. |
| `posting_time_heatmap.weekday`/`hour` | `EXTRACT(DOW)` (0=Minggu..6=Sabtu) / `EXTRACT(HOUR)` dari `post_date` WIB | All-time, tanpa filter periode. |
| `story_metric_daily.*` / `story_type_daily.*` | `COUNT`/`SUM` metrik story per `(brand, platform, hari)` (+ `story_type`, `NULL`→'unknown') | IG only. |
| `tiktok_churn_daily.video_views_sum` | `SUM(unified_profile.content_views)` platform TikTok | `content_views` TikTok = video views harian tingkat profil. |
| `ugc_tagged_posts.total_engagement` | `like_count + comment_count` | — |
| `competitor_profile_metric_daily.post_count` | `COUNT(post kompetitor)` dengan `post_date_wib` = hari itu | **Bukan** total post lifetime akun. |
| `competitor_profile_metric_daily.like_count … save_count` | `SUM` harian dari post kompetitor yang tayang hari itu | 0 bila tidak ada post hari itu; ketersediaan per platform ikut §7.1. |
| `competitor_*.followers_growth` | FB: `follows_increase` · IG/TikTok: `followers_growth` dari scraping | — |


