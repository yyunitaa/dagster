# KPI Achievement per Bulan Report — 2026-11-08

> Dibuat: 8 November 2026. Lanjutan dari `dokumentasi_fix_audience_gender_dan_kpi_run_rate_2026-11-03.md`
> (§2, iterasi formula Run Rate). Dipicu pertanyaan client soal Run Rate KPI
> Fitbar `followers_growth` cutoff Oktober 2026 yang di UI tampil `179,82%`.
> Schema `l2_gold.kpi_achievement` + `sp_calculate_kpi_achievement` diubah dan
> sudah di-apply ke DB produksi (`tsdb`), lalu diverifikasi.

---

## 1. Temuan: UI tidak membaca warehouse sama sekali

### 1.1 Gejala
KPI Fitbar · `followers_growth` · target 15.000 · 1 Feb 2026 – 31 Jan 2027,
cutoff bulan report Oktober 2026. UI menampilkan:
```
achieved = 20.174   achievement = 134,49%   run rate = 179,82%
```
Sementara rumus resmi client memberi **179,32%**:
```
Run Rate = Actual / ((Annual Target / 12) x Months Elapsed)
         = 20.174 / (15.000 x 9/12) = 20.174 / 11.250 = 179,32%
```

### 1.2 Tiga angka berbeda untuk satu KPI
Penelusuran menemukan tiga sumber yang tidak sinkron:

| Sumber | achieved | run rate | Keterangan |
|---|---|---|---|
| `l2_gold.kpi_achievement` (tabel) | 21.787 | 174,30% | cutoff `current_date` (November), bukan Oktober |
| Hitungan manual rumus client | 20.600 | 183,11% | `SUM(net_growth_sum)` Feb–Okt |
| UI (`app.kepiai.co`) | 20.174 | 179,82% | selisih EOD, prorata basis HARI |

### 1.3 Root cause
Repo dev (`github.com/gurusingagerry03/autometric_dashboard`) **tidak pernah
`SELECT` dari `l2_gold.kpi_achievement`** — hanya `CALL` procedure-nya saat KPI
dibuat/diubah, dan `DELETE` saat KPI dihapus (`src/lib/kpi/queries.ts:78,126,161`).
Angka di report dihitung sendiri di `src/lib/reports/data/kpiTargetQuery.ts`,
dengan dua perbedaan yang memang disengaja dan terdokumentasi di sana:

1. Cutoff-nya `period_end` (akhir bulan report), bukan `current_date` — karena
   procedure tidak punya parameter periode dan hanya menyimpan satu baris per
   KPI, jadi tidak bisa menjawab "capaian per akhir Oktober".
2. `followers_growth` memakai selisih `follower_count_eod`, bukan
   `SUM(net_growth_sum)` (lihat `src/lib/metrics/followerGrowth.ts`).

Yang **tidak** disengaja: prorata-nya masih basis HARI.
```ts
// kpiTargetQuery.ts:164
const timeGone = r.total_days > 0 ? round2((r.days_elapsed / r.total_days) * 100) : null
```
273/365 = 74,79% padahal mestinya 9/12 = 75,00%. Selisih 0,21 poin inilah yang
bikin 179,82 bukan 179,32.

### 1.4 Kenapa tidak bisa diperbaiki dari data saja
Sempat dicoba: satu-satunya "lever" dari sisi DB adalah `start_date`/`end_date`
di `public.kpi_setting`, karena `total_days`/`days_elapsed` dihitung on-the-fly
di query app, bukan dibaca dari kolom manapun. Menggeser `end_date` ke
30 Jan 2027 (`total_days` = 364) memang bikin Oktober pas 75,00% — tapi bulan
lain tetap meleset (Feb 7,69% vs 8,33%; Sep 66,48% vs 66,67%; Des 91,76% vs
91,67%), plus label periode di kartu ikut berubah. Prorata hari tidak akan
pernah sama dengan prorata bulan karena panjang bulan beda-beda. **Ditolak.**

---

## 2. Keputusan: warehouse menyimpan capaian per BULAN

Supaya rumus hidup di satu tempat saja dan app cukup `SELECT`.

### 2.1 Perubahan schema
```
l2_gold.kpi_achievement
  kpi_id
  period_month   date     BARU -- bulan report (tanggal 1), PK bareng kpi_id
  period_end     date     BARU -- LEAST(akhir period_month, end_date, current_date)
  achieved_value
  value_target
  achievement_rate
  run_rate
  time_gone      numeric  BARU -- months_elapsed / total_months * 100
  computed_at
```
PK `(kpi_id)` → `(kpi_id, period_month)`. Baris lama dihapus (tidak punya
periode, dan memang tidak pernah dibaca aplikasi) lalu diisi ulang procedure.

### 2.2 Perubahan procedure
`sp_calculate_kpi_achievement(p_kpi_id uuid DEFAULT NULL)` — signature tetap,
jadi asset Dagster dan pemanggilan dari app tidak perlu diubah. Isinya sekarang
me-loop bulan: dari `date_trunc('month', start_date)` sampai
`date_trunc('month', LEAST(current_date, end_date))`, UPSERT satu baris per bulan.

`months_elapsed` dihitung dari `period_month` (bukan `current_date`), basis
kalender `year*12 + month` — bukan estimasi 30 hari/bulan, supaya Feb 28 dan
Jan 31 sama-sama dihitung satu bulan.

### 2.3 Perubahan definisi `followers_growth` (DISENGAJA)
Dari `SUM(net_growth_sum)` → **selisih `follower_count_eod`**, disamakan dengan
definisi Nett Followers Growth yang dipakai dashboard & report
(`src/lib/metrics/followerGrowth.ts`): `net_growth_sum` berasal dari
follows−unfollows platform dan sering tidak tally dengan total follower-nya.
Yang dicocokkan client adalah selisih total follower.

Implementasinya per akun: `last_eod − COALESCE(prev_eod, first_eod)`, di mana
`prev_eod` = EOD terakhir SEBELUM `start_date`. Kalau akunnya memang baru mulai
di dalam jendela, jatuh ke `first_eod` supaya bulan onboarding tidak terbaca
sebagai lonjakan. Ini setara dengan telescoping `EOD_GROWTH` + `PREV_EOD_JOIN`
di repo dev.

Efek: Fitbar IG Feb–Okt 2026 = **20.600 (lama) → 20.174 (baru, = angka UI)**.

Metric lain tidak berubah: `total_followers` tetap posisi (EOD terakhir di
jendela), metric rate/average tetap `run_rate = achievement_rate` (tidak
diprorata), metric kumulatif lain tetap `SUM(...)` harian.

---

## 3. Verifikasi

Sudah di-apply ke DB produksi dan di-commit. Hasil: **179 baris, 27 KPI,
Jan 2026 – Nov 2026**.

Fitbar · `followers_growth` · target 15.000:

| period_month | period_end | achieved | achievement | time gone | run rate |
|---|---|---|---|---|---|
| 2026-08-01 | 31 Agu | 11.224 | 74,83% | 58,33% | 128,27% |
| 2026-09-01 | 30 Sep | 16.497 | 109,98% | 66,67% | **164,97%** |
| 2026-10-01 | 31 Okt | **20.174** | **134,49%** | **75,00%** | **179,32%** |
| 2026-11-01 | 8 Nov | 21.335 | 142,23% | 83,33% | 170,68% |

- Baris September **16.497 → 164,97%** persis sama dengan contoh manual yang
  dikasih client (Target 15.000, 8/12 bulan, Actual 16.497). Validasi
  independen kedua, di luar kasus Oktober.
- `achieved` Oktober dicek silang dengan hitung manual selisih EOD: 20.174 ✓
- Procedure dipanggil ulang → tetap 179 baris, tidak ada duplikat (UPSERT) ✓
- Metric rate & `total_followers`: `run_rate = achievement_rate` di semua baris,
  0 pelanggaran ✓

---

## 4. Efek pada run harian (`daily_pipeline_schedule`, 03:15 WIB)

Asset `kpi_achievement` (`autometric/assets/gold_assets.py:572`) tidak diubah —
hanya `CALL` + `count_rows`, tidak membaca kolom. Metadata `rows` akan naik dari
27 ke 179; itu wajar, bukan duplikasi.

**Perilaku normal:**
- Hitung ulang penuh tiap run (bukan incremental). 179 baris = 0,1 detik.
  Plafon 300 baris kalau semua KPI aktif jalan sampai periodenya habis.
- Bulan-bulan lama ikut dihitung ulang → self-healing kalau data historis
  dikoreksi (mis. insiden duplikat IG, lihat
  `dokumentasi_insiden_duplikat_instagram_2026-10-14.md`).
- Tanggal 1 tiap bulan, baris bulan baru muncul sendiri.
- KPI baru dari app langsung ter-backfill semua bulannya lewat
  `CALL sp_calculate_kpi_achievement($kpi_id)` yang sudah ada.
- KPI dihapus → `DELETE WHERE kpi_id` di app menghapus semua barisnya, FK aman.

**Yang perlu diantisipasi:**
- **Run rate turun tiap tanggal 1.** `months_elapsed` naik satu langkah penuh
  sementara `achieved` baru nambah sedikit → pola gigi gergaji. Besarnya pasti:
  Fitbar per 1 Des nanti turun otomatis ×10/11 = **−9,1%** sebelum pertumbuhan
  Desember mengejar. Melekat di rumus client, bukan bug — perlu dijelaskan ke
  client sebelum ditanya.
- **Angka report bulan lalu bisa berubah setelah report dikirim**, karena bulan
  lama dihitung ulang tiap hari. Trade-off langsung dari self-healing. Kalau
  mau dibekukan, perlu kolom `is_final` + aturan "bulan yang sudah lewat tidak
  di-update". **Belum diputuskan, masih open.**
- **KPI non-aktif** (`is_active = false`) di-skip procedure, jadi barisnya
  membeku dengan angka lama dan tidak pernah dihapus. Saat ini tidak ada KPI
  non-aktif, jadi belum jadi masalah.
- **KPI dengan `value_target = 0`** bikin `achievement_rate` NULL → kena
  constraint NOT NULL → asset gagal → seluruh `daily_pipeline_job` gagal.
  Risiko ini sudah ada sebelum perubahan ini; saat ini tidak ada satupun KPI
  bertarget 0. Guard `COALESCE` **belum dipasang, masih open.**

---

## 5. Masih perlu perubahan di repo dev

**UI belum berubah.** Kartu KPI masih menampilkan 179,82% sampai
`kpiTargetQuery.ts` diubah, karena app belum membaca tabel ini.

Dua opsi untuk dev, urut dari yang paling disarankan:

1. **Baca dari warehouse** — duplikasi rumus hilang sekalian:
   ```sql
   SELECT achieved_value, achievement_rate, run_rate, time_gone
     FROM l2_gold.kpi_achievement
    WHERE kpi_id = $1 AND period_month = DATE '2026-10-01';
   ```
2. **Tambal rumusnya saja** — ganti `timeGone` di `kpiTargetQuery.ts` dari
   `days_elapsed / total_days` jadi `months_elapsed / total_months` (basis
   `year*12 + month`). Patch-nya sudah dibuat dan diverifikasi, tapi tidak
   di-push ke repo dev.

Catatan untuk opsi 2: baris **"Time gone"** ikut berubah (74,79% → 75,00%)
kalau toggle-nya nyala — memang harus ikut, karena
`runRate = achievementRate / timeGone`.

---

## 6. File SQL terkait
- `060_kpi-achievement-per-period.sql` (di folder ini) — schema + procedure
  baru, lengkap dengan Down Migration. **Perlu di-commit ke `migrations/` repo
  dev** supaya DB dan repo tidak drift: procedure KPI ternyata
  di-version-control di sana (`057_kpi-ratio-metrics.sql`,
  `059_kpi-total-followers.sql`) — berbeda dari catatan lama di `CLAUDE.md`
  yang menyebut source SQL procedure tidak disimpan di git.
