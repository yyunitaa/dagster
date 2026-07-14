# Dokumentasi Infrastruktur Autometric

> Dokumen internal — kenapa Autometric pakai Tiger Cloud (TimescaleDB) sebagai warehouse dan GCP Compute Engine buat Dagster. Ditulis biar gak perlu investigasi ulang tiap ganti sesi/orang.

---

## Ringkasan Cepat

| Komponen | Pilihan | Region | Kenapa singkatnya |
|---|---|---|---|
| Data warehouse | Tiger Cloud (TimescaleDB, managed) | Singapore (`ap-southeast-1`) | Produk komersial butuh DB publicly-accessible & managed, bukan cuma buat riset internal. Awalnya kejebak US free tier → latency tinggi. |
| Orchestration (Dagster) | GCP Compute Engine (`e2-medium`) | `asia-southeast1` (Singapore) | Daemon Dagster harus nyala 24 jam (scheduler tick tiap 30 detik) → butuh persistent VM, bukan serverless. Region disamain sama DB biar round-trip query cepat. |

Riwayat singkat: **server kantor (lokal, gak reliable)** → dievaluasi Railway/Render/Dagster+ (semua direject) → **DigitalOcean VPS Singapore** (sempat jalan) → **GCP Compute Engine** (final, DO di-decommission setelah 2-3 malam validasi paralel).

---

## 1. Database: Kenapa Tiger Cloud (TimescaleDB) Singapore

### Situasi awal
Mulai dari Tiger Cloud **free tier**, yang ternyata **permanen terkunci di region `us-east-1` (US)** dan gak bisa dimigrasi ke region lain selama masih di tier gratis. Ini bikin latency query dari Indonesia ke DB jadi tinggi — kerasa banget di stored procedure yang query-heavy kayak yang di `l2_gold`.

### Opsi yang dipertimbangkan
- **Tetap di Tiger Cloud tapi upgrade ke paid + pindah region** (Singapore)
- **Balik ke self-hosted PostgreSQL** (kayak sebelum pakai TimescaleDB)

### Keputusan: Tiger Cloud Singapore (paid)
Tiga faktor yang mastiin keputusan ini:
1. **Autometric bakal jadi produk komersial** yang dijual ke klien, bukan proyek riset internal — jadi butuh infra yang publicly accessible dan reliable dari awal.
2. **TimescaleDB itu rekomendasi, bukan hard requirement** — jadi keputusan pindah provider gak terikat harus TimescaleDB doang, tapi tetap dipilih karena udah cocok sama pattern hypertable yang dipakai.
3. **Biaya cloud ditanggung company** — biaya bukan blocker utama buat pilih managed service dibanding self-hosted.

### Proses migrasi (US free tier → Singapore paid, service `autometric_v2`)
- `pg_dump` belum ada di mesin Windows → install PostgreSQL 18 client tools dulu.
- Output dump awalnya gagal ditulis ke folder `Program Files` (permission) → dialihkan ke folder user (`C:\Users\...`).
- Restore pertama gagal pakai `ON_ERROR_STOP=1` karena skema sistem Timescale (`timescale_functions`) udah ada duluan di instance target → flag itu dilepas biar psql skip object yang udah ada.
- Restore final pakai pattern resmi Tiger Cloud: `timescaledb_pre_restore()` → restore → `timescaledb_post_restore()`, biar chunk hypertable ikut pindah dengan benar.

### Validasi performa pasca-migrasi
Sempat khawatir performa turun karena query di **web console** Tiger Cloud kelihatan makan waktu 1-2 detik. Setelah dicek pakai `EXPLAIN ANALYZE` langsung (bukan lewat console):

```sql
EXPLAIN ANALYZE SELECT count(*) FROM l1_silver.unified_post;
```

- Tiger Cloud Singapore: **0.160 ms** execution time
- Postgres self-hosted lama: **0.084 ms** execution time

Beda gak signifikan buat kebutuhan praktis — delay 1-2 detik yang kelihatan di console itu murni **overhead web console**, bukan performa DB asli. Ini mengonfirmasi Tiger Cloud Singapore aman dipakai.

### Koneksi (⚠️ verifikasi ulang ke `.env` aktual sebelum dipakai — bisa berubah)
- Host: `roznf0z9he.fqsuf1q5d8.tsdb.cloud.timescale.com`
- Port: `31279`
- Database: `tsdb`
- User: `tsdbadmin`
- SSL: wajib (`sslmode=require`), di DBeaver aktifkan tab SSL sebelum connect.

---

## 2. Orchestration: Kenapa Dagster di GCP Compute Engine

### Situasi awal
Dagster sempat jalan di **server kantor lokal** — gak reliable karena scheduler (dan sensor loop) berhenti tiap kali server itu mati/restart. Butuh compute yang nyala terus.

### Opsi yang dievaluasi dan kenapa direject

**Railway / Render**
Dipertimbangkan sebagai alternatif murah/cepat setup, tapi gak lanjut — VPS dinilai lebih predictable dari sisi biaya dan kontrol buat kebutuhan long-running process.

**Dagster+ (Hybrid)**
Di mode Hybrid, eksekusi job tetap jalan di infra sendiri (agent lokal) — Dagster+ cuma host control plane/UI-nya. Artinya **server sendiri tetap harus nyala 24 jam**, jadi gak nyelesein masalah utama (reliability). Ditambah lagi, per **Mei 2026** paket Solo/Starter Dagster+ udah gak dapet kredit gratis sama sekali — semua kredit ditagih dari nol ($0.035–$0.040/kredit) di atas biaya dasar. Contoh dampaknya: pengguna Starter yang tadinya ~$100/bulan bisa naik ke >$1.000/bulan dengan usage sama; pengguna Solo dari $10 bisa naik ke $310. Biaya jadi jauh kurang predictable dibanding VPS flat-rate.

**Dagster+ (Serverless)**
Direject karena dua hal: (1) workload NLP-nya (SentenceTransformer + RoBERTa sentiment) berat dan kurang cocok buat model eksekusi serverless, dan (2) concern data privacy — nambah pihak ketiga (Dagster+ sebagai data processor) yang megang data klien, sementara Autometric adalah platform komersial dengan data klien sensitif.

> **Catatan koreksi**: sempat ada kebingungan apakah "Dagster+ bisa dipakai buat scheduler doang". Yang pernah dibahas sebenarnya bukan Dagster+, tapi **GCP Cloud Scheduler** (produk Google, beda dari Dagster+) sebagai opsi one-shot trigger jam 1 malam tanpa VM nyala 24 jam — cukup Cloud Scheduler → HTTP POST ke Dagster GraphQL endpoint (`launchRun` mutation). Opsi ini gak dipakai karena keputusan akhirnya migrasi penuh (daemon + webserver + code location) ke satu VM, bukan cuma nambah trigger eksternal.

### DigitalOcean VPS Singapore (sempat dipakai)
Setelah opsi cloud terkelola direject, pilihan jatuh ke VPS: **DigitalOcean, region SGP1** — Ubuntu 24.04, 2vCPU/4GB RAM, ~$32/bulan, jalanin `dagster-webserver`, `dagster-daemon`, dan Redis via `docker-compose`. Kenapa Singapore dan bukan region Indonesia:
- DigitalOcean **gak punya data center di Indonesia** — region Asia mereka cuma Singapore (SGP1) dan Bangalore (BLR1).
- DB (Tiger Cloud) udah di Singapore. Dagster itu backend pipeline yang sering banget round-trip query ke DB (stored procedures Gold layer dll), jadi kedekatan ke DB lebih penting daripada kedekatan ke end-user. Ini beda layer sama dashboard/frontend — lokasi dashboard yang nentuin latency buat user Indonesia, bukan lokasi Dagster.

### Migrasi DO → GCP Compute Engine (final)
DO VPS akhirnya dimigrasi ke GCP. Alasan pilih **Compute Engine**, bukan Cloud Run atau GKE:
- **Cloud Run** gak cocok — servernya scale-to-zero (mati kalau gak ada traffic), padahal Dagster daemon harus proses long-running yang ngecek jadwal tiap ~30 detik.
- **GKE** overkill buat deployment single-node kayak sekarang — baru masuk akal kalau udah butuh multi-node/HA atau banyak code location terpisah.
- **Compute Engine** paling mirip pattern VPS DO yang udah ada — `docker-compose` bisa jalan hampir tanpa modifikasi.

**Spek final:**
- Machine type: `e2-medium` (2 vCPU / 4 GB RAM) — setara spek DO sebelumnya
- Region: `asia-southeast1` (Singapore) — tetap satu region sama Tiger Cloud
- OS: Ubuntu 22.04 LTS
- Boot disk: 30 GB (image Docker + model SentenceTransformer/RoBERTa lumayan makan space)
- Static external IP — buat akses Dagster webserver dari luar
- Firewall: port webserver (3000) dibatasi, gak dibuka ke `0.0.0.0/0`

**Config yang dibawa dari DO (nyaris gak berubah):**
- `docker-compose.yml`, `dagster.yaml` (pakai `PostgresRunStorage`/`EventLogStorage`/`ScheduleStorage`, bukan SQLite — perlu karena webserver & daemon jalan sebagai container terpisah yang akses storage bersama)
- `local_startup_timeout: 600` di `dagster.yaml` — tetap dipertahankan buat isu startup SentenceTransformer
- `NoOpComputeLogManager` — ditambahin belakangan buat fix timeout spesifik saat eksekusi dari Windows (`execute_windows_tail`)

**Validasi sebelum decommission DO:** DO VPS dijalanin paralel 2-3 malam buat mastiin scheduler jam 1 malam di GCP nembak dengan bener, baru DO di-shutdown.

**Data privacy check:** karena Autometric platform komersial dengan concern soal data processor (alasan yang sama yang bikin Dagster+ dicoret), region GCP (Singapore) juga perlu dipastikan OK dari sisi kontrak/compliance klien — sama kayak pertimbangan waktu evaluasi Dagster+.

---

## 3. Hal yang Masih Perlu Ditindaklanjuti / Diverifikasi

- [ ] Pastikan `DAGSTER_HOME` di VM GCP nunjuk ke folder `.dagster_home` yang fixed (persisten, gak ke `/tmp` atau default yang ke-reset).
- [ ] Konfirmasi ulang kontrak/compliance klien soal data residency region Singapore (GCP & Tiger Cloud) — belum ada konfirmasi final tertulis di sesi manapun.
- [ ] Verifikasi host/port/kredensial Tiger Cloud di atas masih sama dengan `.env` yang aktif sekarang.
- [ ] Cek apakah Postgres metadata Dagster (run/event log storage) ikut pindah ke GCP atau tetap containerized di VM yang sama (trade-off: containerized lebih murah tapi single point of failure; alternatif: Cloud SQL).

---

*Terakhir diperbarui berdasarkan sesi hingga 10 Juli 2026. Update bagian ini kalau ada perubahan infra selanjutnya.*
