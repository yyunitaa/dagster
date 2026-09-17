# Migrasi Autometric Dagster ke Railway — Panduan Step-by-Step

> Dokumen ini untuk deployment Dagster (autometric) ke Railway. Dibuat berdasarkan kode aktual project per September 2026.
> **Arsitektur: 1 Railway service tunggal** — Main container menjalankan Dagster (webserver + daemon) dan NLP model sekaligus. Tidak ada service terpisah. NLP di-load sekali saat job pertama dijalankan (`setup_for_execution`), bukan saat container startup, supaya cold start cepat.

---

## Ringkasan Arsitektur

```
┌─────────────────────────────────────────────────────────────────────┐
│  Railway Project                                                     │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  Main Dagster Service (SINGLE container)                    │    │
│  │                                                              │    │
│  │  dagster-web (port $PORT)                                   │    │
│  │  dagster-daemon                                             │    │
│  │                                                              │    │
│  │  Feature assets: SentenceTransformer + IndoRoBERTa          │    │
│  │  (loaded in-process, on-demand via setup_for_execution)     │    │
│  │                                                              │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                │                                       │
│               ┌────────────────┴────────────────┐                   │
│               │                                 │                    │
│    ┌──────────┴──────────┐          ┌──────────┴──────────┐         │
│    │ Tiger Cloud DB       │          │ Railway Redis       │         │
│    │ (autometric_v2)      │          │ (cache invalidation)│         │
│    │ l0_raw → l1_silver  │          └─────────────────────┘         │
│    │ → feature → gold    │                                           │
│    └─────────────────────┘                                           │
└─────────────────────────────────────────────────────────────────────┘
```

**Alur data feature assets (NLP):**

```
feature_assets.py
  └─ resources.py  (SentenceTransformerResource / SentimentModelResource)
       └─ sentence-transformers / transformers (in-process, local)
            └─ paraphrase-multilingual-MiniLM-L12-v2 + IndoRoBERTa
```

---

## 1. Konfigurasi Repository

### 1.1 `pyproject.toml` (sudah benar — NLP deps di dependencies utama)

`sentence-transformers` dan `transformers` sudah ada di `dependencies` — ini sudah sesuai arsitektur single-container. **Tidak perlu dipindahkan** ke optional dependencies.

```toml
[project]
name = "autometric"
version = "0.1.0"
description = "Autometric data pipeline — Dagster orchestration for the Silver/Gold/Feature layers."
requires-python = ">=3.10"

dependencies = [
    "dagster",
    "dagster-webserver",
    "psycopg",
    "pandas",
    "numpy",
    "redis",
    "sentence-transformers",   # ← NLP model, di container yang sama
    "transformers",             # ← IndoRoBERTa, di container yang sama
]

[project.optional-dependencies]
dev = [
    "pytest",
    "pytest-cov",
    "black",
    "ruff",
]

[tool.setuptools.packages.find]
include = ["autometric*"]

[tool.dagster]
module_name = "autometric.repository"

[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"
```

> ⚠️ **Tidak ada conditional resource selection.** `repository.py` langsung pakai `SentenceTransformerResource` dan `SentimentModelResource` (local, in-process). Tidak ada `RemoteSentenceTransformerResource`, `NLP_SERVICE_URL`, atau HTTP call ke service terpisah.

---

## 2. File Docker & Konfigurasi Railway

### 2.1 `Dockerfile`

```dockerfile
# Main Dagster + NLP — single container
# Berat karena torch (~800 MB), tapi semua dalam satu service.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# HF_HOME di-volume agar cache model persist antar redeploy
# (meski container ephemeral, volume bisa di-attach sebagai PVC di Railway)
ENV HF_HOME=/root/.cache/huggingface
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/app/.dagster_home

# Install Python deps
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]"

# Copy source
COPY . .

# Pre-warm model cache directory
RUN mkdir -p $HF_HOME

EXPOSE 3000

CMD ["bash", "start.sh"]
```

> 💡 **Cold start cepat:** Model **tidak** di-load saat container startup. `setup_for_execution` di `SentenceTransformerResource`/`SentimentModelResource` baru load model saat job yang butuh resource tersebut dijalankan. Container startup ~30 detik, tapi job pertama (cold NLP) bisa 2-5 menit.

### 2.2 `start.sh`

```bash
#!/bin/bash
set -e

echo "[$(date)] Starting dagster-daemon..."
dagster-daemon run &
DAEMON_PID=$!

sleep 5

echo "[$(date)] Starting dagster-webserver on 0.0.0.0:$PORT..."
dagster-webserver \
    --host 0.0.0.0 \
    --port "${PORT:-3000}" \
    --workspace workspace.yaml

kill $DAEMON_PID
```

### 2.3 `railway.toml`

```toml
[railway]
startCommand = "bash start.sh"
healthcheckPath = "/"
healthcheckPort = "$PORT"

[build]
dockerfilePath = "Dockerfile"
```

### 2.4 `dagster.yaml` (PostgreSQL storage)

```yaml
code_servers:
  local_startup_timeout: 600

compute_logs:
  module: dagster._core.storage.noop_compute_log_manager
  class: NoOpComputeLogManager

# PostgreSQL storage — persist run history & schedule state di Tiger Cloud
storage:
  postgres:
    postgres_db:
      hostname:
        env: DAGSTER_PG_HOST
      port:
        env: DAGSTER_PG_PORT
      db_name:
        env: DAGSTER_PG_DB
      username:
        env: DAGSTER_PG_USER
      password:
        env: DAGSTER_PG_PASSWORD
      ssl_context:
        require: true
```

---

## 3. Buat Schema `dagster` di Tiger Cloud (Sekali Saja)

Jalankan di DBeaver atau psql:

```sql
CREATE SCHEMA IF NOT EXISTS dagster;
```

---

## 4. Konfigurasi Railway

### 4.1 Buat Railway Project

```bash
npm install -g @railway/cli
railway login
railway init
```

Atau via dashboard: New Project → Connect GitHub repo.

### 4.2 Deploy Service

Di Railway dashboard, pilih repo dan branch `main`. Set:

**Build:** Dockerfile (`Dockerfile`)

**Start Command:** `bash start.sh`

**Environment Variables:**

| Variable | Value |
|---|---|
| `AUTOMETRIC_DB_URL` | `postgresql://tsdbadmin:<PASSWORD>@host:port/autometric_v2?sslmode=require` |
| `DAGSTER_HOME` | `/app/.dagster_home` |
| `DAGSTER_PG_HOST` | Tiger Cloud hostname |
| `DAGSTER_PG_PORT` | Tiger Cloud port |
| `DAGSTER_PG_DB` | `autometric_v2` |
| `DAGSTER_PG_USER` | `tsdbadmin` |
| `DAGSTER_PG_PASSWORD` | `<PASSWORD>` |
| `REDIS_HOST` | `redis` |
| `HF_HOME` | `/root/.cache/huggingface` |

### 4.3 Provision Redis

```bash
railway add redis
```

Pastikan `REDIS_HOST` diset ke hostname Redis dari Railway (biasanya `redis` untuk internal DNS).

---

## 5. Deployment

### 5.1 Deploy via CLI

```bash
railway up
```

Atau via dashboard: connect repo → deploy.

### 5.2 Cek Build Logs

Build akan lama (~5-15 menit) karena:
- `torch` CPU (~200-800 MB)
- `sentence-transformers` + `transformers`
- Download model weights (`paraphrase-multilingual-MiniLM-L12-v2` + `w11wo/indonesian-roberta-base-sentiment-classifier`)

### 5.3 Cek Status

```bash
railway status
```

---

## 6. Verifikasi Pasca-Deploy

### 6.1 Cek Main Dagster UI

```
https://<domain>.up.railway.app
```

Cek:
- [ ] Assets graph terlihat (harmonization → silver → feature → gold)
- [ ] `daily_pipeline_schedule` (03:15 WIB) terdaftar
- [ ] `new_account_sensor` status `RUNNING`

### 6.2 Trigger Feature Asset (NLP)

Di UI: **Jobs** → `daily_pipeline_job` → Materialize → pilih **satu** feature asset (misal `comment_relevance_scores`).

> ⚠️ Job pertama akan lambat (2-5 menit) karena model di-load dari disk ke RAM untuk pertama kali. Ini normal.

---

## 7. Resource Sizing

| Resource | Minimum | Direkomendasikan |
|---|---|---|
| RAM | **4 GB** | **6-8 GB** |
| CPU | 2 vCPU | 4 vCPU |
| Disk | 5 GB | 10 GB |

torch (CPU) + SentenceTransformer + IndoRoBERTa + Dagster = ~3-4 GB RAM idle, 5-7 GB saat batch inference.

### 7.1 Railway Plan

- **Hobby tier (512 MB): TIDAK CUKUP** — OOM saat model load
- **Professional (4 GB):** cukup, tapi mungkin OOM saat batch inference besar
- **Pro Performance (8 GB):** direkomendasikan

---

## 8. Checklist Pre-Deploy

**File sudah ada (tidak perlu dibuat/ubah):**
- [x] `autometric/resources.py` — `SentenceTransformerResource` + `SentimentModelResource` (local, in-process)
- [x] `autometric/repository.py` — langsung pakai local resources
- [x] `pyproject.toml` — `sentence-transformers` + `transformers` di dependencies

**File baru perlu dibuat:**
- [ ] `Dockerfile` (single container)
- [ ] `start.sh` (webserver + daemon)
- [ ] `railway.toml` (Railway config)
- [ ] `dagster.yaml` (PostgreSQL storage)

**Setup:**
- [ ] Schema `dagster` dibuat di Tiger Cloud
- [ ] Railway project dibuat, repo connected
- [ ] Redis provisioned
- [ ] Env vars diset (AUTOMETRIC_DB_URL, DAGSTER_PG_*, REDIS_HOST, HF_HOME)

**Verifikasi:**
- [ ] Dagster UI accessible
- [ ] Schedule registered
- [ ] Sensors running
- [ ] Feature asset (NLP) trigger test berhasil

---

## 9. Troubleshooting

### Exit code 137 (Out of Memory)

RAM tidak cukup. Upgrade Railway plan ke minimal 4 GB (direkomendasikan 8 GB).

### Schedule tidak trigger

Cek apakah `dagster-daemon` berjalan:
```bash
railway logs | grep -i daemon
```

### Run gagal di feature assets (NLP)

1. Cek apakah model sudah ter-download:
   ```bash
   railway logs | grep -i "sentence\|roberta\|nlp"
   ```
2. Cek HF_HOME punya cukup space.
3. Cek apakah OOM — naikkan RAM.

### psycopg connection error

Pastikan `AUTOMETRIC_DB_URL` punya `?sslmode=require`. Tanpa ini Tiger Cloud menolak koneksi.

---

## 10. Rollback Plan

```bash
# Rollback kode
git revert HEAD
git push origin main

# Redeploy via Railway dashboard
# Buka deployment history → pilih commit lama → Redeploy
```

---

## 11. Catatan Penting

### 11.1 Pipeline Data vs Dagster Metadata

| Data | Lokasi | Persistensi |
|---|---|---|
| Pipeline data (l0_raw → gold) | Tiger Cloud (autometric_v2) | ✅ Permanen |
| Run history, event log, schedule | Tiger Cloud (`dagster` schema) | ✅ Dengan PostgreSQL storage |
| NLP models + cache | Container ephemeral, re-download tiap deploy | ⚠️ Model di-cache di `HF_HOME` yang bisa di-PVC-kan |
| Redis cache | Railway Redis | ✅ Managed |

### 11.2 Model Cache di Railway

Model weights (~600 MB) di-download dari HuggingFace saat pertama kali dibutuhkan. Setiap redeploy container ephemeral — model di-download ulang kecuali:
- Mount PVC ke `HF_HOME`, atau
- Bake model ke image (tambah `RUN python -c "from sentence_transformers import ..."` di Dockerfile)

Untuk production, consider bake model ke image untuk cold start lebih cepat dan hemat bandwidth.

### 11.3 SSL Tiger Cloud

`AUTOMETRIC_DB_URL` dan `DAGSTER_PG_*` harus punya `?sslmode=require`. Tanpa ini koneksi ditolak.

### 11.4 Perbandingan: Single vs Split Architecture

| Aspek | Single Container (ini) | Split (NLP Worker) |
|---|---|---|
| Setup | Simpel — 1 service | Kompleks — 2 service + HTTP |
| Cold start container | ~30 detik | Main ~30s, NLP ~2-5 menit |
| First NLP run | ~2-5 menit (model load) | NLP Worker langsung ready |
| subsequent NLP runs | ~2-5 menit (model load ulang) | ~menit kedua (model reuse) |
| Resource per service | 1 service: 4-8 GB RAM | Main: 1 GB, NLP: 4-8 GB |
| Scaling | Skala bareng (Dagster + NLP) | Bisa scale NLP independent |
| Complexity | Rendah | Tinggi |

Split architecture (NLP Worker) cocok jika:
- NLP inference sering / paralel
- Butuh scale NLP independent dari orchestrator
- Budget untuk 2 service

Single container cocok untuk:
- Pipeline harian (batch, bukan real-time)
- Team kecil / cepat deploy
- Budget terbatas

Kode saat ini mendukung **single container**. Jika nanti butuh split, baru perlu:
1. Buat `nlp_service/` (Flask API)
2. Buat `resources/remote_nlp.py` (HTTP wrapper)
3. Update `repository.py` (conditional resource)
4. Update `pyproject.toml` (pindah NLP ke optional dep)
