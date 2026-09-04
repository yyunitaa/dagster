"""
Dagster sensors.

new_account_sensor (Sensor Akun Baru)
  Polling tiap 1 menit: "ada nggak akun connected yang initial scrape-nya
  udah SELESAI (ada log success), datanya udah masuk l0_raw, tapi belum
  pernah diolah sampai l2_gold?" Kalau ada -> trigger daily_pipeline_job
  (job yang SAMA dengan jadwal malam), jadi akun baru nggak perlu nunggu
  run malam.

  Kondisi trigger (idempotent, tanpa cursor) -- tiga syarat, tiga peran:
    connected = true
    AND EXISTS raw post/media/video   (fb_post_snapshots / ig_media_snapshots / tt_video_snapshots)
                                       -- "datanya beneran ada" (pagar anti log bohong)
    AND NOT EXISTS l2_gold.post_metric (brand_id = social_accounts.id, grain per-channel
                                        -- diverifikasi 2026-07-14: 20/20 match social_accounts,
                                        -- 0 match brands, plus ada FK eksplisit)
                                       -- "belum diolah" (pembeda akun baru vs lama,
                                       -- sekaligus tombol off otomatis setelah run selesai)
    AND EXISTS initial_scrape_logs status='success'
                                       -- "scrape sudah kelar" (gerbang anti data parsial;
                                       -- ditulis backend SEKALI di AKHIR initial scrape,
                                       -- lihat DDL 2026-07-19)

  Kenapa driver-nya social_accounts WHERE connected = true:
    - Tabel kecil, dan memisahkan akun competitor secara natural
      (diverifikasi 2026-07-14: semua akun competitor connected = false).
    - Semua tabel l0_raw sudah punya index btree di social_account_id,
      jadi tiap EXISTS = index lookup murah.

  Strategi run_key: gabungan sorted account IDs ("new_accounts:<id1>,<id2>").
    - Satu poll nemu N akun baru -> tetap SATU run (job-nya full pipeline,
      N run identik cuma numpuk antrian).
    - Run gagal dengan set akun yang sama -> TIDAK auto-retry (dedupe Dagster).
      Backstop: re-run manual dari UI, atau jadwal 02:00 (full rebuild) nyapu semua.
    - Kalau muncul akun baru LAIN sebelum yang gagal ke-handle, set berubah ->
      run_key baru -> run baru jalan, dan karena gold full rebuild
      (TRUNCATE+INSERT), akun yang gagal tadi ikut kebawa. Self-healing.
      ⚠️ KOREKSI 2026-09-04: self-healing ini cuma berlaku buat asset yang MASIH
      TRUNCATE+INSERT (post_wordcloud, comment_relevance_scores/word_frequencies
      di Feature). 9 SP leaderboard/snapshot (community_contributors,
      comment_relevance_distribution, posting_time_heatmap, post_comment_timeline,
      ugc_tagged_posts, unified_competitor_post, unified_competitor_profile_daily,
      competitor_post_metric, competitor_profile_metric_daily) sudah diubah jadi
      UPSERT -- akun yang gagal di run sebelumnya TIDAK otomatis ke-bersihin lagi
      di tabel-tabel itu, data basi bisa nyangkut sampai akun itu berhasil diproses
      ulang. Lihat dokumentasi_silver_dan_gold_layer.md untuk detail.

  Catatan overlap: kalau sensor kepicu pas run lain masih jalan,
  QueuedRunCoordinator (max_concurrent_runs: 1) bikin dia ngantri.
  Run susulan jadi redundant full-rebuild -- boros dikit, tapi aman
  (nggak ada TRUNCATE tabrakan).

csv_upload_sensor (Sensor Upload CSV) -- ditambahkan 30 Juli 2026
  Polling tiap 1 menit: "ada nggak baris baru status='success' di
  public.csv_upload_logs yang belum pernah ditrigger?" Kalau ada -> trigger
  daily_pipeline_job (job yang SAMA persis), jadi data csv yang baru
  di-upload nggak perlu nunggu run malam.

  BEDA DESAIN dari new_account_sensor (kenapa nggak bisa pakai NOT EXISTS):
    Grain new_account_sensor per-akun, dan begitu akun itu sudah punya row
    di l2_gold.post_metric, NOT EXISTS otomatis jadi tombol-off -- akun itu
    nggak akan trigger lagi.
    Grain csv_upload_sensor per-BARIS-UPLOAD (bisa banyak file per batch_id,
    dan brand yang sama bisa upload csv berkali-kali). Kalau dipaksa pakai
    NOT EXISTS ke gold, upload csv KEDUA untuk brand yang sama nggak akan
    pernah trigger, karena gold brand itu udah keburu keisi dari upload
    pertama. Makanya sensor ini pakai CURSOR (context.cursor), bukan
    NOT EXISTS: sensor nyimpen created_at row terakhir yang udah ditrigger,
    lalu tiap poll cuma ambil row yang created_at-nya lebih baru dari itu.

  Kenapa cukup 1 gerbang (status='success') tanpa EXISTS raw / NOT EXISTS gold:
    - status di csv_upload_logs cuma keisi kalau proses tulis ke l0_raw
      beneran udah selesai (success/failed/skipped) -- nggak ada state
      "in progress" yang perlu difilter (CHECK constraint cuma izinkan
      3 nilai itu, dan row baru muncul setelah proses kelar).
    - failed & skipped sengaja DIABAIKAN (dianggap "tidak ada apa-apa buat
      diproses"), backstop tetap jadwal malam kalau row itu nantinya
      di-reupload dan sukses.

  Batch semantics (diverifikasi manual dgn dev, 30 Juli 2026):
    - 1 batch_id = 1 aksi upload, BISA berisi banyak file -> banyak row,
      berbagi batch_id yang sama.
    - Row-row dalam 1 batch bisa muncul dengan jeda waktu, TAPI cuma
      beda sepersekian detik -- jauh di bawah interval polling 60 detik,
      jadi praktis semua row 1 batch ketangkep di poll yang sama.
      Makanya sensor ini sengaja TIDAK nunggu "semua row di batch selesai"
      (nggak ada cara murah buat tau itu -- nggak ada kolom "total file
      per batch" di tabel ini) -- cukup ambil apa pun yang lolos filter
      created_at > cursor di tiap poll, per BARIS (bukan per batch).

  Inisialisasi cursor (Opsi A, disepakati 30 Juli 2026):
    Tick pertama kali sensor RUNNING, context.cursor masih None -> sensor
    TIDAK memproses histori csv_upload_logs yang lama. Cursor langsung
    diisi ke NOW() (dari DB, bukan waktu Python, biar konsisten timezone),
    lalu sensor return SkipReason. Baru row-row SETELAH titik itu yang
    akan memicu run. Ini sesuai kebutuhan asli ("kalau ada baris BARU"),
    bukan mau reprocessing histori lama.

  run_key: gabungan sorted row id ("csv_upload:<id1>,<id2>") -- pola sama
  seperti new_account_sensor, prefix beda supaya gampang dibedakan di UI.

Sensor lama dari blueprint Fase 5 (replica_lag_sensor) masih gugur --
alasan lengkap ada di history git / dokumentasi Dagster. csv_upload_sensor
tadinya juga masuk daftar gugur, tapi diaktifkan lagi 30 Juli 2026 karena
kebutuhan riil upload csv manual (bukan cuma ingest API Meta/TikTok).
"""

import hashlib

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from autometric.jobs import daily_pipeline_job
from autometric.resources import PostgresResource


NEW_ACCOUNT_SQL = """
SELECT sa.id, sa.username
FROM public.social_accounts sa
WHERE sa.connected = true
  AND (
        EXISTS (SELECT 1 FROM l0_raw.fb_post_snapshots  r WHERE r.social_account_id = sa.id)
     OR EXISTS (SELECT 1 FROM l0_raw.ig_media_snapshots r WHERE r.social_account_id = sa.id)
     OR EXISTS (SELECT 1 FROM l0_raw.tt_video_snapshots r WHERE r.social_account_id = sa.id)
  )
  AND NOT EXISTS (
        SELECT 1 FROM l2_gold.post_metric pm WHERE pm.brand_id = sa.id
  )
  -- GERBANG INITIAL SCRAPE (2026-07-19): row di initial_scrape_logs ditulis
  -- backend SEKALI, DI AKHIR, setelah semua data initial scrape ter-commit
  -- ke l0_raw. Ada row success = jaminan raw utuh -> aman diolah.
  -- Tanpa row = scrape belum selesai / gagal -> sensor diam,
  -- backstop tetap jadwal malam (full rebuild).
  AND EXISTS (
        SELECT 1 FROM public.initial_scrape_logs l
        WHERE l.social_account_id = sa.id
          AND l.status = 'success'
  )
ORDER BY sa.id
"""


@sensor(
    job=daily_pipeline_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Trigger daily_pipeline_job saat ada akun connected yang datanya "
        "sudah masuk l0_raw tapi belum ada di l2_gold.post_metric."
    ),
)
def new_account_sensor(
    context: SensorEvaluationContext,
    postgres: PostgresResource,
):
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(NEW_ACCOUNT_SQL)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return SkipReason(
            "Tidak ada akun baru yang siap diolah "
            "(semua sudah punya data gold, atau initial scrape belum ada log success)."
        )

    account_ids = sorted(str(r[0]) for r in rows)
    usernames = [r[1] for r in rows]

    joined = ",".join(account_ids)
    run_key = f"new_accounts:{joined}"
    if len(run_key) > 200:
        # banyak akun sekaligus -> run_key di-hash biar nggak kepanjangan,
        # determinismenya tetap sama (sorted ids).
        run_key = "new_accounts:" + hashlib.sha1(joined.encode()).hexdigest()

    context.log.info(
        "Akun baru terdeteksi (%d): %s -> trigger daily_pipeline_job",
        len(rows),
        ", ".join(usernames),
    )

    return RunRequest(
        run_key=run_key,
        tags={
            "trigger": "new_account_sensor",
            "new_accounts": ", ".join(usernames)[:250],
        },
    )


# --- csv_upload_sensor -----------------------------------------------------
# Lihat penjelasan lengkap di docstring modul di atas.

CSV_UPLOAD_SQL = """
SELECT id, created_at, file_name
FROM public.csv_upload_logs
WHERE status = 'success'
  AND created_at > %s
ORDER BY created_at ASC
"""


@sensor(
    job=daily_pipeline_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Trigger daily_pipeline_job saat ada baris baru status='success' di "
        "public.csv_upload_logs (upload csv baru masuk ke l0_raw)."
    ),
)
def csv_upload_sensor(
    context: SensorEvaluationContext,
    postgres: PostgresResource,
):
    conn = postgres.get_conn()
    try:
        with conn.cursor() as cur:
            if context.cursor is None:
                # Tick pertama sensor ini RUNNING: OPSI A -- jangan proses
                # histori csv_upload_logs yang lama. Cursor diinisialisasi
                # ke NOW() dari DB (bukan waktu Python) supaya konsisten
                # dengan timezone/clock server DB.
                cur.execute("SELECT NOW()")
                (now,) = cur.fetchone()
                context.update_cursor(now.isoformat())
                return SkipReason(
                    "Inisialisasi pertama: cursor diset ke waktu sekarang. "
                    "Histori csv_upload_logs lama tidak diproses, hanya baris "
                    "baru setelah titik ini yang akan memicu run."
                )

            cur.execute(CSV_UPLOAD_SQL, (context.cursor,))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return SkipReason(
            "Tidak ada baris baru status='success' di csv_upload_logs "
            "sejak cursor terakhir."
        )

    row_ids = sorted(str(r[0]) for r in rows)
    file_names = [r[2] for r in rows]
    latest_created_at = max(r[1] for r in rows)

    joined = ",".join(row_ids)
    run_key = f"csv_upload:{joined}"
    if len(run_key) > 200:
        # banyak baris sekaligus (misal 1 batch banyak file) -> run_key
        # di-hash biar nggak kepanjangan, determinismenya tetap sama
        # (sorted ids).
        run_key = "csv_upload:" + hashlib.sha1(joined.encode()).hexdigest()

    context.log.info(
        "Upload csv sukses terdeteksi (%d baris): %s -> trigger daily_pipeline_job",
        len(rows),
        ", ".join(file_names),
    )

    # Majukan cursor SETELAH baris ini diproses jadi RunRequest, supaya
    # kalau run-nya nanti gagal, baris ini tidak "hilang" (Dagster yang
    # akan dedupe run_key kalau di-retry, bukan sensor yang skip lagi).
    context.update_cursor(latest_created_at.isoformat())

    return RunRequest(
        run_key=run_key,
        tags={
            "trigger": "csv_upload_sensor",
            "csv_files": ", ".join(file_names)[:250],
        },
    )


sensors = [new_account_sensor, csv_upload_sensor]