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

  Catatan overlap: kalau sensor kepicu pas run lain masih jalan,
  QueuedRunCoordinator (max_concurrent_runs: 1) bikin dia ngantri.
  Run susulan jadi redundant full-rebuild -- boros dikit, tapi aman
  (nggak ada TRUNCATE tabrakan).

Sensor lama dari blueprint Fase 5 (csv_upload_sensor, replica_lag_sensor)
gugur -- alasan lengkap ada di history git / dokumentasi Dagster.
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


sensors = [new_account_sensor]