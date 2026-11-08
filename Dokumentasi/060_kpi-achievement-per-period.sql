-- Up Migration
-- KPI achievement PER BULAN REPORT, bukan lagi satu baris "per hari ini".
--
-- LATAR
--   Report maker bisa dibuka per bulan ("report Oktober dibuka di November"),
--   tapi `l2_gold.kpi_achievement` hanya menyimpan satu baris per KPI dengan
--   cutoff `current_date`. Akibatnya report tidak bisa membaca tabel ini dan
--   menghitung ulang sendiri di `src/lib/reports/data/kpiTargetQuery.ts` --
--   dua rumus untuk satu angka, dan yang di report memakai pro-rata per HARI
--   sementara warehouse sudah per BULAN (run rate Fitbar followers_growth
--   cutoff Okt 2026: 179.82% di UI vs 179.32% rumus klien).
--
--   Migration ini membuat warehouse menyimpan satu baris per KPI per BULAN,
--   supaya report cukup SELECT dan rumusnya hidup di satu tempat saja.
--
-- DUA PERUBAHAN PERILAKU
--   1. Grain: PK (kpi_id) -> (kpi_id, period_month). Baris lama dihapus dan
--      diisi ulang oleh procedure; tabel ini tidak pernah di-SELECT aplikasi
--      (hanya CALL dan DELETE di src/lib/kpi/queries.ts), jadi tidak ada yang
--      kehilangan data.
--   2. followers_growth sekarang memakai SELISIH follower_count_eod, bukan
--      SUM(net_growth_sum) -- disamakan dengan definisi Nett Followers Growth
--      yang dipakai dashboard & report (src/lib/metrics/followerGrowth.ts):
--      net_growth_sum dari platform sering tidak tally dengan total follower.
--      Fitbar IG Feb-Okt 2026: 20.600 (lama) vs 20.174 (baru, = angka UI).

ALTER TABLE l2_gold.kpi_achievement
  ADD COLUMN IF NOT EXISTS period_month date,
  ADD COLUMN IF NOT EXISTS period_end   date,
  ADD COLUMN IF NOT EXISTS time_gone    numeric;

-- Baris lama tidak punya periode dan tidak bisa dipetakan ke bulan manapun.
DELETE FROM l2_gold.kpi_achievement;

ALTER TABLE l2_gold.kpi_achievement
  ALTER COLUMN period_month SET NOT NULL,
  ALTER COLUMN period_end   SET NOT NULL;

ALTER TABLE l2_gold.kpi_achievement DROP CONSTRAINT IF EXISTS kpi_achievement_pkey;
ALTER TABLE l2_gold.kpi_achievement
  ADD CONSTRAINT kpi_achievement_pkey PRIMARY KEY (kpi_id, period_month);

COMMENT ON COLUMN l2_gold.kpi_achievement.period_month IS
  'Bulan report (tanggal 1). Kunci yang dipakai aplikasi: WHERE period_month = DATE 2026-10-01.';
COMMENT ON COLUMN l2_gold.kpi_achievement.period_end IS
  'Cutoff efektif = LEAST(akhir period_month, kpi_setting.end_date, current_date).';
COMMENT ON COLUMN l2_gold.kpi_achievement.time_gone IS
  'Porsi periode KPI yang sudah lewat, basis BULAN kalender: months_elapsed / total_months * 100.';

CREATE OR REPLACE PROCEDURE l2_gold.sp_calculate_kpi_achievement(IN p_kpi_id uuid DEFAULT NULL::uuid)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_kpi              public.kpi_setting%ROWTYPE;
    v_platform_key     text;
    v_today            date := current_date;
    v_last_month       date;
    v_period_month     date;
    v_period_end       date;
    v_total_months     int;
    v_months_elapsed   int;
    v_achieved         numeric;
    v_achievement_rate numeric;
    v_time_gone        numeric;
    v_prorated_target  numeric;
    v_run_rate         numeric;
    v_is_rate_metric   boolean;
    v_found_any        boolean := false;
BEGIN
    FOR v_kpi IN
        SELECT * FROM public.kpi_setting
        WHERE (p_kpi_id IS NOT NULL AND kpi_id = p_kpi_id)
           OR (p_kpi_id IS NULL AND is_active)
    LOOP
        v_found_any := true;

        IF v_kpi.metrics_target NOT IN (
            -- harian (brand_metric_daily)
            'followers_growth', 'total_followers', 'engagement', 'reach', 'impressions',
            'views', 'video_views', 'likes', 'comments', 'shares', 'saves', 'post_count',
            'profile_visits', 'profile_reach', 'new_followers', 'link_clicks',
            -- per post (post_metric)
            'er_reach', 'avg_er_reach', 'er_views', 'avg_er_views',
            'avg_engagement', 'avg_reach', 'avg_views',
            'avg_likes', 'avg_comments', 'avg_shares', 'avg_saves'
        ) THEN
            RAISE EXCEPTION 'Unsupported metrics_target: % (extend l2_gold.sp_calculate_kpi_achievement to add it)', v_kpi.metrics_target;
        END IF;

        SELECT key INTO v_platform_key FROM public.platforms WHERE id = v_kpi.platform_id;
        IF v_platform_key IS NULL THEN
            RAISE EXCEPTION 'platform_id % not found in public.platforms', v_kpi.platform_id;
        END IF;

        v_is_rate_metric := v_kpi.metrics_target IN (
            'er_reach', 'avg_er_reach', 'er_views', 'avg_er_views',
            'avg_engagement', 'avg_reach', 'avg_views',
            'avg_likes', 'avg_comments', 'avg_shares', 'avg_saves'
        );

        -- Basis bulan kalender (year*12 + month), bukan estimasi 30 hari/bulan,
        -- supaya Feb 28 dan Jan 31 sama-sama dihitung satu bulan.
        v_total_months := (EXTRACT(YEAR FROM v_kpi.end_date)::int * 12 + EXTRACT(MONTH FROM v_kpi.end_date)::int)
                        - (EXTRACT(YEAR FROM v_kpi.start_date)::int * 12 + EXTRACT(MONTH FROM v_kpi.start_date)::int)
                        + 1;

        -- Satu baris per bulan report, dari bulan mulai KPI sampai bulan
        -- berjalan (atau bulan berakhirnya KPI, mana yang lebih dulu).
        v_last_month   := date_trunc('month', LEAST(v_today, v_kpi.end_date))::date;
        v_period_month := date_trunc('month', v_kpi.start_date)::date;

        WHILE v_period_month <= v_last_month LOOP
            v_period_end := LEAST(
                (v_period_month + INTERVAL '1 month - 1 day')::date,
                v_kpi.end_date,
                v_today
            );

            v_months_elapsed := GREATEST(1,
                                   (EXTRACT(YEAR FROM v_period_month)::int * 12 + EXTRACT(MONTH FROM v_period_month)::int)
                                 - (EXTRACT(YEAR FROM v_kpi.start_date)::int * 12 + EXTRACT(MONTH FROM v_kpi.start_date)::int)
                                 + 1
                               );

            IF v_kpi.metrics_target = 'total_followers' THEN
                -- Posisi, bukan jumlah: follower EOD terakhir di jendela per akun,
                -- dijumlah antar akun platform itu.
                SELECT COALESCE(SUM(last_eod), 0)
                INTO v_achieved
                FROM (
                    SELECT DISTINCT ON (account_id) follower_count_eod AS last_eod
                    FROM l2_gold.brand_metric_daily
                    WHERE brand_id = v_kpi.brand_id
                      AND platform = v_platform_key
                      AND metric_date BETWEEN v_kpi.start_date AND v_period_end
                      AND (v_kpi.social_account_id IS NULL OR account_id = v_kpi.social_account_id)
                      AND follower_count_eod IS NOT NULL
                    ORDER BY account_id, metric_date DESC
                ) x;

            ELSIF v_kpi.metrics_target = 'followers_growth' THEN
                -- Nett Followers Growth = SELISIH follower_count_eod, bukan
                -- SUM(net_growth_sum): angka follows-unfollows dari platform
                -- sering tidak tally dengan total followernya, dan yang
                -- dicocokkan klien adalah selisih total follower.
                -- Pembanding = EOD terakhir SEBELUM start_date; kalau akunnya
                -- memang baru mulai di dalam jendela, pakai EOD pertama di
                -- jendela (bulan onboarding tidak terbaca sebagai lonjakan).
                -- Sama dengan EOD_GROWTH + PREV_EOD_JOIN yang menelescope di
                -- src/lib/metrics/followerGrowth.ts.
                SELECT COALESCE(SUM(acc.last_eod - COALESCE(acc.prev_eod, acc.first_eod)), 0)
                INTO v_achieved
                FROM (
                    SELECT a.account_id,
                           (SELECT b.follower_count_eod FROM l2_gold.brand_metric_daily b
                             WHERE b.brand_id = v_kpi.brand_id AND b.platform = v_platform_key
                               AND b.account_id = a.account_id
                               AND b.metric_date BETWEEN v_kpi.start_date AND v_period_end
                               AND b.follower_count_eod IS NOT NULL
                             ORDER BY b.metric_date DESC LIMIT 1) AS last_eod,
                           (SELECT b.follower_count_eod FROM l2_gold.brand_metric_daily b
                             WHERE b.brand_id = v_kpi.brand_id AND b.platform = v_platform_key
                               AND b.account_id = a.account_id
                               AND b.metric_date BETWEEN v_kpi.start_date AND v_period_end
                               AND b.follower_count_eod IS NOT NULL
                             ORDER BY b.metric_date ASC LIMIT 1) AS first_eod,
                           (SELECT b.follower_count_eod FROM l2_gold.brand_metric_daily b
                             WHERE b.brand_id = v_kpi.brand_id AND b.platform = v_platform_key
                               AND b.account_id = a.account_id
                               AND b.metric_date < v_kpi.start_date
                               AND b.follower_count_eod IS NOT NULL
                             ORDER BY b.metric_date DESC LIMIT 1) AS prev_eod
                      FROM (SELECT DISTINCT account_id
                              FROM l2_gold.brand_metric_daily
                             WHERE brand_id = v_kpi.brand_id AND platform = v_platform_key
                               AND metric_date BETWEEN v_kpi.start_date AND v_period_end
                               AND follower_count_eod IS NOT NULL
                               AND (v_kpi.social_account_id IS NULL OR account_id = v_kpi.social_account_id)
                           ) a
                ) acc;

            ELSIF v_is_rate_metric THEN
                -- post_metric.brand_id = social_accounts.id -> naik ke brand lewat
                -- brand_social_accounts (kpi_setting.brand_id = public.brands.id).
                SELECT COALESCE(
                    CASE v_kpi.metrics_target
                        WHEN 'er_reach'       THEN SUM(p.engagement_owned) FILTER (WHERE p.reach > 0)::numeric
                                                   / NULLIF(SUM(p.reach) FILTER (WHERE p.reach > 0), 0) * 100
                        WHEN 'avg_er_reach'   THEN AVG(p.er_reach) * 100
                        WHEN 'er_views'       THEN SUM(p.engagement_owned) FILTER (WHERE p.views > 0)::numeric
                                                   / NULLIF(SUM(p.views) FILTER (WHERE p.views > 0), 0) * 100
                        WHEN 'avg_er_views'   THEN AVG(p.er_views) * 100
                        WHEN 'avg_engagement' THEN SUM(p.engagement_owned)::numeric / NULLIF(COUNT(*), 0)
                        WHEN 'avg_reach'      THEN SUM(p.reach)::numeric            / NULLIF(COUNT(*), 0)
                        WHEN 'avg_views'      THEN SUM(p.views)::numeric            / NULLIF(COUNT(*), 0)
                        WHEN 'avg_likes'      THEN SUM(p.likes)::numeric            / NULLIF(COUNT(*), 0)
                        WHEN 'avg_comments'   THEN SUM(p.comments)::numeric         / NULLIF(COUNT(*), 0)
                        WHEN 'avg_shares'     THEN SUM(p.shares)::numeric           / NULLIF(COUNT(*), 0)
                        WHEN 'avg_saves'      THEN SUM(p.saves)::numeric            / NULLIF(COUNT(*), 0)
                    END, 0)
                INTO v_achieved
                FROM l2_gold.post_metric p
                JOIN public.brand_social_accounts bsa ON bsa.social_account_id = p.brand_id
                WHERE bsa.brand_id = v_kpi.brand_id
                  AND p.platform = v_platform_key
                  AND p.post_date::date BETWEEN v_kpi.start_date AND v_period_end
                  AND (v_kpi.social_account_id IS NULL OR p.brand_id = v_kpi.social_account_id);
                v_achieved := ROUND(v_achieved, 2);

            ELSE
                SELECT COALESCE(
                    CASE v_kpi.metrics_target
                        WHEN 'engagement'       THEN SUM(engagement_sum)
                        WHEN 'reach'            THEN SUM(reach_sum)
                        WHEN 'impressions'      THEN SUM(impressions_sum)
                        WHEN 'views'            THEN SUM(views_sum)
                        WHEN 'video_views'      THEN SUM(video_views_sum)
                        WHEN 'likes'            THEN SUM(likes_sum)
                        WHEN 'comments'         THEN SUM(comments_sum)
                        WHEN 'shares'           THEN SUM(shares_sum)
                        WHEN 'saves'            THEN SUM(saves_sum)
                        WHEN 'post_count'       THEN SUM(post_count)
                        WHEN 'profile_visits'   THEN SUM(profile_visit_sum)
                        WHEN 'profile_reach'    THEN SUM(profile_reach_sum)
                        WHEN 'new_followers'    THEN SUM(new_followers_sum)
                        WHEN 'link_clicks'      THEN SUM(link_clicks_sum)
                    END, 0)
                INTO v_achieved
                FROM l2_gold.brand_metric_daily
                WHERE brand_id = v_kpi.brand_id
                  AND platform = v_platform_key
                  AND metric_date BETWEEN v_kpi.start_date AND v_period_end
                  AND (v_kpi.social_account_id IS NULL OR account_id = v_kpi.social_account_id);
            END IF;

            v_achievement_rate := ROUND(v_achieved / NULLIF(v_kpi.value_target, 0) * 100, 2);
            v_time_gone        := ROUND(v_months_elapsed::numeric / NULLIF(v_total_months, 0) * 100, 2);

            IF v_is_rate_metric OR v_kpi.metrics_target = 'total_followers' THEN
                -- Posisi / rata-rata: tidak menumpuk seiring waktu, jadi target
                -- TIDAK diprorata -> run_rate = achievement_rate.
                v_run_rate := v_achievement_rate;
            ELSE
                -- Rumus klien: Actual / ((Annual Target / 12) x Months Elapsed).
                v_prorated_target := v_kpi.value_target * v_months_elapsed / NULLIF(v_total_months, 0);
                v_run_rate        := ROUND(v_achieved / NULLIF(v_prorated_target, 0) * 100, 2);
            END IF;

            INSERT INTO l2_gold.kpi_achievement
                (kpi_id, period_month, period_end, achieved_value, value_target,
                 achievement_rate, run_rate, time_gone, computed_at)
            VALUES
                (v_kpi.kpi_id, v_period_month, v_period_end, v_achieved, v_kpi.value_target,
                 v_achievement_rate, v_run_rate, v_time_gone, now())
            ON CONFLICT (kpi_id, period_month) DO UPDATE SET
                period_end       = EXCLUDED.period_end,
                achieved_value   = EXCLUDED.achieved_value,
                value_target     = EXCLUDED.value_target,
                achievement_rate = EXCLUDED.achievement_rate,
                run_rate         = EXCLUDED.run_rate,
                time_gone        = EXCLUDED.time_gone,
                computed_at      = EXCLUDED.computed_at;

            v_period_month := (v_period_month + INTERVAL '1 month')::date;
        END LOOP;
    END LOOP;

    IF p_kpi_id IS NOT NULL AND NOT v_found_any THEN
        RAISE EXCEPTION 'public.kpi_setting.kpi_id % not found', p_kpi_id;
    END IF;
END;
$procedure$;

CALL l2_gold.sp_calculate_kpi_achievement();

-- Down Migration
-- Kembali ke satu baris per KPI (definisi migration 059). Jalankan ulang
-- procedure versi 059 setelahnya untuk mengisi tabelnya.
-- DELETE FROM l2_gold.kpi_achievement;
-- ALTER TABLE l2_gold.kpi_achievement DROP CONSTRAINT kpi_achievement_pkey;
-- ALTER TABLE l2_gold.kpi_achievement ADD CONSTRAINT kpi_achievement_pkey PRIMARY KEY (kpi_id);
-- ALTER TABLE l2_gold.kpi_achievement
--   DROP COLUMN period_month, DROP COLUMN period_end, DROP COLUMN time_gone;
