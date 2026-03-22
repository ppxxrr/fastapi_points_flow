-- MySQL 8.x reference SQL for parking policy impact analysis.
-- Purpose:
-- 1. Build an analysis view that maps each parking record to the effective policy version.
-- 2. Calculate actual receivable amount and simulated receivable amount under the other version.
-- 3. Support BI comparison for the 2026-03-03 policy switch.
--
-- Assumptions:
-- - Member level comes from member_account.level_name and is normalized into:
--   非会员 / 普卡 / 银卡 / 金卡 / 钻石卡
-- - If parking.member_id exists but level is missing or unrecognized, it falls back to 普卡.
-- - "当天有积分入账" means there is at least one point_flow row on the same exit date with positive signed points.
-- - The comparison view applies the daily cap of 60 yuan to both actual and simulated receivable amounts.
-- - Duration is billed by CEIL(duration_seconds / 3600). If parking_duration_seconds is missing,
--   the SQL falls back to TIMESTAMPDIFF(SECOND, entry_time, exit_time).

DROP VIEW IF EXISTS fact_parking_impact_analysis;

CREATE OR REPLACE VIEW fact_parking_impact_analysis AS
SELECT
    parking_base.parking_record_id,
    parking_base.entry_time,
    parking_base.exit_time,
    DATE(parking_base.exit_time) AS exit_date,
    parking_base.member_id,
    parking_base.mobile_no,
    parking_base.plate_no,
    parking_base.member_level,
    parking_base.actual_version_id,
    CASE
        WHEN parking_base.actual_version_id = 'PARKING_RULE_V1' THEN 'PARKING_RULE_V2'
        ELSE 'PARKING_RULE_V1'
    END AS simulated_version_id,
    parking_base.duration_seconds,
    parking_base.total_hours,
    parking_base.point_bonus_hours,
    parking_base.recorded_fee_yuan,
    actual_policy.base_free_hours AS actual_base_free_hours,
    actual_policy.is_diamond_full_free AS actual_is_diamond_full_free,
    simulated_policy.base_free_hours AS simulated_base_free_hours,
    simulated_policy.is_diamond_full_free AS simulated_is_diamond_full_free,
    actual_policy.base_free_hours + parking_base.point_bonus_hours AS actual_total_free_hours,
    simulated_policy.base_free_hours + parking_base.point_bonus_hours AS simulated_total_free_hours,
    ROUND(
        CASE
            WHEN parking_base.total_hours <= 0 THEN 0
            WHEN actual_policy.is_diamond_full_free = 1 THEN 0
            WHEN (actual_policy.base_free_hours + parking_base.point_bonus_hours) >= 1 THEN
                LEAST(
                    60,
                    GREATEST(0, parking_base.total_hours - (actual_policy.base_free_hours + parking_base.point_bonus_hours)) * 5
                )
            ELSE
                LEAST(
                    60,
                    10 + GREATEST(0, parking_base.total_hours - 1) * 5
                )
        END,
        2
    ) AS actual_receivable_yuan,
    ROUND(
        CASE
            WHEN parking_base.total_hours <= 0 THEN 0
            WHEN simulated_policy.is_diamond_full_free = 1 THEN 0
            WHEN (simulated_policy.base_free_hours + parking_base.point_bonus_hours) >= 1 THEN
                LEAST(
                    60,
                    GREATEST(0, parking_base.total_hours - (simulated_policy.base_free_hours + parking_base.point_bonus_hours)) * 5
                )
            ELSE
                LEAST(
                    60,
                    10 + GREATEST(0, parking_base.total_hours - 1) * 5
                )
        END,
        2
    ) AS simulated_receivable_yuan,
    ROUND(
        (
            CASE
                WHEN parking_base.total_hours <= 0 THEN 0
                WHEN actual_policy.is_diamond_full_free = 1 THEN 0
                WHEN (actual_policy.base_free_hours + parking_base.point_bonus_hours) >= 1 THEN
                    LEAST(
                        60,
                        GREATEST(0, parking_base.total_hours - (actual_policy.base_free_hours + parking_base.point_bonus_hours)) * 5
                    )
                ELSE
                    LEAST(
                        60,
                        10 + GREATEST(0, parking_base.total_hours - 1) * 5
                    )
            END
        ) - (
            CASE
                WHEN parking_base.total_hours <= 0 THEN 0
                WHEN simulated_policy.is_diamond_full_free = 1 THEN 0
                WHEN (simulated_policy.base_free_hours + parking_base.point_bonus_hours) >= 1 THEN
                    LEAST(
                        60,
                        GREATEST(0, parking_base.total_hours - (simulated_policy.base_free_hours + parking_base.point_bonus_hours)) * 5
                    )
                ELSE
                    LEAST(
                        60,
                        10 + GREATEST(0, parking_base.total_hours - 1) * 5
                    )
            END
        ),
        2
    ) AS receivable_delta_yuan,
    ROUND(
        parking_base.recorded_fee_yuan - (
            CASE
                WHEN parking_base.total_hours <= 0 THEN 0
                WHEN simulated_policy.is_diamond_full_free = 1 THEN 0
                WHEN (simulated_policy.base_free_hours + parking_base.point_bonus_hours) >= 1 THEN
                    LEAST(
                        60,
                        GREATEST(0, parking_base.total_hours - (simulated_policy.base_free_hours + parking_base.point_bonus_hours)) * 5
                    )
                ELSE
                    LEAST(
                        60,
                        10 + GREATEST(0, parking_base.total_hours - 1) * 5
                    )
            END
        ),
        2
    ) AS recorded_vs_simulated_delta_yuan
FROM (
    SELECT
        pr.id AS parking_record_id,
        pr.entry_time,
        pr.exit_time,
        pr.member_id,
        pr.mobile_no,
        pr.plate_no,
        GREATEST(
            0,
            COALESCE(
                pr.parking_duration_seconds,
                CASE
                    WHEN pr.entry_time IS NOT NULL AND pr.exit_time IS NOT NULL THEN TIMESTAMPDIFF(SECOND, pr.entry_time, pr.exit_time)
                    ELSE 0
                END
            )
        ) AS duration_seconds,
        CASE
            WHEN GREATEST(
                0,
                COALESCE(
                    pr.parking_duration_seconds,
                    CASE
                        WHEN pr.entry_time IS NOT NULL AND pr.exit_time IS NOT NULL THEN TIMESTAMPDIFF(SECOND, pr.entry_time, pr.exit_time)
                        ELSE 0
                    END
                )
            ) <= 0 THEN 0
            ELSE CEIL(
                GREATEST(
                    0,
                    COALESCE(
                        pr.parking_duration_seconds,
                        CASE
                            WHEN pr.entry_time IS NOT NULL AND pr.exit_time IS NOT NULL THEN TIMESTAMPDIFF(SECOND, pr.entry_time, pr.exit_time)
                            ELSE 0
                        END
                    )
                ) / 3600.0
            )
        END AS total_hours,
        ROUND(COALESCE(pr.total_fee_cent, 0) / 100.0, 2) AS recorded_fee_yuan,
        CASE
            WHEN pr.exit_time < '2026-03-03 00:00:00' THEN 'PARKING_RULE_V1'
            ELSE 'PARKING_RULE_V2'
        END AS actual_version_id,
        CASE
            WHEN ma.level_name IS NULL OR TRIM(ma.level_name) = '' THEN
                CASE
                    WHEN pr.member_id IS NOT NULL AND TRIM(pr.member_id) <> '' THEN '普卡'
                    ELSE '非会员'
                END
            WHEN TRIM(ma.level_name) IN ('钻石卡', '钻卡', '钻石会员', '黑钻卡', '黑钻会员') OR TRIM(ma.level_name) LIKE '%钻%' THEN '钻石卡'
            WHEN TRIM(ma.level_name) IN ('金卡', '黄金卡', '金卡会员') OR TRIM(ma.level_name) LIKE '%金%' THEN '金卡'
            WHEN TRIM(ma.level_name) IN ('银卡', '白银卡', '银卡会员') OR TRIM(ma.level_name) LIKE '%银%' THEN '银卡'
            WHEN TRIM(ma.level_name) IN ('普卡', '普通卡', '普通会员') OR TRIM(ma.level_name) LIKE '%普%' THEN '普卡'
            ELSE
                CASE
                    WHEN pr.member_id IS NOT NULL AND TRIM(pr.member_id) <> '' THEN '普卡'
                    ELSE '非会员'
                END
        END AS member_level,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM member_point_flow pf
                WHERE COALESCE(pf.signed_change_points, 0) > 0
                  AND DATE(COALESCE(pf.consume_time, pf.create_time)) = DATE(pr.exit_time)
                  AND (
                        (
                            pr.member_id IS NOT NULL
                            AND TRIM(pr.member_id) <> ''
                            AND pf.member_id = pr.member_id
                        )
                        OR
                        (
                            (pr.member_id IS NULL OR TRIM(pr.member_id) = '')
                            AND pr.mobile_no IS NOT NULL
                            AND TRIM(pr.mobile_no) <> ''
                            AND pf.mobile_no = pr.mobile_no
                        )
                  )
            ) THEN 2
            ELSE 0
        END AS point_bonus_hours
    FROM parking_record pr
    LEFT JOIN member_account ma
        ON ma.member_id = pr.member_id
    WHERE pr.exit_time IS NOT NULL
) AS parking_base
INNER JOIN dim_parking_policy actual_policy
    ON actual_policy.version_id = parking_base.actual_version_id
   AND actual_policy.member_level = parking_base.member_level
   AND parking_base.exit_time >= actual_policy.start_date
   AND (actual_policy.end_date IS NULL OR parking_base.exit_time <= actual_policy.end_date)
INNER JOIN dim_parking_policy simulated_policy
    ON simulated_policy.version_id = CASE
        WHEN parking_base.actual_version_id = 'PARKING_RULE_V1' THEN 'PARKING_RULE_V2'
        ELSE 'PARKING_RULE_V1'
    END
   AND simulated_policy.member_level = parking_base.member_level;


-- Query 1: Compare new-policy records against old-policy simulation by member level.
-- Interpretation:
-- - old_policy_receivable_yuan: what would have been receivable under V1.
-- - new_policy_receivable_yuan: receivable under the actual V2 rule.
-- - avoided_revenue_leakage_yuan: extra receivable retained after the new rule.
-- - realized_revenue_growth_rate_pct: recorded fee growth vs old-policy simulated receivable.
SELECT
    member_level AS 会员等级,
    COUNT(*) AS 停车笔数,
    ROUND(SUM(simulated_receivable_yuan), 2) AS 旧规模拟应收,
    ROUND(SUM(actual_receivable_yuan), 2) AS 新规应收,
    ROUND(SUM(actual_receivable_yuan - simulated_receivable_yuan), 2) AS 避免减损额,
    ROUND(SUM(recorded_fee_yuan), 2) AS 实际记录金额,
    ROUND(
        CASE
            WHEN SUM(simulated_receivable_yuan) = 0 THEN NULL
            ELSE (SUM(recorded_fee_yuan) - SUM(simulated_receivable_yuan)) / SUM(simulated_receivable_yuan) * 100
        END,
        2
    ) AS 实收增长率_pct
FROM fact_parking_impact_analysis
WHERE actual_version_id = 'PARKING_RULE_V2'
GROUP BY member_level
ORDER BY 新规应收 DESC, 会员等级;


-- Query 2: Revenue leakage analysis for 普卡 after the policy switch.
-- Goal: measure how many users changed from zero-fee exit under V1 to paid exit under V2.
SELECT
    member_level AS 会员等级,
    COUNT(*) AS 停车笔数,
    SUM(CASE WHEN simulated_receivable_yuan = 0 AND actual_receivable_yuan > 0 THEN 1 ELSE 0 END) AS 零元转付费笔数,
    ROUND(
        SUM(CASE WHEN simulated_receivable_yuan = 0 AND actual_receivable_yuan > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS 零元转付费占比_pct
FROM fact_parking_impact_analysis
WHERE actual_version_id = 'PARKING_RULE_V2'
  AND member_level = '普卡'
GROUP BY member_level;


-- Query 3: Behavioral shift before and after the policy switch.
-- Goal: compare average billed hours by level across policy versions.
SELECT
    member_level AS 会员等级,
    actual_version_id AS 政策版本,
    COUNT(*) AS 停车笔数,
    ROUND(AVG(total_hours), 2) AS 平均计费小时数,
    ROUND(AVG(actual_receivable_yuan), 2) AS 平均应收金额,
    ROUND(AVG(recorded_fee_yuan), 2) AS 平均记录金额
FROM fact_parking_impact_analysis
GROUP BY member_level, actual_version_id
ORDER BY member_level, actual_version_id;


-- Query 4: Point-parking leverage.
-- Goal: see whether point-earned members are more likely to trigger extra free hours.
SELECT
    member_level AS 会员等级,
    COUNT(*) AS 停车笔数,
    SUM(CASE WHEN point_bonus_hours > 0 THEN 1 ELSE 0 END) AS 积分加成笔数,
    ROUND(SUM(CASE WHEN point_bonus_hours > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS 积分加成占比_pct,
    ROUND(AVG(CASE WHEN point_bonus_hours > 0 THEN actual_receivable_yuan END), 2) AS 积分加成后平均应收,
    ROUND(AVG(CASE WHEN point_bonus_hours = 0 THEN actual_receivable_yuan END), 2) AS 无积分加成平均应收
FROM fact_parking_impact_analysis
WHERE actual_version_id = 'PARKING_RULE_V2'
GROUP BY member_level
ORDER BY 积分加成笔数 DESC, 会员等级;
