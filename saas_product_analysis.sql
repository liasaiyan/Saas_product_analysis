
--  SaaS Product Analytics — SQL
--  Dataset : saas_users.csv + saas_events.csv

-------------

--  SETUP (SQLite):
--    sqlite3 saas.db
--    .mode csv
--    .import saas_users.csv users
--    .import saas_events.csv events
--    DELETE FROM users WHERE user_id = 'user_id';
--    DELETE FROM events WHERE user_id = 'user_id';

-- 0. TABLE DEFINITIONS

CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY,
    signup_date TEXT,
    plan        TEXT,
    mrr         REAL,
    churn_date  TEXT
);
CREATE TABLE IF NOT EXISTS events (
    user_id    INTEGER,
    event_date TEXT,
    feature    TEXT
-- 1. OVERVIEW

-- Total users by plan
SELECT
    plan,
    COUNT(*)                            AS total_users,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (), 1)   AS pct_of_total
FROM users
GROUP BY plan
ORDER BY total_users DESC;

-- Signups per month
    STRFTIME('%Y-%m', signup_date)      AS signup_month,
    COUNT(*)                            AS new_signups,
    SUM(COUNT(*)) OVER (
        ORDER BY STRFTIME('%Y-%m', signup_date)
    )                                   AS cumulative_signups
GROUP BY signup_month
ORDER BY signup_month;

-- 2. MONTHLY ACTIVE USERS (MAU)

-- MAU: distinct users with at least one event per month
    STRFTIME('%Y-%m', event_date)       AS month,
    COUNT(DISTINCT user_id)             AS mau
FROM events
GROUP BY month
ORDER BY month;

-- DAU: average daily active users per month
    month,
    ROUND(AVG(dau), 1)                  AS avg_dau
FROM (
    SELECT
        STRFTIME('%Y-%m', event_date)   AS month,
        DATE(event_date)                AS day,
        COUNT(DISTINCT user_id)         AS dau
    FROM events
    GROUP BY month, day
) daily

-- DAU/MAU stickiness ratio
    mau.month,
    mau.mau,
    ROUND(dau.avg_dau, 1)               AS avg_dau,
    ROUND(dau.avg_dau / mau.mau, 3)     AS dau_mau_ratio
    SELECT STRFTIME('%Y-%m', event_date) AS month,
           COUNT(DISTINCT user_id)       AS mau
    FROM events GROUP BY month
) mau
JOIN (
    SELECT month, ROUND(AVG(dau), 1) AS avg_dau
    FROM (
        SELECT STRFTIME('%Y-%m', event_date) AS month,
               DATE(event_date)              AS day,
               COUNT(DISTINCT user_id)       AS dau
        FROM events GROUP BY month, day
    ) GROUP BY month
) dau ON mau.month = dau.month
ORDER BY mau.month;

-- 3. RETENTION COHORT ANALYSIS

    cohort_month,
    period,
    COUNT(DISTINCT user_id)             AS retained_users,
    ROUND(
        COUNT(DISTINCT user_id) * 100.0
        / FIRST_VALUE(COUNT(DISTINCT user_id))
            OVER (PARTITION BY cohort_month ORDER BY period),
        1
    )                                   AS retention_pct
        e.user_id,
        STRFTIME('%Y-%m', u.signup_date)    AS cohort_month,
        STRFTIME('%Y-%m', e.event_date)     AS activity_month,
        CAST(
            (STRFTIME('%Y', e.event_date) - STRFTIME('%Y', u.signup_date)) * 12
            + STRFTIME('%m', e.event_date)  - STRFTIME('%m', u.signup_date)
        AS INTEGER)                         AS period
    FROM events e
    JOIN users u ON e.user_id = u.user_id
 cohort_data
GROUP BY cohort_month, period
ORDER BY cohort_month, period;

-- 4. FEATURE ADOPTION FUNNEL

-- Users who reached each feature 
SELECT 'Signed Up'             AS step, 1 AS step_order, COUNT(*) AS users
UNION ALL
SELECT 'Viewed Dashboard',     2, COUNT(DISTINCT user_id)
FROM events WHERE feature = 'dashboard_view'
SELECT 'Created a Report',     3, COUNT(DISTINCT user_id)
FROM events WHERE feature = 'report_created'
SELECT 'Connected Integration',4, COUNT(DISTINCT user_id)
FROM events WHERE feature = 'integration_connected'
SELECT 'Exported Data',        5, COUNT(DISTINCT user_id)
FROM events WHERE feature = 'export_used'
SELECT 'Invited Team Member',  6, COUNT(DISTINCT user_id)
FROM events WHERE feature = 'team_member_invited'
ORDER BY step_order;

-- Feature adoption rate per plan
    u.plan,
    e.feature,
    COUNT(DISTINCT e.user_id)               AS users_who_used_it,
    COUNT(DISTINCT u.user_id)               AS total_plan_users,
    ROUND(COUNT(DISTINCT e.user_id) * 100.0
          / COUNT(DISTINCT u.user_id), 1)   AS adoption_pct
FROM users u
LEFT JOIN events e ON u.user_id = e.user_id
GROUP BY u.plan, e.feature
ORDER BY u.plan, adoption_pct DESC;

-- 5. CHURN ANALYSIS

-- Overall churn rate for paid users
    SUM(CASE WHEN churn_date IS NOT NULL THEN 1 ELSE 0 END)
                                        AS churned,
        SUM(CASE WHEN churn_date IS NOT NULL THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1
    )                                   AS churn_rate_pct
WHERE plan != 'free'
ORDER BY churn_rate_pct DESC;

-- Monthly churn rate (churned users / active at start of month)
    churn_month,
    churned_users,
    active_at_start,
    ROUND(churned_users * 100.0
          / NULLIF(active_at_start, 0), 2) AS monthly_churn_rate_pct
        STRFTIME('%Y-%m', churn_date)   AS churn_month,
        COUNT(*)                        AS churned_users
    FROM users
    WHERE churn_date IS NOT NULL
      AND plan != 'free'
    GROUP BY churn_month
) churned
        months.m                        AS churn_month,
        COUNT(u.user_id)                AS active_at_start
        SELECT DISTINCT STRFTIME('%Y-%m', churn_date) AS m
        FROM users WHERE churn_date IS NOT NULL
    ) months
    JOIN users u
      ON u.signup_date < months.m || '-01'
     AND (u.churn_date IS NULL OR u.churn_date >= months.m || '-01')
     AND u.plan != 'free'
    GROUP BY months.m
) active USING (churn_month)
ORDER BY churn_month;

-- Average days to churn by plan
    ROUND(AVG(
        JULIANDAY(churn_date) - JULIANDAY(signup_date)
    ), 0)                               AS avg_days_to_churn,
    COUNT(*)                            AS churned_users
WHERE churn_date IS NOT NULL
  AND plan != 'free'
ORDER BY avg_days_to_churn;

-- 6. REVENUE — MRR AND ARR

-- MRR by month (active paying users only)
    STRFTIME('%Y-%m', months.d)         AS month,
    SUM(u.mrr)                          AS total_mrr,
    SUM(u.mrr) * 12                     AS arr,
    COUNT(u.user_id)                    AS paying_users
    SELECT DISTINCT STRFTIME('%Y-%m-01', signup_date) AS d
) months
JOIN users u
  ON u.signup_date <= months.d
 AND (u.churn_date IS NULL OR u.churn_date > months.d)
 AND u.plan != 'free'

-- New MRR per month (from new signups)
    STRFTIME('%Y-%m', signup_date)      AS month,
    SUM(mrr)                            AS new_mrr,
    COUNT(*)                            AS new_paying_users

-- Churned MRR per month
    STRFTIME('%Y-%m', churn_date)       AS churn_month,
    SUM(mrr)                            AS churned_mrr,
GROUP BY churn_month

-- MRR breakdown by plan per month
    SUM(u.mrr)                          AS plan_mrr,
GROUP BY month, u.plan
ORDER BY month, plan_mrr DESC;

-- 7. EXECUTIVE SUMMARY VIEW

    (SELECT STRFTIME('%Y-%m', MAX(event_date)) FROM events)
                                        AS latest_month,
    (SELECT COUNT(DISTINCT user_id)
     FROM events
     WHERE STRFTIME('%Y-%m', event_date) = (
         SELECT STRFTIME('%Y-%m', MAX(event_date)) FROM events
     ))                                 AS mau,
    (SELECT COUNT(*) FROM users WHERE plan != 'free'
       AND (churn_date IS NULL OR churn_date > DATE('now')))
                                        AS active_paying_users,
    (SELECT SUM(mrr) FROM users WHERE plan != 'free'
                                        AS current_mrr,
    (SELECT SUM(mrr) * 12 FROM users WHERE plan != 'free'
                                        AS current_arr,
    (SELECT ROUND(
        / COUNT(*), 1)
     FROM users WHERE plan != 'free')   AS overall_churn_rate_pct;