INSERT INTO users_growth_accounting
WITH yesterday AS (
    SELECT *
    FROM users_growth_accounting
    WHERE date = DATE('2023-01-09')
),
today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE(DATE_TRUNC('day', event_time::timestamp)) AS date,
        COUNT(*)
    FROM events
    WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-10')
        AND user_id IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(y.first_active_date, t.date) AS first_active_date,
    COALESCE(t.date, y.last_active_date) AS last_active_date,
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
        WHEN t.user_id IS NULL AND y.date = y.last_active_date THEN 'Churned'
        WHEN y.last_active_date < t.date - INTERVAL '1' DAY THEN 'Resurrected'
        WHEN y.last_active_date = t.date - INTERVAL '1' DAY THEN 'Retained'
        ELSE 'Stale'
    END AS daily_active_state,
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
        WHEN t.user_id IS NULL AND y.date - INTERVAL '6' DAY = y.last_active_date THEN 'Churned'
        WHEN y.last_active_date < t.date - INTERVAL '7' DAY THEN 'Resurrected'
        WHEN y.last_active_date > y.date - INTERVAL '7' DAY THEN 'Retained'
        ELSE 'Stale'
    END AS weekly_active_state,
    COALESCE(y.dates_active, ARRAY[]::DATE[]) ||
        CASE WHEN t.user_id IS NOT NULL
        THEN ARRAY[t.date] ELSE ARRAY[]::DATE[]
        END AS dates_active,
    COALESCE(t.date, y.date + INTERVAL '1' DAY) AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;


SELECT * FROM users_growth_accounting
where date = date('2023-01-10')
    and weekly_active_state = 'Resurrected'
limit 100;

SELECT
    date - first_active_date AS days_since_first_active,
    SUM(CASE WHEN daily_active_state IN ('New', 'Resurrected', 'Retained') THEN 1 ELSE 0 END) AS active_users,
    SUM(CASE WHEN daily_active_state IN ('New', 'Resurrected', 'Retained') THEN 1 ELSE 0 END) / CAST(COUNT(*) AS REAL) AS pct_active_users,
    COUNT(*) AS total_users
FROM users_growth_accounting
WHERE first_active_date = date('2023-01-01')
GROUP BY 1
ORDER BY 1;

