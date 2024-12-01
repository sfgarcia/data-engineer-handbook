--INSERT INTO host_activity_reduced

WITH daily_agg AS (
    SELECT
        DATE_TRUNC('month', event_time::DATE) AS date_month,
        host,
        COUNT(*) AS hits,
        COUNT(DISTINCT user_id) AS unique_users
    FROM events
    WHERE event_time::DATE = DATE('2023-01-01')
    GROUP BY 1, 2
),
yesterday as (
    SELECT
        host,
        hit_array,
        unique_visitors
    FROM host_activity_reduced
    WHERE date_month = DATE('2023-01-01')
)

SELECT
    date_month,
    COALESCE(d.host, y.host) AS host,
    CASE WHEN y.hit_array IS NULL THEN ARRAY[d.hits]
        ELSE ARRAY[d.hits] || y.hit_array END AS hit_array,
    CASE WHEN y.unique_visitors IS NULL THEN ARRAY[d.unique_users]
        ELSE ARRAY[d.unique_users] || y.unique_visitors END AS unique_visitors
FROM daily_agg d
FULL OUTER JOIN yesterday y
    ON d.host = y.host;