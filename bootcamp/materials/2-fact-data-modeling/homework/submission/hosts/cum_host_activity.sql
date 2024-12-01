--TRUNCATE TABLE host_activity_reduced;

INSERT INTO host_activity_reduced

WITH daily_agg AS (
    SELECT
        event_time::DATE AS curr_date,
        host,
        CAST(COUNT(*) AS INT) AS hits,
        CAST(COUNT(DISTINCT user_id) AS INT) AS unique_users
    FROM events
    WHERE event_time::DATE = DATE('2023-01-03')
    GROUP BY 1, 2
),

yesterday AS (
    SELECT
        date_month,
        host,
        hit_array,
        unique_visitors
    FROM host_activity_reduced
    WHERE date_month = DATE('2023-01-01')
)

SELECT
    COALESCE(DATE_TRUNC('month', d.curr_date), y.date_month) AS date_month,
    COALESCE(d.host, y.host) AS host,
    CASE WHEN y.hit_array IS NOT NULL
        THEN y.hit_array || ARRAY[COALESCE(d.hits, 0)]
        ELSE ARRAY_FILL(0, ARRAY[GREATEST(d.curr_date - y.date_month, 0)]) || ARRAY[COALESCE(d.hits, 0)]
        END AS hit_array,
    CASE WHEN y.unique_visitors IS NOT NULL
        THEN y.unique_visitors || ARRAY[COALESCE(d.unique_users, 0)]
        ELSE ARRAY_FILL(0, ARRAY[GREATEST(d.curr_date - y.date_month, 0)]) || ARRAY[COALESCE(d.unique_users, 0)]
        END AS unique_visitors
FROM daily_agg d
FULL OUTER JOIN yesterday y
    ON d.host = y.host;

select * from host_activity_reduced
limit 10;