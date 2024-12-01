-- TRUNCATE TABLE user_devices_cumulated;
INSERT INTO hosts_cumulated

WITH yesterday AS (
    SELECT
        curr_date,
        host,
        host_activity_datelist
    FROM hosts_cumulated
    WHERE curr_date = DATE('2023-01-02')
),

today AS (
    SELECT DISTINCT
        event_time::DATE AS curr_date,
        host
    FROM events
    WHERE event_time::DATE = DATE('2023-01-03')
)

SELECT
    COALESCE(yesterday.host, today.host) AS host,
    CASE WHEN yesterday.host_activity_datelist IS NULL THEN ARRAY[today.curr_date]
        WHEN today.curr_date IS NULL THEN yesterday.host_activity_datelist
        ELSE ARRAY[today.curr_date] || yesterday.host_activity_datelist END AS host_activity_datelist,
    COALESCE(today.curr_date, yesterday.curr_date + interval '1' day) AS curr_date
FROM yesterday
FULL OUTER JOIN today
    ON yesterday.host = today.host;

select * from hosts_cumulated
limit 10;