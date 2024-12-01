-- TRUNCATE TABLE user_devices_cumulated;
INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT
        curr_date,
        user_id,
        browser_type,
        device_activity_datelist
    FROM user_devices_cumulated
    WHERE curr_date = DATE('2023-01-02')
),
today AS (
    SELECT DISTINCT
        event_time::DATE AS curr_date,
        events.user_id,
        devices.browser_type
    FROM events
    JOIN devices ON events.device_id = devices.device_id
    WHERE events.user_id IS NOT NULL
        AND event_time::DATE = DATE('2023-01-03')
)

SELECT
    COALESCE(yesterday.user_id, today.user_id) AS user_id,
    COALESCE(yesterday.browser_type, today.browser_type) AS browser_type,
    CASE WHEN yesterday.device_activity_datelist IS NULL THEN ARRAY[today.curr_date]
        WHEN today.curr_date IS NULL THEN yesterday.device_activity_datelist
        ELSE ARRAY[today.curr_date] || yesterday.device_activity_datelist END AS device_activity_datelist,
    COALESCE(today.curr_date, yesterday.curr_date + interval '1' day) AS curr_date
FROM yesterday
FULL OUTER JOIN today
    ON yesterday.user_id = today.user_id
    AND yesterday.browser_type = today.browser_type;

-- select *
-- from user_devices_cumulated
-- where cardinality(device_activity_datelist) > 1
-- limit 10
