-- INSERT INTO user_devices_cumulated
-- WITH activity AS (
--     SELECT
--         events.user_id,
--         devices.browser_type,
--         array_agg(
--             CASE
--                 WHEN events.event_time IS NOT NULL THEN events.event_time::date
--                 ELSE NULL
--             END
--         ) AS device_activity_datelist
--     FROM events
--     JOIN devices ON events.device_id = devices.device_id
--     WHERE user_id IS NOT NULL
--     GROUP BY 1, 2
-- )
-- SELECT
--     user_id,
--     browser_type,
--     device_activity_datelist
-- FROM activity

WITH yesterday AS (
    SELECT
        user_id,
        browser_type,
        device_activity_datelist
    FROM user_devices_cumulated
    WHERE curr_date = date('2022-12-31')
),
today AS (
    SELECT DISTINCT
        event_time::date AS curr_date,
        events.user_id,
        devices.browser_type
    FROM events
    JOIN devices ON events.device_id = devices.device_id
    WHERE events.user_id IS NOT NULL
        AND event_time::date = date('2023-01-01')
)

SELECT
    COALESCE(yesterday.user_id, today.user_id) AS user_id,
    COALESCE(yesterday.browser_type, today.browser_type) AS browser_type,
    ARRAY[today.curr_date] || yesterday.device_activity_datelist AS device_activity_datelist,
    today.curr_date
FROM yesterday
FULL OUTER JOIN today
    ON yesterday.user_id = today.user_id
    AND yesterday.browser_type = today.browser_type;

-- select *
-- from events
-- where user_id = 2780254311411550000

WITH date_events AS (
    SELECT DISTINCT
        event_time::date AS event_date,
        user_id,
        device_id
    FROM events
    WHERE event_time::date = date('2023-01-01')
)

select *
from date_events
where user_id = 70132547320211180