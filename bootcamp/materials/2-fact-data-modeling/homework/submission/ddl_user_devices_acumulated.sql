-- drop table if exists user_devices_cumulated;

-- create table if not exists user_devices_cumulated (
--     user_id numeric,
--     browser_type text,
--     device_activity_datelist date[]
-- );

INSERT INTO user_devices_cumulated
WITH activity AS (
    SELECT
        events.user_id,
        devices.browser_type,
        array_agg(
            CASE
                WHEN events.event_time IS NOT NULL THEN events.event_time::date
                ELSE NULL
            END
        ) AS device_activity_datelist
    FROM events
    JOIN devices ON events.device_id = devices.device_id
    WHERE user_id IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    user_id,
    browser_type,
    device_activity_datelist
FROM activity
