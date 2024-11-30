WITH users AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE curr_date = DATE('2023-01-03')
),
series AS (
    SELECT generate_series(
        (SELECT MIN(curr_date) FROM user_devices_cumulated),
        (SELECT MAX(curr_date) FROM user_devices_cumulated),
        '1 day'::interval
    )::date AS date_series
),
placeholder_ints AS (
    SELECT
        CAST(
            CASE WHEN device_activity_datelist @> ARRAY[series.date_series]
            THEN CAST(POW(2, 32 - (curr_date - series.date_series)) AS BIGINT)
            ELSE 0 END
            AS BIT(32)
         ) AS placeholder_int,
        *
    FROM users
    CROSS JOIN series
)

select *
from placeholder_ints
