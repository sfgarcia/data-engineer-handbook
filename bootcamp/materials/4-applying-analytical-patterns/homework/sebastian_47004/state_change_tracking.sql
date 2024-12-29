INSERT INTO players_state_tracking
WITH yesterday AS (
    SELECT *
    FROM players_state_tracking
    WHERE season = 1999
),
today AS (
    SELECT
        player_name,
        season
    FROM player_seasons
    WHERE season = 2000
    GROUP BY 1, 2
)

SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(y.first_active_season, t.season) AS first_active_season,
    COALESCE(t.season, y.last_active_season) AS last_active_season,
    CASE
        WHEN y.player_name IS NULL AND t.player_name IS NOT NULL THEN 'New'
        WHEN t.player_name IS NULL AND y.season = y.last_active_season THEN 'Retired'
        WHEN y.last_active_season < t.season - 1 THEN 'Returned from Retirement'
        WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
        ELSE 'Stayed Retired'
    END::player_state AS player_state,
    COALESCE(y.seasons_active, ARRAY[]::INT[]) ||
        CASE WHEN t.player_name IS NOT NULL
        THEN ARRAY[t.season] ELSE ARRAY[]::INT[]
        END AS seasons_active,
    COALESCE(t.season, y.season + 1) AS season
FROM today t
FULL OUTER JOIN yesterday y
    ON t.player_name = y.player_name;

select *
from players_state_tracking
where season = 2000;

select *
from player_seasons
where player_name = 'A.C. Green';

select
    season,
    count(*)
from player_seasons
group by 1
order by 1;
