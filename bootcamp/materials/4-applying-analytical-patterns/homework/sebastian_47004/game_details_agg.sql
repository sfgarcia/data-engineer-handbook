INSERT INTO game_details_agg
WITH game_details_dedup AS (
    SELECT
        game_id,
        team_id,
        player_name,
        COALESCE(pts, 0) AS scored_points
    FROM game_details
    GROUP BY 1, 2, 3, 4
),
home_team_join AS (
    SELECT
        g.season,
        g.game_id,
        gd.team_id,
        g.home_team_wins AS team_wins,
        player_name,
        gd.scored_points
    FROM games g
    JOIN game_details_dedup gd
        ON g.game_id = gd.game_id
        AND g.home_team_id = gd.team_id
),
visitor_team_join AS (
    SELECT
        g.season,
        g.game_id,
        gd.team_id,
        1- g.home_team_wins AS team_wins,
        player_name,
        gd.scored_points
    FROM games g
    JOIN game_details_dedup gd
        ON g.game_id = gd.game_id
        AND g.visitor_team_id = gd.team_id
),
union_teams AS (
    SELECT *
    FROM home_team_join
    UNION ALL
    SELECT *
    FROM visitor_team_join
)

SELECT
    CASE
        WHEN GROUPING(season) = 1 AND GROUPING(player_name) = 1 AND GROUPING(team_id) = 0 THEN 'team_id'
        WHEN GROUPING(season) = 1 AND GROUPING(player_name) = 0 AND GROUPING(team_id) = 0 THEN 'team_id__player_name'
        WHEN GROUPING(season) = 0 AND GROUPING(player_name) = 0 AND GROUPING(team_id) = 1 THEN 'season__player_name'
    END AS aggregation_level,
    season,
    team_id,
    player_name,
    SUM(team_wins) AS match_wins,
    SUM(scored_points) AS total_points
FROM union_teams
GROUP BY GROUPING SETS (
    (team_id), 
    (player_name, team_id),
    (season, player_name)
);

select *
from game_details_agg


select
    team_abbreviation,
    player_name
from game_details
limit 10;

select *
from game_details
where game_id = 22200477
limit 100;

select *
from games
where game_id = 22200477
limit 10;

select
    game_id,
    player_id
from game_details
group by 1, 2
having count(*) > 1;

select
    game_id
from games
group by 1
having count(*) > 1;