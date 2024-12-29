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
        1 - g.home_team_wins AS team_wins,
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
),
one_team_wins_for_all_players AS (
    SELECT
        season,
        game_id,
        team_id,
        player_name,
        CASE
            WHEN player_name = FIRST_VALUE(player_name) OVER (PARTITION BY season, game_id, team_id) THEN team_wins
            ELSE 0
        END AS team_wins,
        scored_points
    FROM union_teams
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
FROM one_team_wins_for_all_players
GROUP BY GROUPING SETS (
    (team_id), 
    (player_name, team_id),
    (season, player_name)
);

-- Who scored the most points playing for one team?
select *
from game_details_agg
where aggregation_level = 'team_id__player_name'
order by total_points desc
limit 10;

-- Who scored the most points in one season?
select *
from game_details_agg
where aggregation_level = 'season__player_name'
order by total_points desc
limit 10;

-- Which team has won the most games?
select *
from game_details_agg
where aggregation_level = 'team_id'
order by match_wins desc
limit 10;

-- Queries to explore the tables
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