-- What is the most games a team has won in a 90 game stretch?
with games_long as (
    select
        home_team_id as team_id,
        game_date_est,
        home_team_wins as team_wins
    from games
    union all
    select
        visitor_team_id as team_id,
        game_date_est,
        1 - home_team_wins as team_wins
    from games
),
ninety_game_stretch as (
    select
        team_id,
        game_date_est,
        team_wins,
        sum(team_wins) over (partition by team_id order by game_date_est rows between 89 preceding and current row) as last_90_games_won
    from games_long
)

select
    team_id,
    last_90_games_won
from ninety_game_stretch
order by 2 desc
limit 10;

-- How many games in a row did LeBron James score over 10 points a game?
with lebron_games as (
    select
        gd.player_name,
        gd.game_id,
        g.game_date_est,
        COALESCE(gd.pts, 0) as scored_points
    from game_details gd
    join games g
        on gd.game_id = g.game_id
    where gd.player_name = 'LeBron James'
),
lebron_streaks as (
    select
        player_name,
        game_id,
        scored_points,
        sum(case when scored_points < 11 then 1 else 0 end) over (partition by player_name order by game_date_est) as streak
    from lebron_games
)

select
    player_name,
    streak,
    sum(case when scored_points > 10 then 1 else 0 end) streak_length
from lebron_streaks
group by 1, 2
order by 3 desc
limit 1000;



select *
from game_details
where player_name = 'LeBron James'
limit 10;