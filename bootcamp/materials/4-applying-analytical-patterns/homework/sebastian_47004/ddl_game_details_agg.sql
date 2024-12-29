DROP TABLE IF EXISTS game_details_agg;
CREATE TABLE IF NOT EXISTS game_details_agg (
    aggregation_level TEXT,
    season INT,
    team_id INT,
    player_name TEXT,
    match_wins INT,
    total_points INT
);