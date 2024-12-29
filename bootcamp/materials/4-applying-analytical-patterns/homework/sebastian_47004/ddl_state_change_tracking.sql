CREATE TYPE player_state AS ENUM (
    'New',
    'Retired',
    'Continued Playing',
    'Returned from Retirement',
    'Stayed Retired'
);

drop table if exists players_state_tracking;
CREATE TABLE players_state_tracking (
     player_name TEXT,
     first_active_season INT,
     last_active_season INT,
     player_state player_state,
     seasons_active INT[],
     season INT,
     PRIMARY KEY (player_name, season)
 );