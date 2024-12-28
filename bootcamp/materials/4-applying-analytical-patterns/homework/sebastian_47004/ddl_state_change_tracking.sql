CREATE TYPE player_state AS ENUM (
    'New',
    'Retired',
    'Continued Playing',
    'Returned from Retirement',
    'Stayed Retired'
);


CREATE TABLE players_state_tracking (
     player_name TEXT,
     first_active_year INT,
     last_active_year INT,
     player_state player_state,
     years_active INT[],
     year INT,
     PRIMARY KEY (player_name, year)
 );