-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated_source (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
);

CREATE TABLE IF NOT EXISTS aggregated_sessionazed_events (
    host VARCHAR,
    avg_events_per_session REAL,
    total_sessions BIGINT,
    total_events BIGINT,
    PRIMARY KEY (host)
);

select *
from processed_events;

select *
from processed_events_aggregated;

select *
from processed_events_aggregated_source;

select *
from aggregated_sessionazed_events;

