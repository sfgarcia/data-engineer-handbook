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

DROP TABLE IF EXISTS aggregated_sessionized_events;
CREATE TABLE IF NOT EXISTS aggregated_sessionized_events (
    session_start TIMESTAMP,
    host VARCHAR,
    ip VARCHAR,
    events_in_session BIGINT
    --PRIMARY KEY (event_hour, host, ip)
);

select *
from processed_events
where host is null;

select *
from processed_events_aggregated;

select *
from processed_events_aggregated_source;

select *
from aggregated_sessionized_events;

