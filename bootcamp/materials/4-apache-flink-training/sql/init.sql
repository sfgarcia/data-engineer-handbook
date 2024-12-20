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
    num_hists BIGINT
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated_source (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hists BIGINT
);

select *
from processed_events;

select *
from processed_events_aggregated;

select *
from processed_events_aggregated_source;

