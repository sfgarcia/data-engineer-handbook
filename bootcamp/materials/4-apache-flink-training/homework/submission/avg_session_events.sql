-- What is the average number of web events of a session from a user on Tech Creator?
SELECT
    AVG(events_in_session) AS avg_events_in_session
FROM aggregated_sessionazed_events
WHERE host = 'zachwilson.techcreator.io'

-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT
    host,
    AVG(events_in_session) AS avg_events_in_session
FROM aggregated_sessionazed_events
GROUP BY host
