import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    StreamTableEnvironment,
)
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

# Setup instructions for the Flink job
# 1. Start docker engine
# 2. Open one terminal in this directory: 1-dimensional-data-modeling
# 3. Run `make up` to start Postgres
# 4. Open another terminal in this directory: 4-apache-flink-training
# 5. Run `make up` to start Flink
# 6. Run `make sessionization_job` to trigger the Flink job
# 7, Watch the job in the Flink dashboard: http://localhost:8081


def create_aggregated_events_sink_postgres(t_env):
    table_name = "aggregated_sessionized_events"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP,
            host VARCHAR,
            ip VARCHAR,
            events_in_session BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1_000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create Kafka table
    source_table = create_processed_events_source_kafka(t_env)
    aggregated_table = create_aggregated_events_sink_postgres(t_env)

    source = t_env.from_path(source_table)

    # First create sessions by IP and host
    windowed = source.window(
        Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
    )

    # Group by window, IP, and host to get individual sessions
    sessions = windowed.group_by(col("w"), col("ip"), col("host")).select(
        col("w").start.alias("session_start"),
        col("host"),
        col("ip"),
        col("host").count.alias("events_in_session"),
    )

    sessions.execute_insert(aggregated_table).wait()


if __name__ == "__main__":
    log_aggregation()
