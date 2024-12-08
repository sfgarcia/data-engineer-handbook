from pyspark.sql import SparkSession


def transform(spark, dfs):
    query = """
    WITH yesterday AS (
        SELECT
            user_id,
            browser_type,
            device_activity_datelist,
            curr_date
        FROM user_devices_cumulated
        WHERE curr_date = DATE('2023-01-02')
    ),
    today AS (
        SELECT DISTINCT
            DATE(event_time) AS curr_date,
            events.user_id,
            devices.browser_type
        FROM events
        JOIN devices ON events.device_id = devices.device_id
        WHERE events.user_id IS NOT NULL
            AND DATE(event_time) = DATE('2023-01-03')
    )

    SELECT
        COALESCE(today.curr_date, yesterday.curr_date + interval '1' day) AS curr_date,
        COALESCE(yesterday.user_id, today.user_id) AS user_id,
        COALESCE(yesterday.browser_type, today.browser_type) AS browser_type,
        CASE WHEN yesterday.device_activity_datelist IS NULL THEN ARRAY(today.curr_date)
            WHEN today.curr_date IS NULL THEN yesterday.device_activity_datelist
            ELSE CONCAT(ARRAY(today.curr_date), yesterday.device_activity_datelist) END AS device_activity_datelist
    FROM yesterday
    FULL OUTER JOIN today
        ON yesterday.user_id = today.user_id
        AND yesterday.browser_type = today.browser_type
    """
    for table_name, df in dfs.items():
        df.createOrReplaceTempView(table_name)
    return spark.sql(query)


def main():
    spark = (
        SparkSession.builder.master("local")
        .appName("cum_devices_datelist")
        .getOrCreate()
    )
    dfs = {
        "user_devices_cumulated": spark.table("user_devices_cumulated"),
        "events": spark.table("events"),
        "devices": spark.table("devices"),
    }
    output_df = transform(spark, dfs)
    output_df.write.mode("overwrite").insertInto("cum_devices_datelist")
