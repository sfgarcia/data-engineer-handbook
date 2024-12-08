from chispa.dataframe_comparer import assert_df_equality
from ..jobs.cum_devices_datelist import transform
from collections import namedtuple

UserDevicesCumulated = namedtuple(
    "UserDevicesCumulated", "curr_date user_id browser_type device_activity_datelist"
)
Events = namedtuple("Events", "event_time user_id device_id")
Devices = namedtuple("Devices", "device_id browser_type")


def test_scd_generation(spark):
    user_devices_cumulated = [
        UserDevicesCumulated("2023-01-02", 1, "chrome", ["2023-01-02"]),
        UserDevicesCumulated("2023-01-02", 2, "safari", ["2023-01-02"]),
    ]
    events = [
        Events("2023-01-03 00:00:00", 1, 1),
        Events("2023-01-03 00:00:00", 2, 2),
    ]
    devices = [
        Devices(1, "chrome"),
        Devices(2, "safari"),
    ]
    user_devices_cumulated_df = spark.createDataFrame(user_devices_cumulated)
    events_df = spark.createDataFrame(events)
    devices_df = spark.createDataFrame(devices)
    dfs = {
        "user_devices_cumulated": user_devices_cumulated_df,
        "events": events_df,
        "devices": devices_df,
    }

    actual_df = transform(spark, dfs)

    expected_data = [
        UserDevicesCumulated("2023-01-03", 1, "chrome", ["2023-01-03", "2023-01-02"]),
        UserDevicesCumulated("2023-01-03", 2, "safari", ["2023-01-03", "2023-01-02"]),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
