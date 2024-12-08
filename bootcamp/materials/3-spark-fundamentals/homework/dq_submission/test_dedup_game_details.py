from chispa.dataframe_comparer import assert_df_equality
from ..jobs.dedup_game_details import transform
from collections import namedtuple

GameDetails = namedtuple("GameDetails", "game_id team_id player_id pts")


def test_scd_generation(spark):
    source_data = [
        GameDetails(1, 11, 111, 10),
        GameDetails(1, 11, 111, 10),
        GameDetails(2, 22, 222, 20),
        GameDetails(2, 22, 222, 20),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = transform(spark, source_df)
    expected_data = [
        GameDetails(1, 11, 111, 10),
        GameDetails(2, 22, 222, 20),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
