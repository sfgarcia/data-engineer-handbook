from pyspark.sql import SparkSession


def transform(spark, dataframe):
    query = """
        with deduped_game_details as (
            select
                *,
                row_number() over (partition by game_id, team_id, player_id order by game_id) as row_num
            from game_details
        )

        select
            game_id,
            team_id,
            player_id,
            pts
        from deduped_game_details
        where row_num = 1
    """
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = (
        SparkSession.builder.master("local").appName("dedup_game_details").getOrCreate()
    )
    output_df = transform(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("deduped_game_details")
