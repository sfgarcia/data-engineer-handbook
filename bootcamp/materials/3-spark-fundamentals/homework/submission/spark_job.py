from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("spark_job")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.sources.v2.bucketing.enabled", "true")
    .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
    .getOrCreate()
)


match_details = spark.read.csv(
    "../../data/match_details.csv", header=True, inferSchema=True
)
matches = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)
medal_matches_players = spark.read.csv(
    "../../data/medals_matches_players.csv", header=True, inferSchema=True
)
medals = spark.read.csv("../../data/medals.csv", header=True, inferSchema=True)
maps = spark.read.csv("../../data/maps.csv", header=True, inferSchema=True)

#### Crear las tablas bucketizadas


spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")


match_details.select("match_id", "player_gamertag", "player_total_kills").write.mode(
    "overwrite"
).bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")


spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")


matches.select("match_id", "mapid", "playlist_id").write.mode("overwrite").bucketBy(
    16, "match_id"
).saveAsTable("bootcamp.matches_bucketed")


spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")


medal_matches_players.select(
    "match_id", "player_gamertag", "medal_id", "count"
).write.mode("overwrite").bucketBy(16, "match_id").saveAsTable(
    "bootcamp.medal_matches_players_bucketed"
)

#### Compare the bucketed vs non bucketed joins


joined_df = match_details.select(
    "match_id", "player_gamertag", "player_total_kills"
).join(matches.select("match_id", "mapid", "playlist_id"), on=["match_id"])


match_details_bucketed = spark.read.table("bootcamp.match_details_bucketed")
matches_bucketed = spark.read.table("bootcamp.matches_bucketed")
medal_matches_players_bucketed = spark.read.table(
    "bootcamp.medal_matches_players_bucketed"
)


match_full = match_details_bucketed.join(matches_bucketed, on=["match_id"], how="inner")


match_full.filter(
    (F.col("match_id") == "3668fd3c-53f2-42f2-a337-8af81fcf551b")
    & (F.col("player_gamertag") == "EcZachly")
).show()


medal_matches_players_bucketed.filter(
    (F.col("match_id") == "3668fd3c-53f2-42f2-a337-8af81fcf551b")
    & (F.col("player_gamertag") == "EcZachly")
).show()

#### Aggregate dfs to find metrics

#### Matches metrics


avg_kills = match_full.groupBy("player_gamertag").agg(
    F.coalesce(F.avg("player_total_kills", F.lit(0))).alias("avg_kills")
)
avg_kills = avg_kills.orderBy("avg_kills", "player_gamertag", ascending=False)


avg_kills.show()


playlist_count = match_full.groupBy("playlist_id").agg(
    F.count("player_total_kills").alias("playlist_count")
)
playlist_count = playlist_count.orderBy("playlist_count", ascending=False)


playlist_count.show()


map_count = match_full.groupBy("mapid").agg(
    F.count("player_total_kills").alias("map_count")
)
map_count = map_count.orderBy("map_count", ascending=False)

map_count.show()


##### Medals metrics


medals_full = (
    medal_matches_players_bucketed.join(
        F.broadcast(medals.select("medal_id", "name")),
        on=["medal_id"],
        how="inner",
    )
    .join(
        matches_bucketed,
        on=["match_id"],
        how="inner",
    )
    .join(
        F.broadcast(maps.select("mapid", "name").withColumnRenamed("name", "map_name")),
        on=["mapid"],
        how="inner",
    )
)


ks_medals = medals_full.filter(F.col("name") == "Killing Spree")


ks_medals_count = ks_medals.groupBy("mapid").agg(F.count("medal_id").alias("map_count"))
ks_medals_count = ks_medals_count.orderBy("map_count", ascending=False)


ks_medals_count.show()

#### Try different sorting strategies on medals_full


sorted_medals_1 = medals_full.sortWithinPartitions(
    "mapid", "playlist_id", "player_gamertag", "match_id"
)


sorted_medals_1.write.parquet("../../output")


sorted_medals_2 = medals_full.sortWithinPartitions(
    "playlist_id", "mapid", "player_gamertag", "match_id"
)


sorted_medals_2.write.parquet("../../output")

sorted_medals_2 = medals_full.sortWithinPartitions(
    "player_gamertag", "playlist_id", "mapid", "match_id"
)


sorted_medals_2.write.parquet("../../output")
