# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("storage_name", "adlssmartdata1910")
dbutils.widgets.text("catalogo", "catalog_anime")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_favorites= spark.table(f"{catalogo}.{esquema_source}.animes_favorites_users")
df_genres = spark.table(f"{catalogo}.{esquema_source}.animes_genres")
df_animes = spark.table(f"{catalogo}.{esquema_source}.animes_transformed")
df_profiles = spark.table(f"{catalogo}.{esquema_source}.profiles_transformed")
df_reviews = spark.table(f"{catalogo}.{esquema_source}.reviews_transformed")

# COMMAND ----------

df_anime_kpis = (
    df_reviews
    .groupBy("anime_id")
    .agg(
        F.count("*").alias("review_count"),
        F.avg("review_score").alias("avg_score"),
        F.avg("score_overall").alias("avg_overall"),
        F.avg("score_story").alias("avg_story"),
        F.avg("score_animation").alias("avg_animation"),
        F.avg("score_sound").alias("avg_sound"),
        F.avg("score_character").alias("avg_character"),
        F.avg("score_enjoyment").alias("avg_enjoyment"),
    )
    .join(df_animes.select("anime_id", "title", "popularity", "ranked"),
          on="anime_id",
          how="left")
)


# COMMAND ----------

df_genre_trends = (
    df_genres
    .join(df_reviews, on="anime_id", how="left")
    .groupBy("genre")
    .agg(
        F.countDistinct("anime_id").alias("anime_count"),
        F.count("review_id").alias("review_count"),
        F.avg("review_score").alias("avg_score")
    )
)

# COMMAND ----------

df_user_engagement = (
    df_profiles
    .join(df_reviews, on="profile", how="left")
    .groupBy("profile")
    .agg(
        F.count("review_id").alias("total_reviews"),
        F.avg("review_score").alias("avg_given_score")
    )
)

df_fav_metrics = (
    df_favorites
    .groupBy("profile")
    .agg(F.countDistinct("anime_id").alias("favorites_count"))
)

df_reviewed_favs = (
    df_reviews
    .join(df_favorites, ["profile", "anime_id"], "inner")
    .groupBy("profile")
    .agg(F.countDistinct("anime_id").alias("reviewed_favorites_count"))
)

df_user_engagement = (
    df_user_engagement
    .join(df_fav_metrics, "profile", "left")
    .join(df_reviewed_favs, "profile", "left")
)

# COMMAND ----------

df_anime_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/anime_kpis"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.anime_kpis")

# COMMAND ----------

df_genre_trends.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/genre_trends"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.genre_trends")

# COMMAND ----------

df_user_engagement.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/user_engagement"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.user_engagement")
