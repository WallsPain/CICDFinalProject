# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import MapType, StringType

# COMMAND ----------

dbutils.widgets.text("storage_name", "adlssmartdata1910")
dbutils.widgets.text("catalogo", "catalog_anime")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_animes= spark.table(f"{catalogo}.{esquema_source}.animes")
df_profiles = spark.table(f"{catalogo}.{esquema_source}.profiles")
df_reviews = spark.table(f"{catalogo}.{esquema_source}.reviews")

# COMMAND ----------

window_anime = Window.partitionBy("anime_id").orderBy(F.col("ingestion_date").desc())
df_animes = (
    df_animes
    .withColumn("row_num", F.row_number().over(window_anime))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

window_profile = (Window.partitionBy("profile").orderBy(F.col("ingestion_date").desc()))
df_profiles = (
    df_profiles
    .withColumn("row_num", F.row_number().over(window_profile))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

window_review = (Window.partitionBy("review_id").orderBy(F.col("ingestion_date").desc()))
df_reviews = (
    df_reviews
    .withColumn("row_num", F.row_number().over(window_review))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

df_animes_clean = (
    df_animes
    .withColumn("anime_id", F.col("anime_id").cast("int"))
    .withColumn("title", F.trim(F.col("title")))
    .withColumn("episodes", F.col("episodes").cast("int"))
    .withColumn("popularity", F.col("popularity").cast("int"))
    .withColumn("ranked", F.col("ranked").cast("int"))
    .withColumn("score", F.col("score").cast("double"))
)

df_profiles_clean = (
    df_profiles
    .withColumn("profile", F.trim(F.col("profile")))
    .withColumn(
        "gender",
        F.when(F.col("gender").isNull(), F.lit("Unknown"))
         .otherwise(F.initcap(F.trim(F.col("gender"))))
    )
)

df_reviews_clean = (
    df_reviews
    .withColumn("review_id", F.col("review_id").cast("int"))
    .withColumn("anime_id", F.col("anime_id").cast("int"))
    .withColumn("profile", F.trim(F.col("profile")))
    .withColumn("text", F.trim(F.col("text")))
    .withColumn("score", F.col("score").cast("double"))
)

# COMMAND ----------

df_transform_aired = (
    df_animes_clean
    .withColumn("aired_clean", F.trim(F.col("aired")))
)
df_transform_aired = df_transform_aired.withColumn(
    "aired_start_str",
    F.when(F.col("aired_clean").contains("to"),
           F.split(F.col("aired_clean"), " to ").getItem(0))
     .otherwise(F.col("aired_clean"))
)
df_transform_aired = df_transform_aired.withColumn(
    "aired_start_date",
    F.to_date(F.col("aired_start_str"), "MMM d, yyyy")
)
df_transform_aired = df_transform_aired.withColumn(
    "aired_end_str",
    F.when(
        (F.col("aired_clean").contains("to")) &
        (F.col("aired_clean").endswith("?") == False),
        F.split(F.col("aired_clean"), " to ").getItem(1)
    )
)
df_transform_aired = df_transform_aired.withColumn(
    "aired_end_date",
    F.to_date(F.col("aired_end_str"), "MMM d, yyyy")
)

# COMMAND ----------

df_transform_birthday = df_profiles_clean.withColumn(
    "birthday_date",
    F.to_date(F.col("birthday"), "MMM d, yyyy")
)
df_transform_birthday = df_transform_birthday.withColumn(
    "age",
    F.when(
        F.col("birthday_date").isNotNull(),
        F.floor(F.months_between(F.current_date(), F.col("birthday_date")) / 12)
    )
)

# COMMAND ----------

df_transform_scores = df_reviews_clean.withColumn(
    "scores_json",
    F.regexp_replace(F.col("scores"), "'", '"')
)

score_schema = MapType(StringType(), StringType())

df_transform_scores = df_transform_scores.withColumn(
    "scores_map",
    F.from_json(F.col("scores_json"), score_schema)
)

# COMMAND ----------

df_transform_scores = (
    df_transform_scores
    .withColumn("score_overall", F.col("scores_map")["Overall"].cast("int"))
    .withColumn("score_story", F.col("scores_map")["Story"].cast("int"))
    .withColumn("score_animation", F.col("scores_map")["Animation"].cast("int"))
    .withColumn("score_sound", F.col("scores_map")["Sound"].cast("int"))
    .withColumn("score_character", F.col("scores_map")["Character"].cast("int"))
    .withColumn("score_enjoyment", F.col("scores_map")["Enjoyment"].cast("int"))
)

df_transform_scores = df_transform_scores.drop("scores", "scores_json", "scores_map")

# COMMAND ----------

df_anime_transformed = df_transform_aired.select(
    "anime_id",
    "title",
    "synopsis",
    "episodes",
    "popularity",
    "ranked",
    "score",
    "aired_start_date",
    "aired_end_date"
)

df_profile_transformed = df_transform_birthday.select(
    "profile",
    "gender",
    "birthday_date",
    "age"
)

r = df_transform_scores.alias("r")
p = df_profile_transformed.alias("p")
a = df_anime_transformed.alias("a")

df_valid_review = (
    r
    .join(p, F.col("r.profile") == F.col("p.profile"), "inner")
    .join(a, F.col("r.anime_id") == F.col("a.anime_id"), "inner")
)

df_review_transformed = df_valid_review.select(
    F.col("r.review_id"),
    F.col("r.profile"),
    F.col("r.anime_id"),
    F.col("r.text"),
    F.col("r.score").alias("review_score"),
    F.col("r.score_overall"),
    F.col("r.score_story"),
    F.col("r.score_animation"),
    F.col("r.score_sound"),
    F.col("r.score_character"),
    F.col("r.score_enjoyment")
)

# COMMAND ----------

df_genres = df_animes_clean.select("anime_id", "genre")

df_genres = df_genres.withColumn(
    "genre_clean",
    F.regexp_replace(F.col("genre"), r"[\[\]']", "")
)


df_genres = df_genres.withColumn(
    "genre_array",
    F.split(F.col("genre_clean"), ",\s*")
)

# COMMAND ----------

df_anime_genres = (
    df_genres
    .select(
        "anime_id",
        F.explode("genre_array").alias("genre")
    )
    .withColumn("genre", F.trim(F.col("genre")))
)


# COMMAND ----------

df_favorites = df_profiles_clean.select("profile", "favorites_anime")

df_favorites = df_favorites.withColumn(
    "favorites_clean",
    F.regexp_replace(F.col("favorites_anime"), r"[\[\]']", "")
)

df_favorites = df_favorites.withColumn(
    "favorites_array",
    F.split(F.col("favorites_clean"), ",\s*")
)

# COMMAND ----------

df_user_favorites = (
    df_favorites
    .select(
        "profile",
        F.explode("favorites_array").alias("anime_id")
    )
    .withColumn("anime_id", F.col("anime_id").cast("int"))
    .filter(F.col("anime_id").isNotNull())
)

# COMMAND ----------

df_anime_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/animes_transformed"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.animes_transformed")

# COMMAND ----------

df_profile_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/profiles_transformed"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.profiles_transformed")

# COMMAND ----------

df_review_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/reviews_transformed"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.reviews_transformed")

# COMMAND ----------

df_anime_genres.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/animes_genres"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.animes_genres")

# COMMAND ----------

df_user_favorites.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema_sink}@{storage_name}.dfs.core.windows.net/animes_favorites_users"
    ) \
    .saveAsTable(f"{catalogo}.{esquema_sink}.animes_favorites_users")
