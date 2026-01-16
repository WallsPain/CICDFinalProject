# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("storage_name", "adlssmartdata1910")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_anime")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/animes.csv"

# COMMAND ----------

animes_schema = StructType(fields=[StructField("uid", IntegerType(), False),
                                     StructField("title", StringType(), True),
                                     StructField("synopsis", StringType(), True),
                                     StructField("genre", StringType(), True),
                                     StructField("aired", StringType(), True),
                                     StructField("episodes", DoubleType(), True),
                                     StructField("members", IntegerType(), True),
                                     StructField("popularity", IntegerType(), True),
                                     StructField("ranked", DoubleType(), True),
                                     StructField("score", DoubleType(), True),
                                     StructField("img_url", StringType(), True),
                                     StructField("link", StringType(), True)
])

# COMMAND ----------

df_anime = spark.read\
.option('header', True)\
.option("sep", ",")\
.option("quote", '"')\
.option("escape", '"')\
.option("multiLine", True)\
.schema(animes_schema)\
.csv(ruta)

# COMMAND ----------

anime_selected_df = df_anime.select(col("uid"), 
                                                col("title"), 
                                                col("synopsis"), col("genre"), 
                                                col("aired"), 
                                                col("episodes"), 
                                                col("popularity"), 
                                                col("ranked"),
                                                col("score"))


# COMMAND ----------

anime_renamed_df = anime_selected_df.withColumnRenamed("uid", "anime_id")

# COMMAND ----------

anime_final_df = anime_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

anime_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema}@{storage_name}.dfs.core.windows.net/animes"
    ) \
    .saveAsTable(f"{catalogo}.{esquema}.animes")
