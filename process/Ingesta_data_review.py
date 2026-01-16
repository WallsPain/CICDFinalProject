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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/reviews.csv"

# COMMAND ----------

reviews_schema = StructType(fields=[StructField("uid", IntegerType(), False),
                                     StructField("profile", StringType(), True),
                                     StructField("anime_uid", IntegerType(), True),
                                     StructField("text", StringType(), True),
                                     StructField("score", IntegerType(), True),
                                     StructField("scores", StringType(), True),
                                     StructField("link", StringType(), True)
])

# COMMAND ----------

df_review = spark.read\
.option('header', True)\
.option("sep", ",")\
.option("quote", '"')\
.option("escape", '"')\
.option("multiLine", True)\
.schema(reviews_schema)\
.csv(ruta)

# COMMAND ----------

review_selected_df = df_review.select(col("uid"), 
                                                col("profile"), 
                                                col("anime_uid"),
                                                col("text"), 
                                                col("score"), 
                                                col("scores"))

# COMMAND ----------

reviews_renamed_df = review_selected_df.withColumnRenamed("uid", "review_id") \
                                            .withColumnRenamed("anime_uid", "anime_id") 

# COMMAND ----------

reviews_final_df = reviews_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

reviews_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        f"abfss://{esquema}@{storage_name}.dfs.core.windows.net/reviews"
    ) \
    .saveAsTable(f"{catalogo}.{esquema}.reviews")
