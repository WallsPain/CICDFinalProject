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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/profiles.csv"

# COMMAND ----------

profiles_schema = StructType(fields=[StructField("profile", StringType(), False),
                                     StructField("gender", StringType(), True),
                                     StructField("birthday", StringType(), True),
                                     StructField("favorites_anime", StringType(), True),
                                     StructField("link", StringType(), True)
])

# COMMAND ----------

df_profile = spark.read\
.option('header', True)\
.schema(profiles_schema)\
.csv(ruta)

# COMMAND ----------

profile_selected_df = df_profile.select(col("profile"), 
                                                col("gender"), 
                                                col("birthday"),
                                                col("favorites_anime"))

# COMMAND ----------

profiles_final_df = profile_selected_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

profiles_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.profiles")
