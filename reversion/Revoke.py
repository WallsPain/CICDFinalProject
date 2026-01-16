# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("nameStorage","adlssmartdata1910")
dbutils.widgets.text("catalogo","catalog_anime")

# COMMAND ----------

nameStorage = dbutils.widgets.get("nameStorage")
catalogo = dbutils.widgets.get("catalogo")

ruta_bronze = f"abfss://bronze@{nameStorage}.dfs.core.windows.net"
ruta_silver = f"abfss://silver@{nameStorage}.dfs.core.windows.net"
ruta_golden = f"abfss://golden@{nameStorage}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.animes;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.profiles;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.reviews;

# COMMAND ----------

## REMOVE DATA (Bronze)
dbutils.fs.rm(f"{ruta_bronze}/animes", True)
dbutils.fs.rm(f"{ruta_bronze}/profiles", True)
dbutils.fs.rm(f"{ruta_bronze}/reviews", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.animes_transformed;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.profiles_transformed;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.reviews_transformed;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.animes_genres;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.animes_favorites_users;

# COMMAND ----------

# REMOVE DATA (Silver)
dbutils.fs.rm(f"{ruta_silver}/animes_transformed", True)
dbutils.fs.rm(f"{ruta_silver}/profiles_transformed", True)
dbutils.fs.rm(f"{ruta_silver}/reviews_transformed", True)
dbutils.fs.rm(f"{ruta_silver}/animes_genres", True)
dbutils.fs.rm(f"{ruta_silver}/animes_favorites_users", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.anime_kpis;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.genre_trends;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.user_engagement;

# COMMAND ----------

# REMOVE DATA (Golden)
dbutils.fs.rm(f"{ruta_golden}/anime_kpis", True)
dbutils.fs.rm(f"{ruta_golden}/genre_trends", True)
dbutils.fs.rm(f"{ruta_golden}/user_engagement", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA ${catalogo}.bronze FROM `data_analysts`;
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA ${catalogo}.bronze FROM `bi_users`;
# MAGIC REVOKE CREATE TABLE ON SCHEMA ${catalogo}.bronze FROM `Data_Engineers`;
