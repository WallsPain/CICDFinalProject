# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalogo","catalog_anime")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.animes;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.profiles;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.animes_transformed;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.profiles_transformed;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.reviews_transformed;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.animes_genres;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.animes_favorites_users;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.anime_kpis;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.genre_trends;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.user_engagement;

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA ${catalogo}.bronze FROM `data_analysts`;
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA ${catalogo}.bronze FROM `bi_users`;
# MAGIC REVOKE CREATE TABLE ON SCHEMA ${catalogo}.bronze FROM `Data_Engineers`;
