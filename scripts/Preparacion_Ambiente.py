# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageName default "adlssmartdata1910";

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS catalog_anime CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_anime;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_anime.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_anime.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_anime.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_anime.golden;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS catalog_anime.raw.datasets;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw`
# MAGIC URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para los archivos de raw del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
# MAGIC URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
# MAGIC URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas silver del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
# MAGIC URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas golden del Data Lake';
