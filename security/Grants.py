# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalogo","catalog_anime")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON SCHEMA ${catalogo}.bronze TO `Data_Engineers`;
# MAGIC GRANT SELECT ON SCHEMA ${catalogo}.bronze TO `Data_Engineers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON SCHEMA ${catalogo}.silver TO `Data_Engineers`;
# MAGIC GRANT USAGE ON SCHEMA ${catalogo}.silver TO `data_analysts`;
# MAGIC GRANT USAGE ON SCHEMA ${catalogo}.silver TO `bi_users`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalogo}.silver TO `Data_Engineers`;
# MAGIC
# MAGIC GRANT SELECT ON SCHEMA ${catalogo}.silver TO `data_analysts`;
# MAGIC GRANT SELECT ON SCHEMA ${catalogo}.silver TO `bi_users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON SCHEMA ${catalogo}.golden TO `data_analysts`;
# MAGIC GRANT USAGE ON SCHEMA ${catalogo}.golden TO `bi_users`;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalogo}.golden TO `Data_Engineers`;
# MAGIC
# MAGIC GRANT SELECT ON SCHEMA ${catalogo}.golden TO `bi_users`;
