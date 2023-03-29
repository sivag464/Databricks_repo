# Databricks notebook source
#Access Azure Data Lake Storage Gen2 or Blob Storage using the account key
spark.conf.set(
    "fs.azure.account.key.adlsgen233.dfs.core.windows.net",
    "QWyh3Gb6ijbn6IE9RLGjnBRQTJT4Ls9B/1drDkxzxVL8OMkja3P63Dmdv6kJb6oWzZUy/syaze7d+AStJ7ABPg==")

# COMMAND ----------

df = spark.read.csv(path ="abfss://input@adlsgen233.dfs.core.windows.net/test1.csv",header ="true" )
display(df)

# COMMAND ----------

# MAGIC %md ##mounting  and access adls gen2 using access keys and secret scopes

# COMMAND ----------

# template

# spark.conf.set(
#     "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
#     dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

storage_account_name = 'adlsgen233'

spark.conf.set(
    "fs.azure.account.key.<storage_account_name>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

# https://adb-7644048526291037.17.azuredatabricks.net/?o=7644048526291037#secrets/createScope - reference
