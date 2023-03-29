# Databricks notebook source
# variable assignments
SASTokenKey = '?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-29T22:40:55Z&st=2023-03-29T14:40:55Z&spr=https&sig=Z%2F6LF2qwYjPO669NB6wQEVozqtf0XVGZCU6PgWiE8ic%3D'
storageContainer ='sas'
storageAccount='adlsgen233'


# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storageAccount}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storageAccount}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storageAccount}.dfs.core.windows.net", f"{SASTokenKey}")

# COMMAND ----------

df2 = spark.read.csv(path ="abfss://input@adlsgen233.dfs.core.windows.net/test1.csv",header ="true")
display(df2)

# sas is easiest way of mounting
