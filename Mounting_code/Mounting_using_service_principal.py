# Databricks notebook source
# MAGIC %md #approach 1

# COMMAND ----------

# <scope> with the Databricks secret scope name.

# <service-credential-key> with the name of the key containing the client secret.

# <storage-account> with the name of the Azure storage account.

# <application-id> with the Application (client) ID for the Azure Active Directory application.

# <directory-id> with the Directory (tenant) ID for the Azure Active Directory application.

# COMMAND ----------


storage_account_name ="adlsgen233"
client_id = "09917a46-e157-4af0-900b-4805436d6f25"
tenant_id ="6a5ae5c1-f1ca-47ed-9e9d-d65aadd1baf7"
client_secret = "x0C8Q~tQw5Q0mOXuFborlC7cX__aDUzdU7rNtc8A"
#service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>") # client secret value

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", f"{client_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md #approach 2

# COMMAND ----------

#Sample Mount ADLS Gen2 storage with azure databricks  using service principal -configuration format

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

#variable assignments:

storage_account_name ="adlsgen233"
client_id = "09917a46-e157-4af0-900b-4805436d6f25"
tenant_id ="6a5ae5c1-f1ca-47ed-9e9d-d65aadd1baf7"
client_secret = "x0C8Q~tQw5Q0mOXuFborlC7cX__aDUzdU7rNtc8A"




configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://siva@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/siva",
  extra_configs = configs)

# COMMAND ----------

df = spark.read.csv(path ="abfss://siva@adlsgen233.dfs.core.windows.net/test1.csv",header ="true")
display(df)

# COMMAND ----------

# for your reference check below code 

# COMMAND ----------

# MAGIC %md ##Mounting & accessing ADLS Gen2 in Azure Databricks using Service Principal and Secret Scopes

# COMMAND ----------

# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
# Author: Dhyanendra Singh Rathore

# Define the variables used for creating connection strings
adlsAccountName = "dlscsvdataproject"
adlsContainerName = "csv-data-store"
adlsFolderName = "covid19-data"
mountPoint = "/mnt/csvFiles"

# Application (Client) ID
applicationId = dbutils.secrets.get(scope="CSVProjectKeyVault",key="ClientId")

# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="CSVProjectKeyVault",key="ClientSecret")

# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="CSVProjectKeyVault",key="TenantId")

endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)


    
    
    
    
    
## Unmount only if directory is mounted
# if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#   dbutils.fs.unmount(mountPoint)


# COMMAND ----------


