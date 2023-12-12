# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake for all containers

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Get Secrets from key vault
    client_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-id-client")
    tenant_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-tentat")
    client_secret_value = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-secret")

    # Set Spark Configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret_value,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    mountpoint = f"/mnt/{storage_account_name}/{container_name}"

    #Unmount the mount if it already exists
    if any(mount.mountPoint ==  mountpoint for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(mountpoint)

    # Mount the storage account
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = mountpoint,
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

#Mounting the demo container
mount_adls("formula1itassadl","demo")

# COMMAND ----------

#Mounting the raw container
mount_adls("formula1itassadl","raw")

# COMMAND ----------

#Mounting the presentation container
mount_adls("formula1itassadl","presentation")

# COMMAND ----------

#Mounting the processed container
mount_adls("formula1itassadl","processed")

# COMMAND ----------


