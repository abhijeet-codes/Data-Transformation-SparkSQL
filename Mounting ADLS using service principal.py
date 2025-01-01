# Databricks notebook source
apporcientID="ac8b1c37-c685-4a1e-83a0-3a59f1c3cfcc"
dirortenantID="d65a99bd-4288-4b51-9d7b-b3f7f8392825"
clientsecret="fhd8Q~7WU_zaUBnRDKaginPABb4kZMzGnHxqrbbu"

# COMMAND ----------

#service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.adlstokolym.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlstokolym.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlstokolym.dfs.core.windows.net", apporcientID)
spark.conf.set("fs.azure.account.oauth2.client.secret.adlstokolym.dfs.core.windows.net", clientsecret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlstokolym.dfs.core.windows.net", f"https://login.microsoftonline.com/{dirortenantID}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://azure-bde-project@adlstokolym.dfs.core.windows.net")

# COMMAND ----------

df2=spark.read.csv("abfss://azure-bde-project@adlstokolym.dfs.core.windows.net/LandingZone/olympicstokyo/Athletes",header=True,inferSchema=True,sep=";")
display(df2)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": apporcientID,
          "fs.azure.account.oauth2.client.secret": clientsecret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dirortenantID}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://azure-bde-project@adlstokolym.dfs.core.windows.net/",
  mount_point = "/mnt/azure-bde-project",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/azure-bde-project/LandingZone/olympicstokyo")
