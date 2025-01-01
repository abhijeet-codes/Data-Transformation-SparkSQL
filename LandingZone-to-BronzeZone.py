# Databricks notebook source
#Access Base Parameter - "dbName"
dbutils.widgets.text("dbName","")
db_Name=dbutils.widgets.get("dbName")
print(db_Name)

# COMMAND ----------

landing_zone_path=f"/mnt/azure-bde-project/LandingZone/{db_Name}/"
bronze_zone_path=f"/mnt/azure-bde-project/BronzeZone/{db_Name}/"

# COMMAND ----------

dbutils.fs.ls(landing_zone_path)

# COMMAND ----------

df_athletes=spark.read.csv(landing_zone_path+"Athletes/*",header=True,inferSchema=True,sep=';')
df_coaches=spark.read.csv(landing_zone_path+"Coaches/*",header=True,inferSchema=True,sep=';')
df_entriesGender=spark.read.csv(landing_zone_path+"EntriesGender/*",header=True,inferSchema=True,sep=';')
df_medals=spark.read.csv(landing_zone_path+"Medals/*",header=True,inferSchema=True,sep=';')
df_teams=spark.read.csv(landing_zone_path+"Teams/*",header=True,inferSchema=True,sep=';')


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_athletes=df_athletes.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
df_athletes.write.mode('append').partitionBy("process_date").parquet(bronze_zone_path+"Athletes")

# COMMAND ----------

df_coaches=df_coaches.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
df_coaches.write.mode('append').partitionBy("process_date").parquet(bronze_zone_path+"Coaches")

# COMMAND ----------

df_entriesGender=df_entriesGender.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
df_entriesGender.write.mode('append').partitionBy("process_date").parquet(bronze_zone_path+"EntriesGender")

# COMMAND ----------

df_medals=df_medals.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
df_medals.write.mode('append').partitionBy("process_date").parquet(bronze_zone_path+"Medals")

# COMMAND ----------

df_teams=df_teams.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
df_teams.write.mode('append').partitionBy("process_date").parquet(bronze_zone_path+"Teams")
