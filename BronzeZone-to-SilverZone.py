# Databricks notebook source
#Access Base Parameter - "dbName"
dbutils.widgets.text("dbName","")
db_Name=dbutils.widgets.get("dbName")
print(db_Name)

#Access Base Parameter - "loadType"
dbutils.widgets.text("loadType","")
load_type=dbutils.widgets.get("loadType")
print(load_type)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

tables_list=["Athletes","Coaches","EntriesGender","Medals","Teams"]

# COMMAND ----------

bronze_zone_path=f"/mnt/azure-bde-project/BronzeZone/{db_Name}/"
silver_zone_path=f"/mnt/azure-bde-project/SilverZone/{db_Name}/"

# COMMAND ----------

schema=StructType([
    StructField("path", StringType()),
    StructField("name", StringType()),
    StructField("size", IntegerType()),
    StructField("modificationTime", LongType())
])


# COMMAND ----------

# Generate the date string
date_str = spark.sql("SELECT date_format(current_date(), 'yyyy-MM-dd') AS formatted_date").collect()[0]['formatted_date']
print(date_str)

# COMMAND ----------

db_prefix='silver_db_'
silver_db=db_prefix+db_Name
print(silver_db)

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {silver_db}')
spark.sql(f'USE {silver_db}')

# COMMAND ----------

for folder_name in tables_list:      
    directory_path=f'{bronze_zone_path}{folder_name}/process_date={date_str}/'
    files=dbutils.fs.ls(directory_path)
    df_file=spark.createDataFrame(files,schema)
    df_file=df_file.select('path','name','size',(df_file.modificationTime/1000).cast('timestamp').alias('UpdatedTime'))
    df_file=df_file.select('path','name','size','UpdatedTime').where(df_file.name.endswith('.parquet')).orderBy(df_file.UpdatedTime.desc()).limit(1)
    str_file_name=df_file.select('name').collect()[0]['name']
 
    #Applying cleaning/processing to latest .parquet file of BronzeZone
    input_path = f'{directory_path}{str_file_name}'
       
    #Read data from input path
    df = spark.read.parquet(input_path)

    #Dropping lastModifiedDate and createdDate columns
    df = df.drop('lastModifiedDate').drop('createdDate')

    #Apply NULL handling transformation to each record/data of each folder/table
    for column, data_type in df.dtypes:
        if data_type == 'string':
            df = df.withColumn(column, F.when(df[column].isNull(), 'NA').otherwise(df[column]))
        elif data_type == 'int':
            df = df.withColumn(column, F.when(df[column].isNull(), -1).otherwise(df[column]))
        elif data_type.startswith('date') or data_type.startswith('timestamp'):
            df = df.withColumn(column, F.when(df[column].isNull(), '1900-01-01').otherwise(df[column]))
        
        #Correct the incorrect datatype for 'Gold' column in 'Medals' file of 'tokyo-olympics' database 
        #if db_Name == 'olympicstokyo' and column == 'Gold' :
            #df = df.withColumn("Gold", df["Gold"].cast(IntegerType()))

        #Changing the datatype of process_date to DateType()
        if column == 'process_date' :
            df = df.withColumn("process_date", df["process_date"].cast(DateType()))
    
    #Removal of duplicate records/data from each table/folder
    df=df.dropDuplicates()

    #Including columns 'CreatedDate' and 'ModifiedDate' to every tables/files
    df = df.withColumn("CreatedDate", F.date_format(F.current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("ModifiedDate", F.date_format(F.current_timestamp(),"yyyy-MM-dd HH:mm:ss"))

    #Assuring suitable datatypes for CreatedDate' and 'ModifiedDate'
    df = df.withColumn("CreatedDate", df["CreatedDate"].cast(TimestampType()))
    df = df.withColumn("ModifiedDate", df["ModifiedDate"].cast(TimestampType()))

    #Finally Store the cleaned data to SilverZone in ADLS and SQL table in Databricks Catalog
    if load_type=="FL":
        df.write.mode("overwrite").format("delta").option("path",silver_zone_path+folder_name).saveAsTable(f'{silver_db}.{folder_name}')
    elif load_type=="IL":
        df.write.mode("append").option('mergeSchema','true').format("delta").option("path",silver_zone_path+folder_name).saveAsTable(f'{silver_db}.{folder_name}')     
    else:
        print("Invalid load type") 
