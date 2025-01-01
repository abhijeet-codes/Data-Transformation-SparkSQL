# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

#Access Base Parameter - "dbName"
dbutils.widgets.text("dbName","")
db_Name=dbutils.widgets.get("dbName")
print(db_Name)

# COMMAND ----------

silver_zone_path=f"/mnt/azure-bde-project/SilverZone/{db_Name}/"
gold_zone_path=f"/mnt/azure-bde-project/GoldZone/{db_Name}/"

# COMMAND ----------

db_prefix='gold_db_'
gold_db=db_prefix+db_Name
print(gold_db)

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {gold_db}')
spark.sql(f'USE {gold_db}')

# COMMAND ----------

if db_Name == "olympicstokyo":
    # 1.Find the top countries with the highest number of gold medals
    df_medals=spark.read.format('delta').load(silver_zone_path+"Medals")

    df_topcountriesGold = df_medals.orderBy("Gold",ascending=False).select('Team_Country','Gold')
    df_topcountriesGold = df_topcountriesGold.withColumn("Gold", when(col("Gold").isNull(), -1).otherwise(col("Gold")))
    display(df_topcountriesGold)

    df_topcountriesGold.write.mode('append').format('delta').option('path',f'{gold_zone_path}TopCountriesGold').saveAsTable(f'{gold_db}.top_countries_gold')


    # 2.Calculate the average number of entries by gender for each discipline
    df_entriesGender=spark.read.format('delta').load(silver_zone_path+"EntriesGender")

    df_averageEntriesByGender=df_entriesGender.withColumn("Avg_Female",df_entriesGender['Female']/df_entriesGender['Total']).withColumn('Avg_Male',df_entriesGender['Male']/df_entriesGender['Total'])
    display(df_averageEntriesByGender)

    df_averageEntriesByGender.write.mode('append').format('delta').option('path',f'{gold_zone_path}AverageEntriesByGender').saveAsTable(f'{gold_db}.average_entries_by_gender')


    # 3.Number of participants in each country -- Athletes
    df_athletes=spark.read.format('delta').load(silver_zone_path+"Athletes")

    df_participantsPerCountry = df_athletes.groupBy('Country').agg(count('PersonName').alias('NumberOfParticipants')).orderBy('Country')
    display(df_participantsPerCountry)

    df_participantsPerCountry.write.mode('append').format('delta').option('path',f'{gold_zone_path}ParticipantsPerCountry').saveAsTable(f'{gold_db}.participants_per_country')


    # 4.Number of Discipline in the olympiad -- Athletes
    df_athletes=spark.read.format('delta').load(silver_zone_path+"Athletes")

    df_numberOfDisciplines=df_athletes.select('Discipline').distinct().orderBy('Discipline')
    display(df_numberOfDisciplines)

    df_numberOfDisciplines.write.mode('append').format('delta').option('path',f'{gold_zone_path}NumberOfDisciplines').saveAsTable(f'{gold_db}.number_of_disciplines')


    # 5.Number of Coaches in each country -- Coaches
    df_coaches=spark.read.format('delta').load(silver_zone_path+"Coaches")

    df_coachesPerCountry=df_coaches.groupBy("Country").agg(count("Name").alias("NumberOfCoaches")).orderBy("Country")
    display(df_coachesPerCountry)

    df_coachesPerCountry.write.mode('append').format('delta').option('path',f'{gold_zone_path}CoachesPerCountry').saveAsTable(f'{gold_db}.coaches_per_country')


    # 6.Number of Medals in each country
    df_medals=spark.read.format('delta').load(silver_zone_path+"Medals")

    df_medalsPerCountry=df_medals.select('Team_Country','Total').orderBy("Team_Country")
    display(df_medalsPerCountry)

    df_medalsPerCountry.write.mode('append').format('delta').option('path',f'{gold_zone_path}MedalsPerCountry').saveAsTable(f'{gold_db}.medals_per_country')


    # 7.Number of participants in each Discipline
    df_athletes=spark.read.format('delta').load(silver_zone_path+"Athletes")

    df_participantsPerDiscipline = df_athletes.groupBy('Discipline').agg(count('PersonName').alias('NumberOfParticipants')).orderBy('Discipline')
    display(df_participantsPerDiscipline)

    df_participantsPerDiscipline.write.mode('append').format('delta').option('path',f'{gold_zone_path}ParticipantsPerDiscipline').saveAsTable(f'{gold_db}.participants_per_discipline')


    # 8.Number of Male and Female in each Discipline
    df_entriesGender=spark.read.format('delta').load(silver_zone_path+"EntriesGender")

    df_maleAndfemalePerDiscipline=df_entriesGender.select("Discipline","Female","Male").orderBy("Discipline")
    display(df_maleAndfemalePerDiscipline)

    df_maleAndfemalePerDiscipline.write.mode('append').format('delta').option('path',f'{gold_zone_path}MaleAndFemalePerDiscipline').saveAsTable(f'{gold_db}.male_and_female_per_discipline')

elif db_Name == 'GlobalAirlines': 
    print('Apply Specific Transformations')    
    
else:
    print('Invalid database name')
