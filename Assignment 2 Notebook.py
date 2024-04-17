# Databricks notebook source
from pyspark.dbutils import DBUtils

# COMMAND ----------

# DBTITLE 1,Mount Azure Storage
dbutils.fs.mount(
    source='wasbs://a2data@assignment2de.blob.core.windows.net',
    mount_point='/mnt/a2data',
    extra_configs={
        'fs.azure.account.key.assignment2de.blob.core.windows.net': dbutils.secrets.get('assignment2secretscope', 'storageKey')}
)

# COMMAND ----------

# DBTITLE 1,Check inside a2data
# MAGIC %fs
# MAGIC ls "/mnt/a2data"

# COMMAND ----------

# DBTITLE 1,read VGSales
VGSales = spark.read.format("csv").load("/mnt/a2data/bronze/VGSales.csv")

# COMMAND ----------

# DBTITLE 1,show VGSales
VGSales.show()

# COMMAND ----------

# DBTITLE 1,make headers
VGSales = spark.read.format("csv").option("header","true").load("/mnt/a2data/bronze/VGSales.csv")

# COMMAND ----------

# DBTITLE 1,show new table
VGSales.show()

# COMMAND ----------

# DBTITLE 1,Print Schema
VGSales.printSchema()

# COMMAND ----------

# DBTITLE 1,function and type import
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType


# COMMAND ----------

# DBTITLE 1,New DataTypes
VGSales = VGSales.withColumn("Rank", col("Rank").cast(IntegerType()))
VGSales = VGSales.withColumn("NA_Sales", col("NA_Sales").cast(IntegerType()))
VGSales = VGSales.withColumn("EU_Sales", col("EU_Sales").cast(IntegerType()))
VGSales = VGSales.withColumn("JP_Sales", col("JP_Sales").cast(IntegerType()))
VGSales = VGSales.withColumn("Other_Sales", col("Other_Sales").cast(IntegerType()))
VGSales = VGSales.withColumn("Global_Sales", col("Global_Sales").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Top 20 rank
all_games_rank_highest = VGSales.orderBy("Rank", ascending=True).limit(20).show()

# COMMAND ----------

# DBTITLE 1,Move transformed data to Azure
VGSales.write.option("header","true").csv("/mnt/a2data/silver/GameSales")

# COMMAND ----------

# DBTITLE 1,Fix date
from pyspark.sql.functions import col, date_format

VGSales = VGSales.withColumn("year", date_format(col("year"), "yyyy"))

# COMMAND ----------

# DBTITLE 1,Overwrite
VGSales.write.mode("overwrite").option("header","true").csv("/mnt/a2data/silver/GameSales")

# COMMAND ----------


