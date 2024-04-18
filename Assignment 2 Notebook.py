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

# DBTITLE 1,fix error - double

VGSales = VGSales.withColumn("NA_Sales", col("NA_Sales").cast(DoubleType()))
VGSales = VGSales.withColumn("EU_Sales", col("EU_Sales").cast(DoubleType()))
VGSales = VGSales.withColumn("JP_Sales", col("JP_Sales").cast(DoubleType()))
VGSales = VGSales.withColumn("Other_Sales", col("Other_Sales").cast(DoubleType()))
VGSales = VGSales.withColumn("Global_Sales", col("Global_Sales").cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Overwrite
VGSales.write.mode("overwrite").option("header","true").csv("/mnt/a2data/silver/GameSales")

# COMMAND ----------

# DBTITLE 1,data frame
VGSales_df = spark.read.csv("/mnt/a2data/silver/GameSales", header=True)

# COMMAND ----------

VGSales_df.show()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# DBTITLE 1,Load as dataframe

VGSales_df = spark.read.format("csv") \
                  .option("header", "true") \
                  .option("inferSchema", "true") \
                  .load("/mnt/a2data/silver/GameSales")


VGSales_df.createOrReplaceTempView("temp_table")


# COMMAND ----------

# DBTITLE 1,Genre Query
genre_counts = VGSales_df.groupBy('Genre').count().withColumnRenamed('count', 'Count')

genre_counts.show()

# COMMAND ----------

# DBTITLE 1,Plotly
import plotly.express as px


# COMMAND ----------

# DBTITLE 1,Pandas DF
pandas_df = genre_counts.toPandas()

# COMMAND ----------

# DBTITLE 1,Genre Chart

genre_counts_df = VGSales_df.groupBy('Genre').count().withColumnRenamed('count', 'Count')

genre_counts_pandas = genre_counts_df.toPandas()

print(genre_counts_pandas)

fig = px.bar(genre_counts_pandas, x="Genre", y="Count")
fig.update_layout(width=900, height=600)
fig.show()

# COMMAND ----------

# DBTITLE 1,Sales Per Platform
from pyspark.sql.functions import sum as spark_sum

platform_counts_df = VGSales_df.groupby('Platform').agg(spark_sum('Global_Sales'))

platform_counts_pandas = platform_counts_df.toPandas()

print(platform_counts_pandas)

fig = px.bar(platform_counts_pandas, x="Platform", y="sum(Global_Sales)",labels={"sum(Global_Sales)": "Global Sales"})
fig.update_layout(width=900, height=600)
fig.show()

# COMMAND ----------

# DBTITLE 1,Top 100 - year of release
import plotly.express as px

top_100_ranks_df = VGSales_df.filter(VGSales_df['Rank'] <= 100)

top_100_ranks_df = top_100_ranks_df.withColumn("Year", top_100_ranks_df["Year"].cast("int"))

top_100_ranks_df = top_100_ranks_df.sort("Year")

top_100_ranks_pandas = top_100_ranks_df.toPandas()

fig = px.scatter(top_100_ranks_pandas, x="Year", y="Rank", labels={"Rank": "Top 100 Ranks"})
fig.update_layout(width=900, height=600)
fig.show()

# COMMAND ----------

# DBTITLE 1,Average sales per genre
from pyspark.sql.functions import avg as spark_avg

genre_avg_sales_df = VGSales_df.groupby('Genre').agg(spark_avg('Global_Sales').alias('Average_Global_Sales'))

genre_avg_sales_pandas = genre_avg_sales_df.toPandas()

print(genre_avg_sales_pandas)

fig = px.bar(genre_avg_sales_pandas, x="Genre", y="Average_Global_Sales", labels={"Average_Global_Sales": "Average Global Sales (Million)"})
fig.update_layout(width=900, height=600)
fig.show()

# COMMAND ----------

# DBTITLE 1,Sales Per Year
from pyspark.sql.functions import sum as spark_sum
import plotly.express as px

yearly_sales_df = VGSales_df.groupby('Year').agg(spark_sum('Global_Sales').alias('Total_Sales'))
yearly_sales_df = yearly_sales_df.orderBy('Year')

yearly_sales_pandas = yearly_sales_df.toPandas()

print(yearly_sales_pandas)

fig = px.line(yearly_sales_pandas, x="Year", y="Total_Sales", labels={"Total_Sales": "Total Sales (Million)"})
fig.update_layout(width=900, height=600)
fig.show()
