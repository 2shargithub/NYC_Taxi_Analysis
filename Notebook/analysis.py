# Databricks notebook source
from pyspark.sql.functions import mean,median,mode, hour, col, count, dayofweek, corr, when
from pyspark.sql.types import DoubleType

# COMMAND ----------

# Define file path and table name
file_path = "dbfs:/FileStore/tables/yellow_tripdata_2024_09.parquet"
table_name = "nyctaxi_data"

# Create the table using Spark SQL
spark.sql(f"""
    CREATE TABLE {table_name}
    USING PARQUET
    LOCATION '{file_path}'
""")

# Read the table
df = spark.table(table_name)




# COMMAND ----------

# Display data
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.nyctaxi_data limit 10

# COMMAND ----------

df.toPandas()

# COMMAND ----------

#Pre-process the data

for i in df.columns:
    print(f"{i}: {df.filter(col(i).isNull()).count()}")

# COMMAND ----------

#Drop columns with 90% NULL values

n = df.count()
print(n)

for i in df.columns:
    x = df.filter(col(i).isNull()).count()
    if((x/n)*100 > 90):
        df = df.drop(i)
        print(f"{i} has been dropped {(x/n)*100}")


# COMMAND ----------

for i in df.columns:
    x = df.select(mode(i).alias("nn")).head()[0]
    
    if df.filter(col(i).isNull()).count() > 0:
        df = df.fillna({i:x})

# COMMAND ----------

for i in df.columns:
    print(f"{i}: {df.filter(col(i).isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA ANALYSIS

# COMMAND ----------

# How many trips recorded

df.count()

# COMMAND ----------

#Unique value of all columns

for i in df.columns:
    df.select(i).distinct().show()

# COMMAND ----------

df.columns

# COMMAND ----------

df.select(col("trip_distance")).show()

# COMMAND ----------

#Average Trip Distance

df.select(mean("trip_distance").alias("Average_trip_distancee")).show()

# COMMAND ----------

#Top 5 Pickup Location

df.groupBy("PULocationID").count().orderBy(col("count").desc()).limit(5).show()

# COMMAND ----------

# Top 5 Drop Location

df.groupBy("DOLocationID").count().orderBy(col("count").desc()).limit(5).show()

# COMMAND ----------

#Peak Hours

df.groupBy(hour("tpep_pickup_datetime").alias("PickupHours")).count().orderBy(col("count").desc()).limit(5).show()

# COMMAND ----------

# mode of payments
df.groupBy("payment_type").count().orderBy("payment_type").show()

# COMMAND ----------

#Average fare amount

df = df.withColumn("totalamount",col("total_amount").cast(DoubleType()))
df.groupBy("VendorID").mean("total_amount").show()

# COMMAND ----------

df.groupBy(col("passenger_count").alias("No of passenger")).count().orderBy(col("count").desc()).show()
