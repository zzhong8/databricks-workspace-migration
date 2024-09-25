# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Intro To DataFrames, Lab #3
# MAGIC ## De-Duping Data

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Instructions
# MAGIC
# MAGIC In this exercise, we're doing ETL on a file we've received from some customer. That file contains data about people, including:
# MAGIC
# MAGIC * first, middle and last names
# MAGIC * gender
# MAGIC * birth date
# MAGIC * Social Security number
# MAGIC * salary
# MAGIC
# MAGIC But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:
# MAGIC
# MAGIC * In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL"). 
# MAGIC * The Social Security numbers aren't consistent, either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").
# MAGIC
# MAGIC The name fields are guaranteed to match, if you disregard character case, and the birth dates will also match. (The salaries will match, as well,
# MAGIC and the Social Security Numbers *would* match, if they were somehow put in the same format).
# MAGIC
# MAGIC The file's path is: `dbfs:/mnt/training/dataframes/people-with-dups.txt`
# MAGIC
# MAGIC Your job is to remove the duplicate records. The specific requirements of your job are:
# MAGIC
# MAGIC * Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
# MAGIC * Preserve the data format of the columns. That is, if you write the first name column in all lower-case, you haven't met this requirement.
# MAGIC * Write the result as a Parquet file, to `dbfs:/tmp/people.parquet`

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Hints
# MAGIC
# MAGIC * Use the <a href="http://spark.apache.org/docs/latest/api/python/index.html" target="_blank">API docs</a>. Specifically, you might find 
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame" target="_blank">DataFrame</a> and
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">functions</a> to be helpful.
# MAGIC * It's helpful to look at the file first, so you can check the format. `dbutils.fs.head()` (or just `%fs head`) is a big help here.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/training/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/training/dataframes/

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-with-dups.txt

# COMMAND ----------

# MAGIC %fs head /mnt/training/dataframes/people-with-dups.txt

# COMMAND ----------

# TODO
from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

sourceFile = "dbfs:/mnt/training/dataframes/people-with-dups.txt"

# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# I've already gone through the exercise to determine
# how many partitions I want and in this case it is...
partitions = 8

# Make sure wide operations don't repartition to 200
spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

# Create our initial DataFrame. We can let it infer the 
# schema because the cost for parquet files is really low.
initialDF = (spark.read
  .option("delimiter", ":")
  .option("header", "true")   # Use first line of all files as header# The default, but not costly w/Parquet
  .option("inferSchema", "true")  # Automatically infer data types
  .csv(sourceFile)          # Read the data in
  .repartition(partitions)       # From 7 >>> 8 partitions
  .cache()                       # Cache the expensive operation
)
# materialize the cache
initialDF.count()

# COMMAND ----------

initialDF.printSchema()

# COMMAND ----------

display(initialDF)

# COMMAND ----------

NameColumns_to_Format = ['firstName', 'middleName', 'lastName']

for NameColumn in NameColumns_to_Format:
    initialDF = initialDF.withColumn(NameColumn, pyf.lower(pyf.col(NameColumn)))\
                         .withColumn(NameColumn, pyf.initcap(pyf.col(NameColumn)))

formattedDF = initialDF.withColumn('ssn', pyf.regexp_replace('ssn', '-', ''))

finalDF = formattedDF.dropDuplicates()

finalDF.count()

# COMMAND ----------

display(finalDF)

# COMMAND ----------

print(userhome)

# COMMAND ----------

destFile = userhome + "/people.parquet"

# Save the data set
finalDF \
    .write.mode('overwrite').format('parquet') \
    .save(destFile)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
