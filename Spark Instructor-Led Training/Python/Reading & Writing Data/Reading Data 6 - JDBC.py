# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data - JDBC Connections
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC - Read Data from Relational Database

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Reading from JDBC
# MAGIC
# MAGIC Working with a JDBC data source is significantly different than any of the other data sources.
# MAGIC * Configuration settings can be a lot more complex.
# MAGIC * Often required to "register" the JDBC driver for the target database.
# MAGIC * We have to juggle the number of DB connections.
# MAGIC * We have to instruct Spark how to partition the data.
# MAGIC
# MAGIC **NOTE:** The database is read-only
# MAGIC * For security reasons. 
# MAGIC * The notebook does not demonstrate writing to a JDBC database. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * For examples of writing via JDBC, see 
# MAGIC   * <a href="https://docs.databricks.com/spark/latest/data-sources/sql-databases.html" target="_blank">Connecting to SQL Databases using JDBC</a>
# MAGIC   * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases" target="_blank">JDBC To Other Databases</a>

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Ensure that the driver class is loaded. 
# MAGIC // Seems to be necessary sometimes.
# MAGIC Class.forName("org.postgresql.Driver") 

# COMMAND ----------

tableName = "training.people_1m"
jdbcURL = "jdbc:postgresql://54.213.33.240/training"

# Username and Password w/read-only rights
connProperties = {
  "user" : "training",
  "password" : "training"
}

# And for some consistency in our test to come
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

exampleOneDF = spark.read.jdbc(
  url=jdbcURL,                # the JDBC URL
  table=tableName,            # the name of the table
  properties=connProperties)  # the connection properties

exampleOneDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Question:** Compared to CSV and even Parquet, what is missing here?
# MAGIC
# MAGIC **Question:** Based on the answer to the previous question, what are the ramifications of the missing...?
# MAGIC
# MAGIC **Question:** Before you run the next cell, what's your best guess as to the number of partitions?

# COMMAND ----------

print("Partitions: " + str(exampleOneDF.rdd.getNumPartitions()) )

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) That's not Parallelized
# MAGIC
# MAGIC Let's try this again, and this time we are going to increase the number connections to the database.
# MAGIC
# MAGIC ** *Note:* ** *If any one of these properties is specified, they must all be specified:*
# MAGIC * `partitionColumn` - the name of a column of an integral type that will be used for partitioning.
# MAGIC * `lowerBound` - the minimum value of columnName used to decide partition stride.
# MAGIC * `upperBound` - the maximum value of columnName used to decide partition stride
# MAGIC * `numPartitions` - the number of partitions/connections
# MAGIC
# MAGIC To quote the <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>:
# MAGIC > These options must all be specified if any of them is specified. They describe how to partition the table when reading in parallel from multiple workers. `partitionColumn` must be a numeric column from the table in question. Notice that `lowerBound` and `upperBound` are just used to decide the partition stride, not for filtering the rows in a table. So all rows in the table will be partitioned and returned. This option applies only to reading.

# COMMAND ----------

jdbcURL = "jdbc:postgresql://54.213.33.240/training"

exampleTwoDF = spark.read.jdbc(
  url=jdbcURL,                  # the JDBC URL
  table=tableName,              # the name of the table
  column="id",                  # the name of a column of an integral type that will be used for partitioning.
  lowerBound=1,                 # the minimum value of columnName used to decide partition stride.
  upperBound=200000,            # the maximum value of columnName used to decide partition stride
  numPartitions=8,              # the number of partitions/connections
  properties=connProperties)    # the connection properties

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start with checking how many partitions we have (it should be 8)

# COMMAND ----------

print("Partitions: " + str(exampleTwoDF.rdd.getNumPartitions()) )

# COMMAND ----------

# MAGIC %md
# MAGIC But how many records were loaded per partition?

# COMMAND ----------

# MAGIC %md
# MAGIC Using the utility method we created above we can print the per-partition count.

# COMMAND ----------

printRecordsPerPartition(exampleTwoDF)

# COMMAND ----------

# MAGIC %md
# MAGIC That might be a problem... notice how many records are in the last partition?
# MAGIC
# MAGIC **Question:** What is the performance ramifications of leaving our partitions like this?

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) That's Not [Well] Distributed
# MAGIC
# MAGIC And this is one of the little gotchas with working with JDBC - to properly specify the stride, we need to know the minimum and maximum value of the IDs.

# COMMAND ----------

from pyspark.sql.functions import *

minimumID = (exampleTwoDF
  .select(min("id"))   # Compute the minimum ID
  .first()["min(id)"]  # Extract as an integer
)
maximumID = (exampleTwoDF
  .select(max("id"))   # Compute the maximum ID
  .first()["max(id)"]  # Extract as an integer
)
print("Minimum ID: " + str(minimumID))
print("Maximum ID: " + str(maximumID))
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's try this one more time... this time with the proper stride:

# COMMAND ----------

exampleThree = spark.read.jdbc(
  url="jdbc:postgresql://54.213.33.240/training", # the JDBC URL
  table=tableName,                                # the name of the table
  column="id",                                    # the name of a column of an integral type that will be used for partitioning.
  lowerBound=minimumID,                           # the minimum value of columnName used to decide partition stride.
  upperBound=maximumID,                           # the maximum value of columnName used to decide partition stride
  numPartitions=8,                                # the number of partitions/connections
  properties=connProperties)                      # the connection properties

print("Partitions: " + str(exampleThree.rdd.getNumPartitions()) )
print("-"*80)
print("")

printRecordsPerPartition(exampleThree)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC And of course we can view that data here:

# COMMAND ----------

display(exampleThree)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC * [Reading Data #1 - CSV]($./Reading Data 1 - CSV)
# MAGIC * [Reading Data #2 - Parquet]($./Reading Data 2 - Parquet)
# MAGIC * [Reading Data #3 - Tables]($./Reading Data 3 - Tables)
# MAGIC * [Reading Data #4 - JSON]($./Reading Data 4 - JSON)
# MAGIC * [Reading Data #5 - Text]($./Reading Data 5 - Text)
# MAGIC * Reading Data #6 - JDBC
# MAGIC * [Reading Data #7 - Summary]($./Reading Data 7 - Summary)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
