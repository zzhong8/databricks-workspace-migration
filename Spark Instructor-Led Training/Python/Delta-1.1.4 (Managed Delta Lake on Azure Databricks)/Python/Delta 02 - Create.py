# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Batch Operations - Create Table
# MAGIC
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Work with a traditional data pipeline using online shopping data
# MAGIC * Identify problems with the traditional data pipeline
# MAGIC * Use Databricks Delta features to mitigate those problems
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analysts and Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC
# MAGIC ## Datasets Used
# MAGIC We will use online retail datasets from `/mnt/training/online_retail` 

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/s8bs0vhivz?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/s8bs0vhivz?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC You will notice that throughout this course, there is a lot of context switching between PySpark/Scala and SQL.
# MAGIC
# MAGIC This is because:
# MAGIC * `read` and `write` operations are performed on DataFrames using PySpark or Scala
# MAGIC * table creates and queries are performed directly off Databricks Delta tables using SQL
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls /mnt/

# COMMAND ----------

print(databaseName)

# COMMAND ----------

inputPath = "/mnt/training/online_retail/data-001/data.csv"

basePath         = userhome + "/delta/python"
genericDataPath  = basePath + "/generic-data/"
deltaDataPath    = basePath + "/customer-data/"
backfillDataPath = basePath + "/backfill-data/"

# Remove any old files
dbutils.fs.rm(basePath, True)
# Remove any temp tables
deleteTables(databaseName)

# Configure our shuffle partitions for these exercises
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###  READ CSV data then WRITE to Parquet / Databricks Delta
# MAGIC
# MAGIC Read the data into a DataFrame. We suppply the schema.
# MAGIC
# MAGIC Use overwrite mode so that it is not a problem to re-write data in case you end up running the cell again.
# MAGIC
# MAGIC Partition on `Country` because there are only a few unique countries and because we will use `Country` as a predicate in a `WHERE` clause.
# MAGIC
# MAGIC More information on the how and why of partitioning is contained in the links at the bottom of this notebook.
# MAGIC
# MAGIC Then write the data to Parquet and Databricks Delta.

# COMMAND ----------

inputSchema = "InvoiceNo INT, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"

rawDataDF = (spark.read 
    .option("header", "true")
    .schema(inputSchema)
    .csv(inputPath) 
)

# COMMAND ----------

# write to generic dataset
rawDataDF.write.mode("overwrite").format("parquet").partitionBy("Country").save(genericDataPath)

# COMMAND ----------

# write to delta dataset
rawDataDF.write.mode("overwrite").format("delta").partitionBy("Country").save(deltaDataPath)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### CREATE Using Non-Databricks Delta Pipeline
# MAGIC
# MAGIC Create a table called `customer_data` using `parquet` out of the above data.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we do not need to specify the schema and partition info!

# COMMAND ----------

spark.sql("""
    DROP TABLE IF EXISTS customer_data
  """)

spark.sql("""
    CREATE TABLE customer_data 
    USING parquet 
    OPTIONS (path = '{}')
  """.format(genericDataPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a simple `count` query to verify the number of records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Wait, no results? 
# MAGIC
# MAGIC What is going on here is a problem that stems from its Apache Hive origins.
# MAGIC
# MAGIC It's the concept of
# MAGIC <b>schema on read</b> where data is applied to a plan or schema as it is pulled out of a stored location, rather than as it goes into a stored location.
# MAGIC
# MAGIC This means that as soon as you put data into a data lake, the schema (and partition info) is unknown <i>until</i> you perform a read operation.
# MAGIC
# MAGIC To remedy, you repair the table using `MSCK REPAIR TABLE`.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Only after table repair is our count of customer data correct.
# MAGIC
# MAGIC Schema on read is explained in more detail <a href="https://stackoverflow.com/a/11764519/53495#" target="_blank">in this article</a>.

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE customer_data;
# MAGIC
# MAGIC SELECT count(*) FROM customer_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### CREATE Using Databricks Delta Pipeline
# MAGIC
# MAGIC Create a table called `customer_data_delta` using `DELTA` out of the above data.
# MAGIC
# MAGIC The notation is:
# MAGIC > `CREATE TABLE <table-name>` <br>
# MAGIC   `USING DELTA` <br>
# MAGIC   `LOCATION <path-do-data> ` <br>
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Since Delta stores schema (and partition) info in the `_delta_log` directory, we do not have to specify partition columns!

# COMMAND ----------

# spark.sql("""
#     DROP TABLE IF EXISTS customer_data
#   """)

# spark.sql("""
#     CREATE TABLE customer_data 
#     USING parquet 
#     OPTIONS (path = '{}')
#   """.format(genericDataPath))

# COMMAND ----------

spark.sql("""
    DROP TABLE IF EXISTS customer_data_delta
""")

spark.sql("""
    CREATE TABLE customer_data_delta 
    USING DELTA 
    LOCATION '{}' 
""".format(deltaDataPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Perform a simple `count` query to verify the number of records.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Notice how the count is right off the bat; no need to worry about table repairs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Metadata
# MAGIC
# MAGIC Since we already have data backing `customer_data_delta` in place, 
# MAGIC the table in the Hive metastore automatically inherits the schema, partitioning, 
# MAGIC and table properties of the existing data. 
# MAGIC
# MAGIC Note that we only store table name, path, database info in the Hive metastore,
# MAGIC the actual schema is stored in the `_delta_log` directory as shown below.

# COMMAND ----------

display(dbutils.fs.ls(deltaDataPath + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC Metadata is displayed through `DESCRIBE DETAIL <tableName>`.
# MAGIC
# MAGIC As long as we have some data in place already for a Databricks Delta table, we can infer schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC
# MAGIC Read data in `outdoorSmallPath`. Re-use `inputSchema` as defined above.

# COMMAND ----------

outdoorSmallPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"

backfillDF = (spark.read 
    .option("header", "true")
    .schema(inputSchema)
    .csv(outdoorSmallPath) 
)

# COMMAND ----------

backfillCount = backfillDF.count()

dbTest("Delta-02-schemas", 99999, backfillCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 2
# MAGIC
# MAGIC Create a Databricks Delta table `backfill_data_delta` backed by `backfillDataPath`.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** 
# MAGIC * Don't forget to use overwrite mode just in case
# MAGIC * Partititon by `Country`

# COMMAND ----------

print(backfillDataPath)

# COMMAND ----------

# write to delta dataset
backfillDF.write.mode("overwrite").format("delta").partitionBy("Country").save(backfillDataPath)

spark.sql("""
    DROP TABLE IF EXISTS backfill_data_delta
 """)

spark.sql("""
    CREATE TABLE backfill_data_delta 
    USING DELTA 
    LOCATION '{}' 
""".format(backfillDataPath))

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
    tableExists = (spark.table("backfill_data_delta") is not None)
except:
    tableExists = False
  
dbTest("Delta-02-backfillTableExists", True, tableExists)  

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC
# MAGIC Count number of records from `backfill_data_delta` where the `Country` is `Sweden`.

# COMMAND ----------

# TODO
count = spark.sql("SELECT COUNT(*) FROM backfill_data_delta WHERE Country == 'Sweden'").collect()[0][0]

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-L2-backfillDataDelta-count", 2925, count)
print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Using Databricks Delta to create tables is quite straightforward and you do not need to specify schemas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC
# MAGIC **Q:** What is the Databricks Delta command to display metadata?<br>
# MAGIC **A:** Metadata is displayed through `DESCRIBE DETAIL tableName`.
# MAGIC
# MAGIC **Q:** Where does the schema for a Databricks Delta data set reside?<br>
# MAGIC **A:** The table name, path, database info are stored in Hive metastore, the actual schema is stored in the `_delta_log` directory.
# MAGIC
# MAGIC **Q:** What is the general rule about partitioning and the cardinality of a set?<br>
# MAGIC **A:** We should partition on sets that are of small cardinality to avoid penalties incurred with managing large quantities of partition info meta-data.
# MAGIC
# MAGIC **Q:** What is schema-on-read?<br>
# MAGIC **A:** It stems from Hive and roughly means: the schema for a data set is unknown until you perform a read operation.
# MAGIC
# MAGIC **Q:** How does this problem manifest in Databricks assuming a `parquet` based data lake?<br>
# MAGIC **A:** It shows up as missing data upon load into a table in Databricks.
# MAGIC
# MAGIC **Q:** How do you remedy this problem in Databricks above?<br>
# MAGIC **A:** To remedy, you repair the table using `MSCK REPAIR TABLE` or switch to Databricks Delta!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Start the next lesson, [Append]($./Delta 03 - Append).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>
# MAGIC * <a href="https://en.wikipedia.org/wiki/Partition_(database)#" target="_blank">Database Partitioning</a>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
