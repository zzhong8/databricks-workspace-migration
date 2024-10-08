// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Caching
// MAGIC
// MAGIC **Technical Accomplishments:**
// MAGIC * Understand how caching works
// MAGIC * Explore the different caching mechanisims
// MAGIC * Discuss tips for the best use of the cache

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) A Fresh Start
// MAGIC For this section, we need to clear the existing cache.
// MAGIC
// MAGIC There are several ways to accomplish this:
// MAGIC   * Remove each cache one-by-one, fairly problematic
// MAGIC   * Restart the cluster - takes a fair while to come back online
// MAGIC   * Just blow the entire cache away - this will affect every user on the cluster!!

// COMMAND ----------

// !!! DO NOT RUN THIS ON A SHARED CLUSTER !!!
// YOU WILL CLEAR YOUR CACHE AND YOUR COWORKER'S

// spark.catalog.clearCache()

// COMMAND ----------

// MAGIC %md
// MAGIC This will ensure that any caches produced by other labs/notebooks will be removed.
// MAGIC
// MAGIC Next, open the **Spark UI** and go to the **Storage** tab - it should be empty.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) The Data Source
// MAGIC
// MAGIC This data uses the **Pageviews By Seconds** data set.
// MAGIC
// MAGIC The parquet files are located on the DBFS at **dbfs:/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet**.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

val pageviewsDF = spark.read
  .option("header", "true")
  .option("delimiter", "\t")
  .schema(schema)
  .csv(fileName)

// COMMAND ----------

// MAGIC %md
// MAGIC The 255 MB pageviews file is currently in our object store, which means each time you scan through it, your Spark cluster has to read the 255 MB of data remotely over the network.

// COMMAND ----------

// MAGIC %md
// MAGIC Once again, use the `count()` action to scan the entire 255 MB file from disk and count how many total records (rows) there are:

// COMMAND ----------

val total = pageviewsDF.count()

printf("Record Count: %,d%n%n", total)

// COMMAND ----------

// MAGIC %md
// MAGIC The pageviews DataFrame contains 7.2 million rows.
// MAGIC
// MAGIC Make a note of how long the previous operation takes.
// MAGIC
// MAGIC Re-run it several times trying to establish an average.
// MAGIC
// MAGIC Let's try a slightly more complicated operation, such as sorting, which induces an "expensive" shuffle.

// COMMAND ----------

pageviewsDF
  .orderBy("requests")
  .count()

// COMMAND ----------

// MAGIC %md
// MAGIC Again, make note of how long the operation takes.
// MAGIC
// MAGIC Rerun it several times to get an average.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Every time we re-run these operations, it goes all the way back to the original data store.
// MAGIC
// MAGIC This requires pulling all the data across the network for every execution.
// MAGIC
// MAGIC In many/most cases, this network IO is the most expensive part of a job.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) cache()
// MAGIC
// MAGIC We can avoid all of this overhead by caching the data on the executors.
// MAGIC
// MAGIC Go ahead and run the following command.
// MAGIC
// MAGIC Make note of how long it takes to execute.

// COMMAND ----------

pageviewsDF.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC The `cache(..)` operation doesn't do anything other than mark a `DataFrame` as cacheable.
// MAGIC
// MAGIC And while it does return an instance of `DataFrame` it is not technically a transformation or action
// MAGIC
// MAGIC In order to actually cache the data, Spark has to process over every single record.
// MAGIC
// MAGIC As Spark processes every record, the cache will be materialized.
// MAGIC
// MAGIC A very common method for materializing the cache is to execute a `count()`.
// MAGIC
// MAGIC **BUT BEFORE YOU DO** Check the **Spark UI** to make sure it's still empty even after calling `cache()`.

// COMMAND ----------

pageviewsDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC The last `count()` will take a little longer than normal.
// MAGIC
// MAGIC It has to perform the cache and do the work of materializing the cache.
// MAGIC
// MAGIC Now that `pageviewsDF` is cached **AND** the cache has been materialized.
// MAGIC
// MAGIC Before we rerun our queries, check the **Spark UI** and the **Storage** tab.
// MAGIC
// MAGIC Now, run the two queries and compare their execution time to the ones above.

// COMMAND ----------

pageviewsDF.count()

// COMMAND ----------

pageviewsDF
  .orderBy("requests")
  .count()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Faster, right?
// MAGIC
// MAGIC All of our data is being stored in RAM on the executors.
// MAGIC
// MAGIC We are no longer making network calls.
// MAGIC
// MAGIC Our plain `count()` should be sub-second.
// MAGIC
// MAGIC Our `orderBy(..)` & `count()` should be around 3 seconds.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Spark UI - Storage

// COMMAND ----------

// MAGIC %md
// MAGIC Now that the pageviews `DataFrame` is cached in memory let's go review the **Spark UI** in more detail.
// MAGIC
// MAGIC In the **RDDs** table, you should see only one record - multiple if you reran the `cache()` operation.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's review the **Spark UI**'s **Storage** details
// MAGIC * RDD Name
// MAGIC * Storage Level
// MAGIC * Cached Partitions
// MAGIC * Fraction Cached
// MAGIC * Size in Memory
// MAGIC * Size on Disk

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's dig deeper into the storage details...
// MAGIC
// MAGIC Click on the link in the **RDD Name** column to open the **RDD Storage Info**.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's review the **RDD Storage Info**
// MAGIC * Size in Memory
// MAGIC * Size on Disk
// MAGIC * Executors
// MAGIC
// MAGIC If you recall...
// MAGIC * We should have 8 partitions.
// MAGIC * With 255MB of data divided into 8 partitions...
// MAGIC * The first seven partitions should be 32MB each.
// MAGIC * The last partition will be significantly smaller than the others.
// MAGIC
// MAGIC **Question:** Why is the **Size in Memory** nowhere near 32MB?
// MAGIC
// MAGIC **Question:** What is the difference between **Size in Memory** and **Size on Disk**?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) persist()
// MAGIC
// MAGIC `cache()` is just an alias for `persist()`
// MAGIC
// MAGIC Let's take a look at the API docs for
// MAGIC * `Dataset.persist(..)` if using Scala
// MAGIC * `DataFrame.persist(..)` if using Python
// MAGIC
// MAGIC `persist()` allows one to specify an additional parameter (storage level) indicating how the data is cached:
// MAGIC * DISK_ONLY
// MAGIC * DISK_ONLY_2
// MAGIC * MEMORY_AND_DISK
// MAGIC * MEMORY_AND_DISK_2
// MAGIC * MEMORY_AND_DISK_SER
// MAGIC * MEMORY_AND_DISK_SER_2
// MAGIC * MEMORY_ONLY
// MAGIC * MEMORY_ONLY_2
// MAGIC * MEMORY_ONLY_SER
// MAGIC * MEMORY_ONLY_SER_2
// MAGIC * OFF_HEAP
// MAGIC
// MAGIC ** *Note:* ** *The default storage level for...*
// MAGIC * *RDDs are **MEMORY_ONLY**.*
// MAGIC * *DataFrames are **MEMORY_AND_DISK**.* 
// MAGIC * *Streaming is **MEMORY_AND_DISK_2**.*

// COMMAND ----------

// MAGIC %md
// MAGIC Before we can use the various storage levels, it's necessary to import the enumerations...

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

// COMMAND ----------

// MAGIC %md
// MAGIC **Question:** How do we purge data from the cache?
// MAGIC
// MAGIC `unpersist(..)` or `uncache()`?
// MAGIC
// MAGIC Try it...

// COMMAND ----------

// pageviewsDF.uncache()
// pageviewsDF.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC Real quick, go check the **Storage** tab in the **Spark UI** and confirm that the cache has been expunged.

// COMMAND ----------

// MAGIC %md
// MAGIC **Question:** What will happen if you take 75% of the cache and then I come along and try to use %50 (of the total)...
// MAGIC * with **MEMORY_ONLY**?
// MAGIC * with **MEMORY_AND_DISK**?
// MAGIC * with **DISK_ONLY**?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) RDD Name
// MAGIC
// MAGIC If you haven't noticed yet, the **RDD Name** on the **Storage** tab in the **Spark UI** is a big ugly name.
// MAGIC
// MAGIC It's a bit hacky, but there is a workaround for assigning a name.
// MAGIC 0. Create your `DataFrame`.
// MAGIC 0. From that `DataFrame`, create a temporary view with your desired name.
// MAGIC 0. Specifically, cache the table via the `SparkSession` and its `Catalog`.
// MAGIC 0. Materialize the cache.

// COMMAND ----------

pageviewsDF.unpersist()

pageviewsDF.createOrReplaceTempView("Pageviews_DF_Scala")
spark.catalog.cacheTable("Pageviews_DF_Scala")

pageviewsDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC And now to clean up after ourselves...

// COMMAND ----------

pageviewsDF.unpersist()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
