# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Review Questions
# MAGIC The following questions are intended to be reviewed the following day and are simply suggestions based on the material covered in the corresponding notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Programming
# MAGIC * What is Apache Spark?
# MAGIC * Who is Databricks?
# MAGIC * What environment can Spark run on?
# MAGIC * Which environment does Spark run best on?
# MAGIC * What is the relationship between a shard and a cluster?
# MAGIC * What is a Cluster Manager?
# MAGIC * Why did we have to create a new cluster (next day)?
# MAGIC * Where do I go to ask really obscure and off-topic questions?
# MAGIC * What is the relationship between a core, thread & slot
# MAGIC * What is the relationship between a job & a stage
# MAGIC * RDDs are the a??????? l??????? of the DataFrames API

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Databricks Environment
# MAGIC * What is the DBFS (Databricks File System)?
# MAGIC * What is the name of the utility class for interacting with the DBFS?
# MAGIC * What does the `display()` command do?
# MAGIC * How many "magic" commands did we cover?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Data
# MAGIC * What does it mean to partition data?
# MAGIC * How do the `DataFrameReader`s decide how to partition data?
# MAGIC * Which technique for reading data provided control over the number of partitions being created & what was so special about it?
# MAGIC * What would be the danger with creating 1,000, 10,000 or even 100,000 partitions from a JDBC data source?
# MAGIC * What is the side effect of infering the schema when reading in CSV or JSON data?
# MAGIC * Why is the Parquet file format better than CSV, JSON, ORC, JDBC?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Introduction to DataFrames, Part #1
# MAGIC * In Spark 2.x, what is the name of the class for the "entry point" for the DataFrames API.
# MAGIC * In the Scala API, what class do I need to search for in order to load the DataFrames API docs?
# MAGIC * What is the difference between a `DataFrame` and a `DataSet[Row]`?
# MAGIC * What operations did we cover so far?
# MAGIC * DataFrame operations generally fall into one of two categories. 
# MAGIC   * What category do operations like `limit()`, `select()`, `drop()` and `distinct()` fall into?
# MAGIC   * What category do operations like `show()`, `display()` and `count()` fall into?
# MAGIC * Which type of operation **always** triggers a job?
# MAGIC * In what case **might** a transformation trigger a job?
# MAGIC * What is the difference between `display()` and `show()`?
# MAGIC * What is the difference between `distinct()` and `dropDuplicates()`?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Transformations & Actions
# MAGIC * What set of operations are considered "lazy"?
# MAGIC * What set of operations are considered "eager"?
# MAGIC * Why do transformations not trigger a job (aside from some of the initial read operations)?
# MAGIC * What bennifits do a lazy execution aford us?
# MAGIC * What is the name of the entity responsible for optimizing our DataFrames and SQL and converting them into RDDs?
# MAGIC * What is the difference between a wide and narrow transformation?
# MAGIC * What triggers a shuffle operation?
# MAGIC * When are stages created?
# MAGIC * Can we begin executing tasks in Stage #2 if other tasks on other executors are still running in Stage #1?
# MAGIC * Why must we wait for all operations in a given stage to complete before moving on to the next stage?
# MAGIC * What is the term used for the scenario when data is read into RAM and several transformations are sequentially executed against that data?
# MAGIC * How is Pipelining in Apache Spark different when processing transformations compared to MapReduce?
# MAGIC * Describe how Apache Spark can determine that certain stages can be skipped when re-executing a query?
# MAGIC * Where in the Spark UI can I see how long it took to execute a task?
# MAGIC * Where in the Spark UI can I see if my data is evenly distributed amongst each partition?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Introduction to DataFrames, Part #2
# MAGIC * What are some of the transformations we reviewed yesterday?
# MAGIC * What are some of the actions we reviewed yesterday?
# MAGIC * What two new classes did we discuss yesterday?
# MAGIC   * Hint: The first class is used in methods like `filter()` and `where()`
# MAGIC   * Hint: The second method is the object backing every row of a `DataFrame`.
# MAGIC * What potential problem might I run into by calling `collect()`?
# MAGIC * What type of object does `collect()` and `take(n)` return?
# MAGIC * What type of object does `head()` and `first()` return?
# MAGIC * What are some of the operations we can perform on a `Column` object?
# MAGIC * With the Scala API, how do you access the 3rd column of a `DataFrame`'s `Row` object which happens to be a `Boolean` value?
# MAGIC * What happens if you do not use the correct accessor method on a `Row` object?
# MAGIC   * Hint: What happens if you ask for a `Boolean` when the backing object is a `String`?
# MAGIC * Why don't we have these problems with the Scala API?
# MAGIC * What is different about the transformations `orderBy(String)` and `filter(String)`?
# MAGIC * What methods do you call on a `Column` object to test for equality, in-equality and null-safe equality?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Partitioning
# MAGIC * On a non-autoscaling cluster, what is the magic ration between slots and partitions?
# MAGIC * If you have 125 partitions and 200 cores, what type of problems might you run into?
# MAGIC * What about 101 partitions and 100 cores?
# MAGIC * How do I find out how many cores my clsuter has?
# MAGIC * What is the recomended size in MB for each partition (in RAM, i.e. cached)?
# MAGIC * If I need to increase the number of partions to match my cores, should I use `repartition(n)` or `coalesce(n)`?
# MAGIC * What side affect might I run into by using `coalesce(n)`?
# MAGIC * Which of the two operations is a wide transformation, `repartition(n)` or `coalesce(n)`?
# MAGIC * What is the significance of the configuration setting `spark.sql.shuffle.partitions`?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Introduction to DataFrames, Part #3
# MAGIC * What is the name of the class returned by the `groupBy()` transformation?
# MAGIC   * For Python: `GroupedData`
# MAGIC   * For Scala: `RelationalGroupedDataset`
# MAGIC * What are some of the aggregate functions available for a `groupBy()` operation?
# MAGIC * With the various aggregate functions for `groubBy()`, what happens if you do not specify the column to `sum(..)`, for example?
# MAGIC * How many different ways can you think of to rename/add a column?
# MAGIC   * `withColumn()`
# MAGIC   * `select()`
# MAGIC   * `as()`
# MAGIC   * `alias()`
# MAGIC   * `withColumnRenamed()`
# MAGIC * What are some of the `...sql.functions` methods we saw yesterday?
# MAGIC   * `unix_timestamp()`
# MAGIC   * `cast()`
# MAGIC   * `year()`
# MAGIC   * `month()`
# MAGIC   * `dayofyear()`
# MAGIC   * `sum()`
# MAGIC   * `count()`
# MAGIC   * `avg()`
# MAGIC   * `min()`
# MAGIC   * `max()`
# MAGIC * In Python, can I use the `as()` to rename a column?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Caching
# MAGIC * Should I cache data?
# MAGIC * Why might it be better to convert a CSV file to Parquet instead of caching it?
# MAGIC * What is the default storage level for the `DataFrame` API?
# MAGIC * What is the default storage level for the `RDD` API?
# MAGIC * When would I use the storage level `DISK_ONLY`?
# MAGIC * When would I use any of the `XXXX_2` storage levels?
# MAGIC * When would I use any of the `XXXX_SER` storage levels?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Introduction to DataFrames, Part #4
# MAGIC * What is a UDF?
# MAGIC * Whare are some the gotcha's with UDFs?
# MAGIC   * Cannot be optimized
# MAGIC   * Have to be careful about serialization
# MAGIC   * Slower than built-in functions
# MAGIC   * There is almost always a built in function that alrady does what you want.
# MAGIC * How do you avoid problems with UDFs?
# MAGIC * How does the performance with UDFs compare with Python vs Scala?
# MAGIC * How is a join operation in the DataFrames API different than in MySQL or Oracle?
# MAGIC * Is the join operation wide or narrow?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Broadcasting
# MAGIC * What is a broadcast join?
# MAGIC * What is the threshold for automatically triggering a broadcast join (at least on Databricks)
# MAGIC * How do we change the threshold?
# MAGIC * spark.sql.autoBroadcastJoinThreshold

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Catalyst Optimizer
# MAGIC * Does it matter which language I use?
# MAGIC * Does it matter which API I use? SQL / DataFrame / Dataset?
# MAGIC * What is the difference between the logical and physical plans?
# MAGIC * Why does the Catalyst Opimizer produce multiple physical models?
# MAGIC * What is the last step/stage for the Catalyst Optimizer?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
