// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Tuning Shuffling

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## When does shuffle come into play?
// MAGIC - When we transfer data from one stage to the next
// MAGIC - May cause repartitioning
// MAGIC - Possible network traffic (very expensive)
// MAGIC
// MAGIC ## Wide vs Narrow transformations
// MAGIC
// MAGIC ### Narrow transformations
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/transformations-narrow.png" alt="Narrow Transformations" style="height: 300px"/>
// MAGIC
// MAGIC Narrow transformations can be pipelined together into one stage
// MAGIC
// MAGIC ### Wide transformations
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/transformations-wide.png" alt="Wide Transformations" style="height: 300px"/>
// MAGIC
// MAGIC - Wide transformations cause shuffling as they introduce stage splits.
// MAGIC - Some wide transformations we can perform on a DataFrame: `distinct`, `cube`, `join`, `orderBy`, `groupBy`.

// COMMAND ----------

val employees = spark.createDataFrame(List((3, "Jack"), (11, "Lynn"), (16 , "Venn"))).toDF("id", "name")
val customers = spark.createDataFrame(List((0, "Venn"), (1, "Lola"), (12 ,"Britney"))).toDF("id", "name")

println("Unique names: " + employees.join(customers, Seq("name"), "outer").select("name").distinct.count())

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Size of shuffled data between stages was small, but we can still do better! 
// MAGIC - The join causes a stage split that can be avoided for datasets of similar columns, specifically column numbers. 
// MAGIC - We will avoid the join by instead using a union.

// COMMAND ----------

println("Unique names: " + employees.union(customers).select("name").distinct.count())

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Notice that using a join required a lot more stages. Also the **overall** volume of data written to the shuffle buffer was significantly larger.
// MAGIC
// MAGIC - Because `union` is a narrow transformation, it avoided a stage split.
// MAGIC - `union` can merge two DataFrames with identical columns. `join` can join them by matching different columns.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ####Speedups
// MAGIC
// MAGIC - cache intermediate data
// MAGIC - control parallelism on wide transformations.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Settings involved in shuffling 
// MAGIC `spark.default.parallelism`: Default parallelism used when partitioning RDDs after wide transformations or when using `parallelize`.
// MAGIC For distributed shuffle operations like `reduceByKey` (RDD API) and `join`, this value defines the largest number of partitions 
// MAGIC in a parent RDD. For operations like `parallelize` with no parent RDDs, it depends on the cluster manager:
// MAGIC
// MAGIC - Local mode: number of cores on the local machine
// MAGIC - Mesos fine grained mode: 8
// MAGIC - Others: total number of cores on all executor nodes or 2, whichever is larger
// MAGIC
// MAGIC This value also plays a part in the initial partitioning of a DataFrame.
// MAGIC
// MAGIC It can be difficult to set at runtime. Note that changing the value of the configuration parameter has no effect, below.

// COMMAND ----------

println(spark.sparkContext.parallelize(1 to 50).getNumPartitions + " " + spark.sparkContext.defaultParallelism)
spark.conf.set("spark.default.parallelism", "3")
println(spark.sparkContext.parallelize(50 to 1 by -1).getNumPartitions + " " + spark.sparkContext.defaultParallelism)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC - `spark.io.compression.codec` - Algorithm used to compress internal data including shuffle data (default LZ4).
// MAGIC - `spark.local.dir` - Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored on disk.
// MAGIC     - Preferably fast storage, SSD / high performance disks.
// MAGIC     - Use more than 1 Disk / SSD to avoid IO bottleneck (can be csv list of paths)
// MAGIC     - Overridden by environment variables: `SPARK_LOCAL_DIRS` in standalone / Mesos and `LOCAL_DIRS` in YARN.
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/tuning/partition-agg-spill-to-disk.png" alt="Spill to disk"/><br/><br/>    
// MAGIC
// MAGIC - `spark.reducer.maxSizeInFlight` - Maximum size of map outputs to fetch simultaneously from each reduce task. (default 48M)
// MAGIC     - Keep low for clusters with limited memory 
// MAGIC - `spark.reducer.maxReqsInFlight` - Limits the number of remote requests to fetch blocks at any given point. (default Int.MaxValue)
// MAGIC     - As cluster size grows, the increasing number of in-bound connections can lead to executors failing under load, reducing this setting can help mitigate failures.
// MAGIC - `spark.shuffle.compress` - Whether to compress map output files, less memory overhead vs more processing (default yes).
// MAGIC - `spark.shuffle.sort.bypassMergeThreshold` - In the sort-based shuffle manager, avoid merge-sorting data if there is no map-side aggregation and there are at most this many reduce partitions. 
// MAGIC     - For small numbers of reducers hashing to separate files and then joining these files would work faster. ([Impl. in BypassMergeSortShuffleWriter](https://github.com/apache/spark/blob/master/core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java))
// MAGIC - `spark.sql.files.maxPartitionBytes` - The maximum number of bytes to pack into a single partition when reading files (default 128MB)
// MAGIC     - We can control partitioning by reducing max size of each partition allowed
// MAGIC     - `fileSize / maxPartitionSize` has to be higher than `spark.sparkContext.defaultParallelism`
// MAGIC     
// MAGIC Let's read in a CSV file of approximate size 2GB and modify the size we want per partition.

// COMMAND ----------

// We're working with MB.
val fileSize = 2 * Math.pow(1024, 3)
// At the very least 1 whole partition is required.
val partitionByBytes = Math.ceil(fileSize / spark.conf.get("spark.sql.files.maxPartitionBytes").toDouble).toInt

// https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/input/PortableDataStream.scala#L54
val partitionSize = Math.max(partitionByBytes, spark.sparkContext.defaultParallelism)
println(s"numb partitions: $partitionSize")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC - `spark.sql.shuffle.partitions` - Configures the number of partitions to use when shuffling data for joins or aggregations (default 200)
// MAGIC     - 200 Might be high for small clusters. 
// MAGIC     - Redundant tasks being created are on stand-by until a core is available for execution.
// MAGIC     - Look at length of task execution. Depends on hardware / size of partition. Ideally between 50ms - 200ms.
// MAGIC     - Cluster sizing and resource allocation is hard, benchmark!
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/tuning/overallocation.png" alt="over-allocation of partitioning" style="width: 800px;"/><br/>    

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Tuning Shuffle Exercise
// MAGIC
// MAGIC This lab walks us through a scenario where carrying out filtering and controlling partitioning can greatly help reduce network traffic involved with the processing carried out. Less network traffic also means we're pulling smaller volumes of data into spark thus we carry out less processing. One way we can see how much less traffic is actually generated is through to the spark UI. Lets get started by creating two DataFrames.

// COMMAND ----------

val names = spark.read.parquet("dbfs:/mnt/training/ssn/names.parquet")
val people = spark.read.option("delimiter", ":")
                       .option("inferSchema", "true")
                       .option("header", "true")
                       .csv("dbfs:/mnt/training/dataframes/people-with-header-100k.txt")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Lets see how many unique entries we have. The below command will carry out a distinct transformation across all the rows. <br/>
// MAGIC It can be rather slow (more than 1 minute).

// COMMAND ----------

names.join(people, names("firstName") === people("firstName")).distinct.count

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC There are a number of incorrect things going on in the previous line:
// MAGIC  - First is that using `distinct` in this manner won't show us unique  entries for individuals, but rather number of unique rows.
// MAGIC  - Joining the entirety of both DataFrames to count unique first names is unnecessary. A simple filter will help out a lot.

// COMMAND ----------

// ANSWER
val n = names.select("firstName")
// the final withColumnRenames is to allow the join below to be more simplistic syntactically. 
val p = people.select("firstName")

n.join(p, Seq("firstName")).distinct.count


// OR 2nd solution. Why is 1st one better? 
// It helps deal with situations where two DF's are being joined on columns with the same name.

// val n = names.select("firstName")
// val p = people.select("firstNname")

// n.join(p, n("firstName") === p("firstName")).select("firstName").distinct.count

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC - Review the stages generated for the optimized query
// MAGIC - Compare it to the previous distinct query across all columns
// MAGIC
// MAGIC Which query generated a more efficient shuffle?
// MAGIC
// MAGIC |     Stage   | Duration | Task S/T | Shuffle Read | Shuffle Write |
// MAGIC |-------------|----------|----------|--------------|---------------|
// MAGIC |     Basic 3 |    0.1 s |      1/1 |      11.5 KB |               |
// MAGIC |     Basic 2 |     11 s |  200/200 |     950.7 MB |       11.5 KB |
// MAGIC |     Basic 1 |  1.0 min |      2/2 |              |      950.7 MB |
// MAGIC |             |          |          |              |               |
// MAGIC | Optimized 3 |    51 ms |      1/1 |      11.5 KB |               |
// MAGIC | Optimized 2 |      1 s |  200/200 |     150.8 KB |       11.5 KB |
// MAGIC | Optimized 1 |      2 s |      2/2 |              |      150.8 KB |
// MAGIC
// MAGIC From the above comparison of the data shuffled, we can see that a simple vertical filter in the form of a select allowed us to greatly reduce the data shuffled during the distinct transformation. 
// MAGIC
// MAGIC But, another potential problem crops up:
// MAGIC
// MAGIC  - Why 200 tasks?
// MAGIC  - Let's check the DataFrame's full size in memory from the Spark UI and decide on appropriate partitioning. 
// MAGIC  - You can access the UI by going to `Clusters` > `Spark UI`

// COMMAND ----------

// ANSWER
// Cache the dataset from the join without removing any columns. 
val fullyJoinedDF = names.join(people, names("firstName") === people("firstName"))

// Counting involves all partitions in the DF. 
// show() will only cache first partition; we want the entire DF, not just a fraction. 
fullyJoinedDF.cache()
// if you are only caching a fraction, think about the action you are using.
// SHOW / FIRST will only fetch from an individual partition
fullyJoinedDF.count

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Now that we know the size of the DF we can decide on the number of partitions we desire.
// MAGIC - Typically want `numbPartitions = (threads available to app * 2)`
// MAGIC - Another recommendation, 50-200MB
// MAGIC
// MAGIC So, some questions to answer before deciding the optimal partition count:
// MAGIC 1. How many threads are available to our application? *Hint, find the default parallelism.*
// MAGIC 2. What is the current partition count of the DataFrame?
// MAGIC 3. What is the approximate partition size of the DataFrame?

// COMMAND ----------

// ANSWER
val numberPartitions = fullyJoinedDF.rdd.getNumPartitions
val dfSize = 591.5 // from Spark UI
val dfPartitionSize = dfSize / numberPartitions

// 1 How many threads are avaiable to our application?
println("Default parallelism: " + spark.sparkContext.defaultParallelism)
// 2 What is the current partition count of the DataFrame?
println("DF partition numb: " + numberPartitions)
// 3 What is the approximate partition size of the DataFrame?
println("DF partitions size (approx): " + dfPartitionSize)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The partition size here is less than optimal. We have a 591.5-megabyte DataFrame split across 2 partitions. This can be corrected a number of ways. The first is using the `repartition` transformation available in Spark. 

// COMMAND ----------

// ANSWER
// - repartition the DataFrame  to something more optimal 
// - review the above statements to decide on your numPartitions
// - cache a single partition from the now repartitionedDF using the show() action. 

val executorCount =  1 // from Spark UI: cluster > spark_UI > Executor (typically 1 for CE)
val newPartitionSize = 100 // Between 50 and 200MB.
val defaultParallelism = spark.sparkContext.defaultParallelism
val numPartitions = Math.max(defaultParallelism * 2, Math.ceil(dfSize / newPartitionSize).toInt)
println(s"New partition count: $numPartitions")

val repartitionedDF = fullyJoinedDF.repartition(numPartitions)
println("repartitionedDF.rdd.getNumPartitions: " + repartitionedDF.rdd.getNumPartitions)

repartitionedDF.cache()
repartitionedDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC An interesting observation can be made. The overall size of the DataFrame changed.
// MAGIC
// MAGIC As partition size increases, so does the cached DataFrame size. This is because there is fixed overhead involed with each partition.
// MAGIC
// MAGIC - Although this transformation is simple to use, it means that the data has already been partitioned in some manner.
// MAGIC - Repartitioning may involve a shuffle (expensive)
// MAGIC
// MAGIC Another option is to set `spark.default.parallelism`. This setting will automatically give us a different number of partitions after a shuffle occurs. 

// COMMAND ----------

// Finally, make sure you remove any DF from the cache if you don't plan on using it. 
fullyJoinedDF.unpersist()
repartitionedDF.unpersist()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
