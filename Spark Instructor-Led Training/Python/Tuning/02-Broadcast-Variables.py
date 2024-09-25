# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Broadcasting
# MAGIC * Broadcast Variables
# MAGIC * Broadcast Joins
# MAGIC * Automatic and Manual broadcasting
# MAGIC * Broadcast Cleanup
# MAGIC * Accumulators

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Broadcast Variables
# MAGIC
# MAGIC Broadcast variables allow us to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. This can be useful when tasks of a job require access to the same variable. Typically tasks **larger than approximately 20 KB** should be optimized to use broadcast variables.
# MAGIC
# MAGIC Popular use cases:
# MAGIC - Sharing a variable between multiple tasks
# MAGIC - Joining a small table to a very large table 
# MAGIC
# MAGIC #### Sharing a variable
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/tuning/broadcast_isin.png" style="height:240px;" alt="Spill to disk"/><br/><br/>    

# COMMAND ----------

a = spark.createDataFrame([(1, "CS:GO", "FPS"), (2, "CS 1.6", "Shooter"), (3, "WC3", "RTS"), (4, "D2", "RPG")], ["id", "game", "genre"])
genres = ["FPS", "MOBA", "RPG", "RTS"]
bgenres = spark.sparkContext.broadcast(genres)
b = a.select(a["game"], a["genre"], a["genre"].isin(bgenres.value).alias("valid"))
b.filter(b["valid"] == False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC It's generally a good idea to destroy the broadcast variable when you're done with it.

# COMMAND ----------

bgenres.destroy()

# COMMAND ----------

# MAGIC %md
# MAGIC In practice, Spark *automatically* broadcasts the common data needed by tasks within each stage; thus, broadcast variables are useful when data is required across  multiple stages. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Broadcast Joins
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/tuning/broadcast-join.png" style="height:300px;"  alt="Spill to disk"/><br/><br/>    
# MAGIC
# MAGIC The high level idea is that sharing an entire small table is more efficient than splitting it up and shuffling both the large and small tables. This means that the small table doesn't need to be shuffled, as Spark has a full copy of the smaller table and can carry out the join on the mapper side. 

# COMMAND ----------

# approx 18.6 MB in memory
names = spark.read.parquet("dbfs:/mnt/training/ssn/names.parquet")

display(names)

# COMMAND ----------

# approx 500K MB in memory
people = (
    spark
        .read
        .option("delimiter", ":")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("dbfs:/mnt/training/dataframes/people-with-header.txt")
)

names.join(people, ["firstName"])
people.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Why were jobs triggered and executed?
# MAGIC - Spark scans files to check their size and other metadata.
# MAGIC - This allows Spark to decide the initial partitioning of the file and in this case, whether to use a broadcast join for the two files or not.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Automatic and Manual broadcasting
# MAGIC
# MAGIC - Depending on size of the data that is being loaded into Spark, Spark uses internal heuristics to decide how to join that data to other data.
# MAGIC - Automatic broadcast depends on `spark.sql.autoBroadcastJoinThreshold`
# MAGIC     - The setting configures the **maximum size in bytes** for a table that will be broadcast to all worker nodes when performing a join 
# MAGIC     - Default is 10MB
# MAGIC
# MAGIC - A `broadcast` function can be used in Spark to instruct Catalyst that it should probably broadcast one of the tables that is being joined. 
# MAGIC - The function is important, as sometimes our table might fall just outside of the limit of what Spark will broadcast automatically.
# MAGIC
# MAGIC If the `broadcast` hint isn't used, but one side of the join is small enough (i.e., its size is below the threshold), that data source will be read into
# MAGIC the Driver and broadcast to all Executors.
# MAGIC
# MAGIC If both sides of the join are small enough to be broadcast, the [current Spark source code](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala#L153)
# MAGIC will choose the right side of the join to broadcast.
# MAGIC
# MAGIC Below we join two DataFrames where both DataFrames exceed the default 10MB limit of `autoBroadcastJoinThreshold` by a significant amount.
# MAGIC
# MAGIC Note that we're supplying the schema explicitly, to speed things up.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To get a rough sense of the sizes of the DataFrames in memory, you can cache each one, run an action that traverses the whole data set (e.g., `count`), and then check the UI. e.g.:
# MAGIC
# MAGIC ```
# MAGIC names.cache()
# MAGIC people2.cache()
# MAGIC names.distinct.count
# MAGIC people2.distinct.count
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import *

people_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("firstName", StringType(), True),
    StructField("middleName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthDate", TimestampType(), True),
    StructField("ssn", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# 229.5 MB in tungsten format.
people1 = (
    spark
        .read
        .option("header", "true")
        .option("delimiter", ":")
        .schema(people_schema)
        .csv("dbfs:/mnt/training/dataframes/people-with-header-5m.txt")
)

# 46 MB in tungsten format.
people2 = (
    spark
        .read
        .option("header", "true")
        .option("delimiter", ":")
        .schema(people_schema)
        .csv("dbfs:/mnt/training/dataframes/people-with-header-1m.txt")
)

# If we were to join the two tables on say, the first name, spark wouldn't carry out a broadcast.
peopleNames = people2.join(people1, people1["firstName"] == people2["firstName"])
peopleNames.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Both tables are above the default 10 MB limit of `spark.sql.autoBroadcastJoinThreshold` but we can hint that we want a broadcasting to happen on one of the tables. Using the explain function to render the final physical execution plan a `BroadcastHashJoin` can be seen.

# COMMAND ----------

# We can hint that we want a broadcasting to happen on one of the tables.
from pyspark.sql.functions import broadcast

peopleNamesBcast = people2.join(broadcast(names), names["firstName"] == people2["firstName"])
peopleNamesBcast.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We should also see a performance benefit of broadcasting.<br/> The `names` DataFrame is over the 10MB limit but thanks to the `broadcast` function, the optimization can be achieved.

# COMMAND ----------

peopleNames.count()

# COMMAND ----------

peopleNamesBcast.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In Spark 2.2 and later after <a target="blank" href="https://issues.apache.org/jira/browse/SPARK-16475">SPARK-16475</a>, a broadcast hint function has been introduced to Spark SQL.

# COMMAND ----------

names.createOrReplaceTempView("names")
broadcastedSQLDF = spark.sql("SELECT /*+ BROADCAST(names) */ * FROM names")
broadcastedSQLDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Broadcast Cleanup
# MAGIC
# MAGIC We can  clean up memory used by broadcast variables. There are two different options:
# MAGIC - `unpersist` - cleans up the broadcasted variable from all executors, keeps a copy in the driver.
# MAGIC - `destroy` - cleans broadcast variable from driver and executors.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Accumulators
# MAGIC
# MAGIC - An accumulator is typically used as a distributed counter.
# MAGIC - Can be customized to be a List of items etc. by implementing the `AccumulatorParam` interface.
# MAGIC - DataFrames use a domain-specific language (DSL), thus accumulators don't fit well in DF world. 
# MAGIC - Datasets allow for usage of lambdas / anonymous functions, accumulators can be useful for debugging.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC import org.apache.spark.sql.functions.length
# MAGIC
# MAGIC case class Names(firstName: String, 
# MAGIC                  gender: String, 
# MAGIC                  total: Int, 
# MAGIC                  year: Int)
# MAGIC
# MAGIC // convert the DF to a DataSet so we can use an anonymous function to carry out a filter.
# MAGIC val namesDS = spark.read.parquet("dbfs:/mnt/training/ssn/names.parquet").as[Names]
# MAGIC val filteredRows = spark.sparkContext.longAccumulator("Test")
# MAGIC
# MAGIC // build a multi-column filter and count how many rows were emitted. 
# MAGIC // find most popular female names between 2000 and 2004 starting with char A and B
# MAGIC val topFNameByYearChar = namesDS.filter{ x => 
# MAGIC   if ((x.firstName(0) == 'A' || x.firstName(0) == 'B') &&
# MAGIC       x.gender == "F" && 
# MAGIC       x.year > 1999 &&
# MAGIC       x.year < 2005) {
# MAGIC     true
# MAGIC   } else {
# MAGIC     filteredRows.add(1)
# MAGIC     false
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC topFNameByYearChar.orderBy($"total".desc).show(10)
# MAGIC println("Filtered out rows: " + filteredRows.value + " of " + namesDS.count + " overall rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - The accumulator requires an anonymous function / lambda in order to function and avoid the DSL.
# MAGIC - Usage of anonymous functions and lambdas means data stored in the tungsten format has to be decoded.
# MAGIC - Decoding is extra work, this causes a performance hit.
# MAGIC - No DataSet means accumulators would require a UDF in python, thus making the performance hit even bigger.

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercises
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Exercise 1: Broadcast variables
# MAGIC
# MAGIC Let's begin by creating a variable to hold some data to be used with a Spark Dataset within a closure. Remember, don't make the data too large, the driver JVM has a default size of **1GB**. 
# MAGIC
# MAGIC **NOTE**: Because we're using the Dataset API for Exercise 1, we cannot use Python.

# COMMAND ----------

# MAGIC %scala
# MAGIC val limit = 50 * 1000 * 1000
# MAGIC val data = (1 to limit).toArray

# COMMAND ----------

# MAGIC %scala
# MAGIC case class Number(numb: Int, squared: Int)
# MAGIC val dataSet = (1 to 5).map(x => (x, x*x)).toArray
# MAGIC val df = spark.createDataFrame(dataSet).toDF("numb", "squared")
# MAGIC val ds = df.as[Number]
# MAGIC ds.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now use the data we declared earlier to manipulate the newly created Dataset when using lambdas / anonymous functions. 

# COMMAND ----------

# MAGIC %scala
# MAGIC for (i <- 1 to 5) ds.map { x => data.length * x.numb }.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Update the above code that carries out a map on the Dataset to use a broadcast variable for efficiency. 

# COMMAND ----------

# MAGIC %scala
# MAGIC val broadcastVar = sc.broadcast(dataSet)
# MAGIC for (i <- 1 to 5) ds.map(x => broadcastVar.value.length * x.numb).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure to clean the broadcast variable from the cluster once it's no longer needed.

# COMMAND ----------

# MAGIC %scala
# MAGIC broadcastVar.destroy

# COMMAND ----------

# MAGIC %md
# MAGIC What happens if we try to use the broadcast variable after garbage collecting it from memory?

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Exercise 2: Using broadcasting to optimize joins
# MAGIC
# MAGIC Earlier in the course we saw that using the `broadcast` hint can help in situations where a DataFrame is larger than `spark.sql.autoBroadcastJoinThreshold`. Another option is to simply increase `autoBroadcastJoinThreshold`. But what if we want to prevent broadcasting? One sure way to prevent broadcasts from happening is to set `autoBroadcastJoinThreshold` to **-1**. Update the threshold below to prevent any broadcasting from happening and verify by looking at the selected physical plan for execution.

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

names = spark.read.parquet("dbfs:/mnt/training/ssn/names.parquet")
people = (
    spark
        .read
        .option("delimiter", ":")
        .option("header", "true") 
        .option("inferSchema", "true")
        .csv("dbfs:/mnt/training/dataframes/people-with-header.txt")
)

joinedDF = names.join(people, "firstName")
# verify that neither of the tables was broadcasted.
joinedDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Even though we've set `autoBroadcastJoinThreshold` to **-1**, this configuration can be circumvented by using the `broadcast` hint option. Use it below to force broadcasting of the people dataframe.

# COMMAND ----------

print("autoBroadcastJoinThreshold: " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# We can hint that we want a broadcasting to happen on one of the tables.
from pyspark.sql.functions import broadcast

joinedDF2 = names.join(broadcast(people), names["firstName"] == people["firstName"])
joinedDF2.explain()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
