# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations & Actions Lab
# MAGIC ## Exploring T&As in the Spark UI

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Instructions
# MAGIC 0. Run the cell below.<br/>** *Note:* ** *There is no real rhyme or reason to this code.*<br/>*It simply includes a couple of actions and a handful of narrow and wide transformations.*
# MAGIC 0. Answer each of the questions.
# MAGIC   * All the answers can be found in the **Spark UI**.
# MAGIC   * All aspects of the **Spark UI** may or may not have been reviewed with you - that's OK.
# MAGIC   * The goal is to get familiar with diagnosing applications.
# MAGIC 0. Submit your answers for review.
# MAGIC
# MAGIC **WARNING:** Run the following cell only once. Running it multiple times will change some of the answers and make validation a little harder.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

initialDF = (spark                                                       
    .read                                                                     
    .parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")   
    .cache()
)

initialDF.foreach(lambda x: None) # materialize the cache

displayHTML("All done<br/><br/>")

# COMMAND ----------

def check_partitions(df):
    print("Number of partitions = {0}\n".format(df.rdd.getNumPartitions()))

    def count_partition(index, iterator):
        yield (index, len(list(iterator)))

    data = (
        df
            .rdd
            .mapPartitionsWithIndex(count_partition, True)
            .collect()
    )

    for index, count in data:
        print("Partition {0:2d}: {1} bytes".format(index, count))
    
check_partitions(initialDF)

# COMMAND ----------

initialDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Round #1 Questions
# MAGIC 0. How many jobs were triggered? 2
# MAGIC 0. Open the Spark UI and select the **Jobs** tab.
# MAGIC   0. What action triggered the first job? initialDF.foreach(lambda x: None)
# MAGIC   0. What action triggered the second job? initialDF.foreach(lambda x: None)
# MAGIC 0. Open the details for the second job, how many MB of data was read in? Hint: Look at the **Input** column. 22.8 MB
# MAGIC 0. Open the details for the first stage of the second job, how many records were read in? Hint: Look at the **Input Size / Records** column. 2345943

# COMMAND ----------

someDF = (initialDF
    .withColumn("first", upper( col("article").substr(0,1)) )
    .where( col("first").isin("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z") )
    .groupBy("project", "first").sum()
    .drop("sum(bytes_served)")
    #.orderBy("first", "project")
    .select( col("first"), col("project"), col("sum(requests)").alias("total"))
    .filter( col("total") > 10000)
)

total = someDF.count()

displayHTML("All done<br/><br/>")

# COMMAND ----------

someDF.explain()

# COMMAND ----------

someDF2 = (initialDF
    .withColumn("first", upper( col("article").substr(0,1)) )
    .where( col("first").isin("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z") )
    .groupBy("project", "first").sum()
    .drop("sum(bytes_served)")
    .orderBy("first", "project")
    .select( col("first"), col("project"), col("sum(requests)").alias("total"))
    .filter( col("total") > 10000)
)

total = someDF2.count()

# COMMAND ----------

someDF2.explain()

# COMMAND ----------

check_partitions(someDF)

# COMMAND ----------

someDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Round #2 Questions
# MAGIC 0. How many jobs were triggered? 2
# MAGIC 0. How many actions were executed? 1
# MAGIC 0. Open the **Spark UI** and select the **Jobs** tab.
# MAGIC   0. What action triggered the first job? total = someDF.count()
# MAGIC   0. What action triggered the second job? total = someDF.count()
# MAGIC 0. Open the **SQL** tab - what is the relationship between these two jobs? They correspond to the same SQL query. My guess is that the second job is a sampling operation done in order to use the RangePartitioner, which "partitions sortable records by range into roughly equal ranges", for the orderBy transformation. The ranges are determined by sampling the content, and because of the size of the data being processed, the query optimizer (Catalyst) decided to preprocess the data by sampling it, so it could choose more efficient partition sizes for processing.
# MAGIC 0. For the first job...
# MAGIC   0. How many stages are there? 2
# MAGIC   0. Open the **DAG Visualization**. What do you suppose the green dot refers to? Reading of the cached data (indicated by an InMemoryTableScan in the physical plan)
# MAGIC 0. For the second job...
# MAGIC   0. How many stages are there? 3 (1 stage was skipped due to the implicit caching of the dataframe)
# MAGIC   0. Open the **DAG Visualization**. Why do you suppose the first stage is grey? Because this stage was already done in the first job??
# MAGIC   0. Can you figure out what transformation is triggering the shuffle at the end of 
# MAGIC     0. The first stage? groupBy
# MAGIC     0. The second stage? orderBy
# MAGIC     0. The third stage? HINT: It's not a transformation but an action. count
# MAGIC 0. For the second job, the second stage (i.e. first stage after the skipped stage), how many records (total) 
# MAGIC   0. Were read in as a result of the previous shuffle operation? 3665
# MAGIC   0. Were written out as a result of this shuffle operation? 55
# MAGIC   Hint: look for the **Aggregated Metrics by Executor**
# MAGIC 0. Open the **Event Timeline** for the second stage of the second job.
# MAGIC   * Make sure to turn on all metrics under **Show Additional Metrics**.
# MAGIC   * Note that there were 200 tasks executed.
# MAGIC   * Visually compare the **Scheduler Delay** to the **Executor Computing Time**
# MAGIC   * Then in the **Summary Metrics**, compare the median **Scheduler Delay** to the median **Duration** (aka **Executor Computing Time**)
# MAGIC   * What is taking longer? scheduling, execution, task deserialization, garbage collection, result serialization or getting the result? scheduling delay takes on median 7 ms. executor computation takes on median 11 ms

# COMMAND ----------

someDF.take(total)

displayHTML("All done<br/><br/>")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Round #3 Questions
# MAGIC 0. Collectively, `someDF.count()` produced 2 jobs and 6 stages.  
# MAGIC However, `someDF.taken(total)` produced only 1 job and 2 stages.  
# MAGIC   0. Why did it only produce 1 job? No idea... because an automatic repartition was not required?
# MAGIC   0. Why did the last job only produce 2 stages? Because we were able to reuse the results of our last shuffle (since it was still available)
# MAGIC 0. Look at the **Storage** tab. How many partitions were cached? 8
# MAGIC 0. True or False: The cached data is fairly evenly distributed. True (I see 7.0 MB in memory in each block/partition)
# MAGIC 0. How many MB of data is being used by our cache? 55.8 MB
# MAGIC 0. How many total MB of data is available for caching? 55.8 MB
# MAGIC 0. Go to the **Executors** tab. How many executors do you have? 3
# MAGIC 0. Go to the **Executors** tab. How many total cores do you have available? 8
# MAGIC 0. Go to the **Executors** tab. What is the IP Address of your first executor? 10.139.64.6:33323
# MAGIC 0. How many tasks is your cluster able to execute simultaneously? 8
# MAGIC 0. What is the path to your **Java Home**? /usr/lib/jvm/java-8-openjdk-amd64/jre/

# COMMAND ----------

# MAGIC %sh
# MAGIC echo $JAVA_HOME

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
