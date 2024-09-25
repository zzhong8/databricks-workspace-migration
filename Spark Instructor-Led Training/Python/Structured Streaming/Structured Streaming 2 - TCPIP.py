# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
# MAGIC
# MAGIC # Structured Streaming
# MAGIC ## TCP/IP Examples
# MAGIC
# MAGIC **What you will learn:**
# MAGIC * Streaming DataFrames (SDFs).
# MAGIC * How to create a SDF from a TCP/IP socket.
# MAGIC * How to work with SDFs.
# MAGIC * Special considerations for SDFs.
# MAGIC * About Windowing.
# MAGIC * About Watermarking.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Streaming DataFrame
# MAGIC
# MAGIC So, how do you create a streaming DataFrame? As with regular DataFrames, you start with the `SparkSession`. We already have one available to us in the notebook, called `spark`.
# MAGIC
# MAGIC Here's a simple example. We're going to consume log data that looks something like this:
# MAGIC
# MAGIC ```[2016/12/22 17:23:49.506] (WARN) Backup server 192.168.182.194 is offline.```  
# MAGIC ```[2016/12/22 17:23:49.506] (INFO) Pinging watchdog timer process: Process is alive.```  
# MAGIC ```[2016/12/22 17:23:49.506] (INFO) Backed up all server logs.  ```  
# MAGIC ```[2016/12/22 17:23:49.506] (INFO) Rolling 8 log file(s)...  ```  
# MAGIC ```[2016/12/22 17:23:49.507] (INFO) Flushed 22 buffers to disk.  ```  
# MAGIC ```[2016/12/22 17:23:49.507] (INFO) Backed up all server logs.  ```  
# MAGIC ```[2016/12/22 17:23:49.507] (INFO) Network heartbeat check: All OK. ```   
# MAGIC ```...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Okay, let's create our streaming DataFrame.
# MAGIC
# MAGIC **Note:** While we're using the "socket" data source here, you should _not_ use the "socket" source for production streaming jobs. As noted in the
# MAGIC [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets),
# MAGIC this data source "should be used only for testing, as [it] does not provide end-to-end fault-tolerance guarantees."
# MAGIC
# MAGIC We'll talk a bit more about fault tolerance later.

# COMMAND ----------

logsDF = (spark.readStream
    .format("socket")
    .option("host", "server1.databricks.training")
    .option("port", 9001)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that what we just created is a regular DataFrame, with a single `value` column.
# MAGIC
# MAGIC Each line of streamed data will be a row in the DataFrame, and it'll be stored entirely within the `value` column.

# COMMAND ----------

logsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## What can we do with this DataFrame?
# MAGIC
# MAGIC Even before we figure out what to do with the content in the DataFrame, we can do some interesting things. For example, if you run the following cell, you'll get a continuously updating display of the number of records read from the stream so far. If you convert the display to a graph (say, a bar graph), you'll see the bar graph update automatically, as new data comes in. Try it.
# MAGIC
# MAGIC Note that we're just calling `display()` on our DataFrame, _exactly_ as if it were a DataFrame reading from a static data source.
# MAGIC
# MAGIC To stop the continuous update, just cancel the query.

# COMMAND ----------

from pyspark.sql.functions import *

runningCountsDF = logsDF.agg(count("*"))

display(runningCountsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### It's just a DataFrame
# MAGIC
# MAGIC We can use normal DataFrame transformations on our streaming DataFrame. 
# MAGIC
# MAGIC For example, let's parse out the timestamp calling it `capturedAt`, parse out the log data calling it `logData` and then filter out all non-error messages:
# MAGIC
# MAGIC To extract the timestamp we will:
# MAGIC 0. Use the built-in `substr()` function to extract the timestamp string. 
# MAGIC 0. Then, we'll use `unix_timestamp()` to parse out the <a href="https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html" target="_blank">Java epoch</a>.
# MAGIC   * Represented as the number of seconds since January 1, 1970.
# MAGIC   * See Java's <a href="http://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html" target="_blank">SimpleDateFormat</a> for more information.
# MAGIC 0. Cast the Java epoc to a SQL-timestamp.
# MAGIC
# MAGIC To extract the log data we will use a regular expression.
# MAGIC
# MAGIC To filter all but the error messages, we'll just look for rows that have "` (ERROR) `" in them.

# COMMAND ----------

# Start by parsing out the timestamp and the log data
cleanDF = (logsDF
    .withColumn("ts_string", col("value").substr(2, 23))
    .withColumn("epoc", unix_timestamp("ts_string", "yyyy/MM/dd HH:mm:ss.SSS"))
    .withColumn("capturedAt", col("epoc").cast("timestamp"))
    .withColumn("logData", regexp_extract("value", """^.*\]\s+(.*)$""", 1))
)
cleanDF.printSchema()

# Keep only the columns we want and then filter the data
errorsDF = (cleanDF
    .select("capturedAt", "logData")
    .filter(col("value").like("% (ERROR) %"))
)
errorsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Again, we can pass this DataFrame to `display()`, to see a continually updated display of the Result Table.

# COMMAND ----------

display(errorsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC It would make sense here to go ahead and sort the data in decending order so that the newest error messages were on top:

# COMMAND ----------

display(
    errorsDF.orderBy(col("capturedAt").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AnalysisException
# MAGIC In the previous example, we attempted to sort a streaming `DataFrame` just like any other `DataFrame`.
# MAGIC
# MAGIC The problem here is that Spark would have to resort, and consequently reshuffle all the data.
# MAGIC
# MAGIC The overhead of this would make our streaming job unstable and for this reason it is prohibited.
# MAGIC
# MAGIC Let's take a look at what else we cannot do with a streaming DataFrame by looking at the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">Unsupported Operations</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## How many records are we getting at a time? (Or, let's do some windowing.)
# MAGIC
# MAGIC If we were using a static DataFrame, we could just parse the timestamp and use `groupBy()`. But on an ever-increasing DataFrame, you don't really want to do a `groupBy()` over the whole thing.
# MAGIC We _can_ accumulate counts within a sliding window, answering the question, "How many records are we getting every second?"
# MAGIC
# MAGIC Let's talk about windowing a little first.
# MAGIC
# MAGIC We're talking about aggregations over a sliding event-time window. To quote the 
# MAGIC [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time):
# MAGIC
# MAGIC >In a grouped aggregation, aggregate values (e.g., counts) are maintained for each unique value in the user-specified grouping column. In the case of _window_-based
# MAGIC > aggregations, aggregate values are maintained for each window the event-time of a row falls into.
# MAGIC
# MAGIC This illustration, also from the guide, helps us understanding how it works:
# MAGIC
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-window.png" style="height:419px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### What, exactly, is "event time"?
# MAGIC
# MAGIC _Event time_ is _not_ something maintained by Structured Streaming. Structured Streaming only knows about _receipt time_: the time a piece of data arrived in Spark.
# MAGIC
# MAGIC Suppose the data being processed has timestamps corresponding to when the data was generated. For instance, log data has timestamp information that usually corresponds to when each log message was generated. _That_ timestamp, within the data, is the event time.
# MAGIC
# MAGIC Structured Streaming provides operations that can operate on event time. All you have to do is tell Structured Streaming which column contains the event timestamp, in our case `capturedAt` (which must be a `Timestamp`) type.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's do some windowing of our own.
# MAGIC
# MAGIC Since we're only counting the number of records received each second, the `capturedAt` column from our `cleanDF` is the only column we need.
# MAGIC
# MAGIC There are various versions of the `window()` function - we'll be using the simplest one. 
# MAGIC * It takes a `Column` containing the time (`capturedAt`, in our case), and a `windowDuration`. 
# MAGIC * It generates tumbling time windows given a timestamp-specifying column. 
# MAGIC * Window starts are inclusive, but the window ends are exclusive
# MAGIC * e.g. 12:05 will be in the window \[12:05,12:10) but not in \[12:00,12:05).
# MAGIC * Windows can support microsecond precision.
# MAGIC
# MAGIC In our example, we will use a 1-second window:

# COMMAND ----------

messagesPerSecond = (cleanDF
    .select("capturedAt")
    .groupBy( window(col("capturedAt"), '1 second'))
    .count()
    .orderBy("window.start")
)

messagesPerSecond.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's trim that down to the starting timestamp, and see what `display()` does for us:

# COMMAND ----------

display(
  messagesPerSecond.select(col("window.start").alias("start"), 
                           col("window.end").alias("end"), 
                           col("count") )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Considerations
# MAGIC
# MAGIC If you run that query, as is, it will take a surprisingly long time to start generating data. What's the cause of the delay? If you expand the **Spark Jobs** component, you'll see something like this:
# MAGIC
# MAGIC <img src="http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/structured-streaming-shuffle-partitions-200.png"/>
# MAGIC
# MAGIC It's our `groupBy()`. `groupBy()` causes a _shuffle_, and, by default, Spark SQL shuffles to 200 partitions. In addition, we're doing a _stateful_ aggregation: one that requires Structured Streaming to maintain and aggregate data over time.
# MAGIC
# MAGIC When doing a stateful aggregation, Structured Streaming must maintain an in-memory _state map_ for each window within each partition. For fault tolerance reasons, the state map has to be saved after a partition is processed, and it needs to be saved somewhere fault-tolerant. To meet those requirements, the Streaming API saves the maps to a distributed store. On some clusters, that will be HDFS. Databricks uses the DBFS.
# MAGIC
# MAGIC That means that every time it finishes processing a window, the Streaming API writes its internal map to disk. The write has some overhead, typically between 1 and 2 seconds. On Community Edition, we only have 8 concurrent threads available, so we can only process 8 partitions at a time.
# MAGIC
# MAGIC One way to reduce this overhead is to reduce the number of partitions Spark shuffles to.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's re-run our query.

# COMMAND ----------

display(
  messagesPerSecond.select(col("window.start").alias("start"), 
                           col("window.end").alias("end"), 
                           col("count") )
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Additional Performance Considerations
# MAGIC
# MAGIC Adjusting the shuffle partitions helps, but there's another problem: We're generating windows every second. _Every window_ has to be separately persisted and maintained, and, over time, this data will build up, slowing things down.
# MAGIC
# MAGIC **How do we fix that problem?**
# MAGIC
# MAGIC One simple solution is to increase the size of our window (say, to 1 minute). That way, we're generating fewer windows. But if the job runs for a long time, we're still building up an unbounded set of windows. Eventually, we could hit resource limits.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Watermarking
# MAGIC
# MAGIC A better solution to the problem is to define a cut-off: a point after which Structured Streaming is allowed to throw saved windows away.
# MAGIC
# MAGIC That's what _watermarking_ allows us to do.
# MAGIC
# MAGIC Let's use an example from the Structured Streaming Programming Guide to clarify how watermarking works.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Handling Late Data 
# MAGIC
# MAGIC Consider code that is aggregating data within 10-minute windows. What happens if one of the events arrives late to the application? For example, suppose data generated at 12:04 (i.e., the _event time_) is received by the application at 12:11. The application _should_ use the time 12:04, instead of 12:11, to update the older counts for the window 12:00-12:10. Normally, this happens automatically, because Structured Streaming maintains the intermediate state for partial aggregates for a long period of time, allowing late data to update aggregates of old windows correctly.
# MAGIC
# MAGIC The following diagram, from the Structured Streaming Programming Guide, illustrates the scenario.
# MAGIC
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-late-data.png" style="height: 500px"/>
# MAGIC
# MAGIC
# MAGIC However, to run this query for days, Spark must bound the amount of intermediate in-memory state it accumulates. So, Spark needs to know when an old aggregate can be dropped from the in-memory state.
# MAGIC
# MAGIC To enable this, Spark 2.1 introduces _watermarking_, which lets the engine automatically track the current event time in the data and attempt to clean up old state accordingly. 
# MAGIC
# MAGIC You can define the watermark of a query by specifying the event time column (we've been using our `capturedAt` column) and the lateness threshold—that is, how late the data is expected be, measured by event time. 
# MAGIC
# MAGIC For a specific window starting at time _T_, Spark will maintain state and allow late data to update the state until (_max event time seen by Spark_ - _late threshold_) exceeds _T_.
# MAGIC
# MAGIC In plain English, late data _within_ the threshold will be aggregated, but data later than the threshold will be dropped. Let’s understand this with an example. We can easily define watermarking on the previous example using withWatermark() as shown below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refinement of our previous example
# MAGIC
# MAGIC Here's our previous example with watermarking. We're telling Structured Streaming to keep no more than 1 minute of aggregation data, which means late data that comes more in 1 minute _past_ its event time will be dropped. (In this particular data source, it turns out that will never happen.)

# COMMAND ----------

windowedDF = (errorsDF
  .withWatermark("capturedAt", "1 minute")
  .groupBy(window(col("capturedAt"), "1 second"))
  .count()
  .select( col("window.start").alias("start"),
           col("window.end").alias("end"),
           col("count"))
  .orderBy(col("start"))
)

display(windowedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpointing
# MAGIC
# MAGIC Checkpointing is useful when you want to recover from a previous failure or an intentional shutdown. 
# MAGIC
# MAGIC With checkpointing enabled, you can recover the previous progress and state of the streaming query that was running when the system stopped, and your job can continue where it left off.
# MAGIC
# MAGIC To enable checkpointing, you configure a Structured Streaming query with a checkpoint location. 
# MAGIC
# MAGIC During processing, the query will save:
# MAGIC * the progress data—that is, the range of offsets processed by each trigger
# MAGIC * the values of the running aggregates
# MAGIC
# MAGIC The checkpoint location must be a path to an HDFS-compatible file system, and it's just another option on the output sink.

# COMMAND ----------

# MAGIC %md
# MAGIC To get started make sure that the checkpoint directory does not exist:

# COMMAND ----------

print(userhome)

# COMMAND ----------

# checkpointDir = "dbfs:/mnt/artifacts/hugh/checkpoint"

checkpointDir = userhome + "/checkpoint"
dbutils.fs.rm(checkpointDir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC Note below, that we are not simply creating a `DataFrame` and passing it to the `display()` command like previous examples.
# MAGIC
# MAGIC In this case we are taking our `DataFrame` and writing it out to a sink with all the parameters of a typical job.

# COMMAND ----------

streamingQuery = (windowedDF
    .writeStream
    .outputMode("complete")                      # Writing all records
    .option("checkpointLocation", checkpointDir) # Checkpointing config
    .format("memory")                            # Writing to a Memory sink
    .queryName("my_query")                       # We are giving the query a name
    .trigger(processingTime="20 seconds")        # With a trigger interval of 20 seconds
    .start()                                     # Start the job
)

# COMMAND ----------

# MAGIC %md
# MAGIC At this point, we can see that Spark is writing information to our checkpoint directory:

# COMMAND ----------

display(dbutils.fs.ls(checkpointDir))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Mode
# MAGIC
# MAGIC Because our query is writing to a Memory sink, we can query that in-memory table with simple SQL like we would with traditional data sources:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from my_query

# COMMAND ----------

# MAGIC %md
# MAGIC Or read it back as a "standard" DataFrame.
# MAGIC
# MAGIC However, to update our results, we have to manually rerun our query every time.

# COMMAND ----------

display(
    spark.read.table("my_query")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The StreamingQuery Object
# MAGIC Using the `streamingQuery` object returned from `DataStreamWriter.start()` we can also check on the status of the query with call like the ones below:

# COMMAND ----------

print(streamingQuery.name)

# COMMAND ----------

print(streamingQuery.status)

# COMMAND ----------

print(streamingQuery.lastProgress)

# COMMAND ----------

print(streamingQuery.explain())

# COMMAND ----------

# MAGIC %md
# MAGIC And lastly, we can stop the streaming query:

# COMMAND ----------

streamingQuery.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end Fault Tolerance
# MAGIC
# MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ and _Write Ahead Logs_.
# MAGIC
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC
# MAGIC This approach _only_ works if the streaming source is replayable. To ensure fault-tolerance, Structured Streaming assumes that every streaming source has offsets, akin to:
# MAGIC
# MAGIC * <a target="_blank" href="https://kafka.apache.org/documentation/#intro_topics">Kafka message offsets</a>
# MAGIC * <a target="_blank" href="http://docs.aws.amazon.com/streams/latest/dev/key-concepts.html#sequence-number">Kinesis sequence numbers</a>
# MAGIC
# MAGIC At a high level, the underlying streaming mechanism relies on a couple approaches:
# MAGIC
# MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
# MAGIC * Next, the streaming sinks are designed to be _idempotent_—that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.
# MAGIC
# MAGIC **Question for discussion: Why is the TCP source not considered fault-tolerant?**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC * [Structured Streaming 1 - Intro]($./Structured Streaming 1 - Intro)
# MAGIC * Structured Streaming 2 - TCP/IP
# MAGIC * [Structured Streaming 3 - Kafka]($./Structured Streaming 3 - Kafka)
# MAGIC * [Structured Streaming 4 - Lab]($./Structured Streaming 4 - Lab)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
