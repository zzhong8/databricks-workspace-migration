# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Streaming
# MAGIC Databricks&reg; Delta allows you to work with streaming data.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Read and write streaming data into a data lake
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analyst sand Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC * Lesson 4: <a href="$./04-Upsert">Databricks Delta Upsert</a>
# MAGIC
# MAGIC ## Datasets Used
# MAGIC Data from 
# MAGIC `/mnt/training/definitive-guide/data/activity-data`
# MAGIC contains smartphone accelerometer samples from all devices and users. 
# MAGIC
# MAGIC The CSV file consists of the following columns:
# MAGIC
# MAGIC - `Index`
# MAGIC - `Arrival_Time`
# MAGIC - `Creation_Time`
# MAGIC - `x`
# MAGIC - `y`
# MAGIC - `z`
# MAGIC - `User`
# MAGIC - `Model`
# MAGIC - `Device`
# MAGIC - `gt`
# MAGIC
# MAGIC ## CAUTION
# MAGIC * Do not use <b>RunAll</b> mode (next to <b>Permissions</b>, in the menu at the top). 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/cpilwjdr38?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/cpilwjdr38?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

dataPath = "/mnt/training/definitive-guide/data/activity-data"

basePath       = userhome + "/delta/python"
activityPath   = basePath + "/activityCount"
checkpointPath = basePath + "/05/checkpoints"

# Configure our shuffle partitions for these exercises
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Streaming Concepts
# MAGIC
# MAGIC <b>Stream processing</b> is where you continuously incorporate new data into a data lake and compute results.
# MAGIC
# MAGIC The data is coming in faster than it can be consumed.
# MAGIC
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/firehose.jpeg" style="height: 200px"/></div><br/>
# MAGIC
# MAGIC Treat a <b>stream</b> of data as a table to which data is continously appended. 
# MAGIC
# MAGIC In this course we are assuming Databricks Structured Streaming, which uses the DataFrame API. 
# MAGIC
# MAGIC There are other kinds of streaming systems.
# MAGIC
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/stream2rows.png" style="height: 300px"/></div><br/>
# MAGIC
# MAGIC Examples are bank card transactions, Internet of Things (IoT) device data, and video game play events. 
# MAGIC
# MAGIC Data coming from a stream is typically not ordered in any way.
# MAGIC
# MAGIC A streaming system consists of 
# MAGIC * <b>Input source</b> such as Kafka, Azure Event Hub, files on a distributed system or TCP-IP sockets
# MAGIC * <b>Sinks</b> such as Kafka, Azure Event Hub, various file formats, `forEach` sinks, console sinks or memory sinks
# MAGIC
# MAGIC ### Streaming and Databricks Delta
# MAGIC
# MAGIC In streaming, the problems of traditional data pipelines are exacerbated. 
# MAGIC
# MAGIC Specifically, with frequent meta data refreshes, table repairs and accumulation of small files on a secondly- or minutely-basis!
# MAGIC
# MAGIC Many small files result because data (may be) streamed in at low volumes with short triggers.
# MAGIC
# MAGIC Databricks Delta is uniquely designed to address these needs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### READ Stream using Databricks Delta
# MAGIC
# MAGIC The `readStream` method is a <b>transformation</b> that outputs a DataFrame with specific schema specified by `.schema()`. 
# MAGIC
# MAGIC Each line of the streaming data becomes a row in the DataFrame once an <b>action</b> such as `writeStream` is invoked.
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In this lesson, we limit flow of stream to one file per trigger with `option("maxFilesPerTrigger", 1)` so that you do not exceed file quotas you may have on your end. The default value is 1000.
# MAGIC
# MAGIC Notice that nothing happens until you engage an action, i.e. a `writeStream` operation a few cells down.
# MAGIC
# MAGIC Do some data normalization as well:
# MAGIC * Convert `Arrival_Time` to `timestamp` format.
# MAGIC * Rename `Index` to `User_ID`.

# COMMAND ----------

static = spark.read.json(dataPath)
dataSchema = static.schema

deltaStreamWithTimestampDF = (spark
  .readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .schema(dataSchema)
  .json(dataPath)
  .withColumnRenamed('Index', 'User_ID')
  .selectExpr("*","cast(cast(Arrival_Time as double)/1000 as timestamp) as event_time")
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### WRITE Stream using Databricks Delta
# MAGIC
# MAGIC #### General Notation
# MAGIC Use this format to write a streaming job to a Databricks Delta table.
# MAGIC
# MAGIC > `(myDF` <br>
# MAGIC   `.writeStream` <br>
# MAGIC   `.format("delta")` <br>
# MAGIC   `.option("checkpointLocation", somePath)` <br>
# MAGIC   `.outputMode("append")` <br>
# MAGIC   `.table("my_table")` or `.start(path)` <br>
# MAGIC `)`
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you use the `.table()` notation, it will write output to a default location. 
# MAGIC
# MAGIC In this course, we want everyone to write data to their own directory; so, instead, we use the `.start()` notation.
# MAGIC
# MAGIC #### Output Modes
# MAGIC Notice, besides the "obvious" parameters, specify `outputMode`, which can take on these values
# MAGIC * `append`: add only new records to output sink
# MAGIC * `complete`: rewrite full output - applicable to aggregations operations
# MAGIC * `update`: update changed records in place
# MAGIC
# MAGIC #### Checkpointing
# MAGIC
# MAGIC When defining a Delta streaming query, one of the options that you need to specify is the location of a checkpoint directory.
# MAGIC
# MAGIC `.writeStream.format("delta").option("checkpointLocation", <path-to-checkpoint-directory>) ...`
# MAGIC
# MAGIC This is actually a structured streaming feature. It stores the current state of your streaming job.
# MAGIC
# MAGIC Should your streaming job stop for some reason and you restart it, it will continue from where it left off.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you do not have a checkpoint directory, when the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Also note that every streaming job should have its own checkpoint directory: no sharing.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Let's Do Some Streaming
# MAGIC
# MAGIC In the cell below, we write streaming query to a Databricks Delta table. 
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we do not need to specify a schema: it is inferred from the data! 

# COMMAND ----------

deltaStreamingQuery = (deltaStreamWithTimestampDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath+"/stream_1p")
  .outputMode("append")
  .queryName("stream_1p")
  .start(basePath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC See list of active streams.

# COMMAND ----------

for s in spark.streams.active:
  print("{}: {}".format(s.name, s.id))

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("stream_1p")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Exercise 1: Table-to-Table Stream
# MAGIC
# MAGIC Here we read a stream of data from from `basePath` and write another stream to `activityPath`.
# MAGIC
# MAGIC The data consists of a grouped count of `gt` events.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the stream using `deltaStreamingQuery` is still running!
# MAGIC
# MAGIC To perform an aggregate operation, what kind of `outputMode` should you use?

# COMMAND ----------

# ANSWER
activityCountsQuery = (spark.readStream
  .format("delta")
  .load(str(basePath))   
  .groupBy("gt")
  .count()
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/stream_2p")
  .outputMode("complete")
  .queryName("stream_2p")
  .start(activityPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("stream_2p")

# COMMAND ----------

# TEST - Run this cell to test your solution.
activityQueryTruth = spark.streams.get(activityCountsQuery.id).isActive

dbTest("Delta-05-activityCountsQuery", True, activityQueryTruth)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Exercise 2
# MAGIC
# MAGIC Plot the occurrence of all events grouped by `gt`.
# MAGIC
# MAGIC In order to create a LIVE bar chart of the data, you'll need to fill out the <b>Plot Options</b> as shown:
# MAGIC
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/ch5-plot-options.png"/></div><br/>
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In the cell below, we use the `withWatermark` and `window` methods, which aren't covered in this course. 
# MAGIC
# MAGIC To learn more about watermarking, please see <a href="https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html" target="_blank">Event-time Aggregation and Watermarking in Apache Spark’s Structured Streaming</a>.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import hour, window, col

countsDF = (deltaStreamWithTimestampDF      
  .withWatermark("event_time", "180 minutes")
  .groupBy(window("event_time", "60 minute"), "gt")
  .count()
  .withColumn('hour',hour(col('window.start')))     
)

display(countsDF.withColumn('hour',hour(col('window.start'))), streamName = "stream_3p")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait for the stream to initialize

# COMMAND ----------

untilStreamIsReady("stream_3p")

# COMMAND ----------

# TEST - Run this cell to test your solution.

# Compare schemas of countsDF
schemaStr = str(countsDF.schema)

dbTest("Assertion #1", 4, len(countsDF.columns))
dbTest("Assertion #2", True, "(gt,StringType,true)" in schemaStr) 
dbTest("Assertion #3", True, "(count,LongType,false)" in schemaStr) 
dbTest("Assertion #4", True, "(hour,IntegerType,true)" in schemaStr) 

dbTest("Assertion #5", True, "window,StructType" in schemaStr)
dbTest("Assertion #6", True, "(start,TimestampType,true)" in schemaStr) 
dbTest("Assertion #7", True, "(end,TimestampType,true)" in schemaStr) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3: Shut Down
# MAGIC Stop streams.

# COMMAND ----------

# ANSWER
for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# TEST - Run this cell to test your solution.
numActiveStreams = len(spark.streams.active)
dbTest("Delta-05-numActiveStreams", 0, numActiveStreams)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Databricks Delta is ideally suited for use in streaming data lake contexts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** Why is Databricks Delta so important for a data lake that incorporates streaming data?<br>
# MAGIC **A:** Frequent meta data refreshes, table repairs and accumulation of small files on a secondly- or minutely-basis!
# MAGIC
# MAGIC **Q:** What happens if you shut off your stream before it has fully initialized and started and you try to `CREATE TABLE .. USING DELTA` ? <br>
# MAGIC **A:** You will get this: `Error in SQL statement: AnalysisException: The user specified schema is empty;`.
# MAGIC
# MAGIC **Q:** When you do a write stream command, what does this option do `outputMode("append")` ?<br>
# MAGIC **A:** This option takes on the following values and their respective meanings:
# MAGIC * <b>append</b>: add only new records to output sink
# MAGIC * <b>complete</b>: rewrite full output - applicable to aggregations operations
# MAGIC * <b>update</b>: update changed records in place
# MAGIC
# MAGIC **Q:** What happens if you do not specify `option("checkpointLocation", pointer-to-checkpoint directory)`?<br>
# MAGIC **A:** When the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC
# MAGIC **Q:** How do you view the list of active streams?<br>
# MAGIC **A:** Invoke `spark.streams.active`.
# MAGIC
# MAGIC **Q:** How do you verify whether `streamingQuery` is running (boolean output)?<br>
# MAGIC **A:** Invoke `spark.streams.get(streamingQuery.id).isActive`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Start the next lesson, [Optimization]($./Delta 06 - Optimization).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html#as-a-sink" target="_blank">Delta Streaming Write Notation</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tagatha Das. This is an excellent video describing how Structured Streaming works.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
