// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ## SparkML on Streaming Data

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take in the model we saved earlier, and apply it to some streaming data!

// COMMAND ----------

// MAGIC %run "./Includes/Classroom_Setup"

// COMMAND ----------

import org.apache.spark.ml.PipelineModel

val fileName = userhome + "/tmp/DT_Pipeline"
val pipelineModel = PipelineModel.load(fileName)

// COMMAND ----------

// MAGIC %md
// MAGIC We can simulate streaming data.
// MAGIC
// MAGIC NOTE: You must specify a schema when creating a streaming source DataFrame.

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = new StructType()
  .add(StructField("rating",DoubleType)) 
  .add(StructField("review",StringType))

val streamingData = (spark
                    .readStream
                    .schema(schema)
                    .option("maxFilesPerTrigger", 1)
                    .parquet("/mnt/training/movie-reviews/imdb/imdb_ratings_50k.parquet"))

// COMMAND ----------

// MAGIC %md
// MAGIC Why is this stream taking so long? What configuration should we set?

// COMMAND ----------

val stream = (pipelineModel
              .transform(streamingData)
              .groupBy('label, 'prediction)
              .count()
              .sort('label, 'prediction))

display(stream)

// COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's try this again

// COMMAND ----------

val stream = (pipelineModel
              .transform(streamingData)
              .groupBy('label, 'prediction)
              .count()
              .sort('label, 'prediction))

display(stream)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's save our results to a file.

// COMMAND ----------

val streamingView = username.replaceAll("\\W", "")
val checkpointFile = userhome + "/tmp/checkPoint"
dbutils.fs.rm(checkpointFile, true) // Clear out the checkpointing directory

(stream
 .writeStream
 .format("memory")
 .option("checkpointLocation", checkpointFile)
 .outputMode("complete")
 .queryName(streamingView)
 .start())

// COMMAND ----------

display(sql("select * from " + streamingView))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
