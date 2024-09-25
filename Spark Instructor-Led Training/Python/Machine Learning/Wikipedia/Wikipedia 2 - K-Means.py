# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine Learning Pipeline: TF-IDF and K-Means
# MAGIC
# MAGIC **Please run this notebook in a Spark 2.0 cluster.**

# COMMAND ----------

# MAGIC %md
# MAGIC Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
# MAGIC
# MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark 2.0 note:**
# MAGIC
# MAGIC We don't have enough memory to use the Spark 2.0 vectorized Parquet reader. So, let's disable it. Running with it enabled, on CE and with this file, is likely to produce this error:
# MAGIC
# MAGIC ```
# MAGIC org.apache.spark.SparkException: Job aborted due to stage failure: Task 6 in stage 4.0 failed 1 times, most recent failure: Lost task 6.0 in stage 4.0 (TID 10, localhost): java.lang.RuntimeException: Cannot reserve additional contiguous bytes in the vectorized reader (requested = 47478697 bytes). As a workaround, you can disable the vectorized reader by setting spark.sql.parquet.enableVectorizedReader to false.
# MAGIC ```
# MAGIC
# MAGIC **Note that it _must_ be disabled _before_ calling `spark.read.parquet`.**

# COMMAND ----------

# MAGIC %md
# MAGIC As a side note, processing the full dataset on CE will take over an hour.
# MAGIC
# MAGIC To scale things down a bit, we are going to process a sample of the data.
# MAGIC
# MAGIC Once we have our sample, we will repartition it to 128 partitions.

# COMMAND ----------

from pyspark.sql.functions import *

spark.conf.set("spark.sql.shuffle.partitions", "128")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

parquetDir = "/mnt/wikipedia-readonly/en_wikipedia/march_5_snapshot/flattenedParquet_updated3days_30partitions/"

wikiDF = (spark.read
  .parquet(parquetDir)       # Read in the parquet files
  .sample(False, 0.10, 9999) # Take only 10% and use 9999 as our seed
  .repartition(128)          # Repartition (8 works but we grow the data too much as we progress)
)
wikiDF.cache().count()

# COMMAND ----------

wikiDF.printSchema()

# COMMAND ----------

display( wikiDF )

# COMMAND ----------

# MAGIC %md
# MAGIC So that **Kitten**, **kitten** and **KITTEN** all score the same, conver everything to lower case.

# COMMAND ----------

wikiLoweredDF = wikiDF.select(col("*"), lower(col("text")).alias("lowerText"))

wikiLoweredDF.printSchema()

# COMMAND ----------

display( 
  wikiLoweredDF.select("title", "lowerText").limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 1: RegexTokenizer
# MAGIC
# MAGIC Convert the lowerText col to a bag of words

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer

tokenizer = (
  RegexTokenizer()
    .setInputCol("lowerText")
    .setOutputCol("words")
    .setPattern(r'\W+')
)

# COMMAND ----------

wikiWordsDF = tokenizer.transform(wikiLoweredDF)

wikiWordsDF.printSchema()

# COMMAND ----------

(wikiWordsDF
  .select("title", "words")
  .show(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 2: StopWordsRemover
# MAGIC
# MAGIC Remove Stop words

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

remover = (StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("noStopWords")
)

# COMMAND ----------

noStopWordsListDF = remover.transform(wikiWordsDF)

noStopWordsListDF.printSchema()

# COMMAND ----------

(noStopWordsListDF
  .select("title", "words", "noStopWords")
  .show(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 3: HashingTF

# COMMAND ----------

from pyspark.ml.feature import HashingTF

hashingTF = (HashingTF()
  .setInputCol("noStopWords")
  .setOutputCol("hashingTF")
  .setNumFeatures(20000)
)

# COMMAND ----------

featurizedDataDF = hashingTF.transform(noStopWordsListDF)

featurizedDataDF.printSchema()

# COMMAND ----------

(featurizedDataDF
  .select("title", "words", "noStopWords", "hashingTF")
  .show(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 4: IDF
# MAGIC
# MAGIC This will take 15-30 seconds to run.

# COMMAND ----------

from pyspark.ml.feature import IDF

idf = (IDF()
  .setInputCol("hashingTF")
  .setOutputCol("idf")
)
idfModel = idf.fit(featurizedDataDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 5: Normalizer
# MAGIC
# MAGIC A normalizer is a common operation for text classification.
# MAGIC
# MAGIC It simply gets all of the data on the same scale... 
# MAGIC
# MAGIC For example, if one article is much longer than another, it'll normalize the scales for the different features.
# MAGIC
# MAGIC If we don't normalize, an article with more words would be weighted differently

# COMMAND ----------

from pyspark.ml.feature import Normalizer

normalizer = (Normalizer()
  .setInputCol("idf")
  .setOutputCol("features")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 6: k-means

# COMMAND ----------

from pyspark.ml.clustering import KMeans
 
kmeans = (KMeans()
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
  .setK(100)
  .setSeed(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 7: Create The Pipeline

# COMMAND ----------

from pyspark.ml import Pipeline

pipeline = Pipeline().setStages((
    tokenizer, 
    remover, 
    hashingTF, 
    idf, 
    normalizer, 
    kmeans
  ))  

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 8: Fit The Model
# MAGIC
# MAGIC This can take more 1 hour to run!
# MAGIC
# MAGIC Especially if we were to run this on the full set of data.
# MAGIC
# MAGIC For that reason we will skip tihs and load a pre-ran model instead.

# COMMAND ----------

#model = pipeline.fit(wikiLoweredDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 9: Process The Model

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Load the model built from the 5 million articles instead.
# MAGIC
# MAGIC This takes about 30 seconds.

# COMMAND ----------

from pyspark.ml import PipelineModel

model = PipelineModel.load("/mnt/wikipedia-readonly/models/5mill-articles-2.0")

# COMMAND ----------

# MAGIC %md
# MAGIC Now that the model is loaded, we can generate some results.
# MAGIC
# MAGIC This will take anywhere from 1-4 minutes.

# COMMAND ----------

rawPredictionsDF = model.transform(wikiLoweredDF)

rawPredictionsDF.printSchema()

rawPredictionsDF.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Step 10: Evaluate The Result
# MAGIC
# MAGIC Let's take a look at a sample of the data to see if we can see a pattern between predicted clusters and titles.

# COMMAND ----------

display( rawPredictionsDF.limit(10) )

# COMMAND ----------

predictionsDF = rawPredictionsDF.select("title", "prediction")

# COMMAND ----------

display( predictionsDF.limit(10) )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Which cluster/prediction received the most articles?

# COMMAND ----------

display(
  predictionsDF
    .groupBy("prediction")
    .count()
    .orderBy(desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's sample some differnet clusters starting with #31

# COMMAND ----------

display(
  predictionsDF
    .filter("prediction = 31")
    .select("title", "prediction")
)

# COMMAND ----------

display(
  predictionsDF
    .filter("prediction = 32")
    .select("title", "prediction")
)

# COMMAND ----------

display(
  predictionsDF
    .filter("prediction = 45")
    .select("title", "prediction")
    .limit(30)
)

# COMMAND ----------

apacheDF = (predictionsDF
  .filter(col("title").startswith("Apache"))
)
apacheDF.show()

apacheID = (apacheDF
  .first()
  ["prediction"]
)

# COMMAND ----------

display(
  predictionsDF.filter("prediction = " + str(apacheID))
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
