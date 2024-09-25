// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Airbnb in San Francicsco
// MAGIC ![Airbnb logo](http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png)<br>
// MAGIC The dataset we'll be working with is from Airbnb rentals in San Francisco<br>
// MAGIC
// MAGIC You can find more information here:<br>
// MAGIC http://insideairbnb.com/get-the-data.html

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."
// MAGIC
// MAGIC If you are using Scala: Install this package before we start `databricks:xgboost-linux64:0.8-spark2.3-s_2.11` (Distributed XGBoost). Restart the cluster.
// MAGIC
// MAGIC If you are using Python: Install this package before we start `Azure:mmlspark:0.12` (Light GBM). Restart the cluster.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom_Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Reading the data

// COMMAND ----------

val filePath = "/mnt/training/airbnb/sf-listings/sf-listings-clean.parquet"

val initDF = spark.read.parquet(filePath)

// COMMAND ----------

display(initDF.describe())

// COMMAND ----------

// MAGIC %md
// MAGIC In the previous lab we preserved the `price_raw` attribute, but we don't need it for modeling. Let's drop it.

// COMMAND ----------

val airbnbDF = initDF.drop("price_raw")
airbnbDF.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC Let's make sure we don't have any null values in our DataFrame

// COMMAND ----------

val recordCount = airbnbDF.count()
val noNullsRecordCount = airbnbDF.na.drop().count()

println(s"We have ${recordCount - noNullsRecordCount} records that contain null values.")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Exploratory data analysis

// COMMAND ----------

// MAGIC %md
// MAGIC Let's make some histograms of our data to explore it (change the number of bins to 300).  

// COMMAND ----------

airbnbDF.createOrReplaceTempView("airbnb")
display(sql("select price from airbnb"))

// COMMAND ----------

// MAGIC %md
// MAGIC **Exercise**: Is this a logNormal distribution? Take the log of price and check the histogram.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions._

display(airbnbDF.select(log("price")))

// COMMAND ----------

// MAGIC %md
// MAGIC **Exercise**
// MAGIC - Plot `price` vs `bedrooms`
// MAGIC - Plot `price` vs `accomodates`

// COMMAND ----------

// ANSWER
display(airbnbDF)

// COMMAND ----------

// ANSWER
display(airbnbDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the distribution of some of our categorical features

// COMMAND ----------

display(airbnbDF.groupBy("room_type").count())

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise
// MAGIC
// MAGIC Which neighborhoods have the highest number of rentals? Display the neighbourhoods and their associated count in descending order.

// COMMAND ----------

// ANSWER
display(airbnbDF.groupBy("neighbourhood_cleansed").count().orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### How much does the price depend on the location?

// COMMAND ----------

// MAGIC %python
// MAGIC mapDF = spark.table("airbnb")
// MAGIC v = ",\n".join(map(lambda row: "[{}, {}, {}]".format(row[0], row[1], row[2]), mapDF.select(col("latitude"),col("longitude"),col("price")/600).collect()))
// MAGIC displayHTML("""
// MAGIC <html>
// MAGIC <head>
// MAGIC  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.3.1/dist/leaflet.css"
// MAGIC    integrity="sha512-Rksm5RenBEKSKFjgI3a41vrjkw4EVPlJ3+OiI65vTjIdo9brlAacEuKOiQ5OFh7cOI1bkDwLqdLw3Zg0cRJAAQ=="
// MAGIC    crossorigin=""/>
// MAGIC  <script src="https://unpkg.com/leaflet@1.3.1/dist/leaflet.js"
// MAGIC    integrity="sha512-/Nsx9X4HebavoBvEBuyp3I7od5tA0UzAxs+j83KgC8PU0kgB4XiK4Lfe4y4cgBtaRJQEIFCW+oC506aPT2L1zw=="
// MAGIC    crossorigin=""></script>
// MAGIC  <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
// MAGIC </head>
// MAGIC <body>
// MAGIC     <div id="mapid" style="width:700px; height:500px"></div>
// MAGIC   <script>
// MAGIC   var mymap = L.map('mapid').setView([37.7587,-122.4486], 12);
// MAGIC   var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
// MAGIC     attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
// MAGIC }).addTo(mymap);
// MAGIC   var heat = L.heatLayer([""" + v + """], {radius: 25}).addTo(mymap);
// MAGIC   </script>
// MAGIC   </body>
// MAGIC   </html>
// MAGIC """)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Train Linear Regression Model
// MAGIC
// MAGIC Before we can apply the linear regression model, we will need to do some data preparation, such as one hot encoding our categorical variables using `StringIndexer` and `OneHotEncoderEstimator`.
// MAGIC
// MAGIC Let's start by taking a look at all of our columns, and determine which ones are categorical.

// COMMAND ----------

airbnbDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) StringIndexer
// MAGIC
// MAGIC [Python Docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
// MAGIC
// MAGIC [Scala Docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.StringIndexer)

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

println(new StringIndexer().explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC We will StringIndex all categorical features (`neighbourhood_cleansed`, `room_type`, `zipcode`, `property_type`, `bed_type`) and set `handleInvalid` to `skip`.

// COMMAND ----------

val iNeighbourhood = new StringIndexer().setInputCol("neighbourhood_cleansed").setOutputCol("cat_neighborhood").setHandleInvalid("skip")
val iRoomType = new StringIndexer().setInputCol("room_type").setOutputCol("cat_room_type").setHandleInvalid("skip")
val iZipCode = new StringIndexer().setInputCol("zipcode").setOutputCol("cat_zip_code").setHandleInvalid("skip")
val iPropertyType = new StringIndexer().setInputCol("property_type").setOutputCol("cat_property_type").setHandleInvalid("skip")
val iBedType= new StringIndexer().setInputCol("bed_type").setOutputCol("cat_bed_type").setHandleInvalid("skip")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) OneHotEncoder
// MAGIC
// MAGIC **EXERCISE**: One hot encode all previously indexed categorical features

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.feature.OneHotEncoderEstimator

val oneHotEnc = new OneHotEncoderEstimator()
oneHotEnc.setInputCols(Array("cat_neighborhood", "cat_room_type", "cat_zip_code", "cat_property_type", "cat_bed_type"))
oneHotEnc.setOutputCols(Array("vec_neighborhood", "vec_room_type", "vec_zip_code", "vec_property_type", "vec_bed_type"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Train/Test Split
// MAGIC
// MAGIC Let's set aside 20% of our data for the test set.

// COMMAND ----------

val seed = 273
val Array(testDF, trainDF) = airbnbDF.randomSplit(Array(0.20, 0.80), seed=seed)

println(testDF.count, trainDF.count)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Pipeline
// MAGIC
// MAGIC Let's build some of the transformations we'll need in our pipeline, such as `VectorAssembler` and `LinearRegression`.

// COMMAND ----------

val featureCols = Array(
 "host_total_listings_count",
 "accommodates",
 "bathrooms",
 "bedrooms",
 "beds",
 "minimum_nights",
 "number_of_reviews",
 "review_scores_rating",
 "review_scores_accuracy",
 "review_scores_cleanliness",
 "review_scores_checkin",
 "review_scores_communication",
 "review_scores_location",
 "review_scores_value",
 "vec_neighborhood", 
 "vec_room_type", 
 "vec_zip_code", 
 "vec_property_type", 
 "vec_bed_type")

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

// COMMAND ----------

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

val lr = new LinearRegression().setLabelCol("price").setFeaturesCol("features")

println(lr.explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's put this all together in a pipeline!

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val lrPipeline = new Pipeline()

// Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
lrPipeline.setStages(Array(iNeighbourhood, iRoomType, iZipCode, iPropertyType, iBedType, oneHotEnc, assembler, lr))

// Pipelines are themselves Estimators -- so to use them we call fit:
val lrPipelineModel = lrPipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's apply the model to our held-out test set.

// COMMAND ----------

val predictedDF = lrPipelineModel.transform(testDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Evaluate the Model

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator()
println(evaluator.explainParams)

// COMMAND ----------

evaluator.setLabelCol("price")
evaluator.setPredictionCol("prediction")

val metricName = evaluator.getMetricName
val metricVal = evaluator.evaluate(predictedDF)

println(s"$metricName: $metricVal")

// COMMAND ----------

// MAGIC %md
// MAGIC We could wrap this into a function to make it easier to get the output of multiple metrics.

// COMMAND ----------

def printEval(df:org.apache.spark.sql.Dataset[Row], labelCol:String = "price", predictionCol:String = "prediction"):Unit = {
  val evaluator = new RegressionEvaluator()
  evaluator.setLabelCol(labelCol)
  evaluator.setPredictionCol(predictionCol)
  
  val rmse = evaluator.setMetricName("rmse").evaluate(df)
  val r2 = evaluator.setMetricName("r2").evaluate(df)
  println(s"RMSE: $rmse")
  println(s"R2: $r2")
}

// COMMAND ----------

printEval(predictedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Log-Normal
// MAGIC Hmmmm... our RMSE was really high. How could we lower it? Let's try converting our `price` target to a logarithmic scale.

// COMMAND ----------

val logTrainDF = trainDF.withColumn("log_price", log(col("price")))
val logTestDF = testDF.withColumn("log_price", log(col("price")))

// COMMAND ----------

lr.setLabelCol("log_price")
val logPipelineModel = lrPipeline.fit(logTrainDF)

val predictedDF = logPipelineModel.transform(logTestDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Exponentiate
// MAGIC
// MAGIC In order to interpret our RMSE, we need to convert our predictions back from logarithmic scale.

// COMMAND ----------

val expDF = predictedDF.withColumn("exp_pred", exp(col("prediction")))
printEval(expDF, "price", "exp_pred")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) XGBoost
// MAGIC
// MAGIC Our RMSE decreased significantly from switching to log-normal scale!
// MAGIC
// MAGIC We could play around with linear regression some more on this dataset, but perhaps it isn't the right algorithm for our dataset (notice the RMSE increases . Let's look at XGBoost (install this Spark package: `databricks:xgboost-linux64:0.8-spark2.3-s_2.11`). XGBoost is one of the most winning Kaggle submission methods.
// MAGIC
// MAGIC This section is only available in Scala because there is no distributed Python API for XGBoost in Spark yet.

// COMMAND ----------

import ml.dmlc.xgboost4j.scala.spark._

val paramMap = List("num_round" -> 100, "nworkers" -> 8, "objective" -> "reg:linear", "eta" -> 0.1, "max_leaf_nodes" -> 50, "early_stopping_rounds" -> 10, "seed" -> 42, "labelCol" -> "log_price").toMap

val xgboostEstimator = new XGBoostEstimator(paramMap)

val xgboostPipeline = new Pipeline().setStages(Array(iNeighbourhood, iRoomType, iZipCode, iPropertyType, iBedType, oneHotEnc, assembler, xgboostEstimator))

val xgboostPipelineModel = xgboostPipeline.fit(logTrainDF)
val xgboostLogPredictedDF = xgboostPipelineModel.transform(logTestDF)

val expXgboostDF = xgboostLogPredictedDF.withColumn("exp_pred", exp(col("prediction")))
printEval(expXgboostDF, "price", "exp_pred")
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Light GBM
// MAGIC Perhaps try a different algorithm? Let's look at Light GBM (install this Spark package: `Azure:mmlspark:0.12`). Light GBM is an alternative gradient boosting technique to XGBoost to significantly speed up the computation.
// MAGIC
// MAGIC This section is only available in Python.

// COMMAND ----------

// MAGIC %python
// MAGIC from mmlspark import LightGBMRegressor
// MAGIC
// MAGIC gbmModel = LightGBMRegressor(learningRate=.1,
// MAGIC                            numIterations=100,
// MAGIC                            numLeaves=50,
// MAGIC                            labelCol="log_price")
// MAGIC
// MAGIC gbmPipeline = Pipeline(stages = [iNeighbourhood, iRoomType, iZipCode, iPropertyType, iBedType, oneHotEnc, assembler, gbmModel])
// MAGIC
// MAGIC gbmPipelineModel = gbmPipeline.fit(logTrainDF)
// MAGIC gbmLogPredictedDF = gbmPipelineModel.transform(logTestDF)
// MAGIC
// MAGIC expGbmDF = gbmLogPredictedDF.withColumn("exp_pred", exp(col("prediction")))
// MAGIC printEval(expGbmDF, "price", "exp_pred")

// COMMAND ----------

// MAGIC %md
// MAGIC Wow! The gradient boosted trees did much better than linear regression!
// MAGIC
// MAGIC Go back through this notebook and try to see how low you can get the RMSE!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
