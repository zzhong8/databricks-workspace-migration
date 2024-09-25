// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Baseball ML Pipeline Application
// MAGIC
// MAGIC *We are trying to predict the number of runs (R) that a player scores given their number of runs batted in (RBI).*
// MAGIC
// MAGIC A quick refresher on baseball:
// MAGIC - Runs scored is number of times a player crosses home plate
// MAGIC - Every time a batter gets a hit or makes an out and a runner scores on the play, they get an RBI - except in a few special cases (e.g. batting into a double play, even if the runner scores). 
// MAGIC
// MAGIC Our objective is to predict the number of runs that a player scores in a given season, a continuous valued variable, given their number of RBIs that season. This Machine Learning (ML) task is regression since the label (or target) we are trying to predict is numeric.
// MAGIC
// MAGIC The example data is provided by Kaggle at [The History of Baseball](https://www.kaggle.com/seanlahman/the-history-of-baseball).

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Load Your Data
// MAGIC Now that we understand what we are trying to do, we need to load our data and describe it, explore it and verify it.

// COMMAND ----------

spark.read.text("dbfs:/mnt/training/301/batting.csv").show

// COMMAND ----------

// MAGIC %md
// MAGIC We see that there is an inherent schema in our data (as well as a header), so we are going to use Spark's built in CSV reader.

// COMMAND ----------

var baseballDF = spark.read
                .option("header", true)
                .option("inferSchema", true)
                .csv("dbfs:/mnt/training/301/batting.csv")

display(baseballDF)

// COMMAND ----------

// MAGIC %md
// MAGIC **Schema Definition**
// MAGIC
// MAGIC Our schema definition from Kaggle appears below:
// MAGIC
// MAGIC - player_id = Player ID
// MAGIC - year = year of statistics
// MAGIC - stint = player's stint (order of appearances within a season)
// MAGIC - team_id = Team ID
// MAGIC - league_id = league
// MAGIC - g = games
// MAGIC - ab = at bats
// MAGIC - r = runs scored
// MAGIC - h = hits
// MAGIC - double = doubles
// MAGIC - triple = triples
// MAGIC - hr = homeruns
// MAGIC - rbi = runs batted in
// MAGIC - sb = stolen bases
// MAGIC - cs = caught stealing
// MAGIC - bb = base on balls (walk)
// MAGIC - so = strikeouts
// MAGIC - ibb = intentional base on balls
// MAGIC - hbp = hit by pitch
// MAGIC - sh = sacrifice hits
// MAGIC - sf = sacrifice flies
// MAGIC - g_idp = induced double plays
// MAGIC
// MAGIC R is our label or target. This is the value we are trying to predict given the number of RBIs.

// COMMAND ----------

// MAGIC %md
// MAGIC Use a [selectExpr](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset) to extract `rbi` and `r` from `baseballDF` and rename them `features` and `label`, respectively. 
// MAGIC
// MAGIC Then create a temporary view with just these two features called `baseball` using [createOrReplaceTempView](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset).

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
baseballDF.selectExpr(<FILL_IN>).createOrReplaceTempView(<FILL_IN>)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use a SQL query to display our `baseball` view, and visualize it with a scatter plot.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM baseball

// COMMAND ----------

// MAGIC %md
// MAGIC We can see that there is a linear relationship between RBIs and runs. Let's compute the correlation between these two variables with `corr()`.

// COMMAND ----------

import org.apache.spark.sql.DataFrameStatFunctions

// Option 1: Use the DataFrame
println(baseballDF.stat.corr("r", "rbi"))

// Option 2: Use the View
println(spark.table("baseball").stat.corr("features", "label"))

// COMMAND ----------

// MAGIC %md
// MAGIC To get basic statistics of each column in our `baseball` view, use `describe()`.

// COMMAND ----------

display(spark.table("baseball").describe())

// COMMAND ----------

// MAGIC %md
// MAGIC Due to the linear relationship between runs and RBIs, linear regression is a natural model to fit to our data. 
// MAGIC
// MAGIC But first, we need to do some feature pre-processing. Let's read our `baseball` table using `spark.read.table()`, and assign the resulting DataFrame to the variable `baseballDF`.
// MAGIC
// MAGIC Then, remove any rows that have missing values in either the `features` or `label` column.

// COMMAND ----------

val cleanBaseballDF = spark.read.table("baseball").na.drop()

// COMMAND ----------

// MAGIC %md
// MAGIC Before we can use our `baseballDF` to build a linear regression model, we need to split our data into a training and test set to be able to evaluate our model at the end. We use the [randomSplit()](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset) method to split our data, and include a seed for reproducibility.
// MAGIC
// MAGIC We will use the the training set to build our model, and our test set to evaluate our model (how well it generalizes to new data).
// MAGIC
// MAGIC ![trainTest](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/TrainTestSplit.png)

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
// First let's hold out 20% of our data for testing and leave 80% for training. Use the set seed.
val seed = 42
var Array(testDF, trainingDF) = cleanBaseballDF.randomSplit(<FILL_IN>, seed=<FILL_IN>)

println(testDF.count, trainingDF.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Because we will be frequently accessing `testDF` and `trainingDF`, let's cache both of these datasets for increased performance.

// COMMAND ----------

testDF.cache
trainingDF.cache

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have done all of our pre-processing, let's create a Linear Regression estimator with the default parameters using [LinearRegression()](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.LinearRegression).
// MAGIC
// MAGIC Then fit the linear regression model to `trainingDF` using `fit()`.

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel

val lr = new LinearRegression()
val lrModel = lr.fit(trainingDF)

// COMMAND ----------

// MAGIC %md
// MAGIC What was this error message? Well, Spark's Linear Regression model was expecting a column called `features`, and it to be of type `VectorUDT`, but our `features` column is stored as a double (let's check this by printing the schema)

// COMMAND ----------

trainingDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC One way to convert our `features` column to be a vector is to create a [user defined function (UDF)](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf), `oneElementVec`, to convert our features into a dense vector of length one. 
// MAGIC
// MAGIC We need to apply this UDF to the `features` column of both `trainingDF` and `testDF`.

// COMMAND ----------

import org.apache.spark.ml.linalg._

spark.udf.register("oneElementVec", (d:Double) => Vectors.dense(Array(d)))

trainingDF = trainingDF.selectExpr("label", "oneElementVec(features) as features")
testDF = testDF.selectExpr("label", "oneElementVec(features) as features")

trainingDF.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have our features in the proper format, we are ready to fit a linear regression model.

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel

val lr = new LinearRegression()
val lrModel = lr.fit(trainingDF)

// COMMAND ----------

// MAGIC %md
// MAGIC We can extract the weight for RBIs from our fitted linear regression model. In the next lab, we will see how this weight changes in the presence of other predictors (e.g. homeruns, triples, etc).

// COMMAND ----------

lrModel.coefficients(0)

// COMMAND ----------

// MAGIC %md
// MAGIC We are now going to transform our test set to get predictions. It will append a `prediction` column to `testDF` in the new dataframe `predictionsAndLabelsDF`.

// COMMAND ----------

val predictionsAndLabelsDF = lrModel.transform(testDF)

//If want to see how it performed on the non-zero labels, sort it in decreasing order
display(predictionsAndLabelsDF.sort(predictionsAndLabelsDF("label").desc))

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have built a model, we want to compute some metrics to evaluate how well our model performed. We will SparkML's [RegressionEvaluator()](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.RegressionEvaluator), and examine the different parameters by calling `explainParams()`.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
val eval = new RegressionEvaluator()
println(eval.explainParams())

// COMMAND ----------

// MAGIC %md
// MAGIC A common metric to evaluate model performance is RMSE (root mean squared error). RMSE computes the square root of the average squared residual (distance from our prediction to the true label). The key idea is that you want to penalize predictions that are far away from the true label more heavily than ones that are closer to the true label.
// MAGIC
// MAGIC ![rmse](https://cdn-images-1.medium.com/max/800/1*9hQVcasuwx5ddq_s3MFCyw.gif)

// COMMAND ----------

// Default metric is RMSE
eval.evaluate(predictionsAndLabelsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC There are other metrics we can use to evaluate our model, such as R2. 
// MAGIC
// MAGIC R2 = Explained variation / Total variation, and measures how close the data are to the fitted regression line. It is a numeric value between 0 and 1, and the closer it is to 1, the better the model.
// MAGIC
// MAGIC Change the regression evaluator metric to `R2`, and calculate the R2 of `predictionsAndLabelsDF`.

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
eval.<FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC We can see that with just one predictor (RBI), we capture a lot of the variation of the response data (runs). In the next lab, we will build a Linear Regression model using more than one predictor.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
