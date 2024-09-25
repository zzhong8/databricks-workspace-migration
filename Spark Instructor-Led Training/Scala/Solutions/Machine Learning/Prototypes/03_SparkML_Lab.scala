// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Decision Trees
// MAGIC ### Analyzing a bike sharing dataset
// MAGIC
// MAGIC This notebook demonstrates creating an ML Pipeline to preprocess a dataset, train a Machine Learning model, save the model, and make predictions.
// MAGIC
// MAGIC **Data**: The dataset contains bike rental info from 2011 and 2012 in the Capital bikeshare system, plus additional relevant information such as weather.  This dataset is from Fanaee-T and Gama (2013) and is hosted by the [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset).
// MAGIC
// MAGIC **Goal**: We want to learn to predict bike rental counts (per hour) from information such as day of the week, weather, season, etc.  Having good predictions of customer demand allows a business or service to prepare and increase supply as needed.  
// MAGIC
// MAGIC In the next lab, we will also demonstrate hyperparameter tuning using cross-validation, as well as tree ensembles to fine-tune and improve our ML model.
// MAGIC
// MAGIC [Decision Tree Regressor (Scala)](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.DecisionTreeRegressor)
// MAGIC
// MAGIC [Decision Tree Regressor (Python)](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor)

// COMMAND ----------

// MAGIC %run "./Includes/Classroom_Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load and understand the data
// MAGIC
// MAGIC We begin by loading our data, which is stored in [Comma-Separated Value (CSV) format](https://en.wikipedia.org/wiki/Comma-separated_values). 
// MAGIC
// MAGIC Use the `spark.read.csv` method to read the data and set a few options:
// MAGIC - `header`: set to true to indicate that the first line of the CSV data file is a header
// MAGIC - `inferSchema`: set to true to infer the datatypes
// MAGIC - The file is located at `/databricks-datasets/bikeSharing/data-001/hour.csv`.
// MAGIC
// MAGIC [DataFrame Reader (Scala)](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrameReader)
// MAGIC
// MAGIC [DataFrame Reader (Python)](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)

// COMMAND ----------

// ANSWER
var df = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/databricks-datasets/bikeSharing/data-001/hour.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's cache the DataFrame so subsequent uses will be able to read from memory, instead of re-reading the data from disk.

// COMMAND ----------

df.cache

// COMMAND ----------

// MAGIC %md
// MAGIC __Question__: Is the DataFrame in the Storage tab of the Spark UI?

// COMMAND ----------

// MAGIC %md
// MAGIC #### Data description
// MAGIC
// MAGIC From the [UCI ML Repository description](http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset), we have the following schema.
// MAGIC
// MAGIC **Feature columns**:
// MAGIC * dteday: date
// MAGIC * season: season (1:spring, 2:summer, 3:fall, 4:winter)
// MAGIC * yr: year (0:2011, 1:2012)
// MAGIC * mnth: month (1 to 12)
// MAGIC * hr: hour (0 to 23)
// MAGIC * holiday: whether day is holiday or not
// MAGIC * weekday: day of the week
// MAGIC * workingday: if day is neither weekend nor holiday is 1, otherwise is 0.
// MAGIC * weathersit: 
// MAGIC   * 1: Clear, Few clouds, Partly cloudy, Partly cloudy
// MAGIC   * 2: Mist + Cloudy, Mist + Broken clouds, Mist + Few clouds, Mist
// MAGIC   * 3: Light Snow, Light Rain + Thunderstorm + Scattered clouds, Light Rain + Scattered clouds
// MAGIC   * 4: Heavy Rain + Ice Pallets + Thunderstorm + Mist, Snow + Fog
// MAGIC * temp: Normalized temperature in Celsius. The values are derived via `(t-t_min)/(t_max-t_min)`, `t_min=-8`, `t_max=+39` (only in hourly scale)
// MAGIC * atemp: Normalized feeling temperature in Celsius. The values are derived via `(t-t_min)/(t_max-t_min)`, `t_min=-16`, `t_max=+50` (only in hourly scale)
// MAGIC * hum: Normalized humidity. The values are divided to 100 (max)
// MAGIC * windspeed: Normalized wind speed. The values are divided to 67 (max)
// MAGIC
// MAGIC **Label columns**:
// MAGIC * casual: count of casual users
// MAGIC * registered: count of registered users
// MAGIC * cnt: count of total rental bikes including both casual and registered
// MAGIC
// MAGIC **Extraneous columns**:
// MAGIC * instant: record index
// MAGIC
// MAGIC For example, the first row is a record of hour 0 on January 1, 2011---and apparently 16 people rented bikes around midnight!

// COMMAND ----------

// MAGIC %md
// MAGIC Let's look at a subset of our data. We'll use the [sample()](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset) method to sample 10% of the DataFrame without replacement, and call `display()` on the resulting DataFrame.

// COMMAND ----------

// ANSWER
display(df.sample(false, 0.1))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Preprocess data
// MAGIC
// MAGIC So what do we need to do to get our data ready for Machine Learning?
// MAGIC
// MAGIC *Recall our goal*: We want to learn to predict the count of bike rentals (the `cnt` column).  We refer to the count as our target "label".
// MAGIC
// MAGIC *Features*: What can we use as features to predict the `cnt` label?  All the columns except `cnt`, and a few exceptions:
// MAGIC * The `cnt` column we want to predict equals the sum of the `casual` + `registered` columns.  We will remove the `casual` and `registered` columns from the data to make sure we do not use them to predict `cnt`.  (*Warning: This is a danger in careless Machine Learning.  Make sure you do not "cheat" by using information you will not have when making predictions*)
// MAGIC * date column `dteday`: We could keep it, but it is well-represented by the other date-related columns `season`, `yr`, `mnth`, and `weekday`.  We will discard it.
// MAGIC * `holiday` and `weekday`: These features are highly correlated with the `workingday` column.
// MAGIC * row index column `instant`: This is a useless column to us.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's drop the columns `instant`, `dteday`, `casual`, `holiday`, `weekday`, and `registered` from our DataFrame.

// COMMAND ----------

// ANSWER
df = df.drop("instant", "dteday", "casual", "registered", "holiday", "weekday")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have the columns we care about, let's print the schema of our dataset to see the type of each column using `printSchema()`.

// COMMAND ----------

df.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Train/Test Split
// MAGIC
// MAGIC Our final data preparation step will be to split our dataset into separate training and test sets.
// MAGIC
// MAGIC Use `randomSplit` to split the data such that 70% of the data is reserved for training, and the remaining 30% for testing. Use the set `seed` for reproducability (i.e. if you re-run this notebook or compare with your neighbor, you will get the same results).
// MAGIC
// MAGIC Python: [randomSplit()](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit)
// MAGIC
// MAGIC Scala: [randomSplit()](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset)

// COMMAND ----------

// ANSWER
val seed = 42
val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=seed)
println(s"We have ${trainDF.count} training examples and ${testDF.count} test examples.")

assert (trainDF.count() == 12197)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Visualize our data
// MAGIC
// MAGIC Now that we have preprocessed our features and prepared a training dataset, we can quickly visualize our data to get a sense of whether the features are meaningful.
// MAGIC
// MAGIC Calling `display()` on a DataFrame in Databricks and clicking the plot icon below the table will let you draw and pivot various plots.  See the [Visualizations section of the Databricks Guide](https://docs.databricks.com/user-guide/visualizations/index.html) for more ideas.
// MAGIC
// MAGIC We want to compare bike rental counts versus hour of the day.  As one might expect, rentals are low during the night, and they peak in the morning (8am) and in the early evening (6pm).  This indicates the `hr` feature is useful and can help us predict our label `cnt`.  
// MAGIC
// MAGIC Select the `hr` and `cnt` columns from `trainDF`, and visualize it as a bar chart (you might need to adjust the plot options).

// COMMAND ----------

// ANSWER
display(trainDF.select("hr", "cnt"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Train a Machine Learning Pipeline
// MAGIC
// MAGIC Let's learn a ML model to predict the `cnt` of bike rentals given a single `features` column of feature vectors. 
// MAGIC
// MAGIC We will put together a simple Pipeline with the following stages:
// MAGIC * `VectorAssembler`: Assemble the feature columns into a feature vector.
// MAGIC * `VectorIndexer`: Identify columns which should be treated as categorical.  This is done heuristically, identifying any column with a small number of distinct values as being categorical.  For us, this will be the `yr` (2 values), `season` (4 values), `holiday` (2 values), `workingday` (2 values), and `weathersit` (4 values).
// MAGIC * `DecisionTreeRegressor`: This will build a decision tree to learn how to predict rental counts from the feature vectors.

// COMMAND ----------

// MAGIC %md
// MAGIC First, we define the feature processing stages of the Pipeline:
// MAGIC * Assemble feature columns into a feature vector.
// MAGIC * Identify categorical features, and index them.
// MAGIC
// MAGIC ![Image of feature processing](http://training.databricks.com/databricks_guide/2-features.png)

// COMMAND ----------

// MAGIC %md
// MAGIC Steps:
// MAGIC - To create our feature vector, we start by selecting all of the feature columns and calling it `featuresCols`.
// MAGIC - Use [VectorAssembler](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler), and set `inputCols` to `featureCols`, and the `outputCols` as `rawFeatures`. This concatenates all feature columns into a single feature vector into the new column "rawFeatures".
// MAGIC - Use [VectorIndexer](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorIndexer) to identify categorical features in our `rawFeatures` and index them. If any column has `maxCategories` or fewer distinct values, then it is treated as a categorical variable.

// COMMAND ----------

import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}

val featuresCols = df.columns.dropRight(1) //Removes "cnt"

val vectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures")

val vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(4)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Decision Tree Regressor
// MAGIC Second, we define the model training stage of the Pipeline. [Decision Tree Regressor](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.DecisionTreeRegressor) takes feature vectors and labels as input and learns to predict labels of new examples.
// MAGIC
// MAGIC Let's take a look at some of the default parameters.

// COMMAND ----------

import org.apache.spark.ml.regression.DecisionTreeRegressor

val dt = new DecisionTreeRegressor()
println(dt.explainParams)

// COMMAND ----------

// MAGIC %md
// MAGIC DecisionTreeRegressor expects a `labelCol` called `label`, but in our DataFrame we don't have a label column. Let's tell the DecisionTreeRegressor that the label column is called `cnt`.
// MAGIC
// MAGIC Use `dt.setLabelCol("")`

// COMMAND ----------

// ANSWER
dt.setLabelCol("cnt")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Pipeline
// MAGIC Now let's wrap all of these stages into a Pipeline.

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline()
                  .setStages(Array(vectorAssembler, vectorIndexer, dt))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Train
// MAGIC
// MAGIC Train the pipeline model to run all the steps in the pipeline.

// COMMAND ----------

val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's visualize the decision tree

// COMMAND ----------

import org.apache.spark.ml.regression.DecisionTreeRegressionModel

val tree = pipelineModel.stages(2).asInstanceOf[DecisionTreeRegressionModel]
display(tree)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Save Model
// MAGIC
// MAGIC Let's go ahead and save this model.

// COMMAND ----------

val fileName = userhome + "/tmp/MyPipeline"
pipelineModel.write.overwrite().save(fileName)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's read this model back in.

// COMMAND ----------

import org.apache.spark.ml.PipelineModel

val savedModel = PipelineModel.load(fileName)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Predictions
// MAGIC
// MAGIC Next, apply the saved model trained to the test set.

// COMMAND ----------

// ANSWER
val predictionsDF = savedModel.transform(testDF)

display(predictionsDF.select("cnt", "prediction"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Evaluate
// MAGIC
// MAGIC Next, we'll use [RegressionEvaluator](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.RegressionEvaluator) to assess the results. Start by looking at the docs and importing the correct package.
// MAGIC
// MAGIC Next, use RMSE as your evaluation metric, and don't forget to set the correct label column.
// MAGIC
// MAGIC Finally, compute the RMSE of your test dataset.

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator().setLabelCol("cnt")

val rmse = evaluator.evaluate(predictionsDF)
println("Test Accuracy = $rmse")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Next Steps
// MAGIC
// MAGIC Wow! Our RMSE is really high. In the next lab, we will cover ways to decrease the RMSE of our model, including: cross validation, hyperparameter tuning, and ensembles of trees.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
