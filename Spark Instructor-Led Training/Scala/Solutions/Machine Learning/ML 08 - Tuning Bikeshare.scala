// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Machine Learning (ML) Pipelines
// MAGIC ## Analyzing a bike sharing dataset
// MAGIC
// MAGIC This notebook demonstrates creating an ML Pipeline to preprocess a dataset, train a Machine Learning model, and make predictions.
// MAGIC
// MAGIC **Data**: The dataset contains bike rental info from 2011 and 2012 in the Capital bikeshare system, plus additional relevant information such as weather.  This dataset is from Fanaee-T and Gama (2013) and is hosted by the [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset).
// MAGIC
// MAGIC **Goal**: We want to learn to predict bike rental counts (per hour) from information such as day of the week, weather, season, etc.  Having good predictions of customer demand allows a business or service to prepare and increase supply as needed.
// MAGIC
// MAGIC **Approach**: We will use Spark ML Pipelines, which help users piece together parts of a workflow such as feature processing and model training.  We will also demonstrate [model selection (a.k.a. hyperparameter tuning)](https://en.wikipedia.org/wiki/Model_selection) using [Cross Validation](https://en.wikipedia.org/wiki/Cross-validation_&#40;statistics&#41;) in order to fine-tune and improve our ML model.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load and understand the data
// MAGIC
// MAGIC We begin by loading our data, which is stored in [Comma-Separated Value (CSV) format](https://en.wikipedia.org/wiki/Comma-separated_values).  For that, we use the native Spark CSV reader, which creates a [Spark DataFrame](http://spark.apache.org/docs/latest/sql-programming-guide.html) containing the data.  We also cache the data so that we only read it from disk once.
// MAGIC
// MAGIC Use the spark.read.csv method to read the data and set a few options:
// MAGIC - `header`: set to true to indicate that the first line of the CSV data file is a header
// MAGIC - `inferSchema`: set to true to infer the datatypes
// MAGIC - The file is located at `/mnt/training/bikeSharing/data-001/hour.csv`.
// MAGIC
// MAGIC Call cache on the DataFrame to persist it in memory the first time it is used.  The following uses will be able to read from memory, instead of re-reading the data from disk.

// COMMAND ----------

// ANSWER
var df = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/mnt/training/bikeSharing/data-001/hour.csv")
df.cache

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
// MAGIC Let's look at a subset of our data. Use the `sample()` method to sample 10% of the DataFrame without replacement, and call `display()` on the resulting DataFrame.
// MAGIC
// MAGIC Python: [sample()](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample)
// MAGIC
// MAGIC Scala: [sample()](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset)

// COMMAND ----------

// ANSWER
display(df.sample(false, 0.1))

// COMMAND ----------

// MAGIC %md
// MAGIC This dataset is nicely prepared for Machine Learning: values such as weekday are already indexed, and all of the columns except for the date (`dteday`) are numeric.
// MAGIC
// MAGIC Let's print how many rows we have in our dataset 
// MAGIC
// MAGIC HINT: Use `.count()`

// COMMAND ----------

// ANSWER
println(s"Our dataset has ${df.count} rows.")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Preprocess data
// MAGIC
// MAGIC So what do we need to do to get our data ready for Machine Learning?
// MAGIC
// MAGIC *Recall our goal*: We want to learn to predict the count of bike rentals (the `cnt` column).  We refer to the count as our target "label".
// MAGIC
// MAGIC *Features*: What can we use as features (info describing each row) to predict the `cnt` label?  We can use the rest of the columns, with a few exceptions:
// MAGIC * Some of the columns contain duplicate information.  For example, the `cnt` column we want to predict equals the sum of the `casual` + `registered` columns.  We will remove the `casual` and `registered` columns from the data to make sure we do not use them to predict `cnt`.  (*Warning: This is a danger in careless Machine Learning.  Make sure you do not "cheat" by using information you will not have when making predictions.  In this prediction task, we will not have `casual` or `registered` info available when we want to make predictions about the future.*)
// MAGIC * date column `dteday`: We could keep it, but it is well-represented by the other date-related columns `season`, `yr`, `mnth`, and `weekday`.  We will discard it.
// MAGIC * row index column `instant`: This is a useless column to us.
// MAGIC
// MAGIC Terminology: *Examples* are rows of our dataset.  Each example contains the label to predict and its associated features.

// COMMAND ----------

// MAGIC %md
// MAGIC Drop the columns `instant`, `dteday`, `casual`, and `registered` from our DataFrame.

// COMMAND ----------

// ANSWER
df = df.drop("instant", "dteday", "casual", "registered")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have the columns we care about, let's print the schema of our dataset to see the type of each column using `printSchema()`.

// COMMAND ----------

df.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC #### Split data into training and test sets
// MAGIC
// MAGIC Our final data preparation step will be to split our dataset into separate training and test sets.  We can train and tune our model as much as we like on the training set, as long as we do not look at the test set.  After we have a good model (based on the training set), we can validate it on the held-out test set in order to know with high confidence our model will make good predictions on future (unseen) data.
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

assert((trainDF.count + testDF.count) == df.count)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Visualize our data
// MAGIC
// MAGIC Now that we have preprocessed our features and prepared a training dataset, we can quickly visualize our data to get a sense of whether the features are meaningful.
// MAGIC
// MAGIC Calling `display()` on a DataFrame in Databricks and clicking the plot icon below the table will let you draw and pivot various plots.
// MAGIC
// MAGIC We want to compare bike rental counts versus hour of the day.  As one might expect, rentals are low during the night, and they peak in the morning (8am) and in the early evening (6pm).  This indicates the `hr` feature is useful and can help us predict our label `cnt`.  
// MAGIC
// MAGIC Select the `hr` and `cnt` columns from `trainDF`, and visualize it as a bar chart (you might need to adjust the plot options).

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC See also <a href="https://docs.databricks.com/user-guide/visualizations/index.html" target="_blank">Visualizations section of the Databricks Guide</a>.

// COMMAND ----------

// ANSWER
display(trainDF.select("hr", "cnt"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Train a Machine Learning Pipeline
// MAGIC
// MAGIC Now that we have understood our data and prepared it as a DataFrame with numeric values, let's learn a ML model to predict bike sharing rentals in the future.  Most ML algorithms expect to predict a single "label" column (`cnt` for our dataset) using a single "features" column of feature vectors.  For each row in our data, the feature vector should describe what we know: weather, day of the week, etc., and the label should be what we want to predict (`cnt`).
// MAGIC
// MAGIC We will put together a simple Pipeline with the following stages:
// MAGIC * `VectorAssembler`: Assemble the feature columns into a feature vector.
// MAGIC * `VectorIndexer`: Identify columns which should be treated as categorical.  This is done heuristically, identifying any column with a small number of distinct values as being categorical.  For us, this will be the `yr` (2 values), `season` (4 values), `holiday` (2 values), `workingday` (2 values), and `weathersit` (4 values).
// MAGIC * `GBTRegressor`: This will use the [Gradient-Boosted Trees (GBT)](https://en.wikipedia.org/wiki/Gradient_boosting) algorithm to learn how to predict rental counts from the feature vectors.
// MAGIC * `CrossValidator`: The GBT algorithm has several [hyperparameters](https://en.wikipedia.org/wiki/Hyperparameter_optimization), and tuning them to our data can improve accuracy.  We will do this tuning using Spark's [Cross Validation](https://en.wikipedia.org/wiki/Cross-validation_&#40;statistics&#41;) framework, which automatically tests a grid of hyperparameters and chooses the best.
// MAGIC
// MAGIC ![Image of Pipeline](http://training.databricks.com/databricks_guide/1-init.png)

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
// MAGIC - Use `VectorAssembler`, and set `inputCols` to `featureCols`, and the `outputCols` as `rawFeatures`. This concatenates all feature columns into a single feature vector into the new column "rawFeatures".
// MAGIC - Use `VectorIndexer` to identify categorical features in our `rawFeatures` and index them. If any column has `maxCategories` or fewer distinct values, then it is treated as a categorical variable.

// COMMAND ----------

import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}

val featuresCols = df.columns.dropRight(1) //Removes "cnt"

val vectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures")

val vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(4)

// COMMAND ----------

// MAGIC %md
// MAGIC Second, we define the model training stage of the Pipeline. `GBTRegressor` takes feature vectors and labels as input and learns to predict labels of new examples.
// MAGIC
// MAGIC ![RF image](http://training.databricks.com/databricks_guide/3-gbt.png)

// COMMAND ----------

// MAGIC %md
// MAGIC Create a GBTRegressor that takes a `features` column and learns to predict `cnt`

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.regression.GBTRegressor

val gbt = new GBTRegressor().setLabelCol("cnt")

// COMMAND ----------

// MAGIC %md
// MAGIC Third, we wrap the model training stage within a `CrossValidator` stage.  `CrossValidator` knows how to call the GBT algorithm with different hyperparameter settings.  It will train multiple models and choose the best one, based on minimizing some metric.  In this example, our metric is [Root Mean Squared Error (RMSE)](https://en.wikipedia.org/wiki/Root-mean-square_deviation).
// MAGIC
// MAGIC ![Image of CV](http://training.databricks.com/databricks_guide/4-cv.png)

// COMMAND ----------

// MAGIC %md
// MAGIC In this example notebook, we keep these trees shallow and use a relatively small number of trees.  In practice, to get the highest accuracy, you would likely want to try deeper trees (10 or higher) and more trees in the ensemble (>100). However, you want to be careful not to overfit if your trees get too deep.
// MAGIC
// MAGIC Define a grid of hyperparameters to test:
// MAGIC   - maxDepth: max depth of each decision tree in the GBT ensemble (Use the values `2, 5`)
// MAGIC   - maxIter: iterations, i.e., number of trees in each GBT ensemble (Use the values `10, 50`)
// MAGIC   
// MAGIC `addGrid()` accepts the name of the parameter (e.g. `gbt.maxDepth`), and a list/Array of the possible values (e.g. `[2, 5]` in Python or `Array(2,5)` in Scala).

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.tuning.ParamGridBuilder

val paramGrid = new ParamGridBuilder()
                  .addGrid(gbt.maxDepth, Array(2, 5))
                  .addGrid(gbt.maxIter, Array(10, 50))
                  .build()

// COMMAND ----------

// MAGIC %md
// MAGIC We need a way to evaluate how well each configuration performed. We will use RMSE (root mean squared error) as our metric to compare model performance.
// MAGIC
// MAGIC We start by creating a `RegressionEvaluator` with the proper `metricName`, `labelCol`, and `predictionCol`. 
// MAGIC
// MAGIC We pass in the `estimator` (gbt), `evaluator`, and `estimatorParamMaps` to `CrossValidator` so that it knows:
// MAGIC - Which model to use
// MAGIC - How to evaluate the model
// MAGIC - What hyperparamters to set for the model. 
// MAGIC
// MAGIC HINT: You can call `anEstimator.getLabelCol` to get the label column for that estimator

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator()
                    .setMetricName("rmse")
                    .setLabelCol(gbt.getLabelCol)
                    .setPredictionCol(gbt.getPredictionCol)

val cv = new CrossValidator()
            .setEstimator(gbt)
            .setEvaluator(evaluator)
            .setEstimatorParamMaps(paramGrid)

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, we can tie our feature processing and model training stages together into a single `Pipeline`.
// MAGIC
// MAGIC ![Image of Pipeline](http://training.databricks.com/databricks_guide/5-pipeline.png)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ok, here's where the magic happens :)
// MAGIC
// MAGIC Or at least, from our point of view, here's where a new API shows up! Set the stages for the `Pipeline`. 
// MAGIC
// MAGIC NOTE: Ordering is important.

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline()
                  .setStages(Array(vectorAssembler, vectorIndexer, cv))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Train the Pipeline!
// MAGIC
// MAGIC Now that we have set up our workflow, we can train the Pipeline in a single call.  Calling `fit()` will run feature processing, model tuning, and training in a single call.  We get back a fitted Pipeline with the best model found.
// MAGIC
// MAGIC ***Note***: This next cell can take up to **5 minutes**.  This is because it is training *a lot* of trees:
// MAGIC * For each random sample of data in Cross Validation,
// MAGIC   * For each setting of the hyperparameters,
// MAGIC     * `CrossValidator` is training a separate GBT ensemble which contains many Decision Trees.

// COMMAND ----------

// ANSWER
val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Make predictions, and evaluate results
// MAGIC
// MAGIC Our final step will be to use our fitted model to make predictions on new data.  We will use our held-out test set, but you could also use this model to make predictions on completely new data.  For example, if we created some data based on weather predictions for the next week, we could predict bike rentals expected during the next week!
// MAGIC
// MAGIC We will also evaluate our predictions.  Computing evaluation metrics is important for understanding the quality of predictions, as well as for comparing models and tuning parameters.

// COMMAND ----------

// MAGIC %md
// MAGIC Calling `transform()` on a new dataset passes that data through feature processing and uses the fitted model to make predictions.  We get back a DataFrame with a new column `predictions` (as well as intermediate results such as our `rawFeatures` column from feature processing).

// COMMAND ----------

// ANSWER
val predictionsDF = pipelineModel.transform(testDF)

// COMMAND ----------

// MAGIC %md
// MAGIC It is easier to view the results when we limit the columns displayed to:
// MAGIC * `cnt`: the true count of bike rentals
// MAGIC * `prediction`: our predicted count of bike rentals
// MAGIC * feature columns: our original (human-readable) feature columns

// COMMAND ----------

display(predictionsDF.drop("rawFeatures", "features"))

// COMMAND ----------

// MAGIC %md
// MAGIC Are these good results?  They are not perfect, but you can see correlation between the counts and predictions.  And there is room to improve---see the next section for ideas to take you further!
// MAGIC
// MAGIC Before we continue, we give two tips on understanding results:
// MAGIC
// MAGIC **(1) Metrics**: Manually viewing the predictions gives intuition about accuracy, but it can be useful to have a more concrete metric.  Below, we compute an evaluation metric which tells us how well our model makes predictions on all of our test data.  In this case (for [RMSE](https://en.wikipedia.org/wiki/Root-mean-square_deviation)), lower is better.  This metric does not mean much on its own, but it can be used to compare different models.  (This is what `CrossValidator` does internally.)

// COMMAND ----------

// ANSWER
val rmse = evaluator.evaluate(predictionsDF)
println(s"RMSE on our test set: $rmse")

// COMMAND ----------

// MAGIC %md
// MAGIC **(2) Visualization**: Plotting predictions vs. features can help us make sure that the model "understands" the input features and is using them properly to make predictions.  Below, we can see that the model predictions are correlated with the hour of the day, just like the true labels were.
// MAGIC
// MAGIC *Note: For more expert ML usage, check out other Databricks guides on plotting residuals, which compare predictions vs. true labels.*

// COMMAND ----------

display(predictionsDF.select("hr", "prediction"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Improving our model
// MAGIC
// MAGIC You are not done yet!  This section describes how to take this notebook and improve the results even more.  Try copying this notebook into your Databricks account and extending it, and see how much you can improve the predictions.
// MAGIC
// MAGIC There are several ways we could further improve our model:
// MAGIC * **Expert knowledge**: We may not be experts on bike sharing programs, but we know a few things we can use:
// MAGIC   * The count of rentals cannot be negative.  `GBTRegressor` does not know that, but we could threshold the predictions to be `>= 0` post-hoc.
// MAGIC   * The count of rentals is the sum of `registered` and `casual` rentals.  These two counts may have different behavior.  (Frequent cyclists and casual cyclists probably rent bikes for different reasons.)  The best models for this dataset take this into account.  Try training one GBT model for `registered` and one for `casual`, and then add their predictions together to get the full prediction.
// MAGIC * **Better tuning**: To make this notebook run quickly, we only tried a few hyperparameter settings.  To get the most out of our data, we should test more settings.  Start by increasing the number of trees in our GBT model by setting `maxIter=200`; it will take longer to train but can be more accurate.
// MAGIC * **Feature engineering**: We used the basic set of features given to us, but we could potentially improve them.  For example, we may guess that weather is more or less important depending on whether or not it is a workday vs. weekend.  To take advantage of that, we could build a few feature by combining those two base features.  MLlib provides a suite of feature transformers; find out more in the [ML guide](http://spark.apache.org/docs/latest/ml-features.html).
// MAGIC
// MAGIC *Good luck!*

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
