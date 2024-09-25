# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Baseball ML Pipeline Application - Part 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Because a lot of the variation in the number of runs scored is contained in the RBI variable, we did fairly well with the simple one-dimensional regression. But we do have more information available, which might help us build a better model.
# MAGIC
# MAGIC And of course we want to see how to do more interesting modeling with Spark -- so let's try it again. The first part of this lab will be very similar to the one-dimensional case.
# MAGIC
# MAGIC This time, we'll use more predictors, and more importantly, we'll look at more Spark features that can help us manage these modeling tasks.

# COMMAND ----------

baseballDF = (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv("dbfs:/mnt/training/301/batting.csv"))

display(baseballDF.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Definition**
# MAGIC
# MAGIC As a reminder, our schema definition from Kaggle appears below:
# MAGIC
# MAGIC - player_id = Player ID
# MAGIC - year = year of statistics
# MAGIC - stint = player's stint (order of appearances within a season)
# MAGIC - team_id = Team ID
# MAGIC - league_id = league
# MAGIC - g = games
# MAGIC - ab = at bats
# MAGIC - r = runs scored
# MAGIC - h = hits
# MAGIC - double = doubles
# MAGIC - triple = triples
# MAGIC - hr = homeruns
# MAGIC - rbi = runs batted in
# MAGIC - sb = stolen bases
# MAGIC - cs = caught stealing
# MAGIC - bb = base on balls (walk)
# MAGIC - so = strikeouts
# MAGIC - ibb = intentional base on balls
# MAGIC - hbp = hit by pitch
# MAGIC - sh = sacrifice hits
# MAGIC - sf = sacrifice flies
# MAGIC - g_idp = induced double plays
# MAGIC
# MAGIC R is our label or target. This is the value we are trying to predict given the number of RBIs.

# COMMAND ----------

# MAGIC %md
# MAGIC In this linear regression exercise, we are going to predict runs (`r`) as a function of hits (`h`), doubles (`double`), triples (`triple`), homeruns (`hr`), RBIs (`rbi`), and walks (`bb`). But before we build our model, let's look at the correlation between these features.
# MAGIC
# MAGIC Let's use Databricks to get a quick look at the relationships of each variable to our target.

# COMMAND ----------

# Once you display the data, select the option to display it as a scatter chart
display(baseballDF.select("r","h","double","triple","hr","rbi", "bb"))

# COMMAND ----------

# MAGIC %md
# MAGIC Here we can look see...
# MAGIC
# MAGIC * How each predictor relates to our target (`r`)
# MAGIC
# MAGIC and equally important
# MAGIC
# MAGIC * How the predictors relate to each other -- in particular, it's valuable to know if we have *highly correlated predictors* as these partly redundant dimensions can affect our modeling in several ways

# COMMAND ----------

# MAGIC %md
# MAGIC Similar to the last lab, we need to split our data into a training and test set to be able to evaluate our model at the end. But now, we want to select more columns from `baseballVecDF` because we are training a linear regression model with more features. We use the [randomSplit()](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit) method to split our data, and include a seed for reproducibility. 

# COMMAND ----------

(testDF, trainingDF) = baseballDF.select("r","h","double","triple","hr","rbi", "bb").na.drop().randomSplit((0.20, 0.80), seed=42)

testDF.cache()
trainingDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Ok, let's get these records into a proper vector form using an offical API helper tool called a Transformer.
# MAGIC
# MAGIC Internally, it's not magic compared to our UDF -- it's just an organized, standardized way to expose logic in a reusable way that covers a variety of cases.
# MAGIC
# MAGIC The Transformer that we will use is a [VectorAssembler](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler). It takes a list/array of column names as inputs, and transforms those columns into a vector that contains 
# MAGIC
# MAGIC Set the input columns of `vecAssembler` to `"h", "double", "triple", "hr", "rbi", "bb"`, and the output column to `features` (because the default linear regression model expects a column called `features`).

# COMMAND ----------

# ANSWER
from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler()
vecAssembler.setInputCols(["h", "double", "triple", "hr", "rbi", "bb"])
vecAssembler.setOutputCol("features")
print(vecAssembler.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC How do we use it? Transformers' key API method is `.transform(aDataFrame)`, and it appends the transformed features as an extra column to the dataFrame.

# COMMAND ----------

trainingVecDF = vecAssembler.transform(trainingDF)
trainingVecDF.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a LinearRegression estimator with the default parameters, then use `explainParams()` to dump the parameters we can use.

# COMMAND ----------

from pyspark.ml.regression import LinearRegression, LinearRegressionModel

lr = LinearRegression()
print(lr.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that the default label for `labelCol` is `label`. However, our dataset uses the label `r` instead `label`, so let's change that. Use [setLabelCol()](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegression.setLabelCol) to change the `labelCol` to `r`.
# MAGIC
# MAGIC Question: Why didn't we have to do this for the previous lab? 

# COMMAND ----------

# ANSWER
lr.setLabelCol("r")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we're ready to fit a linear model to our data!

# COMMAND ----------

# ANSWER
linearModel = lr.fit(trainingVecDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's apply our model to the test set.

# COMMAND ----------

try:
  linearModel.transform(testDF)
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC What happened? The problem is we need to run our VectorAssembler over the test data to reshape it (and that produces the "features" column).
# MAGIC
# MAGIC It is a bit annoying to apply the same transformations to the training and test set. Luckily, we can encapsulate a series of Transformers and Estimators in a Pipeline. 
# MAGIC
# MAGIC The Pipeline code knows which APIs to call (transform vs. fit + transform) and it encapsulates the computation without requiring the data itself (this is sometimes referred to as point-free style).
# MAGIC
# MAGIC Let's include the `vecAssembler` transformer and `lr` estimator in our pipeline.

# COMMAND ----------

from pyspark.ml import Pipeline

# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline()

# Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
lrPipeline.setStages([vecAssembler, lr])

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipelineModel = lrPipeline.fit(trainingDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have arranged the steps in the pipeline, let's make predictions on our test data.

# COMMAND ----------

predictionsAndLabelsDF = lrPipelineModel.transform(testDF)
display(predictionsAndLabelsDF.select('r', 'prediction').repartition(100)) # Repartition to shuffle the data

# COMMAND ----------

# MAGIC %md
# MAGIC Since Linear Regression is simply a line of best fit over the data that minimizes the sum of squared errors, given multiple input dimensions we can express each predictor as a line function of the form:
# MAGIC
# MAGIC \\[ y = a + b x_1 + b x_2 + b x_i ... \\]
# MAGIC
# MAGIC where a is the intercept and b are coefficients.
# MAGIC
# MAGIC To express the coefficients of that line we can retrieve the Estimator stage from the PipelineModel and express the weights and the intercept for the function. 

# COMMAND ----------

# The intercept is as follows:
intercept = lrPipelineModel.stages[1].intercept
print(intercept)

# COMMAND ----------

# The coefficents (i.e. weights) are as follows:
weights = lrPipelineModel.stages[1].coefficients
print(dict(zip(vecAssembler.getInputCols(), weights)))

# COMMAND ----------

# MAGIC %md
# MAGIC Why would RBIs have a negative coefficient here? RBIs had a positive correlation with runs, and in the 1-dimensonal linear regression setting, RBIs had a coefficient of .9899.
# MAGIC
# MAGIC It's because RBIs is a correlated predictor with hits, walks, doubles, singles, triples, and homeruns, as all of those factors contribute to RBIs. In order to remove correlation among our predictors, we would need to do a pre-processing step, such as Principal Components Analysis (PCA), before we fit our linear regression model. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLlib
# MAGIC
# MAGIC Now that we have real predictions we can use an evaluation metric such as Root Mean Squared Error to evaluate our regression model. The lower the Root Mean Squared Error on the test set, the better our model.
# MAGIC
# MAGIC In the first example, we used a RegressionEvaluator to get a metric. RegressionEvaluator (and other \*Evaluator classes) are the official "modern" way to get statistics on our models, and they are also designed to interface with other Spark classes (which we'll see later) to perform hyperparameter grid search, or find parameters that perform best.
# MAGIC
# MAGIC However, occasionally we might want to use some older classes from the mllib API -- in this case, because we can quickly get several stats out at once. Other reasons might be using functionality like SVD that haven't been ported to SparkML yet.
# MAGIC
# MAGIC Here's how we can take a DataFrame and produce the RDD of pairs of Doubles that the older mllib RegressionMetrics class uses.

# COMMAND ----------

from pyspark.mllib.evaluation import RegressionMetrics 
from pyspark.sql import DataFrame

def printStats(df, predictionColName, labelColName):
  # type: (DataFrame, str, str) -> None
  justPredictionAndLabelDF = df.select(predictionColName, labelColName)
  metrics = RegressionMetrics(justPredictionAndLabelDF.rdd)
  
  rmse = metrics.rootMeanSquaredError
  mae = metrics.meanAbsoluteError
  r2 = metrics.r2
  
  print("Root Mean Squared Error: {}".format(rmse))
  print("Mean Absolute Error: {}".format(mae))
  print("R2: {}".format(r2))

# COMMAND ----------

printStats(predictionsAndLabelsDF, "prediction", "r")

# COMMAND ----------

# MAGIC %md
# MAGIC We significantly decreased our RMSE by adding extra predictors! But was it worth it for the increased model complexity? 
# MAGIC
# MAGIC ** As an extension, try to use fewer predictors, but keep the R2 almost as high. **

# COMMAND ----------

# MAGIC %md
# MAGIC __Manual Tuning__
# MAGIC
# MAGIC Now that we have a model with all of the data let's try to make a better model by tuning over several parameters to see if we can get better results.
# MAGIC
# MAGIC We can add regularization, which helps prevent overfitting by limiting the complexity of the model. By limiting the complexity of the model, our model will not overfit to the training data, and will generalize better. However, we run the risk of underfitting if we regularize too much, so there is a trade-off.
# MAGIC
# MAGIC On the left is an example of a model that overfits to the training data, and we can see that a less complex model (right) will generalize better to new data points (this diagram shows overfitting in the classification setting).
# MAGIC
# MAGIC ![overfitting](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/Overfitting.png)
# MAGIC
# MAGIC *Note: to keep the code super short and simple in the following cells, we're mutating the (stateful) LinearRegression object. So take care to run the following cells in order. If you don't, both Spark and your code will still work, but you might not realize what the current parameters are.

# COMMAND ----------

# MAGIC %md
# MAGIC Regularization imposes a penalty if the weights in our model are very large (indicating a complex and potentially overfitting model). There are two main types of regularization: L1 (lasso) and L2 (ridge) regularization. 
# MAGIC
# MAGIC L1 regularization promotes sparsity in the weights (a form of feature selection), whereas L2 regularization promotes smaller values for the weights. In Spark's linear regression model, the default regularization is L2.

# COMMAND ----------

lr.setRegParam(0.2)  # L2 norm by default: sqrt(x**2 + y**2)

predictionsAndLabelsDF = lrPipeline.fit(trainingDF).transform(testDF)
printStats(predictionsAndLabelsDF, "prediction", "r")

# COMMAND ----------

# MAGIC %md
# MAGIC We can also use elastic net regularization, which balances L1 and L2 regularization. When the parameter is 0, it is purely L2 regularization, and when it is 1, it is purely L1 regularization. Anything in between is a trade-off between the two regularization techniques.
# MAGIC
# MAGIC We need to set the elasticNet parameter in addition to setting the regularization parameter.

# COMMAND ----------

lr.setElasticNetParam(0.1) # 0 == L2 ... 1 == L1
lr.setRegParam(0.10)

predictionsAndLabelsDF = lrPipeline.fit(trainingDF).transform(testDF)
printStats(predictionsAndLabelsDF, "prediction", "r")

# COMMAND ----------

# MAGIC %md
# MAGIC So these tuning options are not changing the results very much. But there are lots of params we could change ... and lots of possible values.
# MAGIC
# MAGIC Once again, this seems like something that could benefit from automation! We will see how to do this using a parameter grid search to automatically test out combinations of parameters and choose the best ones.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
