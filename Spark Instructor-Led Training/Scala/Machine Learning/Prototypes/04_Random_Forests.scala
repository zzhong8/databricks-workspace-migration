// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Random Forests
// MAGIC
// MAGIC In this lab, we are going to use the same dataset as in the last lab, but we are going to use a Random Forest instead of a single decision tree. 
// MAGIC
// MAGIC We will also use a parameter grid and cross-validation to perform hyperparameter tuning, as well as export our final model.
// MAGIC
// MAGIC The code below is taken from the last lab to set up our data transformations.
// MAGIC
// MAGIC [Random Forest Scala Docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.regression.RandomForestRegressor)
// MAGIC
// MAGIC [Random Forest Python Docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.RandomForestRegressor)
// MAGIC
// MAGIC [Spark ML Guide](https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression)

// COMMAND ----------

// MAGIC %run "./Includes/Classroom_Setup"

// COMMAND ----------

// Code taken from previous lab
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}

val df = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/databricks-datasets/bikeSharing/data-001/hour.csv")
        .drop("instant", "dteday", "casual", "registered", "holiday", "weekday")

df.cache()

val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)

val featuresCols = df.columns.dropRight(1) //Removes "cnt"

val vectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures")

val vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(4)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Random Forests
// MAGIC
// MAGIC Random forests and ensembles of decision trees are more powerful than a single decision tree alone. 
// MAGIC
// MAGIC Let's take a look at all the hyperparameters we could change in [RandomForestRegressor](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.regression.RandomForestRegressor).

// COMMAND ----------

import org.apache.spark.ml.regression.RandomForestRegressor

val rf = new RandomForestRegressor()
            .setLabelCol("cnt")
            .setSeed(27)

println(rf.explainParams())

// COMMAND ----------

// MAGIC %md
// MAGIC Try changing the values of `numTrees` and `maxDepth` to any values you like
// MAGIC
// MAGIC HINT: Take a look at the docs

// COMMAND ----------

// TODO
rf.<FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Pipeline
// MAGIC
// MAGIC Now that we have all of the feature transformations and estimators set up, let's put all of the stages together in the pipline.

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(vectorAssembler, vectorIndexer, rf))

pipeline.getStages

// COMMAND ----------

// MAGIC %md
// MAGIC If you want to look at what parameter each stage in the pipeline takes.

// COMMAND ----------

pipeline.getStages(0).extractParamMap()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) ParamGrid
// MAGIC
// MAGIC There are a lot of hyperparamaters we could tune, and it would take a long time to manually configure.
// MAGIC
// MAGIC Instead of a manual (ad-hoc) approach, let's use Spark's [ParamGridBuilder](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.ParamGridBuilder) to find the optimal hyperparameters in a more systematic approach.
// MAGIC
// MAGIC In this example notebook, we keep these trees shallow and use a relatively small number of trees. Let's define a grid of hyperparameters to test:
// MAGIC   - maxDepth: max depth of each decision tree in the RF ensemble (Use the values `2, 5, 10`)
// MAGIC   - numTrees: number of trees in each RF ensemble (Use the values `10, 50`)
// MAGIC
// MAGIC `addGrid()` accepts the name of the parameter (e.g. `rf.maxDepth`), and an Array of the possible values (e.g. `Array(2, 5, 10)`).

// COMMAND ----------

// TODO
import org.apache.spark.ml.tuning.ParamGridBuilder

val paramGrid = new ParamGridBuilder()
                    .addGrid(<FILL_IN>)
                    .addGrid(<FILL_IN>)
                    .build()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Cross Validation
// MAGIC
// MAGIC We are also going to use 3-fold cross validation to identify the optimal maxDepth and numTrees combination.
// MAGIC
// MAGIC ![crossValidation](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/CrossValidation.png)
// MAGIC
// MAGIC With 3-fold cross-validation, we train on 2/3 of the data, and evaluate with the remaining (held-out) 1/3. We repeat this process 3 times, so each fold gets the chance to act as the validation set. We then average the results of the three rounds.

// COMMAND ----------

// MAGIC %md
// MAGIC We pass in the `estimator` (pipeline), `evaluator`, and `estimatorParamMaps` to [CrossValidator](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.CrossValidator) so that it knows:
// MAGIC - Which model to use
// MAGIC - How to evaluate the model
// MAGIC - What hyperparamters to set for the model
// MAGIC
// MAGIC We can also set the number of folds we want to split our data into (3), as well as setting a seed so we all have the same split in the data.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.ml.tuning.CrossValidator

val evaluator = new RegressionEvaluator()
                   .setLabelCol("cnt")
                   .setPredictionCol("prediction")

val cv = new CrossValidator()
            .setEstimator(pipeline)
            .setEvaluator(evaluator)
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(3)
            .setSeed(27)

// COMMAND ----------

val cvModel = cv.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the model with the best hyperparameter configuration

// COMMAND ----------

cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Save Model
// MAGIC
// MAGIC Let's save our model by writing it out. 
// MAGIC
// MAGIC **NOTE:** We cannot save a pipeline model with a cross-validation step in Python. Instead, we have to save the best pipeline model itself.
// MAGIC
// MAGIC Also, there is no `overwrite` method. Our only alternative is to recursively delete the existing directory if we want to remove it.

// COMMAND ----------

import org.apache.spark.ml.PipelineModel

val path = "/tmp/random_forest_pipeline"
val modelPath = userhome + path

dbutils.fs.rm(modelPath, recurse=true)

cvModel.bestModel.asInstanceOf[PipelineModel].save(modelPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's load the saved model back in.

// COMMAND ----------

import org.apache.spark.ml.PipelineModel

val savedPipelineModel = PipelineModel.load(modelPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's apply the trained model to the test data.

// COMMAND ----------

val predictionsDF = savedPipelineModel.transform(testDF)
display(predictionsDF.select("cnt", "prediction"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Evaluate
// MAGIC
// MAGIC Let's see how well we did on the test set.

// COMMAND ----------

// TODO
val rmse = evaluator.<FILL_IN>
println(s"Test RMSE = $rmse")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Improving our model
// MAGIC
// MAGIC You are not done yet!  There are several ways we could further improve our model:
// MAGIC * **Expert knowledge** 
// MAGIC * **Better tuning** 
// MAGIC * **Feature engineering**
// MAGIC
// MAGIC As an exercise: Replace the Random Forest code with a Gradient Boosted tree, and vary the number of trees and depth of the trees. What do you find?
// MAGIC
// MAGIC *Good luck!*

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
