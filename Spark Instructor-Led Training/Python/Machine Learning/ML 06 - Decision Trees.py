# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Decision Trees for handwritten digit recognition
# MAGIC
# MAGIC This notebook demonstrates learning a [Decision Tree](https://en.wikipedia.org/wiki/Decision_tree_learning) using Spark's distributed implementation.  It gives the reader a better understanding of some critical [hyperparameters](https://en.wikipedia.org/wiki/Hyperparameter_optimization) for the tree learning algorithm, using examples to demonstrate how tuning the hyperparameters can improve accuracy.
# MAGIC
# MAGIC **Data**: We use the classic MNIST handwritten digit recognition dataset.  It is from LeCun et al. (1998) and may be found under ["mnist" at the LibSVM dataset page](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#mnist).
# MAGIC
# MAGIC **Goal**: Our goal for our data is to learn how to recognize digits (0 - 9) from images of handwriting.  However, we will focus on understanding trees, not on this particular learning problem.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load MNIST training and test datasets
# MAGIC
# MAGIC Our datasets are vectors of pixels representing images of handwritten digits.  For example:
# MAGIC
# MAGIC ![Image of a digit](http://training.databricks.com/databricks_guide/digit.png)
# MAGIC ![Image of all 10 digits](http://training.databricks.com/databricks_guide/MNIST-small.png)

# COMMAND ----------

# MAGIC %md
# MAGIC These datasets are stored in the popular LibSVM dataset format.  We will load them using the LibSVM dataset reader utility.

# COMMAND ----------

trainingDF = spark.read.format("libsvm").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-train.txt").cache()
testDF = spark.read.format("libsvm").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-test.txt").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Display our data.  Each image has the true label (the `label` column) and a vector of `features` which represent pixel intensities.

# COMMAND ----------

display(trainingDF)

# COMMAND ----------

trainingDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train a Decision Tree
# MAGIC
# MAGIC We begin by training a decision tree using the default settings.  Before training, we want to tell the algorithm that the labels are categories 0-9, rather than continuous values.  We use the `StringIndexer` class to do this.  We tie this feature preprocessing together with the tree algorithm using a `Pipeline`.  ML Pipelines are tools Spark provides for piecing together Machine Learning algorithms into workflows.  To learn more about Pipelines, check out other ML example notebooks in Databricks and the [ML Pipelines user guide](http://spark.apache.org/docs/latest/ml-guide.html).

# COMMAND ----------

# Import the ML algorithms we will use.
from pyspark.ml.classification import DecisionTreeClassifier, DecisionTreeClassificationModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC Steps:
# MAGIC - [StringIndexer](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer) accepts an input column and converts the numerical values to categorical values. Set the `inputCol` to `label` and `outputCol` to `indexedLabel`.
# MAGIC - Set the `labelCol` of the [DecisionTreeClassifier](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.DecisionTreeClassifier) to `indexedLabel` (the column appended by `indexer`)
# MAGIC - Put these two steps together in a pipeline

# COMMAND ----------

indexer = StringIndexer().setInputCol("label").setOutputCol("indexedLabel")

dtc = DecisionTreeClassifier().setLabelCol("indexedLabel")

# Chain indexer + dtc together into a single ML Pipeline.
pipeline = Pipeline().setStages([indexer, dtc])

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's fit a model to our data.

# COMMAND ----------

model = pipeline.fit(trainingDF)

# COMMAND ----------

# MAGIC %md
# MAGIC We can inspect the learned tree by displaying it using Databricks ML visualization.  (Visualization is available for several but not all models.)

# COMMAND ----------

# The tree is the last stage of the Pipeline.  Display it!
print(model.stages[-1].toDebugString)

# COMMAND ----------

# MAGIC %md
# MAGIC Above, we can see how the tree makes predictions.  When classifying a new example, the tree starts at the "root" node (at the top).  Each tree node tests a pixel value and goes either left or right.  At the bottom "leaf" nodes, the tree predicts a digit as the image's label.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring "maxDepth": training trees of different sizes
# MAGIC
# MAGIC In this section, we test tuning a single hyperparameter `maxDepth`, which determines how deep (and large) the tree can be.  We will train trees at varying depths and see how it affects the accuracy on our held-out test set.
# MAGIC
# MAGIC *Note: The next cell can take about 1 minute to run since it is training several trees which get deeper and deeper.*

# COMMAND ----------

variedMaxDepthModels = []

for maxDepth in range(8):
    # For this setting of maxDepth, learn a decision tree.
    dtc.setMaxDepth(maxDepth)
    
    # Create a Pipeline with our feature processing stage (indexer) plus the tree algorithm
    pipeline = Pipeline().setStages([indexer, dtc])
    
    # Run the ML Pipeline to learn a tree.
    variedMaxDepthModels.append(pipeline.fit(trainingDF))

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setMetricName("accuracy")

# COMMAND ----------

# For each maxDepth setting, make predictions on the test data, and compute the accuracy metric.
accuracies_list = []

for maxDepth in range(8):
    model = variedMaxDepthModels[maxDepth]
    
    # Calling transform() on the test set runs the fitted pipeline.
    # The learned model makes predictions on each test example.
    predictions = model.transform(testDF)
    
    # Calling evaluate() on the predictions DataFrame computes our accuracy metric.
    accuracies_list.append((maxDepth, evaluator.evaluate(predictions)))

# Create a DataFrame from a list
accuraciesDF = sc.parallelize(accuracies_list).toDF(["maxDepth", "accuracy"])

display(accuraciesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC We can display our accuracy results and see immediately that deeper, larger trees are more powerful classifiers, achieving higher accuracies. Even though deeper trees are more powerful, they are not always better.  If we kept increasing the depth, training would take longer and longer.  We also might risk [overfitting](https://en.wikipedia.org/wiki/Overfitting) (fitting the training data so well that our predictions get worse on test data); it is important to tune parameters *based on [held-out data](https://en.wikipedia.org/wiki/Test_set#Validation_set)* to prevent overfitting.
# MAGIC
# MAGIC Let's re-write this code using `cross-validation` and Spark's `ParamGridBuilder` to find the optimal hyperparameters in a more systematic approach.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Validation
# MAGIC
# MAGIC Now we are going to use ParamGridBuilder to explore different maxDepths for our decision trees.
# MAGIC
# MAGIC We are also going to use 3-fold cross validation to identify the optimal maxDepth. 
# MAGIC
# MAGIC ![crossValidation](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/CrossValidation.png)
# MAGIC
# MAGIC With 3-fold cross-validation, we train on 2/3 of the data, and evaluate with the remaining (held-out) 1/3. We repeat this process 3 times, so each fold gets the chance to act as the validation set. We then average the results of the three rounds.

# COMMAND ----------

# New version using ParamGridBuilder

from pyspark.ml.tuning import *
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

pipeline = Pipeline().setStages([indexer, dtc])

# run it a little faster on CE
grid = ParamGridBuilder()            \
    .addGrid(dtc.maxDepth, range(4,8)) \
    .build()

evaluator = MulticlassClassificationEvaluator() \
    .setLabelCol("indexedLabel")                  \
    .setMetricName("accuracy")

seed = 42

cv = CrossValidator()          \
    .setNumFolds(3)              \
    .setEstimator(pipeline)      \
    .setEstimatorParamMaps(grid) \
    .setEvaluator(evaluator)     \
    .setSeed(seed)

cvModel = cv.fit(trainingDF)

# COMMAND ----------

for p in zip(cvModel.getEstimatorParamMaps(),cvModel.avgMetrics):
    print(p)

# COMMAND ----------

# MAGIC %md
# MAGIC After identifying the best ParamMap, CrossValidator finally re-fits the Estimator using the best ParamMap on the entire dataset.

# COMMAND ----------

evaluator.evaluate(cvModel.transform(testDF))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resources**
# MAGIC
# MAGIC If you are interested in learning more on these topics, these resources can get you started:
# MAGIC * [Excellent visual description of Machine Learning and Decision Trees](http://www.r2d3.us/visual-intro-to-machine-learning-part-1/)
# MAGIC   * *This gives an intuitive visual explanation of ML, decision trees, overfitting, and more.*
# MAGIC * [Blog post on MLlib Random Forests and Gradient-Boosted Trees](https://databricks.com/blog/2015/01/21/random-forests-and-boosting-in-mllib.html)
# MAGIC   * *Random Forests and Gradient-Boosted Trees combine many trees into more powerful ensemble models.  This is the original post describing MLlib's forest and GBT implementations.*
# MAGIC * Wikipedia
# MAGIC   * [Decision tree learning](https://en.wikipedia.org/wiki/Decision_tree_learning)
# MAGIC   * [Overfitting](https://en.wikipedia.org/wiki/Overfitting)
# MAGIC   * [Hyperparameter tuning](https://en.wikipedia.org/wiki/Hyperparameter_optimization)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/labs.png) Decision Trees Lab
# MAGIC It's time to put what we learned to practice.
# MAGIC
# MAGIC Go ahead and open the notebook [ML 06 - Decision Trees Lab]($./ML 06 - Decision Trees Lab) and complete the exercises.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
