# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Decision Trees for handwritten digit recognition - Lab

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
# MAGIC ![Image of all 10 digits](http://training.databricks.com/databricks_guide/MNIST-small.png)Pet

# COMMAND ----------

# MAGIC %md
# MAGIC These datasets are stored in the popular LibSVM dataset format.  We will load them using the LibSVM dataset reader utility.

# COMMAND ----------

trainingDF = spark.read.format("libsvm").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-train.txt").cache()
testDF = spark.read.format("libsvm").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-test.txt").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train a Decision Tree
# MAGIC
# MAGIC Before training, we want to tell the algorithm that the labels are categories 0-9, rather than continuous values.  We use the `StringIndexer` class to do this.  Later you will tie this feature preprocessing together with the tree algorithm using a `Pipeline`.

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

indexer = StringIndexer().setInputCol("label").setOutputCol("indexedLabel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring "maxBins": discretization for efficient distributed computing
# MAGIC
# MAGIC For efficient distributed training of Decision Trees, Spark and most other libraries discretize (or "bin") continuous features (such as pixel values) into a finite number of values.  This is an important step for the distributed implementation, but it introduces a tradeoff: Larger `maxBins` mean your data will be more accurately represented, but it will also mean more communication (and slower training).
# MAGIC
# MAGIC Remember our digit image from above:
# MAGIC
# MAGIC ![Image of a digit](http://training.databricks.com/databricks_guide/digit.png)
# MAGIC
# MAGIC It is grayscale.  But if we set `maxBins = 2`, then we are effectively making it a black-and-white image, not grayscale.  Will that affect the accuracy of our model?  Let's see! 
# MAGIC
# MAGIC Experiment using 3-fold cross-validation, and the following values for `maxBins`: 2, 4, 8, 16, 32. For this, we are going to fix the `maxDepth` to be 6 and just vary `maxBins`.

# COMMAND ----------

# ANSWER

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.tuning import *

dtc = DecisionTreeClassifier()
dtc.setLabelCol("indexedLabel")
dtc.setMaxDepth(6)

pipeline = Pipeline().setStages([indexer, dtc])

# skip numbers, i.e., use powers of 2 to run it a little faster on CE
grid = ParamGridBuilder()            \
  .addGrid(dtc.maxBins, [2, 4, 8, 16, 32]) \
  .build()

evaluator = MulticlassClassificationEvaluator() \
  .setLabelCol("indexedLabel")                  \
  .setMetricName("accuracy")

cv = CrossValidator()          \
  .setNumFolds(3)              \
  .setEstimator(pipeline)      \
  .setEstimatorParamMaps(grid) \
  .setEvaluator(evaluator)     \
  .setSeed(42)

cvModel = cv.fit(trainingDF)

# COMMAND ----------

zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics)
evaluator.evaluate(cvModel.transform(testDF))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see that extreme discretization (black and white) hurts accuracy, but only a bit.  Using more bins increases the accuracy (but also makes learning more costly).

# COMMAND ----------

# MAGIC %md
# MAGIC #### What's next?
# MAGIC
# MAGIC * **Explore**: Try out tuning other parameters of trees---or even ensembles like [Random Forests or Gradient-Boosted Trees](http://spark.apache.org/docs/latest/ml-classification-regression.html#tree-ensembles).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
