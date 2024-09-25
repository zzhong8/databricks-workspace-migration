# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Understanding Parallelization of Machine Learning Algorithms in Apache Spark™
# MAGIC
# MAGIC ##Parallelize Featurization and Distributed Training

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The dataset used for this example is Bank marketing. Given a set of features about a customer can we predict whether the person will open a term deposit account.
# MAGIC
# MAGIC Original Source: [UCI Machine Learning Repository 
# MAGIC Bank Marketing Data Set](https://archive.ics.uci.edu/ml/datasets/bank+marketing)
# MAGIC [Moro et al., 2014] S. Moro, P. Cortez and P. Rita. A Data-Driven Approach to Predict the Success of Bank Telemarketing. Decision Support Systems, Elsevier, 62:22-31, June 2014

# COMMAND ----------

trainingData = spark.read.format("delta").load("dbfs:/ml-workshop-datasets/employee/delta/trainingData")
testData = spark.read.format("delta").load("dbfs:/ml-workshop-datasets/employee/delta/testData")

# COMMAND ----------

# MAGIC %md Note that for the toy example provided in this notebook the overheads of distribution may mean that this version runs more slowly than the single machine case. However, this will not be the case for large datasets. This version is distributed and hence will horizontally scale, where the single machine version will hit a memory limit, or take significant amounts of time, for large datasets

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# Create initial Random Forest model
rf = RandomForestClassifier(labelCol="label", featuresCol="features", maxDepth=10, numTrees=20, featureSubsetStrategy="all", seed=42, maxBins=100)
pg = ParamGridBuilder().addGrid(rf.numTrees, [30,40,50]).build()
cv = CrossValidator(estimator=rf, estimatorParamMaps=pg, numFolds=3, seed=42, parallelism=4, evaluator=BinaryClassificationEvaluator())
# Train model with Training Data
pipeline = Pipeline(stages=[cv])
rfModel = pipeline.fit(trainingData)

# COMMAND ----------

# Make predictions on test data using the transform() method.
# LogisticRegression.transform() will only use the 'features' column.
predictions = rfModel.transform(testData)
display(predictions)

# COMMAND ----------

# Evaluate model
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
evaluator.evaluate(predictions)

# COMMAND ----------


