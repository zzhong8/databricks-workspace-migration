# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Understanding Parallelization of Machine Learning Algorithms in Apache Spark™
# MAGIC
# MAGIC ##Model Deployment

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The dataset used for this example is Bank marketing. Given a set of features about a customer can we predict whether the person will open a term deposit account.
# MAGIC
# MAGIC Original Source: [UCI Machine Learning Repository 
# MAGIC Bank Marketing Data Set](https://archive.ics.uci.edu/ml/datasets/bank+marketing)
# MAGIC [Moro et al., 2014] S. Moro, P. Cortez and P. Rita. A Data-Driven Approach to Predict the Success of Bank Telemarketing. Decision Support Systems, Elsevier, 62:22-31, June 2014

# COMMAND ----------

# DBTITLE 1,Load the model
import mlflow
model_name = "ml_workshop_demo_model"
client = mlflow.tracking.MlflowClient()
latest_prod_model_detail = client.get_latest_versions(model_name, stages=['Production'])[0]
#latest_prod_model = mlflow.sklearn.load_model(f"runs:/{latest_prod_model_detail.run_id}/model")

# COMMAND ----------

# MAGIC %md ### 1. Batch

# COMMAND ----------

# DBTITLE 1,Read the test data as an example
testData = spark.read.format("delta").load("dbfs:/ml-workshop-datasets/employee/delta/testData")
testData = testData.drop("features")

# COMMAND ----------

import mlflow
import mlflow.pyfunc
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, "runs:/{}/model".format(latest_prod_model_detail.run_id))

#withColumns adds a column to the data by applying the python UDF to the DataFrame
predicted_df = testData.withColumn("prediction", pyfunc_udf("age","balance","previous","day","duration", "pdays"))
display(predicted_df)

# COMMAND ----------

# MAGIC %md ### 2. Streaming

# COMMAND ----------

# DBTITLE 1,Read Delta table as Stream
testData = spark.readStream.format("delta").option("TriggerOnce", True).load("dbfs:/ml-workshop-datasets/employee/delta/testData")
predicted_df = testData.withColumn("prediction", pyfunc_udf("age","balance","previous","day","duration", "pdays"))
#display(predicted_df)

# COMMAND ----------

# Start the stream
outputStream = (
  predicted_df
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("predictions")     # name the in-memory table
    .start()
)

# COMMAND ----------

# Allow the stream time to start
import time
time.sleep(30)

# COMMAND ----------

# MAGIC %sql select * from predictions

# COMMAND ----------

# finally stop the stream
outputStream.stop()

# COMMAND ----------


