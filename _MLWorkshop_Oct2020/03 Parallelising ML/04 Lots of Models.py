# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Understanding Parallelization of Machine Learning Algorithms in Apache Spark™
# MAGIC
# MAGIC ##Pandas_UDFs for training lots of models in Parallel

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The dataset used for this example is Bank marketing. Given a set of features about a customer can we predict whether the person will open a term deposit account.
# MAGIC
# MAGIC Original Source: [UCI Machine Learning Repository 
# MAGIC Bank Marketing Data Set](https://archive.ics.uci.edu/ml/datasets/bank+marketing)
# MAGIC [Moro et al., 2014] S. Moro, P. Cortez and P. Rita. A Data-Driven Approach to Predict the Success of Bank Telemarketing. Decision Support Systems, Elsevier, 62:22-31, June 2014

# COMMAND ----------

train_data_path_2 = "dbfs:/ml-workshop-datasets/employee/delta/trainingData"

# COMMAND ----------

# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC var username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC spark.conf.set("my.fq_username", username)

# COMMAND ----------

username = spark.conf.get("my.fq_username")
user_name = username.replace("@databricks.com", "").replace(".","_")
print("User Name:", user_name)

mlflow_exp = "/Users/"+username+"/example_experiment"
print("MLFlow Experiment:", mlflow_exp)

# COMMAND ----------

# MAGIC %md ### 1. Read in the training data

# COMMAND ----------

trainingData = spark.read.format("delta").load(train_data_path_2)

# COMMAND ----------

display(trainingData)

# COMMAND ----------

# MAGIC %md ### 2. ML Flow Setup

# COMMAND ----------

# ML Flow in pandas udf
import mlflow
cntx = dbutils.entry_point.getDbutils().notebook().getContext()
api_token = cntx.apiToken().get()
api_url = cntx.apiUrl().get()
mlflow.set_tracking_uri("databricks")

# COMMAND ----------

# MAGIC %md ### 3. Define the function to run on each campaign group

# COMMAND ----------

# We need to define the return schema
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField
schema = StructType([StructField('Predictions', DoubleType(), True), 
                     StructField('campaign', DoubleType(), True)
                    ])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def train_rf_groups(data):
  # Library imports
  import pandas as pd
  import mlflow
  import mlflow.sklearn
  import os
  import sklearn
  from sklearn.model_selection import train_test_split
  from sklearn.ensemble import RandomForestClassifier
  from sklearn.metrics import roc_auc_score
  
  
  # ML Flow setup
  os.environ["DATABRICKS_TOKEN"] = api_token
  os.environ["DATABRICKS_HOST"] = api_url
  mlflow.set_experiment(mlflow_exp)
  
  # get relevant info from the input dataframe
  campaign = data['campaign'].iloc[0]
  
  # Train, test split
  train, test = train_test_split(data)

  # The predicted column is "label" 
  train_x = train[["age","balance","previous","day","duration", "pdays"]]
  test_x = test[["age","balance","previous","day","duration", "pdays"]]
  train_y = train[["label"]]
  test_y = test[["label"]]
  
  # log the run into mlflow
  with mlflow.start_run() as run:
    # could also apply feature engineering steps here as well
    
    rf = RandomForestClassifier(n_estimators=100, max_depth=7,random_state=0)
    rf.fit(train_x, train_y)

    predicted_qualities = pd.DataFrame(rf.predict(test_x), columns=["Predictions"])
    auc = roc_auc_score(test_y, predicted_qualities)
    
    # Log mlflow attributes for mlflow UI
    mlflow.log_param("max_depth", 7)
    mlflow.log_param("n_estimators", 100)
    mlflow.log_metric("ROC AUC", auc)
    mlflow.sklearn.log_model(rf, "model")
    
  predicted_qualities['campaign'] = data['campaign'][0]
    
  return predicted_qualities
  

# COMMAND ----------

# MAGIC %md ### Perform some cleaning steps on the data

# COMMAND ----------

# First, limit to those campaigns which have sufficient data for a model
# find the relevant campaign ids
from pyspark.sql.functions import count, col
campaigns = trainingData.groupby("campaign").agg(count("campaign")).where(col("count(campaign)")>300).select("campaign")

# now limit the training data just to those campaigns
trainingData_campaigns = trainingData.join(campaigns, "campaign")
display(trainingData_campaigns)

# COMMAND ----------

# Can't convert a Spark feature vector to pandas here, everything needs to be done in the pandas_udf
trainingData_campaigns = trainingData_campaigns.drop("features")
display(trainingData_campaigns)

# COMMAND ----------

display(trainingData_campaigns.groupBy("campaign").count())

# COMMAND ----------

# MAGIC %md ### 4. Apply the function to the different campaign groups

# COMMAND ----------

output = trainingData_campaigns.groupby("campaign").apply(train_rf_groups)
display(output)

# COMMAND ----------

# DBTITLE 1,Finally, register one of these models to our demo...
import time
model_name = "<FILL IN>"
client = mlflow.tracking.MlflowClient()
registered_model = client.get_registered_model(model_name)
exp_id = client.get_experiment_by_name(mlflow_exp).experiment_id
runs = mlflow.search_runs(exp_id)
artifact_uri=runs["artifact_uri"][0]
model_version = client.create_model_version(model_name, f"{artifact_uri}/model", runs["run_id"][0])
time.sleep(5)
client.update_model_version(model_name, model_version.version, stage="Production", description="My next prod version")

# COMMAND ----------


