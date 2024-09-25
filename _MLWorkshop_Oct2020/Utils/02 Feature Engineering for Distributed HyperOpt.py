# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC ### Feature Engineering for Distributed HyperOpt
# MAGIC
# MAGIC ##### Pre-requisites:
# MAGIC * Data
# MAGIC   * dbfs:/ml-workshop-datasets/employee/delta/trainingData
# MAGIC   * dbfs:/ml-workshop-datasets/employee/delta/testData
# MAGIC   
# MAGIC #### Notebook overview
# MAGIC
# MAGIC The structure of the notebook is as follows:
# MAGIC - Settings
# MAGIC - Data preparation logic
# MAGIC   - Data description recap
# MAGIC   - Read data with Spark
# MAGIC   - Handle missing values, and generate features using Spark pipelines
# MAGIC   - Convert data to Pandas
# MAGIC   - Write data to DBFS

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The dataset used for this example is Bank marketing. Given a set of features about a customer can we predict whether the person will open a term deposit account.
# MAGIC
# MAGIC Original Source: [UCI Machine Learning Repository 
# MAGIC Bank Marketing Data Set](https://archive.ics.uci.edu/ml/datasets/bank+marketing)
# MAGIC [Moro et al., 2014] S. Moro, P. Cortez and P. Rita. A Data-Driven Approach to Predict the Success of Bank Telemarketing. Decision Support Systems, Elsevier, 62:22-31, June 2014

# COMMAND ----------

train_data_path = "/ml-workshop-datasets/employee/delta/trainingData"
test_data_path = "/ml-workshop-datasets/employee/delta/testData"

# COMMAND ----------

# MAGIC %md ## Settings

# COMMAND ----------

# MAGIC %scala
# MAGIC // Set the username
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC spark.conf.set("my.username", username)

# COMMAND ----------

import os

username = spark.conf.get("my.username")
name = username.replace("@databricks.com", "").replace(".","_")

def get_fuse_location(username, file_name):
  """
  This function creates filename localized with the username on the ML optimized FUSE mount point
  
  :param username: the databricks username 
  :param file_name: the name of the file  
  :return: localized filename 
  """
  
  path = "/dbfs/ml/{}/ml_workshop".format(username)
  dbutils.fs.mkdirs(path)
  
  return "{}/{}".format(path, file_name)

# COMMAND ----------

# MAGIC %fs ls dbfs:/ml-workshop-datasets/employee/delta/trainingData

# COMMAND ----------

# MAGIC %fs ls dbfs:/ml-workshop-datasets/employee/delta/testData

# COMMAND ----------

# MAGIC %md ##Data preparation logic

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data description recap
# MAGIC Bank client data:
# MAGIC - age (numeric)
# MAGIC - job : type of job (categorical: 'admin.','blue-collar','entrepreneur','housemaid','management','retired','self-employed','services','student','technician','unemployed','unknown')
# MAGIC - marital : marital status (categorical: 'divorced','married','single','unknown'; note: 'divorced' means divorced or widowed)
# MAGIC - education (categorical: 'basic.4y','basic.6y','basic.9y','high.school','illiterate','professional.course','university.degree','unknown')
# MAGIC - default: has credit in default? (categorical: 'no','yes','unknown')
# MAGIC - housing: has housing loan? (categorical: 'no','yes','unknown')
# MAGIC - loan: has personal loan? (categorical: 'no','yes','unknown')
# MAGIC
# MAGIC Related with the last contact of the current campaign:
# MAGIC - contact: contact communication type (categorical: 'cellular','telephone') 
# MAGIC - month: last contact month of year (categorical: 'jan', 'feb', 'mar', ..., 'nov', 'dec')
# MAGIC - day_of_week: last contact day of the week (categorical: 'mon','tue','wed','thu','fri')
# MAGIC - duration: last contact duration, in seconds (numeric). Important note: this attribute highly affects the output target (e.g., if duration=0 then y='no'). Yet, the duration is not known before a call is performed. Also, after the end of the call y is obviously known. Thus, this input should only be included for benchmark purposes and should be discarded if the intention is to have a realistic predictive model.
# MAGIC
# MAGIC Other attributes:
# MAGIC - campaign: number of contacts performed during this campaign and for this client (numeric, includes last contact)
# MAGIC - pdays: number of days that passed by after the client was last contacted from a previous campaign (numeric; 999 means client was not previously contacted)
# MAGIC - previous: number of contacts performed before this campaign and for this client (numeric)
# MAGIC - poutcome: outcome of the previous marketing campaign (categorical: 'failure','nonexistent','success')
# MAGIC
# MAGIC Output variable (desired target):
# MAGIC 21 - y - has the client subscribed a term deposit? (binary: 'yes','no')

# COMMAND ----------

# MAGIC %md ### Read data with Spark

# COMMAND ----------

def read_data(path):
  """
  This function read data data from s3 and drops columns that are not required for the modelling process
  
  :param path: the input path on dbfs 
  :return: a dataframe referencing the data on the input path
  """
    
  return spark.read.format("delta")\
    .option("path", path)\
    .load()\
    .drop("duration")\
    .drop("features")\
    .drop("label")


train_df = read_data(train_data_path)
test_df = read_data(test_data_path)  

# COMMAND ----------

display(train_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle missing values, and generate features using Spark pipelines

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.sql.functions import col, expr

def fixMissingValues(df):
  """
  This function apply rules regarding missing values to the input dataframe
  
  :param df: the input dataframe 
  :return: a dataframe in which missing value rules have been applied
  """
  
  return (df
  .withColumn("new_prospect", expr("case when pdays = 999 then 1 else 0 end"))
  .withColumn("pdays_clean", expr("case when pdays = 999 then 0 else pdays end")))
  
  
def getNumericalFeaturesPipeline(features):
  """
  This function creates a sub-pipeline that creates numerical features 
  
  :param features: an array of column names containing numerical features 
  :return: a pipeline containing the logic for the numerical features 
  """
  
  numerical_assembler = VectorAssembler(
    inputCols=features,
    outputCol="unscaledNumericalFeatures")
  
  numericalScaler = StandardScaler(inputCol="unscaledNumericalFeatures", outputCol="numericalFeatures", withStd=True, withMean=False)
  
  return Pipeline(stages=[numerical_assembler, numericalScaler])
  
  
def getCategoricalFeaturesPipeline(features):
  """
  This function creates a sub-pipeline that creates categorical features 
  
  :param features: an array of column names containing categorical features
  :return: a pipeline containing the logic for the categorical features 
  """
  
  indexed_features = [feature + "Indexed" for feature in features]
  onehot_features = [feature + "OneHot" for feature in features]
  
  indexers = [StringIndexer(inputCol=feature, outputCol=indexed_feature) for feature, indexed_feature in zip(features, indexed_features)]
  onehot_encoders = OneHotEncoder(inputCols = indexed_features, outputCols = onehot_features)
  categorical_assembler = VectorAssembler(inputCols=onehot_features, outputCol="categoricalFeatures")
  
  return Pipeline(stages=indexers + [onehot_encoders] + [categorical_assembler])  


def getLabelPipeline(target_feature):
  """
  This function creates a sub-pipeline that creates the label column 
  
  :param target_feature: the name of the target feature to be predicted
  :return: a pipeline containing the logic for creating a label column 
  """
  
  indexer = StringIndexer(inputCol=target_feature, outputCol='label')
  return Pipeline(stages=[indexer])
  

def getDataPreparationPipeline(numerical_features, categorical_features, other_features, target_feature):
  """
  This function creates the complete data preparation pipeline that creates both features and labels.
  The pipeline is build using subpipelines for numerical and categorical features.
  
  :param numerical_features: an array of column names containing numerical features 
  :param categorical_features: an array of column names containing categorical features
  :param other_features: an array of column names containing features that do not need processing
  :param target_feature: the name of the target feature to be predicted
  :return: a pipeline containing the logic for the complete data preparation 
  """
  
  feature_assembler = VectorAssembler(
    inputCols=["numericalFeatures", "categoricalFeatures"] + other_features,
    outputCol="features")
  
  return Pipeline(stages=[getNumericalFeaturesPipeline(numerical_features),
                          getCategoricalFeaturesPipeline(categorical_features),
                          getLabelPipeline(target_feature),
                          feature_assembler]) 

# Categories the input columns
numerical_features = ["age", "balance", "pdays_clean", "previous"]
categorical_features = ["job", "education", "housing", "loan", "contact", "poutcome"]
target_feature = "y"

# Apply missing value rules
train_df, test_df = fixMissingValues(train_df), fixMissingValues(test_df)

# Create and fit the data input pipeline
featuresPipeline = getDataPreparationPipeline(
  numerical_features, 
  categorical_features, 
  ["new_prospect"],
  target_feature
)

featuresPipelineModel = featuresPipeline.fit(train_df)

# Apply the input pipeline to both the train and test dataframes
train_input_df = (featuresPipelineModel
            .transform(train_df)
            .select("features", "label"))

test_input_df = (featuresPipelineModel
            .transform(test_df)
            .select("features", "label"))

# COMMAND ----------

# MAGIC %md ### Convert data to Pandas

# COMMAND ----------

import pandas as pd
import numpy as np
import os.path

# Vectorized data is not supported by Arrow
spark.conf.set("spark.sql.execution.arrow.enabled", False)

def convert2numpy(df):
  """
  This function converts the vectorized feature column and the label column of a Spark dataframe into a numpy matrix
  
  :param df: the input Spark dataframe 
  :return: a numpy matrix containing the features and label as columns 
  """
    
  pandas_df = df.toPandas()
  n = len(pandas_df)
  series = pandas_df['features'].apply(lambda x : np.array(x.toArray())).values.reshape(-1,1)
  X = np.apply_along_axis(lambda x : x[0], 1, series)
  y = pandas_df['label'].values
  data = np.concatenate((X, y.reshape(-1,1)), axis=1)
  return data

# COMMAND ----------

# MAGIC %md ### Write data to DBFS

# COMMAND ----------

# Convert the data to numpy
train_data = convert2numpy(train_input_df)
validation_data = convert2numpy(test_input_df)

# COMMAND ----------

# Save to dbfs
path = "/dbfs/ml-workshop-datasets/employee/numpy"
if not os.path.exists(path):
  os.makedirs(path)
np.save("{}/{}".format(path, "train.npy"), train_data)
np.save("{}/{}".format(path, "test.npy"), validation_data)

# COMMAND ----------

n = train_data.shape[0]
m = train_data.shape[1]

assert m == validation_data.shape[1], "The train and test data must have the same number of features"

# COMMAND ----------


