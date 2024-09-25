# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Understanding Parallelization of Machine Learning Algorithms in Apache Spark™
# MAGIC
# MAGIC ##Parallelize Hyperparameter Tuning with Hyperopt

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The dataset used for this example is Bank marketing. Given a set of features about a customer can we predict whether the person will open a term deposit account.
# MAGIC
# MAGIC Original Source: [UCI Machine Learning Repository 
# MAGIC Bank Marketing Data Set](https://archive.ics.uci.edu/ml/datasets/bank+marketing)
# MAGIC [Moro et al., 2014] S. Moro, P. Cortez and P. Rita. A Data-Driven Approach to Predict the Success of Bank Telemarketing. Decision Support Systems, Elsevier, 62:22-31, June 2014
# MAGIC
# MAGIC Requirements: Ensure that the data prep notebooks have been run, should have been done prior to workshop

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Attribute Information:
# MAGIC
# MAGIC ####Input variables:
# MAGIC bank client data:
# MAGIC ```
# MAGIC 1 - age (numeric)
# MAGIC 2 - job : type of job (categorical: 'admin.','blue-collar','entrepreneur','housemaid','management','retired','self-employed','services','student','technician','unemployed','unknown')
# MAGIC 3 - marital : marital status (categorical: 'divorced','married','single','unknown'; note: 'divorced' means divorced or widowed)
# MAGIC 4 - education (categorical: 'basic.4y','basic.6y','basic.9y','high.school','illiterate','professional.course','university.degree','unknown')
# MAGIC 5 - default: has credit in default? (categorical: 'no','yes','unknown')
# MAGIC 6 - housing: has housing loan? (categorical: 'no','yes','unknown')
# MAGIC 7 - loan: has personal loan? (categorical: 'no','yes','unknown')
# MAGIC related with the last contact of the current campaign:
# MAGIC 8 - contact: contact communication type (categorical: 'cellular','telephone') 
# MAGIC 9 - month: last contact month of year (categorical: 'jan', 'feb', 'mar', ..., 'nov', 'dec')
# MAGIC 10 - day_of_week: last contact day of the week (categorical: 'mon','tue','wed','thu','fri')
# MAGIC 11 - duration: last contact duration, in seconds (numeric). Important note: this attribute highly affects the output target (e.g., if duration=0 then y='no'). Yet, the duration is not known before a call is performed. Also, after the end of the call y is obviously known. Thus, this input should only be included for benchmark purposes and should be discarded if the intention is to have a realistic predictive model.
# MAGIC other attributes:
# MAGIC 12 - campaign: number of contacts performed during this campaign and for this client (numeric, includes last contact)
# MAGIC 13 - pdays: number of days that passed by after the client was last contacted from a previous campaign (numeric; 999 means client was not previously contacted)
# MAGIC 14 - previous: number of contacts performed before this campaign and for this client (numeric)
# MAGIC 15 - poutcome: outcome of the previous marketing campaign (categorical: 'failure','nonexistent','success')
# MAGIC social and economic context attributes
# MAGIC 16 - emp.var.rate: employment variation rate - quarterly indicator (numeric)
# MAGIC 17 - cons.price.idx: consumer price index - monthly indicator (numeric) 
# MAGIC 18 - cons.conf.idx: consumer confidence index - monthly indicator (numeric) 
# MAGIC 19 - euribor3m: euribor 3 month rate - daily indicator (numeric)
# MAGIC 20 - nr.employed: number of employees - quarterly indicator (numeric)
# MAGIC
# MAGIC Output variable (desired target):
# MAGIC 21 - y - has the client subscribed a term deposit? (binary: 'yes','no')
# MAGIC ```

# COMMAND ----------

# One additional setup notebook required to run
dbutils.notebook.run("../Utils/02 Feature Engineering for Distributed HyperOpt", 0, {"databricksUsername":"test"})

# COMMAND ----------

train_data_path = "/ml-workshop-datasets/employee/delta/trainingData"
test_data_path = "/ml-workshop-datasets/employee/delta/testData"

# COMMAND ----------

# MAGIC %md ## Introduction
# MAGIC If using the sklearn implementation, one can choose to distribute the model evaluation tasks using distributed HyperOpt in two ways. First one can run 1 core per model, this means that the number of concurrent model evaluations is equal to the total number of cores on the workers. Alternatively one can run 1 model per worker, using all the cores on a worker for optimizing one model. In this case the number of concurrent model evaluations is equal to the number of workers. This setup allows model evaluations to be performed faster if one is willing to run a larger cluster. It requires to set the Spark option spark.task.cpus to the number of cores of the worker nodes. 
# MAGIC
# MAGIC In this notebook a sklearn setup with 1 core per model and distributed HyperOpt will be demonstrated.
# MAGIC
# MAGIC #### Note: the data used in this demo is created by the 'Feature Engineering for Distributed HyperOpt' notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data description recap
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

# MAGIC %md ## Sklearn with distributed HyperOpt 

# COMMAND ----------

import pandas as pd
import numpy as np
import os.path
import datetime
from sklearn.ensemble.forest import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

import mlflow

import hyperopt as hp
from hyperopt import fmin, rand, tpe, hp, Trials, exceptions, space_eval, STATUS_FAIL, STATUS_OK
from hyperopt import SparkTrials

# COMMAND ----------

# MAGIC %md ### Random forest training logic
# MAGIC The following functions are defined:
# MAGIC - read_data - reads numpy data from a path and return X and y data matrices
# MAGIC - get_model - takes a set of hyperparameters and returns a parametrized random forest model
# MAGIC - train - takes a set of hyperparameters and returns a trained model and the out of bootstrap error

# COMMAND ----------

def read_data(path):  
  """
  This method reads numpy data from disk

  :param path: The path to the file containing the data
  :return: X and y numpy arrays
  """
  
  input_data = np.load(path)
  
  X = input_data[:,:-1]
  y = input_data[:,-1]
  
  return X, y


def get_model(params):
  """
  This function creates a random forest model using the given hyperparameters.
  
  :param params: This dict of parameters specifies hyperparameters
  :return: an untrained model 
  """
  min_samples_leaf = int(params['min_samples_leaf'])
  criterion = params['criterion']
  n_estimators = int(params['n_estimators'])
  random_state = params['seed']
    
  model = RandomForestClassifier(bootstrap=True, class_weight=None, criterion=criterion,
            max_depth=None, max_features='auto', max_leaf_nodes=None,
            min_samples_leaf=min_samples_leaf,
            min_samples_split=2, min_weight_fraction_leaf=0.0,
            n_estimators=n_estimators, n_jobs=1, oob_score=True, random_state=random_state,
            verbose=0, warm_start=False)
  
  return model
  
  
def train(params):
  """
  This function trains and evaluates a model using the given hyperparameters.
  
  :param params: This dict of parameters specifies hyperparameters.
  :return: a trained model and the resulting validation loss
  """

  training_path = "/dbfs/ml-workshop-datasets/employee/numpy/train.npy"
  X, y = read_data(training_path)
      
  model = get_model(params)
  model.fit(X, y)
  
  return model, 1 - model.oob_score_

# COMMAND ----------

# MAGIC %md ### Run distrbuted HyperOpt
# MAGIC Key components for running distributed hyperopt are:
# MAGIC - evaluate_hyperparams - a helper function that extracts the out of boostrap score for a trained model
# MAGIC - search_space - the parameter space that HyperOpt will use for its search
# MAGIC - algo - the type of algorithm used by HyperOpt (Tree of Parzen Estimators or random search)
# MAGIC - spark_trails - a helper object that makes HyperOpt distributed
# MAGIC - fmin - the function that performs the search of the hyper parameter space

# COMMAND ----------

def evaluate_hyperparams(params):
  """
  This method will be passed to `hyperopt.fmin()`.  It fits and evaluates the model using the given hyperparameters to get the validation loss.
  
  :param params: This dict of parameters specifies hyperparameter values to test.
  :return: dict with fields 'loss' (scalar loss) and 'status' (success/failure status of run)
  """
  
  # Train the model
  model, score = train(params)
        
  return {'loss': score, 'status': STATUS_OK}


# The hyper parameter space to search
criteria = ['gini', 'entropy']
search_space = {
  'seed': hp.randint('seed', 1000000),
  'n_estimators': hp.uniform('n_estimators', 10, 500),
  'min_samples_leaf': hp.uniform('min_samples_leaf', 1, 20),
  'criterion': hp.choice('criterion', criteria)
}

# The algoritm to perform the parameter search
algo=tpe.suggest  # Tree of Parzen Estimators (a "Bayesian" method)
#algo=random.suggest  # Random search

# Configure parallelization
spark_trials = SparkTrials(parallelism=4)

# name the run
now = datetime.datetime.now()
run_name = now.strftime("%Y%m%d-%H%M")

# Execute the search with MLFlow support
with mlflow.start_run(run_name=run_name):
  argmin = fmin(
    fn=evaluate_hyperparams,
    space=search_space,
    algo=algo,
    max_evals=40,
    trials=spark_trials)

# COMMAND ----------

# MAGIC %md The optimal hyperparameters are now set in argmin

# COMMAND ----------

argmin

# COMMAND ----------

# MAGIC %md ### Recreate the best model

# COMMAND ----------

params = {'criterion': 1,
 'min_samples_leaf': 8.95432038529209,
 'n_estimators': 173.33988549738507,
 'seed': 569904}

# The argmin contains indices for gini and entropy, we have to fix this
params['criterion'] = criteria[params['criterion']]

model, loss = train(params)

print("Validation loss: {}".format(loss))

# COMMAND ----------

model

# COMMAND ----------

# MAGIC %md ### Validate on the test data

# COMMAND ----------

# Read the data and create a test datagenerator
test_path = "/dbfs/ml-workshop-datasets/employee/numpy/test.npy"
X_test, y_test = read_data(test_path)

predictions = model.predict(X_test)

# Output some metrics
print("Accuracy: {}".format(accuracy_score(y_test, predictions)))
print(classification_report(y_test, predictions))

# COMMAND ----------

# MAGIC %md ### Use MLflow to show the hyperparameter effects in a parallel coordinates plot.
# MAGIC
# MAGIC Select the Runs expand option on the right side of the screen: 
# MAGIC  - Look at the fmin_uuid for the hyperopt run
# MAGIC  - Search for relevant entries, for example tags.fmin_uuid = 'a75fb4'
# MAGIC  - Select all observations
# MAGIC  - Select 'Compare'
# MAGIC  - Select 'Parallel coordinates plot'
# MAGIC  

# COMMAND ----------

# MAGIC %md ## Fin

# COMMAND ----------

# MAGIC %md ## Extra: Distributed random forests with single threaded hyperopt
# MAGIC Instead of running multiple models parallel, it is possible to run a single model parallelised by using MLlib's random forrest implementation. In this case HyperOpt will be run in non-distributed mode.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.mllib.tree import RandomForest
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

import mlflow

import hyperopt as hp
from hyperopt import fmin, rand, tpe, hp, Trials, exceptions, space_eval, STATUS_FAIL, STATUS_OK
import datetime

# COMMAND ----------

# MAGIC %md ## Read data
# MAGIC Read the train and test data. This data is already in a dataframe, and the features and label columns for MLlib have already been created.

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
    .select("features", "label")



train_df = read_data(train_data_path)
test_df = read_data(test_data_path) 

# COMMAND ----------

display(train_df)

# COMMAND ----------

# MAGIC %md ## Training function
# MAGIC For each set of parameters selected by HyperOpt a model will be fit. The return values constitue of the fitted model and the validation accuracy.

# COMMAND ----------

def fit_model(params, training_df):
  """
  This function fits a model given the params passed by HyperOpt.
  
  :param params: the params passed by HyperOpt. 
  :param training_df: the data to fit the model with. 
  :return: a fitted model.
  """
    
  # Get the HyperOpt settings
  seed = params['seed']
  num_trees = int(params['num_trees'])
  min_instances_per_node = int(params['min_instances_per_node'])
  impurity = params['impurity']

  # Cache the training data in memory
  training_df.cache()
  n = training_df.count()

  # Create and fit the random forest classifier
  rf = RandomForestClassifier(labelCol="label", 
                              featuresCol="features", 
                              featureSubsetStrategy="sqrt", 
                              numTrees=num_trees, 
                              minInstancesPerNode=min_instances_per_node,
                              impurity=impurity,
                              seed=seed)

  pipeline = Pipeline(stages = [rf])
  rf_model = pipeline.fit(training_df)
  
  return rf_model
  
  # Cleanup memory
  training_df.unpersist()
  
def train_mllib(params):
  """
  This function trains a model given the params passed by HyperOpt.
  
  :param params: the params passed by HyperOpt. 
  :return: a model and the validation score.
  """

  # Get the HyperOpt settings  
  seed = params['seed']

  # There is no support for out-of-bag evaluation
  (training_df, validation_df) = train_df.randomSplit([0.8, 0.2], seed=seed)

  # Fit a model to the training data
  rf_model = fit_model(params, training_df)
  
  # Calculate validation score
  predictions = rf_model.transform(validation_df)
  
  evaluator = MulticlassClassificationEvaluator(metricName='accuracy')
  score = 1.0 - evaluator.evaluate(predictions)
  
  return rf_model, score

# COMMAND ----------

def evaluate_mllib_hyperparams(params):
  """
  This method will be passed to `hyperopt.fmin()`.  It fits and evaluates the model using the given hyperparameters to get the validation loss.
  
  :param params: This dict of parameters specifies hyperparameter values to test.
  :return: dict with fields 'loss' (scalar loss) and 'status' (success/failure status of run)
  """
  
  # Train the model
  model, score = train_mllib(params)
        
  return {'loss': score, 'status': STATUS_OK, 'model': model}

# The hyper parameter space to search
impurities = ['gini', 'entropy']

search_space = {
  'seed': hp.randint('seed', 1000000),
  'num_trees': hp.uniform('num_trees', 10, 500),
  'min_instances_per_node': hp.uniform('min_instances_per_node', 1, 20),
  'impurity': hp.choice('impurity', impurities)
}

# The algoritm to perform the parameter search
algo=tpe.suggest  # Tree of Parzen Estimators (a "Bayesian" method)
#algo=random.suggest  # Random search

# The results of the HyperOpt run will end up in the trials object
trials = Trials()

# name the run
now = datetime.datetime.now()
run_name = now.strftime("%Y%m%d-%H%M")

# Execute the search with MLFlow support
with mlflow.start_run(run_name=run_name):
  argmin = fmin(
    fn=evaluate_mllib_hyperparams,
    space=search_space,
    algo=algo,
    trials = trials,
    max_evals=40)

# COMMAND ----------

argmin

# COMMAND ----------

# MAGIC %md ## Recreate best model using the HyperOpt output

# COMMAND ----------

# Because HyperOpt was run locally, we could return the trained model 
# along with the loss and the status in evaluate_mllib_hyperparams.
# Therefor we can now lookup the best model in the trials object
model = trials.results[np.argmin([r['loss'] for r in trials.results])]['model']

# COMMAND ----------

# Since storing all the model of all the trials requires quite some memory, 
# an alternative would be to not return the model from evaluate_mllib_hyperparams.
# In this case the model needs to be recreated.
params = {'impurity': 1,
 'min_instances_per_node': 14.058068956915708,
 'num_trees': 43.63883612231973,
 'seed': 827362}

params['impurity'] = impurities[params['impurity']]

# Consider using all the data with model = fit_model(params, train_df)
model, score = train_mllib(params)

# COMMAND ----------

# MAGIC %md ## Generate scores on the test data

# COMMAND ----------

predictions = model.transform(test_df)

evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC')
auc = evaluator.evaluate(predictions)
print("AUC score on test data: {}".format(auc))
  
# MulticlassClassificationEvaluator supports the F1 score
evaluator = MulticlassClassificationEvaluator()
f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
print("F1 score on test data: {}".format(f1))

# MulticlassClassificationEvaluator supports the F1 score
evaluator = MulticlassClassificationEvaluator()
acc = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
print("Accuracy score on test data: {}".format(acc))

# COMMAND ----------


