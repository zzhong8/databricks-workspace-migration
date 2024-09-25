# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src="https://mlflow.org/docs/latest/_static/MLflow-logo-final-black.png" style="float: left: margin: 20px; height:5em"/>
# MAGIC # Course Overview and Setup
# MAGIC ## MLflow: Managing the Machine Learning Lifecycle
# MAGIC
# MAGIC In this course data scientists and data engineers learn the best practices for managing experiments, projects, and models using MLflow.
# MAGIC
# MAGIC By the end of this course, you will have built a pipeline to log and deploy machine learning models using the environment they were trained with.
# MAGIC
# MAGIC ## Lessons
# MAGIC
# MAGIC 0. Experiment Tracking 
# MAGIC 0. Packaging ML Projects
# MAGIC 0. Multistep Workflows
# MAGIC 0. Model Management 
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Governance, Data Scientists, and Data Analysts
# MAGIC * Additional Audiences: Data Engineers

# COMMAND ----------

# MAGIC %md
# MAGIC # Experiment Tracking
# MAGIC
# MAGIC The machine learning life cycle involves training multiple algorithms using different hyperparameters and libraries, all with different performance results and trained models.  This lesson explores tracking those experiments to organize the machine learning life cycle.
# MAGIC
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson:<br>
# MAGIC  - Introduce tracking ML experiments in MLflow
# MAGIC  - Log an experiment and explore the results in the UI
# MAGIC  - Record parameters, metrics, and a model
# MAGIC  - Query past runs programatically

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Tracking Experiments with MLflow
# MAGIC
# MAGIC Over the course of the machine learning life cycle, data scientists test many different models from various libraries with different hyperparameters.  Tracking these various results poses an organizational challenge.  In brief, storing experiments, results, models, supplementary artifacts, and code creates significant challenges.
# MAGIC
# MAGIC MLflow Tracking is one of the three main components of MLflow.  It is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC
# MAGIC Each run can record the following information:<br><br>
# MAGIC
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiment
# MAGIC
# MAGIC MLflow tracking also serves as a **model registry** so tracked models can easily be stored and, as necessary, deployed into production.
# MAGIC
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.  This course will use Python, though the majority of MLflow functionality is also exposed in these other APIs.
# MAGIC
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Steps: Experiment Logging and UI
# MAGIC
# MAGIC MLflow is an open source software project developed by Databricks available to developers regardless of which platform they are using.  Databricks hosts MLflow for you, which reduces deployment configuration and adds security benefits.  It is accessible on all Databricks workspaces in Azure and AWS.  It is not currently enabled on Community Edition
# MAGIC
# MAGIC See <a href="https://mlflow.org/docs/latest/quickstart.html#" target="_blank">the MLflow quickstart guide</a> for details on setting up MLflow locally or on your own server.

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks uses two different versions of its software.  The **Databricks Runtime** does not currently come with MLflow pre-installed.  if you are using Databricks runtime, uncomment the code below and run it to install `mlflow`.

# COMMAND ----------

# This installs MLflow for you only on Databricks Runtime
# NOTE: this code does not work with ML runtime (see below)

# dbutils.library.installPyPI("mlflow", "1.0.0")
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC The other version of Databricks is the **Machine Learning Runtime,** which you'll notice has `ML` next to the cluster type.  **Machine Learning Runtime has `mlflow` pre-installed starting at version 5.5.**  If you have runtime **5.5 ML or later** available to you, you do not need to install `mlflow`.
# MAGIC
# MAGIC If your version of Machine Learning Runtime is lower than 5.5, please install the library `mlflow==1.0.0` using `PyPi` manually.  <a href="https://files.training.databricks.com/static/step-by-step/installing-libraries-from-pypi.html" target="_blank">See the instructions on how to install a library from PyPi</a> if you're unfamiliar with the process

# COMMAND ----------

# MAGIC %md
# MAGIC ### OK, Let's start our data science workflow!
# MAGIC Import a dataset of Airbnb listings and featurize the data.  We'll use this to train a model.
# MAGIC
# MAGIC If you are running the below code on Community Edition with DBR 7.0+, please replace it with `df = spark.read.csv("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv", header = True).toPandas()`.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a train/test split.

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

# COMMAND ----------

display(X_train)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC **Navigate to the MLflow UI by clicking on the `Runs` button on the top of the screen.**
# MAGIC
# MAGIC Every Python notebook in a Databricks Workspace has its own experiment. When you use MLflow in a notebook, it records runs in the notebook experiment. A notebook experiment shares the same name and ID as its corresponding notebook. 

# COMMAND ----------

# MAGIC %md
# MAGIC Log a basic experiment by doing the following:<br><br>
# MAGIC
# MAGIC 1. Start an experiment using `mlflow.start_run()` and passing it a name for the run
# MAGIC 2. Train your model
# MAGIC 3. Log the model using `mlflow.sklearn.log_model()`
# MAGIC 4. Log the model error using `mlflow.log_metric()`
# MAGIC 5. Print out the run id using `run.info.run_uuid`

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

with mlflow.start_run(run_name="Basic RF Experiment") as run:
  # Create model, train it, and create predictions
  rf = RandomForestRegressor()
  rf.fit(X_train, y_train)
  predictions = rf.predict(X_test)
  
  # Log model
  mlflow.sklearn.log_model(rf, "random-forest-model")
  
  # Create metrics
  mse = mean_squared_error(y_test, predictions)
  print("  mse: {}".format(mse))
  
  # Log metrics
  mlflow.log_metric("mse", mse)
  
  runID = run.info.run_uuid
  experimentID = run.info.experiment_id
  
  print("Inside MLflow Run with run_id {} and experiment_id {}".format(runID, experimentID))

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take a look at the `Runs` tab again!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Parameters, Metrics, and Artifacts
# MAGIC
# MAGIC But wait, there's more!  In the last example, you logged the run name, an evaluation metric, and your model itself as an artifact.  Now let's log parameters, multiple metrics, and other artifacts including the feature importances.
# MAGIC
# MAGIC First, create a function to perform this.
# MAGIC
# MAGIC To log artifacts, we have to save them somewhere before MLflow can log them.  This code accomplishes that by using a temporary file that it then deletes.

# COMMAND ----------

def log_rf(experimentID, run_name, params, X_train, X_test, y_train, y_test):
  import os
  import matplotlib.pyplot as plt
  import mlflow.sklearn
  import seaborn as sns
  from sklearn.ensemble import RandomForestRegressor
  from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
  import tempfile

  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    # Create model, train it, and create predictions
    rf = RandomForestRegressor(**params)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(rf, "random-forest-model")

    # Log params
    [mlflow.log_param(param, value) for param, value in params.items()]

    # Create metrics
    mse = mean_squared_error(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    print("  mse: {}".format(mse))
    print("  mae: {}".format(mae))
    print("  R2: {}".format(r2))

    # Log metrics
    mlflow.log_metric("mse", mse)
    mlflow.log_metric("mae", mae)  
    mlflow.log_metric("r2", r2)  
    
    # Create feature importance
    importance = pd.DataFrame(list(zip(df.columns, rf.feature_importances_)), 
                                columns=["Feature", "Importance"]
                              ).sort_values("Importance", ascending=False)
    
    # Log importances using a temporary file
    temp = tempfile.NamedTemporaryFile(prefix="feature-importance-", suffix=".csv")
    temp_name = temp.name
    try:
      importance.to_csv(temp_name, index=False)
      mlflow.log_artifact(temp_name, "feature-importance.csv")
    finally:
      temp.close() # Delete the temp file
    
    # Create plot
    fig, ax = plt.subplots()

    sns.residplot(predictions, y_test, lowess=True)
    plt.xlabel("Predicted values for Price ($)")
    plt.ylabel("Residual")
    plt.title("Residual Plot")

    # Log residuals using a temporary file
    temp = tempfile.NamedTemporaryFile(prefix="residuals-", suffix=".png")
    temp_name = temp.name
    try:
      fig.savefig(temp_name)
      mlflow.log_artifact(temp_name, "residuals.png")
    finally:
      temp.close() # Delete the temp file
      
    display(fig)
    return run.info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC We can also use MLflow Autologging to automatically log all parameters of interest with sklearn or any other supported library:

# COMMAND ----------

def log_rf_auto(experimentID, run_name, params, X_train, X_test, y_train, y_test):
  import mlflow.sklearn
  from sklearn.ensemble import RandomForestRegressor
  from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    #Use Autologging
    mlflow.sklearn.autolog()
    
    # Create model, train it, and create predictions
    rf = RandomForestRegressor(**params)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Create metrics
    mse = mean_squared_error(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    print("  mse: {}".format(mse))
    print("  mae: {}".format(mae))
    print("  R2: {}".format(r2))
    
    return run.info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC Run with new parameters.

# COMMAND ----------

params = {
  "n_estimators": 100,
  "max_depth": 5,
  "random_state": 42
}

log_rf(experimentID, "Second Run", params, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC Check the UI to see how this appears.  Take a look at the artifact to see where the plot was saved.
# MAGIC
# MAGIC Now, run a third run.

# COMMAND ----------

params_1000_trees = {
  "n_estimators": 500,
  "max_depth": 10,
  "random_state": 42
}

log_rf_auto(experimentID, "Autologged Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC Explore the MLflow UI in detail. We can also interact with MLflow programmatically.
# MAGIC
# MAGIC Now take a look at the directory structure backing this experiment.  This allows you to retrieve artifacts. Depending on how many times you've run the notebook, you will see different results, one artifact path per run. 

# COMMAND ----------

from mlflow.tracking import MlflowClient

artifactURL = MlflowClient().get_experiment(experimentID).artifact_location
dbutils.fs.ls(artifactURL)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the contents of `random-forest-model`, which match what we see in the UI.

# COMMAND ----------

modelURL = artifactURL + "/" + runID + "/artifacts/random-forest-model"
display(dbutils.fs.ls(modelURL))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Querying Past Runs
# MAGIC
# MAGIC You can query past runs programatically in order to use this data back in Python.  The pathway to doing this is an `MlflowClient` object. 
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also set tags for runs using `client.set_tag(run.info.run_uuid, "tag_key", "tag_value")`

# COMMAND ----------

client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC Now list all the runs for your experiment using `.list_run_infos()`, which takes your `experiment_id` as a parameter.

# COMMAND ----------

client.list_run_infos(experimentID)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull out a few fields and create a pandas DataFrame with it.

# COMMAND ----------

runs = pd.DataFrame([(run.run_uuid, run.start_time, run.artifact_uri) for run in client.list_run_infos(experimentID)])
runs.columns = ["run_uuid", "start_time", "artifact_uri"]

display(runs)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the last run and take a look at the associated artifacts.

# COMMAND ----------

last_run = runs.sort_values("start_time", ascending=False).iloc[0]

dbutils.fs.ls(last_run["artifact_uri"]+"/random-forest-model/")

# COMMAND ----------

# MAGIC %md
# MAGIC Return the evaluation metrics for the last run.

# COMMAND ----------

client.get_run(last_run.run_uuid).data.metrics

# COMMAND ----------

# MAGIC %md
# MAGIC Reload the model and take a look at the feature importance.

# COMMAND ----------

display(dbutils.fs.ls(last_run.artifact_uri))

# COMMAND ----------

import mlflow.sklearn

model = mlflow.sklearn.load_model(last_run.artifact_uri + "/random-forest-model/")
model.feature_importances_

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Steps
# MAGIC
# MAGIC Start the labs for this lesson, Experiment Tracking Lab 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What can MLflow Tracking log?  
# MAGIC **Answer:** MLflow can log the following:
# MAGIC - **Parameters:** inputs to a model
# MAGIC - **Metrics:** the performance of the model
# MAGIC - **Artifacts:** any object including data, models, and images
# MAGIC - **Source:** the original code, including the commit hash if linked to git
# MAGIC
# MAGIC **Question:** How do you log experiments?  
# MAGIC **Answer:** Experiments are logged by first creating a run and using the logging methods on that run object (e.g. `run.log_param("MSE", .2)`).
# MAGIC
# MAGIC **Question:** Where do logged artifacts get saved?  
# MAGIC **Answer:** Logged artifacts are saved in a directory of your choosing.  On Databricks, this would be DBFS, or the Databricks File System, which backed by a blob store.
# MAGIC
# MAGIC **Question:** How can I query past runs?  
# MAGIC **Answer:** This can be done using an `MlflowClient` object.  This allows you do everything you can within the UI programatically so you never have to step outside of your programming environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC **Q:** What is MLflow at a high level?  
# MAGIC **A:** <a href="https://databricks.com/session/accelerating-the-machine-learning-lifecycle-with-mlflow-1-0" target="_blank">Listen to Spark and MLflow creator Matei Zaharia's talk at Spark Summit in 2019.</a>
# MAGIC
# MAGIC **Q:** What is a good source for the larger context of machine learning tools?  
# MAGIC **A:** <a href="https://roaringelephant.org/2019/06/18/episode-145-alex-zeltov-on-mlops-with-mlflow-kubeflow-and-other-tools-part-1/#more-1958" target="_blank">Check out this episode of the podcast Roaring Elephant.</a>
# MAGIC
# MAGIC **Q:** Where can I find the MLflow docs?
# MAGIC **A:** <a href="https://www.mlflow.org/docs/latest/index.html" target="_blank">You can find the docs here.</a>
# MAGIC
# MAGIC **Q:** What is a good general resource for machine learning?  
# MAGIC **A:** <a href="https://www-bcf.usc.edu/~gareth/ISL/" target="_blank">_An Introduction to Statistical Learning_</a> is a good starting point for the themes and basic approaches to machine learning.
# MAGIC
# MAGIC **Q:** Where can I find out more information on machine learning with Spark?
# MAGIC **A:** Check out the Databricks blog <a href="https://databricks.com/blog/category/engineering/machine-learning" target="_blank">dedicated to machine learning</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
