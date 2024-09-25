# Databricks notebook source
# MAGIC %md
# MAGIC # Model Registry
# MAGIC
# MAGIC MLflow already has the ability to track metrics, parameters, and artifacts as part of experiments; package models and reproducible ML projects; and deploy models to batch or real-time serving platforms. Built on these existing capabilities, the **MLflow Model Registry** provides a central repository to manage the model deployment lifecycle.
# MAGIC
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce model registry for model lifecycle management
# MAGIC  - Interact with the model registry UI
# MAGIC  - Programmatically register and query models 
# MAGIC  
# MAGIC This notebook is based off the Databricks blog post <a href="https://databricks.com/blog/2020/04/15/databricks-extends-mlflow-model-registry-with-enterprise-features.html" target="_blank">Use Model Registry APIs for integration and inspection</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managing the model lifecycle with Model Registry
# MAGIC
# MAGIC One of the primary challenges among data scientists in a large organization is the absence of a central repository to collaborate, share code, and manage deployment stage transitions for models, model versions, and their history. A centralized registry for models across an organization affords data teams the ability to:
# MAGIC
# MAGIC * discover registered models, current stage in model development, experiment runs, and associated code with a registered model
# MAGIC * transition models to deployment stages
# MAGIC * deploy different versions of a registered model in different stages, offering MLOps engineers ability to deploy and conduct testing of different model versions
# MAGIC * archive older models for posterity and provenance
# MAGIC * peruse model activities and annotations throughout model’s lifecycle
# MAGIC * control granular access and permission for model registrations, transitions or modifications
# MAGIC
# MAGIC <div><img src="https://databricks.com/wp-content/uploads/2020/04/databricks-adds-access-control-to-mlflow-model-registry_01.jpg" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Use the Model Registry
# MAGIC Typically, data scientists who use MLflow will conduct many experiments, each with a number of runs that track and log metrics and parameters. 
# MAGIC
# MAGIC During the course of this development cycle, they will select the best run within an experiment and register its model with the registry. Thereafter, the registry will let data scientists track multiple versions over the course of model progression as they assign each version with a lifecycle stage: 
# MAGIC * Staging
# MAGIC * Production
# MAGIC * Archived

# COMMAND ----------

# MAGIC %md
# MAGIC ## **NOTE**: This notebook requires the latest MLflow version 
# MAGIC To resolve, manually upgrade the cluster runtime version to DBR ML 7.1+ or make sure you have Conda enabled on your cluster.
# MAGIC
# MAGIC * Add the required Spark Config: `spark.databricks.conda.condaMagic.enabled true`
# MAGIC * See here for more details: https://docs.databricks.com/release-notes/runtime/7.0ml.html#known-issues

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

rf = RandomForestRegressor(n_estimators=100, max_depth=5)
rf.fit(X_train, y_train)

with mlflow.start_run(run_name="RF Model") as run:
  mlflow.sklearn.log_model(rf, "model")
  mlflow.log_metric("mse", mean_squared_error(y_test, rf.predict(X_test)))

  runID = run.info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Model Registry UI Workflows
# MAGIC The Model Registry UI is accessible from the Databricks workspace. From the Model Registry UI, you can conduct the following activities as part of your workflow:
# MAGIC
# MAGIC
# MAGIC * Register a model from the Run’s page
# MAGIC * Edit a model version description
# MAGIC * Transition a model version
# MAGIC * View model version activities and annotations
# MAGIC * Display and search registered models
# MAGIC * Delete a model version
# MAGIC
# MAGIC Run the cell below, which creates a new MLflow experiment similar to previous notebooks, then navigate back to the MLflow screen for this experiment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Model Registry API Workflows
# MAGIC
# MAGIC All aspects of the Model Registry can be called programmatically via API

# COMMAND ----------

# MAGIC %md
# MAGIC Create a unique model name so you don't clash with other workspace users.

# COMMAND ----------

import uuid
model_name = f"airbnb_rf_model_{uuid.uuid4().hex[:10]}"
model_name

# COMMAND ----------

model_uri = "runs:/{run_id}/model".format(run_id=runID)

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC  **Open the *Models* tab on the left of the screen to explore the registered model.**  
# MAGIC  
# MAGIC  Note the following:<br><br>
# MAGIC
# MAGIC * It logged who trained the model and what code was used
# MAGIC * It logged a history of actions taken on this model
# MAGIC * It logged this model as a first version

# COMMAND ----------

# MAGIC %md
# MAGIC Check the status.  It will initially be in `PENDING_REGISTRATION` status, then `READY` when it has been async registered

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
model_version_details = client.get_model_version(name=model_name, version=1)

model_version_details.status

# COMMAND ----------

# MAGIC %md
# MAGIC Now add a model description

# COMMAND ----------

client.update_registered_model(
  name=model_details.name,
  description="This model forecasts Airbnb housing list prices based on various listing inputs."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Add a version-specific description.

# COMMAND ----------

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description="This model version was built using sklearn."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploying a Model
# MAGIC
# MAGIC The MLflow Model Registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC
# MAGIC Users with appropriate permissions can transition models between stages. In private preview, any user can transition a model to any stage. In the near future, administrators in your organization will be able to control these permissions on a per-user and per-model basis.
# MAGIC
# MAGIC If you have permission to transition a model to a particular stage, you can make the transition directly by using the `MlflowClient.update_model_version()` function. If you do not have permission, you can request a stage transition using the REST API; for example: ```%sh curl -i -X POST -H "X-Databricks-Org-Id: <YOUR_ORG_ID>" -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" https://<YOUR_DATABRICKS_WORKSPACE_URL>/api/2.0/preview/mlflow/transition-requests/create -d '{"comment": "Please move this model into production!", "model_version": {"version": 1, "registered_model": {"name": "power-forecasting-model"}}, "stage": "Production"}'
# MAGIC ```
# MAGIC
# MAGIC Now that you've learned about stage transitions, transition the model to the `Production` stage.

# COMMAND ----------

import time
time.sleep(5) # In case the registration is still pending

client.transition_model_version_stage(
  name=model_details.name,
  version=model_details.version,
  stage='Production',
)

# COMMAND ----------

# MAGIC %md
# MAGIC Fetch the model's current status.

# COMMAND ----------

model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

# MAGIC %md
# MAGIC Fetch the latest model using a `pyfunc`.  Loading the model in this way allows us to use the model regardless of the package that was used to train it.

# COMMAND ----------

import mlflow.pyfunc

model_version_uri = "models:/{model_name}/production".format(model_name=model_name)

print("Loading PRODUCTION model stage with name: '{model_uri}'".format(model_uri=model_version_uri))
model_version_1 = mlflow.pyfunc.load_model(model_version_uri)

# COMMAND ----------

# MAGIC %md Apply the model on test data

# COMMAND ----------

predictions = model_version_1.predict(X_test)
pd.DataFrame(predictions).head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC We have now demonstrated how a **model can be loaded from the model registry** in **any flavor** and be used to **make predictions on new data**, all with the help of the model registry!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploying a New Model Version
# MAGIC
# MAGIC The MLflow Model Registry enables you to create multiple model versions corresponding to a single registered model. By performing stage transitions, you can seamlessly integrate new model versions into your staging or production environments.
# MAGIC
# MAGIC Create a new model version and register that model when it's logged.

# COMMAND ----------

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

rf = RandomForestRegressor(n_estimators=300, max_depth=10)
rf.fit(X_train, y_train)

with mlflow.start_run(run_name="RF Model") as run:
  # Specify the `registered_model_name` parameter of the `mlflow.sklearn.log_model()`
  # function to register the model with the MLflow Model Registry. This automatically
  # creates a new model version
  mlflow.sklearn.log_model(
    sk_model=rf,
    artifact_path="sklearn-model",
    registered_model_name=model_name,
  )
  mlflow.log_metric("mse", mean_squared_error(y_test, rf.predict(X_test)))

  runID = run.info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC Go back to the Models UI to see that a new version has been added to your model.
# MAGIC
# MAGIC
# MAGIC Now, use the search functionality to grab the latest model version.

# COMMAND ----------

model_version_infos = client.search_model_versions(f"name = '{model_name}'")
new_model_version = max([model_version_info.version for model_version_info in model_version_infos])

# COMMAND ----------

# MAGIC %md
# MAGIC Put this new model version into `Staging`

# COMMAND ----------

client.transition_model_version_stage(
  name=model_name,
  version=new_model_version,
  stage='Staging',
)

# COMMAND ----------

# MAGIC %md
# MAGIC Return to the Models tab to see this change

# COMMAND ----------

# MAGIC %md
# MAGIC #FIN: The rest of the following code is useful code for interaction with the MLflow Model Registry API.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Archiving and Deleting
# MAGIC
# MAGIC You can now archive and delete old versions of the model.

# COMMAND ----------

#Archive
client.update_model_version(
  name=model_name,
  version=1,
  stage="Archived",
)

# COMMAND ----------

#Delete
client.delete_model_version(
  name=model_name,
  version=1
)

# COMMAND ----------

#Delete ENTIRE registered model (including all runs)
client.delete_registered_model(model_name)

# COMMAND ----------

# MAGIC %md ### Find best run

# COMMAND ----------

runs = client.search_runs(experiment_id, order_by=["metrics.rmse ASC"], max_results=1)
best_run = runs[0]
best_run.info.run_id, best_run.data.metrics["rmse"], best_run.data.params

# COMMAND ----------

# MAGIC %md ### Wait functions due to eventual consistency
# MAGIC * Wait until a version is in the READY status

# COMMAND ----------

import time
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

def wait_until_version_ready(model_name, model_version, sleep_time=1, iterations=100):
    start = time.time()
    for _ in range(iterations):
        version = client.get_model_version(model_name, model_version.version)
        status = ModelVersionStatus.from_string(version.status)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(round(time.time())))
        print(f"{dt}: Version {version.version} status: {ModelVersionStatus.to_string(status)}")
        if status == ModelVersionStatus.READY:
            break
        time.sleep(sleep_time)
    end = time.time()
    print(f"Waited {round(end-start,2)} seconds")

# COMMAND ----------

# MAGIC %md ### Create registered model (if it doen't exist) and remove all versions

# COMMAND ----------

from mlflow.exceptions import MlflowException, RestException

try:
    registered_model = client.get_registered_model(model_name)
    print(f"Found {model_name}")
    versions = client.get_latest_versions(model_name)
    print(f"Found {len(versions)} versions")
    for v in versions:
        print(f"  version={v.version} status={v.status} stage={v.current_stage} run_id={v.run_id}")
        client.transition_model_version_stage(model_name, v.version, stage="Archived")
        client.delete_model_version(model_name, v.version)
except RestException as e:
    print("INFO:",e)
    if e.error_code == "RESOURCE_DOES_NOT_EXIST":
        print(f"Creating {model_name}")
        registered_model = client.create_registered_model(model_name)
    else:
        raise Exception(e)

# COMMAND ----------

registered_model = client.get_registered_model(model_name)
type(registered_model),registered_model.__dict__

# COMMAND ----------

display_registered_model_uri(model_name)

# COMMAND ----------

# MAGIC %md ### Create model version for run
# MAGIC First, create the version:

# COMMAND ----------

source = f"{best_run.info.artifact_uri}/spark-model"
source

# COMMAND ----------

version = client.create_model_version(model_name, source, best_run.info.run_id)
version

# COMMAND ----------

# MAGIC %md **Wait until version is in READY status**

# COMMAND ----------

wait_until_version_ready(model_name, version, sleep_time=2)
version = client.get_model_version(model_name,version.version)
version_id = version.version
version_id, version.status, version.current_stage, version.run_id

# COMMAND ----------

# MAGIC %md ### Tag best run version as Production stage

# COMMAND ----------

# client.update_model_version(model_name, version_id, stage="Production")
client.transition_model_version_stage(model_name, version_id, stage="Production")

# COMMAND ----------

version = client.get_model_version(model_name,version_id)
type(version), version.__dict__

# COMMAND ----------

# MAGIC %md 
# MAGIC #List all registered models

# COMMAND ----------

from pprint import pprint
from mlflow.tracking.client import MlflowClient
client = MlflowClient()
for rm in client.list_registered_models():
    pprint(dict(rm), indent=4)

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC **Q:** Where can I find out more information on MLflow Model Registry?  
# MAGIC **A:** Check out <a href="https://mlflow.org/docs/latest/registry.html#concepts" target="_blank"> for the latest API docs available for Model Registry</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
