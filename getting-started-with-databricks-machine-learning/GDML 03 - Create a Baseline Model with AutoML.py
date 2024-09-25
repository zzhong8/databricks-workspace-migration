# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started with Databricks ML
# MAGIC
# MAGIC ### Module 3: Create a Baseline Model with AutoML
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classroom Setup
# MAGIC Before proceeding with this demonstration, you need to run the `Classroom-Setup` notebook to initialize the environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"User DB Location:  {DA.paths.user_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC For this notebook, we will utilize a dataset of retail customers.
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Experiment Name
# MAGIC
# MAGIC In the next section, we're going to create an experiment and model in the UI. 
# MAGIC
# MAGIC **📌 Important: To ensure you're experiment and model have unique names and match later references, use the output from the command below to get a unique name**. 

# COMMAND ----------

print(f"Experiment name to be used: {DA.unique_name('_')}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create AutoML Experiment
# MAGIC
# MAGIC Let's initiate an AutoML experiment to construct a baseline model for predicting customer retails 
# MAGIC data. The target field for this prediction will be the `units_purchased` field.
# MAGIC
# MAGIC Follow these step-by-step instructions to create an AutoML experiment:
# MAGIC
# MAGIC 1. Navigate to the **Experiments** section in Databricks.
# MAGIC
# MAGIC 2. Click on **Create AutoML Experiment** located in the top-right corner.
# MAGIC
# MAGIC 3. Choose a cluster with **Database Runtime (DBR) ML** to execute the experiment. We recommend to select one of the latest DBR versions. 
# MAGIC
# MAGIC 4. For the ML problem type, opt for **Regression**.
# MAGIC
# MAGIC 5. Select the `customer_features` table, which was created in the previously, as the input training dataset.
# MAGIC
# MAGIC 6. Specify **`units_purchased`** as the prediction target.
# MAGIC
# MAGIC 7. Deselect the **customer_id** field as it's not needed as a feature.
# MAGIC
# MAGIC 8. In the **Advanced Configuration** section, set the **Timeout** to **10 minutes**.
# MAGIC
# MAGIC 9. Enter a name for your experiment. **The experiment name must be the unique name generated in the previous cell.**
# MAGIC
# MAGIC **Optional Advanced Configuration:**
# MAGIC
# MAGIC - You have the flexibility to choose the **evaluation metric** and your preferred **training framework**.
# MAGIC
# MAGIC - If your dataset includes a timeseries field, you can define it when splitting the dataset.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## View the Best Run
# MAGIC
# MAGIC Once the experiment is finished, it's time to examine the best run:
# MAGIC
# MAGIC 1. Access the completed experiment in the **Experiments** section.
# MAGIC
# MAGIC 2. Identify the best model run by evaluating the displayed **metrics**. Alternatively, you can click on **View notebook for the best model** to access the automatically generated notebook for the top-performing model.
# MAGIC
# MAGIC 3. Utilize the **Chart** tab to compare and contrast the various models generated during the experiment.
# MAGIC
# MAGIC You can find all details for the run  on the experiment page. There are different columns such as the framework used (e.g., Scikit-Learn, XGBoost), evaluation metrics (e.g., Accuracy, F1 Score), and links to the corresponding notebooks for each model. This allows you to make informed decisions about selecting the best model for your specific use case.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **📌 Important:** The rest of the demos are dependent on the AutoML experiment created in this demo. Please make sure to complete this notebook before moving to the next section.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
