// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <img src="https://mlflow.org/docs/latest/_static/MLflow-logo-final-black.png" style="float: left: margin: 20px; height:5em"/>
// MAGIC
// MAGIC ## https://mlflow.org/ 
// MAGIC ## https://github.com/mlflow/mlflow
// MAGIC <img src="https://mlflow.org/images/MLflow-header-pic@2x.png" />
// MAGIC
// MAGIC ## Additional Topics & Resources
// MAGIC
// MAGIC **Q:** What is MLflow at a high level?  
// MAGIC **A:** <a href="https://databricks.com/session/accelerating-the-machine-learning-lifecycle-with-mlflow-1-0" target="_blank">Listen to Spark and MLflow creator Matei Zaharia's talk at Spark Summit in 2019.</a>
// MAGIC
// MAGIC **Q:** What is a good source for the larger context of machine learning tools?  
// MAGIC **A:** <a href="https://roaringelephant.org/2019/06/18/episode-145-alex-zeltov-on-mlops-with-mlflow-kubeflow-and-other-tools-part-1/#more-1958" target="_blank">Check out this episode of the podcast Roaring Elephant.</a>
// MAGIC
// MAGIC **Q:** Where can I find the MLflow docs?
// MAGIC **A:** <a href="https://www.mlflow.org/docs/latest/index.html" target="_blank">You can find the docs here.</a>
// MAGIC
// MAGIC **Q:** What is a good general resource for machine learning?  
// MAGIC **A:** <a href="https://www-bcf.usc.edu/~gareth/ISL/" target="_blank">_An Introduction to Statistical Learning_</a> is a good starting point for the themes and basic approaches to machine learning.
// MAGIC
// MAGIC **Q:** Where can I find out more information on machine learning with Spark?
// MAGIC **A:** Check out the Databricks blog <a href="https://databricks.com/blog/category/engineering/machine-learning" target="_blank">dedicated to machine learning</a>

// COMMAND ----------

// MAGIC %md 
// MAGIC # Cluster Requirements
// MAGIC This workshop has been tested on a cluster with the following configurations: 
// MAGIC
// MAGIC `AWS`
// MAGIC `
// MAGIC {
// MAGIC     "spark_version": "7.0.x-cpu-ml-scala2.12",
// MAGIC     "spark_conf": {
// MAGIC         "spark.databricks.conda.condaMagic.enabled": "true"
// MAGIC     },
// MAGIC     "node_type_id": "i3.xlarge",
// MAGIC     "driver_node_type_id": "i3.xlarge"
// MAGIC }
// MAGIC `
// MAGIC
// MAGIC
// MAGIC `Azure`
// MAGIC `
// MAGIC {
// MAGIC     "spark_version": "7.0.x-cpu-ml-scala2.12",
// MAGIC     "spark_conf": {
// MAGIC         "spark.databricks.conda.condaMagic.enabled": "true"
// MAGIC     },
// MAGIC     "node_type_id": "Standard_DS3_v2",
// MAGIC     "driver_node_type_id": "Standard_DS3_v2"
// MAGIC }
// MAGIC `

// COMMAND ----------

// MAGIC
// MAGIC %md # Course: MLflow
// MAGIC * Version 1.3.1-IL
// MAGIC * Built 2020-09-02 21:01:16 UTC
// MAGIC * Git revision: 981e3f978d5b19680d341cfafd97490e72735244
// MAGIC
// MAGIC Copyright © 2020 Databricks, Inc.
