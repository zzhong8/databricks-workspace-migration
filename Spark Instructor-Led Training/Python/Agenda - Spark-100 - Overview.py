# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Apache&reg; Spark&trade; Overview 
# MAGIC ## Databricks Spark 100 (1 Day)
# MAGIC See **<a href="https://databricks.com/training/courses/apache-spark-overview" target="_blank">https&#58;//databricks.com/training/courses/apache-spark-overview</a>**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-Day AM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 30m  | **Introductions**                                                                ||
# MAGIC | 20m  | **Setup**                                                                        | *Registration, Courseware & Q&As* |
# MAGIC | 10m  | **Break**                                                                        ||
# MAGIC | 20m  | **[Apache Spark Overview]($./Apache Spark Overview)**                            | *About Databricks, Spark & Spark Architecture* |
# MAGIC | 30m  | **[Reading Data - CSV]($./Reading & Writing Data/Reading Data 1 - CSV)**         | *Spark Entry Point, Reading Data, Inferring Schemas, API Docs* |
# MAGIC | 10m  | **Break**                                                                        || 
# MAGIC | 10m  | **[Reading Data - Summary]($./Reading & Writing Data/Reading Data 7 - Summary)** | *Review and contrast the differences of various readers* | 
# MAGIC | 10m  | **[Lab: Reading Data]($./Reading & Writing Data/Reading Data 8 - Lab)**           | *Putting to practice what we just learned*<br/>*(completed collaboratively)* |
# MAGIC | 10m  | **[Writing Data]($./Reading & Writing Data/Writing Data)**                       | *Quick intro to DataFrameWriters* |
# MAGIC | 30m  | **[Intro To DataFrames, 1/2]($./Intro To DataFrames/Intro To DF Part 1)**       | *API Docs, DataFrames, cache(), show(), display(), limit()* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-Day PM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 15m  | **House Keeping**                                                                 || 
# MAGIC | 25m  | **[Intro To DataFrames, 2/2]($./Intro To DataFrames/Intro To DF Part 1)**       | *count(), select(), drop(), distinct(), SQL, Temp Views* |
# MAGIC | 10m  | **[Lab: Distinct Articles]($./Intro To DataFrames/Intro To DF Part 1 Lab)**      | *Putting to practice what we just learned*<br/>*(completed collaboratively)* |
# MAGIC | 10m  | **Break**                                                                        || 
# MAGIC | 40m  | **[Transformations And Actions]($./Other Topics/Transformations And Actions)**   | *Lazy, Catalyst, Actions, Transformations, Wide vs. Narrow, Shuffling, Stages, Pipelining, Lineage* |
# MAGIC | 10m  | **[Lab: T&A in the Spark UI]($./Other Topics/Transformations And Actions Lab)**          | *Lab completed collaboratively* |
# MAGIC | 10m  | **Break**                                                                        || 
# MAGIC | 10m  | **[Introduction to Structured Streaming]($./Structured Streaming/Structured Streaming 1 - Intro)**       | *Micro-Batches, Input & Results Table, Ouput Modes & Sinks* |
# MAGIC | 30m  | **[Structured Streaming Examples]($./Structured Streaming/Structured Streaming 2 - TCPIP)**    | *DataStreamReader, Limitations, Windowing, Watermarking, Checkpointing, Fault Tolerance* |
# MAGIC | 10m  | **[Lab: Analyzing Streamings]($./Structured Streaming/Structured Streaming 4 - Lab)**        | *Analyise our stream, aggregating by IP Addresses*<br/>*(completed collaboratively)* |
# MAGIC | 10m  | **Break**                                                                        || 
# MAGIC | 60m  | **[Machine Learning Pipeline]($./Machine Learning/ML Pipeline Demo)**                          | *ML Pipelines, Feature Extractors, Random Forests, Evaluators, Param Grids, Cross Validation* |

# COMMAND ----------

# MAGIC %md
# MAGIC The times indicated here are approximated only - actual times will vary by class size, class participation, and other unforeseen factors.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
