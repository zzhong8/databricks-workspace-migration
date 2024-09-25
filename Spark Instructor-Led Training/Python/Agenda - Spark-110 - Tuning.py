# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Apache&reg; Spark&trade; Tuning and Best Practices
# MAGIC ## Databricks Spark 110 (1 Day)
# MAGIC See **<a href="https://databricks.com/training/courses/apache-spark-tuning-and-best-practices" target="_blank">https&#58;//databricks.com/training/courses/apache-spark-tuning-and-best-practices</a>**

# COMMAND ----------

# MAGIC %md
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 30m  | **Introductions**                                              ||
# MAGIC | 20m  | **Setup**                                                      | *Registration, Courseware & Q&As* |
# MAGIC | 10m  | **Break**                                                      ||
# MAGIC | 50m  | **[01-Memory-Usage]($./Tuning/01-Memory-Usage)**               | *Determining memory usage, Tungsten, data locality, tuning garbage collection* |
# MAGIC | 10m  | **Break**                                                      ||
# MAGIC | 50m  | **[02-Broadcast-Variables]($./Tuning/02-Broadcast-Variables)** | *Exploring broadcast variables & broadcast joins, automatic and manual broadcasting and accumulators* |
# MAGIC | 10m  | **Break**                                                      ||
# MAGIC | 50m  | **[03-Catalyst]($./Tuning/03-Catalyst)**                       | *Review Catalyst anti-patterns, Tungsten encoders, side-effects of lambdas & custom optimizers* |
# MAGIC | 10m  | **Break**                                                      ||
# MAGIC | 50m  | **[04-Tuning-Shuffling]($./Tuning/04-Tuning-Shuffling)**       | *How to tune shuffle operations* |
# MAGIC | 10m  | **Break**                                                      ||
# MAGIC | 50m  | **[05-Cluster-Sizing]($./Tuning/05-Cluster-Sizing)**           | *Dealing with lack of memory, the effect of schema, hardware provisioning and single & multi-tennant clusters* |

# COMMAND ----------

# MAGIC %md
# MAGIC The times indicated here are approximated only - actual times will vary by class size, class participation, and other unforeseen factors.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
