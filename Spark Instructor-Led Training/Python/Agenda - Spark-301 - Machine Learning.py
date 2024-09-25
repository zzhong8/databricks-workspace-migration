# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Apache&reg; Spark&trade; for Machine Learning and Data Science
# MAGIC ## Databricks Spark 301 (3 Days)  
# MAGIC See **<a href="https://databricks.com/training/courses/apache-spark-for-machine-learning-and-data-science" target="_blank">https&#58;//databricks.com/training/courses/apache-spark-for-machine-learning-and-data-science</a>**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day #1 AM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 30m  | **Introductions**                                                                ||
# MAGIC | 20m  | **Setup**                                                                        | *Registration, Courseware & Q&As* |
# MAGIC | 10m  | **Break**                                                                        ||
# MAGIC ||||
# MAGIC | 50m  | **[Apache Spark Overview]($./Apache Spark Overview)**                            | *About Databricks, Spark & Spark Architecture* |
# MAGIC | 10m  | **Break**                                                                        ||
# MAGIC ||||
# MAGIC | 30m  | **[Reading Data - CSV]($./Reading & Writing Data/Reading Data 1 - CSV)**         | *Spark Entry Point, Reading Data, Inferring Schemas, API Docs* |
# MAGIC | 10m  | **[Reading Data - Summary]($./Reading & Writing Data/Reading Data 7 - Summary)** | *Review and contrast the differences of various readers* | 
# MAGIC | 10m  | **[Lab: Reading Data]($./Reading & Writing Data/Reading Data 8 - Lab)**           | *Putting to practice what we just learned*<br/>*(completed collaboratively)* |
# MAGIC | 10m  | **[Writing Data]($./Reading & Writing Data/Writing Data)**                       | *Quick intro to DataFrameWriters* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day #1 PM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 50m  | **[Intro To DataFrames Part-1]($./Intro To DataFrames/Intro To DF Part 1)**      | *API Docs, DataFrames, cache(), show(), display(), limit()*<br/>*count(), select(), drop(), distinct(), SQL, Temp Views* |
# MAGIC | 10m  | **[Lab: Distinct Articles]($./Intro To DataFrames/Intro To DF Part 1 Lab)**      | *Putting to practice what we just learned*<br/>*(completed collaboratively)* |
# MAGIC ||||
# MAGIC | 10m  | **Break** || 
# MAGIC | 50m  | **[Partitioning]($./Other Topics/Partitioning)**                                 | *Partitions vs. Slots, repartition(n), coalesce(n), spark.sql.shuffle.partitions* |
# MAGIC ||||
# MAGIC | 10m  | **Break** || 
# MAGIC | 40m | **[ML 01 - spark-sklearn]($./Machine Learning/ML 01 - spark-sklearn)**               | *Parallelizing Scikit-Learn on Apache Spark with Spark-Sklearn* |
# MAGIC | 10m  | **Break** || 
# MAGIC ||||
# MAGIC | 50m  | **[Intro To DataFrames Part-2]($./Intro To DataFrames/Intro To DF Part 2)**      | *orderBy(), Column, filter(), firs(), Row, collect(), take(n), Dataframe vs. DataSet* |
# MAGIC | 10m  | **[Lab: Washingtons and Adams]($./Intro To DataFrames/Intro To DF Part 2 Lab)**  | *Counting & summing Washingtons*<br/>*(completed collaboratively)* | 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day #2 AM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 20m  | **Review**                                                                       | *What did we discover yesterday?* |     
# MAGIC | 10m  | **[Introduction to Structured Streaming]($./Structured Streaming/Structured Streaming 1 - Intro)**       | *Micro-Batches, Input & Results Table, Ouput Modes & Sinks* |
# MAGIC | 30m  | **[Structured Streaming Examples]($./Structured Streaming/Structured Streaming 2 - TCPIP)**    | *DataStreamReader, Limitations, Windowing, Watermarking, Checkpointing, Fault Tolerance* |
# MAGIC ||||
# MAGIC | 10m  | **Break** || 
# MAGIC | 10m  | **[Lab: Analyzing Streamings]($./Structured Streaming/Structured Streaming 4 - Lab)**        | *Analyise our stream, aggregating by IP Addresses*<br/>*(completed collaboratively)* |
# MAGIC | 50m  | **[Intro To DataFrames Part-3]($./Intro To DataFrames/Intro To DF Part 3)**   | *withColumnRenamed(), withColumn(), unix_timestamp() & cast()*<br/>*year(), month(), dayofyear(), RelationalGroupedDataset, sum(), count(), avg(), min(), max()* |
# MAGIC ||||
# MAGIC | 10m  | **Break** || 
# MAGIC | 40m  | **[Lab: De-Duping Data]($./Intro To DataFrames/Intro To DF Part 3 Lab)**          | *Real world problem solving - removing duplicate records* |
# MAGIC ||||

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day #2 PM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 20m  | **[ML 02 - Introduction to Spark ML]($./Machine Learning/ML 02 - Introduction To)** | *Introduction to Spark ML* |
# MAGIC | 40m  | **[ML 03 - Linear Regression Part-1]($./Machine Learning/ML 03 - Linear Regression Part-1)** | *Exploratory Data Analysis: Databricks Visualization features, df.describe(), dealing with missing values.*|
# MAGIC | 10m  | **Break** | |
# MAGIC | 60m  | **[ML 03 - Linear Regression Part-1]($./Machine Learning/ML 03 - Linear Regression Part-1)** | *Training & Test sets, Model Training and Prediction, Intercept and Coefficients, Evaluation, RMSE, R2.*|
# MAGIC | 10m  | **Break** | |
# MAGIC | 50m  | **[ML 04 - Linear Regression Part-2]($./Machine Learning/ML 04 - Linear Regression Part-2)** | *Transformers, the VectorAssembler, SparseVector & DenseVector representations, Overfitting & Regularization, Introduction to Machine Learning Pipelines* |
# MAGIC | 10m  | **Break** | |
# MAGIC | 40m  | **[ML 05 - Logistic Regression]($./Machine Learning/ML 05 - Logistic Regression)** | *Logistic Regression overview, Data Preparation, Transformers and Evaluators: Tokenization, CountVectorizer, Binarizer* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day #3 AM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 20m  | **Review** | *What did we discover yesterday?* |     
# MAGIC | 30m  | **[ML 05 - Logistic Regression]($./Machine Learning/ML 05 - Logistic Regression)** | *Model Training, Performance Evaluation, The ROC curve, AUC, the Confusion Matrix* |
# MAGIC | 10m  | **Break** | |
# MAGIC | 45m  | **[ML 06 - Decision Trees]($./Machine Learning/ML 06 - Decision Trees)** | *Introduction to Decision Trees, the MNIST dataset, Regularization, CrossValidation, Parameter Grid Search* |
# MAGIC | 20m  | **[ML 06 - Decision Trees Lab]($./Machine Learning/ML 06 - Decision Trees Lab)** | *Self-guided lab* |
# MAGIC | 10m  | **Break** | |
# MAGIC | 45m | **[ML 07 - Neural Nets]($./Machine Learning/ML 07 - Neural Nets)** | *Neural Networks Representation, Training, Exporting and Importing ML pipelines* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day #3 PM
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 45m  | **[ML 08 - Tuning Bikeshare]($./Machine Learning/ML 08 - Tuning Bikeshare)** | *Self guided lab* |
# MAGIC | 20m  | **[ML 08 - Tuning Bikeshare]($./Machine Learning/ML 08 - Tuning Bikeshare)** | *Solutions. VectorIndexer, Gradient Boosted Trees* |
# MAGIC | 10m  | **Break** | |
# MAGIC | 35m  | **[ML 09 - KMeans]($./Machine Learning/ML 09 - KMeans)** | *The Iris dataset. CLutering, K-Means, the kmeans++ algorithm, Databricks matplotlib integration* |
# MAGIC | 10m  | **Break** | |
# MAGIC | 60m  | **[ML 10 - ALS Prediction]($./Machine Learning/ML 10 - ALS Prediction)** | *Self guided lab* |
# MAGIC | 10m  | **Break** | | 
# MAGIC | 20m  | **[ML 10 - ALS Prediction]($./Machine Learning/ML 10 - ALS Prediction)** | *Solutions* |
# MAGIC | 30m  | **Wrapping up** | *Course summary, Further Reading, Q&A* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ML Electives
# MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | | **[ML Elective 01 - GraphFrames]($./Machine Learning/ML Elective 01 - GraphFrames)** | *Graph Analytics of College Football games* |
# MAGIC | | **[ML Elective 02 - LDA Topic Modeling]($./Machine Learning/ML Elective 02 - LDA Topic Modeling)** | *LDA Topic Modeling of UseNet messages* |

# COMMAND ----------

# MAGIC %md
# MAGIC The times indicated here are approximated only - actual times will vary by class size, class participation, and other unforeseen factors.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
