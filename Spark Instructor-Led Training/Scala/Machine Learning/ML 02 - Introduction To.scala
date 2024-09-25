// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Introduction to Spark ML

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Patterns Underlying Spark ML
// MAGIC
// MAGIC ### Snap-together brick model
// MAGIC
// MAGIC ### Encapsulation of processing
// MAGIC * **Transformer**
// MAGIC * **Estimator**
// MAGIC * **Pipeline**
// MAGIC
// MAGIC ### Evaluation / Tuning
// MAGIC * **Evaluator**
// MAGIC * **CrossValidator**
// MAGIC * **ParamGridBuilder**

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Transformer
// MAGIC
// MAGIC ### Processes features
// MAGIC
// MAGIC ### Typically a "map" operation:
// MAGIC   * For example: `Binarizer`
// MAGIC
// MAGIC ### But can (occasionally) contain a reduce:
// MAGIC   * For example: `OneHotEncoder`
// MAGIC
// MAGIC ### Run by calling `transform(..)`
// MAGIC   * For example: `aTransformer.transform(aDataFrame)`
// MAGIC
// MAGIC ### Extend Spark
// MAGIC   * by extending`UnaryTransformer` or
// MAGIC   * by extending`Transformer`

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Estimator
// MAGIC
// MAGIC ### More complex feature processing, and/or predictions
// MAGIC
// MAGIC ### Typically one or many "reduce" operations
// MAGIC * Builds valuable state
// MAGIC
// MAGIC ### Produces a Model (Transformer) to encapsulate state
// MAGIC * Generate state & model by calling `anEstimator.fit(aDataFrame)`
// MAGIC * Resulting model is a `Transformer (q.v.)`
// MAGIC
// MAGIC ### Extend Spark 
// MAGIC * by creating a specific Model subclass and 
// MAGIC * an Estimator that generates it

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Pipeline
// MAGIC
// MAGIC ### Represents composition of
// MAGIC * various Transformers' `.transform(..)` methods
// MAGIC * various Estimators' `.fit(..)` and result's `.transform(..)` methods
// MAGIC
// MAGIC ### "<a href="https://en.wikipedia.org/wiki/Tacit_programming" target="_blank">Point-free</a>" operations
// MAGIC
// MAGIC ### A Pipeline is itself an `Estimator` (supports composition)
// MAGIC
// MAGIC ### Instead of this...
// MAGIC
// MAGIC <pre>
// MAGIC model = est2.fit(
// MAGIC est1.fit(tf2.transform(tf1.transform(data)))
// MAGIC             .transform(tf2.transform(tf1.transform(data)))
// MAGIC )</pre>
// MAGIC
// MAGIC ### We use this...
// MAGIC
// MAGIC <pre>model = Pipeline([tf1, tf2, est1, est2]).fit(data)</pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Uniform API
// MAGIC
// MAGIC ### Feature processors: 
// MAGIC  * `Transformer` or
// MAGIC  * `Estimator` + `Model`
// MAGIC
// MAGIC ### ML Algorithms: 
// MAGIC * `Estimator`
// MAGIC
// MAGIC ### ML Models: 
// MAGIC * `Model`
// MAGIC
// MAGIC ### Using any processor, algorithm, and performance tuning uses the same API!
// MAGIC * Low cognitive load
// MAGIC * "If you're bored, we're all winning!"  
// MAGIC (because your brain is now free to work on the hard and interesting stuff)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Evaluator
// MAGIC ### Calculates statistics on our models indicating
// MAGIC * goodness-of-fit, explanation of variance
// MAGIC * error quantities, precision/recall/etc.
// MAGIC
// MAGIC ### Generates one stat at a time
// MAGIC * "mode-ful" switching of stat via setter
// MAGIC
// MAGIC ### Why? Designed for integration and for Spark, not just us
// MAGIC * In particular, answers question "Which is better?"
// MAGIC
// MAGIC ### RegressionEvaluator, BinaryClassificationEvaluator, ...

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) ParamGridBuilder
// MAGIC
// MAGIC ### Helper to specify a grid of (hyper)params
// MAGIC * Several params, chosen based on algorithm/model type
// MAGIC * Several values for each param
// MAGIC * Allows Spark to find/try every combination of values!
// MAGIC
// MAGIC
// MAGIC ### Example: Parameter Grid for Tuning a Decision Tree
// MAGIC | Parameter  | Test Value 1 | Test Value 2 | Test Value 3 | (etc.) |
// MAGIC |------------|--------------|--------------|--------------|--------|
// MAGIC | `maxDepth` | 6            | 10           | 12           | `...`  |
// MAGIC | `maxBins`  | 16           | 32           | 48           | `...`  | 
// MAGIC | `...`      | `...`        | `...`        | `...`        | `...`  |

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Algorithm Coverage
// MAGIC
// MAGIC (this list is based on Spark 1.5)
// MAGIC
// MAGIC ### Classification
// MAGIC * Logistic regression w/elastic net
// MAGIC * Naive Bayes
// MAGIC * Streaming logistic regression
// MAGIC * Linear SVMs
// MAGIC * Decision trees
// MAGIC * Random forests
// MAGIC * Gradient-boosted trees
// MAGIC * Multilayer perception
// MAGIC * One-vs-rest
// MAGIC
// MAGIC ### Regression
// MAGIC * Least squares w/elastic net
// MAGIC * Isotonic regression
// MAGIC * Decision trees
// MAGIC * Random forests
// MAGIC * Gradient-boosted trees
// MAGIC * Streaming linear methods
// MAGIC
// MAGIC ### Recommendation
// MAGIC * Alternating Least Squares
// MAGIC
// MAGIC ### Frequent itemsets
// MAGIC * FP-growth
// MAGIC * Prefix span
// MAGIC
// MAGIC ### Feature extraction & selection
// MAGIC * Binarizer
// MAGIC * Bucketizer
// MAGIC * Chi-Squared selection
// MAGIC * CountVectorizer
// MAGIC * Discrete cosine transform
// MAGIC * ElementwiseProduct
// MAGIC * Hashing term frequency
// MAGIC * Inverse document frequency
// MAGIC * MinMaxScaler
// MAGIC * Ngram
// MAGIC * Normalizer
// MAGIC * One-Hot Enocder
// MAGIC * PCA
// MAGIC * PolynomialExpansion
// MAGIC * RFormula
// MAGIC * SQLTransformer
// MAGIC * Standard scaler
// MAGIC * StopWordsRemover
// MAGIC * StringIndexer
// MAGIC * Tokenizer
// MAGIC * StringIndexer
// MAGIC * VectorAssembler
// MAGIC * VectorIndexer
// MAGIC * VectorSlicer
// MAGIC * Word2Vec
// MAGIC
// MAGIC ### Clustering
// MAGIC * Gaussian mixture models
// MAGIC * K-Means
// MAGIC * Streaming K-Means
// MAGIC * Latent Dirichlet Allocation
// MAGIC * Power Iteration Clustering
// MAGIC
// MAGIC ### Statistics
// MAGIC * Pearson correlation
// MAGIC * Spearman correlation
// MAGIC * Online summarization
// MAGIC * Chi-squared test
// MAGIC * Kernel density estimation
// MAGIC
// MAGIC ### Linear algebra
// MAGIC * Local dense & sparse vectors & matrices
// MAGIC * Distributed matrices
// MAGIC   * Block-partitioned matrix
// MAGIC   * Row matrix
// MAGIC   * Index row matrix
// MAGIC   * Coordinate matrix
// MAGIC * Matrix decompositions
// MAGIC
// MAGIC ### Model import/export
// MAGIC
// MAGIC ### Pipelines

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
