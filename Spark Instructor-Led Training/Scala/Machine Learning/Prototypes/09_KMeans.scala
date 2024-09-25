// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Unsupervised Learning: K-Means
// MAGIC
// MAGIC KMeans is an *unsupervised* algorithm. This means it can build a model on *unlabeled data*. K-Means will group vectors into clusters based on
// MAGIC the position (in space) of the vectors themselves, relative to one another, with no human-audited labels.
// MAGIC
// MAGIC This fact about unsupervised learning makes it easy to train a model on large amounts of unlabeled data. But the challenge is determining
// MAGIC whether the results have any actual use in reality. E.g. with K-Means, do the clusters correspond to anything useful?
// MAGIC
// MAGIC We will be using the Iris dataset, which has labels (the type of iris), but we will only use the labels to evaluate the model, not to train it. 

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Loading the data
// MAGIC
// MAGIC In this lab, we will be working with the famous Iris dataset. 
// MAGIC
// MAGIC The goal is to predict the type of Iris (Setosa, Versicolour, Virginica) given mesaurements on four different features: sepal length, sepal width, petal length, and petal width.
// MAGIC
// MAGIC First, we need to load data into Spark.  
// MAGIC
// MAGIC We'll use a built-in utility to load a <a href="http://www.csie.ntu.edu.tw/~cjlin/libsvm/faq.html" target="_blank">libSVM file</a>

// COMMAND ----------

val baseDir = "/mnt/training/iris/"
val irisPath = baseDir + "iris.scale"
val irisDF = spark.read.format("libsvm").load(irisPath).cache

//Note that the libSVM format uses SparseVectors.
display(irisDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Transform the data
// MAGIC
// MAGIC If you look at the data you'll notice that there are three values for the label: 1, 2, and 3.  Spark's machine learning algorithms expect a 0 indexed target variable, so we'll want to adjust those labels. Even though K-Means does not use these labels to train, we are going to use them to evaluate our K-Means model.
// MAGIC
// MAGIC This transformation is a simple expression where we'll subtract `1` from our `label` column.  
// MAGIC
// MAGIC This can be accomplished with a [selectExpr()](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.selectExpr). The resulting zero-index DataFrame should have two columns: one named `features` and another named `label`.

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
// Create a new DataFrame with the features from irisDF and with labels that are zero-indexed (just subtract one).
// Also make sure your label column is still called label.

val irisZeroIndexDF = irisDF.<FILL_IN>
display(irisZeroIndexDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Notice that we have four values that are stored as a `SparseVector` within the `features` column.  We'll reduce those down to two values (for visualization purposes) and convert them to a `DenseVector`.  To do that we'll need to create a `udf` and apply it to our dataset. 

// COMMAND ----------

import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.{Vectors, Vector}

// Take the first two values from a SparseVector and convert them to a DenseVector
val firstTwoFeatures = spark.udf.register("firstTwoFeatures", (vector: Vector) => Vectors.dense(Array(vector(0), vector(1))))

val irisTwoFeaturesDF = irisZeroIndexDF.select(firstTwoFeatures('features).as("features"),  $"label").cache()
display(irisTwoFeaturesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### K-Means
// MAGIC
// MAGIC K-Means is an iterative algorithm. You start by defining the number of clusters (`k`), and the algorithm randomly initializes the those k clusters. On each iteration, it computes the distance of each training point to the k clusters, and assigns that point to the closest cluster. It then updates the center of the clusters, and repeats until converge or max iterations.
// MAGIC
// MAGIC One thing we need to be careful with K-means is that it only guarantees us a local optima - not a global optima. To be fairly confidence that our local optima is also a global optima, we would need to re-run K-Means a few times with different initializations.
// MAGIC
// MAGIC Here is an example showing the difference between local and global optima:
// MAGIC
// MAGIC ![minima](https://www.mathworks.com/help/optim/ug/local_vs_global.png)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's start by building the K-means model using `KMeans`, an `ml Estimator`.  Details can be found in the [Python API](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.KMeans) or [Scala API](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.clustering.KMeans). 
// MAGIC
// MAGIC Set the number of clusters (`k`) to 3, `seed` to 221, and `maxIter` to 20.

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
import org.apache.spark.ml.clustering.KMeans

// Create a KMeans Estimator and set k=3, seed=221, maxIter=20
val kmeans = new KMeans()
                .<FILL_IN>
                .<FILL_IN>
                .<FILL_IN>

//  Call fit on the estimator and pass in irisTwoFeaturesDF
val model = <FILL_IN>

// Obtain the clusterCenters from the KMeansModel
val centers = model.clusterCenters

// Use the model to transform the DataFrame by adding cluster predictions
val transformedDF = model.transform(irisTwoFeaturesDF)

// Let's print the three centroids of our model
println(centers)

// COMMAND ----------

// MAGIC %md
// MAGIC Note that our predicted cluster is appended, as a column, to our input `DataFrame`.  Here it would be desirable to see consistency between label and prediction. These don't need to be the same number but if label 0 is usually predicted to be cluster 1 that would indicate that our unsupervised learning is naturally grouping the data into species.

// COMMAND ----------

display(transformedDF)

// COMMAND ----------

display(transformedDF.groupBy('prediction).count)

// COMMAND ----------

// MAGIC %md
// MAGIC #### K-Means Visualized
// MAGIC
// MAGIC The visualization below is written in Python. If you are using Scala, create a view called `irisTwoFeatures`, and then in a Python cell, assign that view to a DataFrame called `irisTwoFeaturesDF`.

// COMMAND ----------

irisTwoFeaturesDF.createOrReplaceTempView("irisTwoFeatures")

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # ONLY need to do if you were using Scala
// MAGIC irisTwoFeaturesDF = spark.table("irisTwoFeatures")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's find the values of the three centers with a varied number of iterations.

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC from pyspark.ml.clustering import KMeans
// MAGIC
// MAGIC modelCenters = []
// MAGIC iterations = [0, 2, 4, 7, 10, 20]
// MAGIC for i in iterations:
// MAGIC     kmeans = KMeans(k=3, seed=221, maxIter=i, initSteps=1)
// MAGIC     model = kmeans.fit(irisTwoFeaturesDF)
// MAGIC     modelCenters.append(model.clusterCenters())   

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC print("modelCenters:")
// MAGIC for centroids in modelCenters:
// MAGIC   print(centroids)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's visualize how our clustering performed against the true labels of our data.
// MAGIC
// MAGIC Remember: K-means doesn't use the true labels when training, but we can use them to evaluate. 
// MAGIC
// MAGIC Here, the star marks the cluster center.

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC import matplotlib.pyplot as plt
// MAGIC import matplotlib.cm as cm
// MAGIC import numpy as np
// MAGIC
// MAGIC def prepareSubplot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999', 
// MAGIC                 gridWidth=1.0, subplots=(1, 1)):
// MAGIC     """Template for generating the plot layout."""
// MAGIC     plt.close()
// MAGIC     fig, axList = plt.subplots(subplots[0], subplots[1], figsize=figsize, facecolor='white', 
// MAGIC                                edgecolor='white')
// MAGIC     if not isinstance(axList, np.ndarray):
// MAGIC         axList = np.array([axList])
// MAGIC     
// MAGIC     for ax in axList.flatten():
// MAGIC         ax.axes.tick_params(labelcolor='#999999', labelsize='10')
// MAGIC         for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
// MAGIC             axis.set_ticks_position('none')
// MAGIC             axis.set_ticks(ticks)
// MAGIC             axis.label.set_color('#999999')
// MAGIC             if hideLabels: axis.set_ticklabels([])
// MAGIC         ax.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
// MAGIC         map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
// MAGIC         
// MAGIC     if axList.size == 1:
// MAGIC         axList = axList[0]  # Just return a single axes object for a regular plot
// MAGIC     return fig, axList

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC data = irisTwoFeaturesDF.collect()
// MAGIC features, labels = zip(*data)
// MAGIC
// MAGIC x, y = zip(*features)
// MAGIC centers = modelCenters[5]
// MAGIC centroidX, centroidY = zip(*centers)
// MAGIC colorMap = 'Set1'  # was 'Set2', 'Set1', 'Dark2', 'winter'
// MAGIC
// MAGIC fig, ax = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(8,6))
// MAGIC plt.scatter(x, y, s=14**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap)
// MAGIC plt.scatter(centroidX, centroidY, s=22**2, marker='*', c='yellow')
// MAGIC cmap = cm.get_cmap(colorMap)
// MAGIC
// MAGIC colorIndex = [.5, .99, .0]
// MAGIC for i, (x,y) in enumerate(centers):
// MAGIC     print(cmap(colorIndex[i]))
// MAGIC     for size in [.10, .20, .30, .40, .50]:
// MAGIC         circle1=plt.Circle((x,y),size,color=cmap(colorIndex[i]), alpha=.10, linewidth=2)
// MAGIC         ax.add_artist(circle1)
// MAGIC
// MAGIC ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to seeing the overlay of the clusters at each iteration, we can see how the cluster centers moved with each iteration (and what our results would have looked like if we used fewer iterations).

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC x, y = zip(*features)
// MAGIC
// MAGIC oldCentroidX, oldCentroidY = None, None
// MAGIC
// MAGIC fig, axList = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(11, 15),
// MAGIC                              subplots=(3, 2))
// MAGIC axList = axList.flatten()
// MAGIC
// MAGIC for i,ax in enumerate(axList[:]):
// MAGIC     ax.set_title('K-means for {0} iterations'.format(iterations[i]), color='#999999')
// MAGIC     centroids = modelCenters[i]
// MAGIC     centroidX, centroidY = zip(*centroids)
// MAGIC     
// MAGIC     ax.scatter(x, y, s=10**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap, zorder=0)
// MAGIC     ax.scatter(centroidX, centroidY, s=16**2, marker='*', c='yellow', zorder=2)
// MAGIC     if oldCentroidX and oldCentroidY:
// MAGIC       ax.scatter(oldCentroidX, oldCentroidY, s=16**2, marker='*', c='grey', zorder=1)
// MAGIC     cmap = cm.get_cmap(colorMap)
// MAGIC     
// MAGIC     colorIndex = [.5, .99, 0.]
// MAGIC     for i, (x1,y1) in enumerate(centroids):
// MAGIC       print(cmap(colorIndex[i]))
// MAGIC       circle1=plt.Circle((x1,y1),.35,color=cmap(colorIndex[i]), alpha=.40)
// MAGIC       ax.add_artist(circle1)
// MAGIC     
// MAGIC     ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
// MAGIC     oldCentroidX, oldCentroidY = centroidX, centroidY
// MAGIC
// MAGIC plt.tight_layout()
// MAGIC
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
