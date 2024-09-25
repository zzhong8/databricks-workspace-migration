# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Scikit-Learn and Spark
# MAGIC
# MAGIC How can we leverage our existing experience with modeling libraries like <a href="http://scikit-learn.org/stable/index.html" target="_blank">scikit-learn</a>?  
# MAGIC
# MAGIC Although scikit-learn builds models on a single machine, we can still benefit from the parallelism provided by Spark to build multiple models concurrently for hyperparameter search, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Install Libraries
# MAGIC
# MAGIC We need to start by installing <a href="https://github.com/databricks/spark-sklearn" target="_blank">Spark-Sklearn</a> by creating a library with this Maven Coordinate: `databricks:spark-sklearn:0.2.3`. 
# MAGIC
# MAGIC To verify that the libraries are properly attached, run the following cell proving that we can import the class `GridSearchCV` from the `spark-sklearn` library:

# COMMAND ----------


from spark_sklearn import GridSearchCV

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC In this lab, we are trying to predict iris type given different features 
# MAGIC * sepal length
# MAGIC * sepal width
# MAGIC * petal length
# MAGIC * and petal width 
# MAGIC
# MAGIC Below are the three types of irises. 
# MAGIC
# MAGIC Can you form any hypotheses about the differences among these irises?
# MAGIC
# MAGIC <img src="http://www.spataru.at/iris-dataset-svm/iris_types.jpg" style="float:center; height: 200px; margin: 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Load the data from `sklearn.datasets`.

# COMMAND ----------


import numpy as np
from sklearn import datasets

# Load the iris data
iris = datasets.load_iris()

# COMMAND ----------

# MAGIC %md
# MAGIC With the dataset loaded, let's take a look at it's description:

# COMMAND ----------


print(iris.DESCR)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at some of the details of this dataset:

# COMMAND ----------


print("Target Names: ",  iris.target_names)
print("Feature Names: ", iris.feature_names)
print() 
print("Flower #0:   ", iris.target[0], iris.data[0])
print("Flower #50:  ", iris.target[50], iris.data[50])
print("Flower #100: ", iris.target[100], iris.data[100])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC This tables shows the relationships among the 4 predictors.
# MAGIC
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/5/56/Iris_dataset_scatterplot.svg/1024px-Iris_dataset_scatterplot.svg.png" style="float:center; height: 400px; margin: 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Before we get into running K-Nearest Neighbors, let's start by using a <a href="https://en.wikipedia.org/wiki/Voronoi_diagram" target="_blank">Voronoi diagram</a> to visualize the partitioning of space. 
# MAGIC
# MAGIC Note: we can look at only two of the four features at a time:

# COMMAND ----------


import matplotlib.pyplot as plt
from scipy.spatial import Voronoi, voronoi_plot_2d

# features = iris.data[:,:2] # sepal length vs sepal width
features = iris.data[:,2:4] # pedal length vs pedal width

labels = iris.target

vor = Voronoi(features)
voronoi_plot_2d(vor)
plt.xlabel('Sepal Length')
plt.ylabel('Sepal Width')

display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC Next we are going to create the test and training sets using <a href="http://scikit-learn.org/stable/modules/generated/sklearn.model_selection.train_test_split.html" target="_blank">sklearn.model_selection.train_test_split</a>. 
# MAGIC
# MAGIC In this case, we will use 80% for traning and save 20% for testing.

# COMMAND ----------


from sklearn.model_selection import train_test_split

yTrain, yTest, xTrain, xTest = train_test_split(
  iris.target,     # Our targets
  iris.data,       # Our features
  test_size=0.2,   # Hold out 20%
  random_state=42  # For reproducability 
)

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to use Scikit-Learn's `KNeighborsClassifier`, but use Spark to distribute the training. 
# MAGIC
# MAGIC Let's take a look at the different parameters we can vary.

# COMMAND ----------


from sklearn.neighbors import KNeighborsClassifier

knn = KNeighborsClassifier()

for key, value in knn.get_params().items():
  print("{:14} {}".format(key+":", value))

# COMMAND ----------

# MAGIC %md
# MAGIC For the current configuration, the number of neighbors is 5.
# MAGIC
# MAGIC Next we will train with 10 different permutations: 1 to 10 neighbors.

# COMMAND ----------


from spark_sklearn import GridSearchCV

parameters = {
  'n_neighbors':[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
}
clf = GridSearchCV(
  sc,                   # The Spark Context
  knn,                  # KNeighborsClassifier
  parameters            # All the different parameters
)
clf.n_splits_ = 3       # Set the number of folds
clf.fit(xTrain, yTrain) # Train the model

clf.cv_results_

# COMMAND ----------

# MAGIC %md
# MAGIC Let's examine the `mean_test_score` of each hyperparameter configuration.

# COMMAND ----------


for i,r in enumerate(clf.cv_results_["mean_test_score"]):
  print("#{}: {:.2f}".format(i, r*100))

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: When multiple estimators produce the same result, how is the "best" one determined?
# MAGIC
# MAGIC We can now ask for the best estimator and from there, evaluate its various parameters:

# COMMAND ----------


best = clf.best_estimator_

print("algorithm:     " + str(best.algorithm))
print("leaf_size:     " + str(best.leaf_size))
print("metric:        " + str(best.metric))
print("metric_params: " + str(best.metric_params))
print("n_jobs:        " + str(best.n_jobs))
print("n_neighbors:   " + str(best.n_neighbors))
print("p:             " + str(best.p))
print("weights:       " + str(best.weights))

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our best estimator, let's apply it to our test set.

# COMMAND ----------


predictions = best.predict(xTest)

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can view our predictions in the following cell:

# COMMAND ----------


# Print out the accuracy of the classifier on the test set
# print sum(predictions == yTest) / float(len(yTest))

accuracy = __builtin__.sum(predictions == yTest) * 100.0 / __builtin__.len(yTest)
print("Accuracy: {:.2f}%".format(accuracy))
print()
print("Test Set:    ", yTest)
print("Predictions: ", predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC So this works well when our data is able to be contained in a single executor.
# MAGIC
# MAGIC But what if our dataset is too large where we can't use our external ML library on the full data set.  
# MAGIC
# MAGIC In this case we might want to build several models on samples of the dataset.  
# MAGIC
# MAGIC We could either build the same model, using different parameters, or try completely different techniques to see what works best.
# MAGIC
# MAGIC To get started, we need to parallelize the iris dataset and distribute it across our cluster using the RDD API.

# COMMAND ----------


# Split the iris dataset into 8 partitions
irisDataRDD = sc.parallelize(zip(iris.target, iris.data), 20)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's take a look at the records in each partition:

# COMMAND ----------


data = irisDataRDD.keys().glom().collect()
for i, r in enumerate(data):
  print("#{} x{}: {}".format(i, len(r), r))

# COMMAND ----------

# MAGIC %md
# MAGIC Since each of the partitions represents a dataset that we'll be using to run our local model, we have a problem.
# MAGIC
# MAGIC The data is ordered, so our partitions are mostly homogenous with regard to our target variable.
# MAGIC
# MAGIC We'll repartition the data so that the data is randomly ordered across partitions.

# COMMAND ----------


randomOrderDataRDD = irisDataRDD.repartition(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Next we can take a look at the new distribution across all partitions:

# COMMAND ----------


data = randomOrderDataRDD.keys().glom().collect()
for i, r in enumerate(data):
  print("#{} x{}: {}".format(i, len(r), r))

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we'll build a function that takes in the target and data from `randomOrderDataRDD` and returns the number of correct and total predictions (with regard to a test set).

# COMMAND ----------


def runNearestNeighborsPartition(labelAndFeatures):
    y, X = zip(*labelAndFeatures)
    yTrain, yTest, XTrain, XTest = train_test_split(y, X, test_size=0.2, random_state=24)
    
    knn = KNeighborsClassifier()
    knn.fit(XTrain, yTrain)
    
    predictions = knn.predict(XTest)
    correct = (predictions == yTest).sum() 
    total = len(yTest)
    return [np.array([correct, total])]

# Runs KNN on each subset of the data
sampleResults = randomOrderDataRDD.mapPartitions(runNearestNeighborsPartition) 

for a in sampleResults.collect():
  print("{}/{}   {:.2f}%".format(a[0], a[1], 100.0*a[0]/a[1]))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
