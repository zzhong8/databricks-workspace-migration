// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's take another approach to same problem of classifying the
// MAGIC MNIST images of handwritten digits, but this time using Spark's
// MAGIC Multilayer perceptron classifier (MLPC), which is a classifier based
// MAGIC on a [feedforward artificial neural
// MAGIC network](https://en.wikipedia.org/wiki/Feedforward_neural_network).

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

val trainingDF = spark.read.format("libsvm").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-train.txt").cache
val testDF = spark.read.format("libsvm").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-test.txt").cache

// COMMAND ----------

display(trainingDF)

// COMMAND ----------

display(testDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Train a ... Multilayer Perception Classifier

// COMMAND ----------

// MAGIC %md
// MAGIC Note that the dimensions of the two sets of vectors are different (780 vs 778). This is an artifact of the file format in which they are stored (take a look at the raw text, with spark.read.text, and you'll be able to deduce the problem). We need the dimension to match for the neural-net classifier. If they don't, you'll get a reasonably friendly error message telling you the problem.
// MAGIC
// MAGIC To solve this problem, we are going to create a UDF that takes in a vector of length 778, and returns a sparse vector of length 780. We are essentially creating features 779 and 780 in the test set, and setting them to zero.

// COMMAND ----------

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SparseVector

val udf_make780 = spark.udf.register("make780", (v:SparseVector) => Vectors.sparse(780, v.indices, v.values))

// COMMAND ----------

// MAGIC %md
// MAGIC Apply the `udf_make780` UDF to the `features` column, and rename the column `features`.

// COMMAND ----------

// ANSWER
val testFixedDF = testDF.select('label, udf_make780('features).as("features"))

// COMMAND ----------

display(testFixedDF)

// COMMAND ----------

// Import the ML algorithms we will use.
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline

// COMMAND ----------

// MAGIC %md
// MAGIC Similar to the decision tree lab, we want to tell the algorithm that the labels are categories 0-9, rather than continuous values.  We use the `StringIndexer` class to do this.  Then, we tie this feature preprocessing together with the Multilayer perceptron algorithm using a `Pipeline`.
// MAGIC
// MAGIC For the MLP algorithm, we need to set the number of neurons to use at each layer. Here, our input layer has 780 neurons (corresponding to our 780 features), we will use two hidden layers (one with 20 neurons and the other with 10 neurons), and have a single output layer with 10 output neurons (corresponding to our 10 labels). 
// MAGIC
// MAGIC Below is an example of a neural network with two hidden layers.
// MAGIC
// MAGIC ![neuralnet](http://cs231n.github.io/assets/nn1/neural_net2.jpeg)

// COMMAND ----------

// ANSWER
val indexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")

// Set the label column
val mlp = new MultilayerPerceptronClassifier().setLabelCol("indexedLabel")

mlp.setLayers(Array(780, 20, 10, 10)) // 780 inputs, two hidden layers (20 and 10 neurons, respectively) and output layer of 10 neurons

val pipeline = new Pipeline().setStages(Array(indexer, mlp))

// COMMAND ----------

// MAGIC %md
// MAGIC Now, let's fit a model to our data.

// COMMAND ----------

val model = pipeline.fit(trainingDF)

// COMMAND ----------

// MAGIC %md
// MAGIC You'll note -- if you play with this -- that neural nets take a long time to train :)
// MAGIC
// MAGIC Part of that is community edition, but part of that has to do with the nature of neural nets themselves. They are very powerful, but require a lot of data and time relative to many other models.
// MAGIC
// MAGIC Because neural networks are expensive to train, let's save this PipelineModel (both the feature transformations and the neural network itself) in Parquet format.

// COMMAND ----------

model.write.overwrite().save(userhome+"/NeuralNetPipeline.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's read the model back in, and apply this model to our test set. To do this, we need to tell Spark what kind of model we are reading in. The model is a PipelineModel.

// COMMAND ----------

import org.apache.spark.ml.PipelineModel

val savedModel = PipelineModel.load(userhome+"/NeuralNetPipeline.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have our model loaded, let's see how well it does on our test dataset.

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel")

// COMMAND ----------

val predictions = savedModel.transform(testFixedDF)

evaluator.setMetricName("accuracy")
evaluator.evaluate(predictions)

// COMMAND ----------

// MAGIC %md
// MAGIC Just a quick stab, and we've already done better than most of our tree-based classifiers. Before we get too pleased though, bear in mind the limited time we've invested in all of the schemes for this data set ... for further reading, look at the best published benchmarks for MNIST (well over 99% accuracy), other architectures that perform well for this sort of problem, like CNNs, and remember the "no free lunch theorem" ...

// COMMAND ----------

// MAGIC %md
// MAGIC **Bonus**: Go back and modify your code to use a different number of hidden layers, and modify the number of neurons in those hidden layers. See how much you can increase the accuracy by increasing the size/number of layers.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
