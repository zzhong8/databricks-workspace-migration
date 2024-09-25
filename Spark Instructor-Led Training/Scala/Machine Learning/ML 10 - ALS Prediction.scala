// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png) + ![Python Logo](http://spark-mooc.github.io/web-assets/images/python-logo-master-v3-TM-flattened_small.png)
// MAGIC
// MAGIC <img src="http://spark-mooc.github.io/web-assets/images/cs110x/movie-camera.png" style="float:right; height: 200px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>
// MAGIC
// MAGIC # Predicting Movie Ratings
// MAGIC
// MAGIC One of the most common uses of big data is to predict what users want.  This allows Google to show you relevant ads, Amazon to recommend relevant products, and Netflix to recommend movies that you might like.  This lab will demonstrate how we can use Apache Spark to recommend movies to a user.  We will start with some basic techniques, and then use the SparkML library's Alternating Least Squares method to make more sophisticated predictions. Here are the SparkML [Python docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html) and the [Scala docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.package).
// MAGIC
// MAGIC For this lab, we will use 20 million movie ratings from the [MovieLens stable benchmark rating dataset](http://grouplens.org/datasets/movielens/). 
// MAGIC
// MAGIC In this lab:
// MAGIC * *Part 1*: Exploring our Dataset
// MAGIC * *Part 2*: Collaborative Filtering
// MAGIC * *Part 3*: Predictions for Yourself

// COMMAND ----------

// MAGIC %md
// MAGIC #### ** Motivation **

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Want to win $1,000,000?
// MAGIC
// MAGIC All you needed to do was improve Netflix’s movie recommendation system by 10% in 2008. This competition is known as the [Netflix Prize](https://en.wikipedia.org/wiki/Netflix_Prize). 
// MAGIC
// MAGIC Good recommendations are vital to sites such as Netflix, where 75 percent of what consumers watch come from movie recommendations.
// MAGIC
// MAGIC So, how do we create recommendations and evaluate their relevance?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 1: Exploring our Dataset
// MAGIC
// MAGIC First, let's take a look at the directory containing our files.

// COMMAND ----------

// MAGIC %fs ls /mnt/training/movielens

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load and Cache
// MAGIC
// MAGIC The Databricks File System (DBFS) sits on top of an object store. 
// MAGIC
// MAGIC We're going to be accessing this data a lot. 
// MAGIC
// MAGIC Rather than reading it from source over and over again, we'll cache both the movies DataFrame and the ratings DataFrame into the executor's memory.

// COMMAND ----------

val moviesDF = spark.read.parquet("dbfs:/mnt/training/movielens/movies.parquet/")
val ratingsDF = spark.read.parquet("dbfs:/mnt/training/movielens/ratings.parquet/")

ratingsDF.cache
moviesDF.cache

val ratingsCount = ratingsDF.count
val moviesCount = moviesDF.count

println(s"There are ${ratingsDF.count} ratings and ${moviesDF.count} movies in the datasets")
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a quick look at some of the data in the two DataFrames.

// COMMAND ----------

display(moviesDF)

// COMMAND ----------

display(ratingsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Part 2: Collaborative Filtering**

// COMMAND ----------

// MAGIC %md
// MAGIC ### (2a) Creating a Training Set
// MAGIC
// MAGIC Before we jump into using machine learning, we need to break up the `ratingsDF` dataset into two DataFrames:
// MAGIC * A training set, which we will use to train models
// MAGIC * A test set, which we will use for our experiments
// MAGIC
// MAGIC To randomly split the dataset into the multiple groups, we can use the [randomSplit()](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit) transformation. `randomSplit()` takes a set of splits and a seed and returns multiple DataFrames. Use the seed given below.

// COMMAND ----------

// TODO: Replace <FILL_IN> with the appropriate code.

// We'll hold out 80% for training and leave 20% for testing 
val seed = 1800009193L
val Array(trainingDF, testDF) = <FILL_IN>

// Let's cache these datasets for performance
trainingDF.cache
testDF.cache

println("Training: %s, test: %s".format(trainingDF.count, testDF.count))

trainingDF.show(3)
testDF.show(3)

// COMMAND ----------

// TEST Creating a Training Set (2a) 
val Array(a, b) = ratingsDF.randomSplit(Array(.8, .2), seed=seed)
assert(trainingDF.count / 10000000 == a.count / 10000000, "Incorrect trainingDF count. Got " + trainingDF.count + " expected " + a.count)
assert(testDF.count() / 10000000 == b.count() / 10000000, "Incorrect testDF count. Got " + testDF.count + " expected " + b.count)

// COMMAND ----------

// MAGIC %md
// MAGIC ### (2b) Alternating Least Squares
// MAGIC
// MAGIC In this part, we will use the Apache Spark ML Pipeline implementation of Alternating Least Squares, [ALS (Python)](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALS) or [ALS (Scala)](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.recommendation.ALS). ALS takes a training dataset (DataFrame) and several parameters that control the model creation process. To determine the best values for the parameters, we will use ALS to train several models, and then we will select the best model and use the parameters from that model in the rest of this lab exercise.
// MAGIC
// MAGIC The process we will use for determining the best model is as follows:
// MAGIC 1. Pick a set of model parameters. The most important parameter to model is the *rank*, which is the number of columns in the Users matrix or the number of rows in the Movies matrix. In general, a lower rank will mean higher error on the training dataset, but a high rank may lead to [overfitting](https://en.wikipedia.org/wiki/Overfitting).  We will train models with ranks of 4 and 12 using the `trainingDF` dataset.
// MAGIC
// MAGIC 2. Set the appropriate parameters on the `ALS` object:
// MAGIC     * The "User" column will be set to the values in our `userId` DataFrame column.
// MAGIC     * The "Item" column will be set to the values in our `movieId` DataFrame column.
// MAGIC     * The "Rating" column will be set to the values in our `rating` DataFrame column.
// MAGIC     * We'll be using a regularization parameter of 0.1.
// MAGIC     
// MAGIC    **Note**: Read the documentation for the ALS class **carefully**. It will help you accomplish this step.
// MAGIC
// MAGIC 4. Create multiple models using the `ParamGridBuilder` and the `CrossValidator`, one for each of our rank values.
// MAGIC
// MAGIC 6. We'll keep the model with the lowest error rate. Such a model will be selected automatically by the CrossValidator.
// MAGIC
// MAGIC [ALS Python docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALS)
// MAGIC
// MAGIC [ALS Scala docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.recommendation.ALS)

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
import org.apache.spark.ml.recommendation.ALS

// Let's initialize our ALS learner
val als = new ALS()

// Now we set the parameters for the method
als.setMaxIter(5)
   .setSeed(seed)
   .setColdStartStrategy("drop")
   .<FILL_IN>
    
// Now let's compute an evaluation metric for our test dataset
import org.apache.spark.ml.evaluation.RegressionEvaluator

// Create an RMSE evaluator using the label and predicted columns 
val regEval = new RegressionEvaluator()  
                    .setLabelCol("rating")
                    .setPredictionCol("prediction")
                    .setMetricName("rmse")

// COMMAND ----------

// TEST  - Run this cell to test your solution.
assert(als.getItemCol == "movieId", "Incorrect choice of " + als.getItemCol + " for ALS item column.")
assert(als.getUserCol == "userId", "Incorrect choice of " + als.getUserCol + " for ALS user column.")
assert(als.getRatingCol == "rating", "Incorrect choice of " + als.getRatingCol + " for ALS rating column.")

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have initialized a model, we need to fit it to our training data, and evaluate how well it does on the validation dataset. Create a `CrossValidator` and `ParamGridBuilder` that will decide whether *rank* value *4* or *12* gives a lower *RMSE*.  
// MAGIC
// MAGIC NOTE: This cell may take up to 5 minutes to run.

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code

import org.apache.spark.ml.tuning._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel

val regEval = <FILL_IN> // Create RegressionEvaluator

val grid = <FILL_IN> // Create grid for rank values 4 and 12 

val seed = 42
val cv = new CrossValidator()          
      .setSeed(seed)
      <FILL_IN> // Set number of folds to 3. Add grid, als and regEval

val cvModel = cv.fit(trainingDF)

val myModel = cvModel.bestModel.asInstanceOf[ALSModel]
println("The best model was trained with rank " + myModel.rank)

// COMMAND ----------

// TEST - Run this cell to test your solution.
assert(myModel.rank == 12, "Unexpected value for best rank. Expected 12, got " + myModel.rank)

// COMMAND ----------

// MAGIC %md
// MAGIC ### (2c) Testing Your Model
// MAGIC
// MAGIC So far, we used the `trainingDF` dataset to select the best model. Since we used this dataset to determine what model is best, we cannot use it to test how good the model is; otherwise, we would be very vulnerable to [overfitting](https://en.wikipedia.org/wiki/Overfitting).  To decide how good our model is, we need to use the `testDF` dataset.  We will use the best model you cou created in part (2b) for predicting the ratings for the test dataset and then we will compute the RMSE.
// MAGIC
// MAGIC The steps you should perform are:
// MAGIC * Run a prediction, using `myModel` as created above, on the test dataset (`testDF`), producing a new `predictedTestDF` DataFrame.
// MAGIC * Use the previously created RMSE evaluator, `regEval` to evaluate the filtered DataFrame.

// COMMAND ----------

// TODO: Replace <FILL_IN> with the appropriate code

val predictedTestDF = myModel.<FILL_IN>

// Run the previously created RMSE evaluator, regEval, on the predictedTestDF DataFrame
val testRmse = <FILL_IN>

println("The model had a RMSE on the test set of " + testRmse)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 3: Predictions for Yourself
// MAGIC The ultimate goal of this lab exercise is to predict what movies to recommend to yourself.  In order to do that, you will first need to add ratings for yourself to the `ratingsDF` dataset.

// COMMAND ----------

// MAGIC %md
// MAGIC **(3a) Your Movie Ratings**
// MAGIC
// MAGIC To help you provide ratings for yourself, we have included the following code to list the names and movie IDs of the 50 highest-rated movies that have at least 500 ratings.

// COMMAND ----------

moviesDF.createOrReplaceTempView("movies")
ratingsDF.createOrReplaceTempView("ratings")

// COMMAND ----------

// MAGIC %md
// MAGIC The user ID 0 is unassigned, so we will use it for your ratings. We set the variable `myUserId` to 0 for you. Next, create a new DataFrame called `myRatingsDF`, with your ratings for at least 10 movie ratings. Each entry should be formatted as `(myUserId, movieId, rating)`.  As in the original dataset, ratings should be between 1 and 5 (inclusive). If you have not seen at least 10 of these movies, you can increase the parameter passed to `LIMIT` in the above cell until there are 10 movies that you have seen (or you can also guess what your rating would be for movies you have not seen).

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
val myUserId = 0

// Note that the movie IDs are the *last* number on each line. A common error was to use the number of ratings as the movie ID.
val myRatedMovies = List(
     <FILL_IN>
      // The format of each line is (myUserId, movie ID, your rating)
      // For example, to give the movie "Star Wars: Episode IV - A New Hope (1977)" a five rating, you would add the following line:
      // (myUserId, 260, 5),
     )

val myRatingsRDD = sc.parallelize(myRatedMovies)
val myRatingsDF = myRatingsRDD.toDF("userId","movieId","rating")
println("My movie ratings:")
display(myRatingsDF.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### (3b) Add Your Movies to Training Dataset
// MAGIC
// MAGIC Now that you have ratings for yourself, you need to add your ratings to the `trainingDF` dataset so that the model you train will incorporate your preferences.  Spark's [union()](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.union) transformation combines two DataFrames; use `union()` to create a new training dataset that includes your ratings and the data in the original training dataset.

// COMMAND ----------

//TODO: Replace <FILL_IN> with appropriate code
val trainingWithMyRatingsDF = <FILL_IN>

println("The training dataset now has %s more entries than the original training dataset".format(trainingWithMyRatingsDF.count -
        trainingDF.count))
assert ((trainingWithMyRatingsDF.count - trainingDF.count) == myRatingsDF.count)

// COMMAND ----------

// MAGIC %md
// MAGIC ### (3c) Train a Model with Your Ratings
// MAGIC
// MAGIC Now, train a model with your ratings added and the parameters you used in in part (2b) and (2c). Make sure you include **all** of the parameters.
// MAGIC
// MAGIC **Note**: This cell will take about 1 minute to run.

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code

// Reset the parameters for the ALS object.
als.setPredictionCol("prediction")
   .setMaxIter(5)
   .setSeed(seed)
   .setColdStartStrategy("drop")
   .<FILL_IN>

// Create the model with these parameters.
val myRatingsModel = als.<FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ### (3d) Predict Your Ratings
// MAGIC
// MAGIC Now that we have trained a new model, let's predict what ratings you would give to the movies that you did not already provide ratings for. The code below filters out all of the movies you have rated, and creates a `predictedRatingsDF` DataFrame of the predicted ratings for all of your unseen movies.

// COMMAND ----------

import org.apache.spark.sql.functions.lit

// Create a list of the my rated movie IDs 
val myRatedMovieIds = for ((userId, movieId, rating) <- myRatedMovies) yield {movieId}

// Filter out the movies I already rated.
val notRatedDF = moviesDF.filter(! $"ID".isin(myRatedMovieIds:_*))

// Rename the "ID" column to be "movieId", and add a column with myUserId as "userId".
val myUnratedMoviesDF = notRatedDF.withColumnRenamed("ID", "movieId").withColumn("userId", lit(myUserId))       
// Use myRatingModel to predict ratings for the movies that I did not manually rate.
val predictedRatingsDF = myRatingsModel.transform(myUnratedMoviesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### (3e) Predict Your Ratings
// MAGIC
// MAGIC We have our predicted ratings. Now we can print out the 25 movies with the highest predicted ratings.

// COMMAND ----------

// MAGIC %md
// MAGIC **NOTE:** Since Spark 2.3, it will be required to enable cross joins.
// MAGIC
// MAGIC Or more specificaly, disable the warning when a cross join is automaticaly detected.

// COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the raw predictions:

// COMMAND ----------

// MAGIC %md
// MAGIC Now print out the 25 movies with the highest predicted ratings. We will only include movies that have at least 75 ratings in total.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
