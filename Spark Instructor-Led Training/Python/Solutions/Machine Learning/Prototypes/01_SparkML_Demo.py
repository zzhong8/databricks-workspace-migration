# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## SparkML
# MAGIC In this notebook, we'll use Spark for:
# MAGIC
# MAGIC * Sentiment Analysis
# MAGIC * Natural Language Processing (NLP)
# MAGIC * Decision Trees
# MAGIC
# MAGIC We will be using a dataset of roughly 50,000 IMDB reviews, which includes the English language text of that review and the rating associated with it (1-10). Based on the text of the review, we want to predict if the rating is "positive" or "negative".

# COMMAND ----------

# MAGIC %run "./Includes/Classroom_Setup"

# COMMAND ----------

reviewsDF = spark.read.parquet("/mnt/training/movie-reviews/imdb/imdb_ratings_50k.parquet")
reviewsDF.createOrReplaceTempView("reviews")
display(reviewsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC What does the distribution of scores look like?
# MAGIC
# MAGIC HINT: Use `count()`

# COMMAND ----------

# MAGIC %sql
# MAGIC --ANSWER
# MAGIC SELECT count(rating), rating FROM reviews GROUP BY rating ORDER BY rating

# COMMAND ----------

# MAGIC %md
# MAGIC The authors of this dataset have removed the "neutral" ratings, which they defined as a rating of 5 or 6.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Test Split
# MAGIC
# MAGIC We'll split our data into training and test samples. We will use 80% for training, and the remaining 20% for testing. We set a seed to reproduce the same results (i.e. if you re-run this notebook, you'll get the same results both times).

# COMMAND ----------

(trainDF, testDF) = reviewsDF.randomSplit([0.8, 0.2], seed=42)
trainDF.cache()
testDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's determine our baseline accuracy

# COMMAND ----------

positiveRatings = trainDF.filter("rating >= 5").count()
totalRatings = trainDF.count()

print("Baseline accuracy: {0:.2f}%".format(positiveRatings/totalRatings*100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformers + Estimators

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer

tokenizer = (RegexTokenizer()
            .setInputCol("review")
            .setOutputCol("tokens")
            .setPattern("\\W+"))

tokenizedDF = tokenizer.transform(reviewsDF)
display(tokenizedDF.limit(5)) # Look at a few tokenized reviews

# COMMAND ----------

# MAGIC %md
# MAGIC There are a lot of words that do not contain much information about the sentiment of the review (e.g. `the`, `a`, etc.). Let's remove these uninformative words using `StopWordsRemover`.

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

remover = (StopWordsRemover()
          .setInputCol("tokens")
          .setOutputCol("stopWordFree"))

removedStopWordsDF = remover.transform(tokenizedDF)
display(removedStopWordsDF.limit(5)) # Look at a few tokenized reviews without stop words

# COMMAND ----------

# MAGIC %md
# MAGIC Where do the stop words actually come from? Spark includes a small English list as a default, which we're implicitly using here.

# COMMAND ----------

stopWords = remover.getStopWords()
stopWords

# COMMAND ----------

# MAGIC %md
# MAGIC Let's remove the `br` from our reviews.

# COMMAND ----------

remover.setStopWords(["br"] + stopWords)
removedStopWordsDF = remover.transform(tokenizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's apply a CountVectorizer model

# COMMAND ----------

from pyspark.ml.feature import CountVectorizer

counts = (CountVectorizer()
          .setInputCol("stopWordFree")
          .setOutputCol("features")
          .setVocabSize(1000))

countModel = counts.fit(removedStopWordsDF) # It's a model

# COMMAND ----------

# MAGIC %md
# MAGIC __Now let's adjust the label (target) values__
# MAGIC
# MAGIC We want to group the reviews into "positive" or "negative" sentiment. So all of the star "levels" need to be collapsed into one of two groups.

# COMMAND ----------

from pyspark.ml.feature import Binarizer

binarizer = (Binarizer()
            .setInputCol("rating")
            .setOutputCol("label")
            .setThreshold(5.0))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we are going to use a Decision Tree model to fit to our dataset.

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline

dtc = DecisionTreeClassifier()

pipeline = Pipeline().setStages([tokenizer, remover, counts, binarizer, dtc])

# COMMAND ----------

pipelineModel = pipeline.fit(trainDF)
decisionTree = pipelineModel.stages[-1]

# COMMAND ----------

print(decisionTree.toDebugString)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's save the pipeline model.

# COMMAND ----------

fileName = userhome + "/tmp/DT_Pipeline"
pipelineModel.write().overwrite().save(fileName)

# COMMAND ----------

from pyspark.ml import PipelineModel
# Load saved model
savedPipelineModel = PipelineModel.load(fileName)

# COMMAND ----------

resultDF = savedPipelineModel.transform(testDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluate

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Accuracy: %(result)s" % {"result": evaluator.evaluate(resultDF)})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Confusion Matrix

# COMMAND ----------

display(resultDF.groupBy("label", "prediction").count())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
