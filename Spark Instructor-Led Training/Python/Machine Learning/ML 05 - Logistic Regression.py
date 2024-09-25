# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook, we'll use Spark for:
# MAGIC
# MAGIC * Logistic Regression
# MAGIC * Sentiment Analysis
# MAGIC * Natural Language Processing (NLP)
# MAGIC * ROC Visualizations
# MAGIC
# MAGIC We will be using a dataset of roughly 50,000 IMDB reviews, which includes the English language text of that review and the rating associated with it (1 to 10). 
# MAGIC
# MAGIC This dataset was published in the following paper:
# MAGIC > Andrew L. Maas, Raymond E. Daly, Peter T. Pham, Dan Huang, Andrew Y. Ng, and Christopher Potts. (2011). [Learning Word Vectors for Sentiment](http://ai.stanford.edu/~amaas/papers/wvSent_acl2011.pdf) Analysis. The 49th Annual Meeting of the Association for Computational Linguistics (ACL 2011).
# MAGIC
# MAGIC *Neutral reviews (rating 5 and 6) are not included in this dataset.*
# MAGIC
# MAGIC We'll use this data to explore the hypothesis that the diction used in a review correlates to sentiment (positive or negative). This hypothesis is possible to test because we have __labeled data__ -- i.e., we have the review scores, so we can train and test. We will use a Logistic Regression model to test this hypothesis.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by reading in our data, and creating a view called `reviews`

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
# MAGIC SELECT count(rating), rating FROM reviews GROUP BY rating ORDER BY rating

# COMMAND ----------

# MAGIC %md
# MAGIC The overwhelming majority of the reviews are good (4-5 stars) -- we'll have to keep an eye on the possible effect of this on our results. In practice, solutions to this might include stratified sampling, or oversampling of the negative reviews.

# COMMAND ----------

# MAGIC %md
# MAGIC What does the distribution look like for reviews including the word `great`?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(rating), rating FROM reviews WHERE review LIKE '%great%' GROUP BY rating ORDER BY rating

# COMMAND ----------

# MAGIC %md
# MAGIC Not too surprising -- but remember that uneven distribution of star ratings! Other things being equal, we should expect a distribution with a shape something like this -- though even for a neutral word like "the" ... the main information here is at the lower end, where a smaller proportion of 1- and 2-star reviews appear with "great".
# MAGIC
# MAGIC How about the word `poor`?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(rating), rating FROM reviews WHERE review LIKE '%poor%' GROUP BY rating ORDER BY rating

# COMMAND ----------

# MAGIC %md
# MAGIC That is both not surprising and fairly non-informative: the disparity in star distribution makes this result even more significant.

# COMMAND ----------

# MAGIC %md
# MAGIC Ok, so what is the strategy for training a machine learning model?
# MAGIC
# MAGIC Since we're predicting two sentiment categories, "positive" and "negative", this is a 2-class classification problem.
# MAGIC
# MAGIC In this notebook, we'll try a logistic regression model, and we'll make the following assumptions about featurization. Naturally, all of these assumptions are subject to question.
# MAGIC
# MAGIC * We can split up our text on sequences of 1 or more "non-word" chars and we'll be ok (this is not a given for all material)
# MAGIC * The most common English words ("of" "the" "and" etc.) carry little information for us, so we want to filter them out
# MAGIC * "Bag-of-words" model -- i.e., we will retain some form of word counts but no higher-order data such as n-grams, part-of-speech, etc.
# MAGIC * Frequent words are more interesting than infrequent words. Why? our hypothesis is that there is a smallish number of sentiment-carrying words in the corpus voabulary, whereas there is a large number of "technical" product-related words which are less informative.
# MAGIC   * Given this hypothesis, we'll use a helper that keeps the most frequent words rather than most of the words (CountVectorizer vs. HashingTF)
# MAGIC
# MAGIC
# MAGIC Given all that, how do we prep the dataset with Spark? 
# MAGIC
# MAGIC Like the "VectorAssembler" we saw earlier, Spark provides "feature helper" classes that will do this work for us.
# MAGIC
# MAGIC We'll use:
# MAGIC * RegexTokenizer
# MAGIC * StopWordsRemover
# MAGIC * CountVectorizer

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
display(removedStopWordsDF.limit(1)) # Look at the first tokenized review without stop words

# COMMAND ----------

# MAGIC %md
# MAGIC Where do the stop words actually come from? Spark includes a small English list as a default, which we're implicitly using here. For many real-world problems, you'll want to supply your own set of stop words which are customized for the business domain and tweaked to tune the model for performance.

# COMMAND ----------

remover.getStopWords()

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
# MAGIC
# MAGIC Spark has two helpers we can choose from:
# MAGIC
# MAGIC * a more general "Bucketizer" that converts a (potentially) continuous feature into a finite set of buckets with cutoff thresholds
# MAGIC * a specific two-class Binarizer ... effectively the same thing, but for just two buckets: negative infinity up through threshold, and anything over threshold
# MAGIC
# MAGIC For these reviews, let's call 6, 7, 8, 9 and 10-star reviews "positive" -- so we can use a threshold of 5. Because this is a two-class problem, we are going to use <a href="https://spark.apache.org/docs/latest/ml-features.html#binarizer" target="_blank">Binarizer</a>.
# MAGIC
# MAGIC > Feature values greater than the threshold are binarized to 1.0; values equal to or less than the threshold are binarized to 0.0.

# COMMAND ----------

from pyspark.ml.feature import Binarizer

binarizer = (Binarizer()
  .setInputCol("rating")
  .setOutputCol("label")
  .setThreshold(5))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll split our data into training and test samples. We will use 80% for training, and the remaining 20% for testing. We set a seed to reproduce the same results (i.e. if you re-run this notebook, you'll get the same results both times).

# COMMAND ----------

(trainDF, testDF) = reviewsDF.randomSplit([0.8, 0.2], seed=42)
trainDF.cache()
testDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Recall how we used a Pipeline object to encapsulate the processing steps in our earlier example. We'll do the same thing here, but with a few more steps in the pipeline.
# MAGIC
# MAGIC Here, we are going to use a `Logistic Regression Model`, which is a [discriminative model](https://en.wikipedia.org/wiki/Discriminative_model) (used for classification). Classification is different from regression in that there is a discrete set of output labels, and you cannot predict an output label that is not present in your training set. 
# MAGIC
# MAGIC Under the hood, `Logistic Regression` relies on the sigmoid function to output a number between 0 and 1, which are interpreted as probabilities. By default, we use a threshold of .5 to separate two classes, but we can change that by passing in the `threshold` parameter to the Logistic Regression model. For example, if we want to be more than 50% certain of assigning a datapoint to a certain class, we would adjust the threshold accordingly.
# MAGIC
# MAGIC Below is an image of the sigmoid function:
# MAGIC
# MAGIC ![sigmoid](https://sebastianraschka.com/images/faq/logisticregr-neuralnet/sigmoid.png)
# MAGIC
# MAGIC Where \\[ z = w^Tx + b \\] (HINT: This equation should look familiar to you from linear regression)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

lr = LogisticRegression()

p = Pipeline().setStages([tokenizer, remover, counts, binarizer, lr])

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll fit the model, and use that model on our test set to see how we did. But we need a way to evaluate how well our model performs, and for this we are going to use the area under the ROC curve (AUROC, commonly abbreviated to AUC). 
# MAGIC
# MAGIC A receiver operating characteristic (ROC) curve plots the true positive rate (TPR) vs. the false positive rate (FPR). 
# MAGIC
# MAGIC - True positive (TP): predicted class is positive, true class is positive
# MAGIC - False positive (FP): predicted class is positive, true class is negative
# MAGIC - True negative (TN): predicted class is negative, true class is negative
# MAGIC - False negative (FN): predicted class is negative, true class is positive
# MAGIC
# MAGIC Where:
# MAGIC - The false positive rate is the number of false positives divided by the total number of negatives: FP/(FP + TN)
# MAGIC - The true positive rate is the number of true positives divided by the total number of positives: TP/(TP + FN)
# MAGIC
# MAGIC The baseline for the ROC curve is simply a straight line, and your model should have a curve above that straight line, otherwise your model is performing worse than random. AUC measures the area under the ROC curve, and is between 0 and 1. 
# MAGIC
# MAGIC Below is an example of a confusion matrix and an ROC curve.

# COMMAND ----------

# MAGIC %md
# MAGIC ![precision](https://www.researchgate.net/profile/Mauno_Vihinen/publication/230614354/figure/fig4/AS:216471646019585@1428622270943/Contingency-matrix-and-measures-calculated-based-on-it-2x2-contigency-table-for.png) ![ROC](https://www.medcalc.org/manual/_help/images/roc_intro3.png)

# COMMAND ----------

model = p.fit(trainDF)
model.stages[-1].summary.areaUnderROC

# COMMAND ----------

# MAGIC %md
# MAGIC Many Spark models support a "training summary" which provides stats on how well the algorithm was able to fit the model to the training set.
# MAGIC
# MAGIC These statistics represent the "apparent error" or the training error. We achieved an AUC around 0.91 when training.
# MAGIC
# MAGIC But of course the real measure of the model is how it does with new data, so let's use it on the test data and take a measurement.

# COMMAND ----------

result = model.transform(testDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Earlier we saw the RegressionEvaluator. Here, we'll use a BinaryClassificationEvaluator. Same idea, but tailored for classification problems, it exposes statistics like AUC and area under PR (precision-recall) curve.

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator()
auc = evaluator.evaluate(result)
print("AUC: {}".format(auc))

# COMMAND ----------

# MAGIC %md
# MAGIC So, even without any tuning -- just using the defaults in Spark, we're doing ok. We'll take a few minutes for you to try and improve the performance the same way we did with linear regression -- by mutating parameters of the LogisticRegression instance or the CountVectorizer instance.
# MAGIC
# MAGIC But, first, let's look at one more thing: Spark can generate a DataFrame that represents the points on the ROC curve, and Databricks can plot it for us.
# MAGIC
# MAGIC The following cell shows the same flow, adjusted a tiny bit to match the Databricks notebook `display` call for ROC. Try to replace all of the `FILL_IN` in the cell below.

# COMMAND ----------

partialPipeline = Pipeline().setStages([tokenizer, remover, counts, binarizer]) # Stages: tokenizer, remover, counts, binarizer

pipelineModel = partialPipeline.fit(trainDF)

preppedDataDF = pipelineModel.transform(trainDF)

# Previously, we put fitting a Logistic Regression model in the pipeline
lrModel = LogisticRegression().fit(preppedDataDF)

# ROC for training data
display(lrModel, preppedDataDF, "ROC")

# COMMAND ----------

# MAGIC %md
# MAGIC Still confused on what a false positive is? Hopefully this image can clarify things a bit (and note when it can be more important to pay attention to false positives vs. false negatives).
# MAGIC
# MAGIC ![pregnancy](http://marginalrevolution.com/wp-content/uploads/2014/05/Type-I-and-II-errors1-625x468.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, here's a chance for you to try and beat our initial test AUC of around 0.84. The code is mostly set up for you here, and you can try some manual hyperparameter tuning by filling in the `FILL_IN` cells. We will cover automatic parameter tuning shortly.

# COMMAND ----------

print (lr.explainParams())

# COMMAND ----------

# NOTE: Can set regularization to any value
lr.setRegParam(2.0)
counts.setVocabSize(2000)
model = p.fit(trainDF)
result = model.transform(testDF)
print("AUC: {}".format(BinaryClassificationEvaluator().evaluate(result)))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
