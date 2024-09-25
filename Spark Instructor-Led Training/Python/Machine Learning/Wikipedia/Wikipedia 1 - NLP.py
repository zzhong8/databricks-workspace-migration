# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
# MAGIC
# MAGIC # NLP and ML Primer using Wikipedia
# MAGIC
# MAGIC #### Business Questions:
# MAGIC
# MAGIC * Question # 1) What percentage of Wikipedia articles were edited in the past month (before the data was collected)?
# MAGIC * Question # 2) How many of the 1 million articles were last edited by ClueBot NG, an anti-vandalism bot?
# MAGIC * Question # 3) Which user in the 1 million articles was the last editor of the most articles?
# MAGIC * Question # 4) Can you display the titles of the articles in Wikipedia that contain a particular word?
# MAGIC * Question # 5) Can you extract out all of the words from the Wikipedia articles? (bag of words)
# MAGIC * Question # 6) What are the top 15 most common words in the English language?
# MAGIC * Question # 7) After removing stop words, what are the top 10 most common words in the english language? 
# MAGIC * Question # 8) How many distinct/unique words are in noStopWordsListDF?
# MAGIC
# MAGIC **Please run this notebook on a Spark 2.0 cluster.**

# COMMAND ----------

# MAGIC %md
# MAGIC Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
# MAGIC
# MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting to know the Data
# MAGIC Locate the Parquet data from the demo using `dbutils`:

# COMMAND ----------

# MAGIC %fs ls "/mnt/wikipedia-readonly/en_wikipedia/march_5_snapshot/flattenedParquet_updated3days_30partitions/"

# COMMAND ----------

# MAGIC %md
# MAGIC These are the 30 parquet files (~660 MB total) from the English Wikipedia Articles (March 5, 2016 snapshot) that were last updated between March 3, 2016 - March 5, 2016, inclusive.
# MAGIC
# MAGIC The first step is to load the articles into memory and lazily cache them:
# MAGIC
# MAGIC **Spark 2.0 note:**
# MAGIC
# MAGIC We don't have enough memory to use the Spark 2.0 vectorized Parquet reader. So, let's disable it. Running with it enabled, on CE and with this file, is likely to produce this error:
# MAGIC
# MAGIC ```
# MAGIC org.apache.spark.SparkException: Job aborted due to stage failure: Task 6 in stage 4.0 failed 1 times, most recent failure: Lost task 6.0 in stage 4.0 (TID 10, localhost): java.lang.RuntimeException: Cannot reserve additional contiguous bytes in the vectorized reader (requested = 47478697 bytes). As a workaround, you can disable the vectorized reader by setting spark.sql.parquet.enableVectorizedReader to false.
# MAGIC ```
# MAGIC
# MAGIC **Note that it _must_ be disabled _before_ calling `spark.read.parquet`.**

# COMMAND ----------

spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's confirm the change was made...

# COMMAND ----------

spark.conf.get("spark.sql.parquet.enableVectorizedReader")

# COMMAND ----------

# MAGIC %md
# MAGIC Before proceeding, we need to know how many partitions our data will be read in as.

# COMMAND ----------

print("{0:,} partitions".format( spark.read.parquet("dbfs:/mnt/wikipedia-readonly/en_wikipedia/march_5_snapshot/flattenedParquet_updated3days_30partitions/").rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md
# MAGIC Even though the data read in as **?** partitions, it **should** fit into **8** partitions.
# MAGIC
# MAGIC Let's readjust our shuffle partitions accordingly.
# MAGIC
# MAGIC **Question:** Why is 8 partitions better than the other options?

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC And now confirm the change to our shuffle partitions...

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's create our `DataFrame`, repartition it and cache it.

# COMMAND ----------

wiki_df = (spark
  .read.parquet("dbfs:/mnt/wikipedia-readonly/en_wikipedia/march_5_snapshot/flattenedParquet_updated3days_30partitions/")
  .repartition(8)
  .cache()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how fast `printSchema()` runs... this is because we can derive the schema from the Parquet metadata:

# COMMAND ----------

wiki_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's count how many total articles we have. (Note that when using a local mode cluster, the next command might take a minute or so.

# COMMAND ----------

# You can monitor the progress of this count + cache materialization via the Spark UI's storage tab
print("{0:,} articles\n".format(wiki_df.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC **Question:** The DataFrame uses **?** GB of Memory (of the **?** GB total available).

# COMMAND ----------

# MAGIC %md
# MAGIC Run `.count()` again to see the speed increase:

# COMMAND ----------

print("{0:,} articles\n".format(wiki_df.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC That's pretty impressive! We can scan through 111 thousand recent articles of English Wikipedia using a single 6 GB Executor in under 1 second.
# MAGIC
# MAGIC Let's take a peak at the data...

# COMMAND ----------

display(
  wiki_df.limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC This lab is meant to introduce you to working with unstructured text data in the Wikipedia articles. 
# MAGIC
# MAGIC In this lab, among other tasks, we will apply basic Natural Language Processing to the article text to extract out a bag of words.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #1:
# MAGIC ** How many days of data is in the DataFrame? **

# COMMAND ----------

# MAGIC %md
# MAGIC ** Challenge 1:**  Can you write this query using DataFrames? Hint: Start by importing the sql.functions.

# COMMAND ----------

# TODO
# Type your answer here...

# COMMAND ----------

# MAGIC %md
# MAGIC At the time of collection (March 5, 2016) Wikipedia had 5.096 million articles. What % of Wikipedia are we working with?

# COMMAND ----------

print("{0:.2f}%\n".format(wiki_df.count()/5096292.0*100))

# COMMAND ----------

# MAGIC %md
# MAGIC About **?%** of English Wikipedia.
# MAGIC
# MAGIC Next, register the `DataFrame` as a temporary table, so we can execute SQL against it:

# COMMAND ----------

wiki_df.createOrReplaceTempView("wikipedia") # .registerTempTable() in Spark 1.x

# COMMAND ----------

# MAGIC %md
# MAGIC Here are 10 of the articles:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, lastrev_pdt_time FROM wikipedia LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC The magic word **%sql** is not the only option.
# MAGIC
# MAGIC We can also use `spark.sql(..)` to execute SQL and from that call get a `DataFrame`.

# COMMAND ----------

temp_df = spark.sql("SELECT title, lastrev_pdt_time FROM wikipedia LIMIT 10")

display(temp_df)
# temp_df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #2:
# MAGIC ** How many of the 111 thousand articles were last edited by <a href="https://en.wikipedia.org/wiki/User:ClueBot_NG" target="_blank">ClueBot NG</a>, an anti-vandalism bot? **

# COMMAND ----------

# MAGIC %md
# MAGIC **Challenge 2:**  Write a SQL query to answer this question. Hint: The username to search for is **ClueBot NG**.

# COMMAND ----------

# TODO
# Type your answer here...

# COMMAND ----------

# MAGIC %md
# MAGIC **Challenge 3:** Update your previous SQL query to show the first 10 articles edited by **ClueBot NG**.

# COMMAND ----------

# TODO
# Type your answer here...

# COMMAND ----------

# MAGIC %md
# MAGIC You can study the specifc revisions like so: **https://<span></span>en.wikipedia.org/?diff=####**
# MAGIC
# MAGIC For example: <a href="https://en.wikipedia.org/?diff=708113872" target="_blank">https://<span></span>en.wikipedia.org/?diff=708113872</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #3:
# MAGIC ** Which user in the 111 thousand articles was the last editor of the most articles? **

# COMMAND ----------

# MAGIC %md
# MAGIC Here's a slightly more complicated query:

# COMMAND ----------

display(
  spark.sql("SELECT contributorusername, COUNT(contributorusername) FROM wikipedia GROUP BY contributorusername ORDER BY COUNT(contributorusername) DESC")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Hmm, looks are bots are quite active in maintaining Wikipedia.

# COMMAND ----------

# MAGIC %md
# MAGIC Interested in learning more about the bots that edit Wikipedia? Check out: <a href="https://en.wikipedia.org/wiki/Wikipedia:List_of_bots_by_number_of_edits" target="_blank">Wikipedia:List_of_bots_by_number_of_edits</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #4:
# MAGIC ** Can you display the titles of the articles in Wikipedia that contain a particular word or phrase? **

# COMMAND ----------

# MAGIC %md
# MAGIC Start by registering a User Defined Function (UDF) that can search for a string in the text of an article.
# MAGIC
# MAGIC **NOTE** We're going to use Scala here, because support for UDFs is better on that side of the house.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Register a function that can search if a string is found.
# MAGIC
# MAGIC val hasWord = (s: String, w: String) => {
# MAGIC   val re = ("""\b""" + w + """\b""").r
# MAGIC   (s != null) && re.findFirstIn(s).isDefined
# MAGIC }
# MAGIC spark.udf.register("containsWord", hasWord)

# COMMAND ----------

# MAGIC %md
# MAGIC Verify that the `hasWord(..)` function is working as intended:

# COMMAND ----------

# MAGIC %scala
# MAGIC // Look for the word 'test' in the first string
# MAGIC assert(! hasWord("hello astronaut, how's space?", "test"))

# COMMAND ----------

# MAGIC %scala
# MAGIC // Look for the word 'space' in the first string
# MAGIC assert(hasWord("hello astronaut, how's space?", "space"))

# COMMAND ----------

# MAGIC %scala
# MAGIC assert(! hasWord("hello, spacey, how's it going up there?", "space"))

# COMMAND ----------

# MAGIC %md
# MAGIC Not only can we use the Scala function `hasWord(..)` as the SQL function `containsWord(..)`, we can also use a parameterized query so you can easily change the word to search for:
# MAGIC
# MAGIC HINT: Try typing in **NASA** or **Manhattan**, **Cat** or even **cat** into the search box and hit **ENTER**.

# COMMAND ----------

# MAGIC %sql
# MAGIC select title from wikipedia where containsWord(title, '$word')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #5:
# MAGIC ** Can you extract out all of the words from the Wikipedia articles? ** (Create a bag of words)

# COMMAND ----------

# MAGIC %md
# MAGIC Before doing anything, we need to take all the words in our `DataFrame` and convert them to lowercase.
# MAGIC
# MAGIC This will preclude issues where we would treat **Kitten** and **kitten** as two different words.

# COMMAND ----------

wiki_lowered_df = (wiki_df
  .select( col("*"), lower(col("text")).alias("lowerText") )
)

wiki_lowered_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Use Spark ML's `RegexTokenizer` to read an input column of text and write a new output column of words:

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer

tokenizer = (RegexTokenizer()
    .setInputCol("lowerText")
    .setOutputCol("words")
    .setPattern(r'\W+')
)

wiki_words_df = tokenizer.transform(wiki_lowered_df)

# COMMAND ----------

wiki_words_df.show(5)

# COMMAND ----------

(wiki_words_df
   .select( col("title"), col("words"))
   .first()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #6:
# MAGIC ** What are the top 15 most common words in the English language? ** Compute this only on a random 10% of the 111 thousand articles.

# COMMAND ----------

# MAGIC %md
# MAGIC For this analysis, we should get reasonably accurate results even if we work on just 10% of the 111 thousand articles. Plus, this will speed things up tremendously. Note that this means about 11,000 articles

# COMMAND ----------

ten_percent_df = wiki_words_df.sample(False, .10, 555)

# COMMAND ----------

print("{0:,} words (total)".format(wiki_words_df.count()))
print("{0:,} words (sample)".format(ten_percent_df.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the `words` column contains arrays of Strings:

# COMMAND ----------

ten_percent_df.select( col("words") )

# COMMAND ----------

# MAGIC %md
# MAGIC Let's explode the `words` column into a table of one word per row:

# COMMAND ----------

# from pyspark.sql import functions as func
ten_percent_words_list_df = (ten_percent_df
  .select( explode(col("words")).alias("word") )
)

ten_percent_words_list_df.printSchema()

# COMMAND ----------

display(ten_percent_words_list_df)

# COMMAND ----------

print("{0:,} words".format(ten_percent_words_list_df.cache().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC The 10% list contains **?** million words.

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, run a word count on the exploded table:

# COMMAND ----------

word_group_count_df = (
  ten_percent_words_list_df
    .groupBy("word")                      # group
    .agg( count("word").alias("counts") ) # aggregate
    .sort( desc("counts") )               # sort
)
word_group_count_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC These would be good <a href="https://en.wikipedia.org/wiki/Stop_words" target="_blank">stop words</a> to filter out before running Natural Language Processing algorithms on our data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #7:
# MAGIC ** After removing stop words, what are the top 10 most common words in the english language? ** Compute this only on a random 10% of the 111 thousand articles.

# COMMAND ----------

# MAGIC %md
# MAGIC Use Spark.ml's stop words remover:

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

remover = (StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("noStopWords")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice below the removal of words like "about", "the",  etc:

# COMMAND ----------

(remover.transform(ten_percent_df)
   .select("id", "title", "words", "noStopWords")
   .show(50)
 )

# COMMAND ----------

no_stop_words_list_df = (remover
  .transform(ten_percent_df)
  .select( explode("noStopWords").alias("word") )
)

# COMMAND ----------

no_stop_words_list_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC The original 10% DataFrame list (which included stop words) contained 29.8 million words. How many words are in this new DataFrame?

# COMMAND ----------

print("{0:,} words".format(no_stop_words_list_df.cache().count()) )
print("{0:,} total".format(ten_percent_words_list_df.count()) )

# COMMAND ----------

# MAGIC %md
# MAGIC **?** million words remain. That means about **?** million words in our 10% sample were actually stop words.

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, let's see the top 15 words now:

# COMMAND ----------

no_stop_words_group_count = (no_stop_words_list_df
  .groupBy("word")                             # group
  .agg( count("word").alias("counts") )        # aggregate
  .sort( desc("counts") )                      # sort
)
no_stop_words_group_count.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question #8:
# MAGIC ** How many distinct/unique words are in noStopWordsListDF?**

# COMMAND ----------

print("{0:,} distinct words".format(no_stop_words_list_df.distinct().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC Looks like the Wikipedia corpus has around **?K** unique words. Probably a lot of these are rare scientific words, numbers, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC **Challenge 4:** Of our thousands of words, how many of them are numbers? That is what is the distinct count of all numbers?

# COMMAND ----------

# TODO
total = (no_stop_words_list_df
  # <fill-in>
  # <fill-in>
  .count()
)

print("{0:,} numbers".format(total))

# COMMAND ----------

# MAGIC %md
# MAGIC This concludes the English Wikipedia NLP lab.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
