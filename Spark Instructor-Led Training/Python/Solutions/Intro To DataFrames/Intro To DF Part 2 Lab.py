# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to DataFrames, Lab #2
# MAGIC ## Washingtons and Adams

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Instructions
# MAGIC
# MAGIC This data was captured in the August before the 2016th presidential election.
# MAGIC
# MAGIC As a result, articles about the candidates were very popular.
# MAGIC
# MAGIC For this exercise, you will...
# MAGIC 0. Filter the result to the **en** Wikipedia project.
# MAGIC 0. Find all the articles where the name of the article ends with **_Washington** (presumably "George Washington", "Martha Washington, etc)
# MAGIC 0. Return all records as an array to the Driver.
# MAGIC 0. Total the requests for the Washingtons and assign it to the variable `totalWashingtons`.
# MAGIC 0. Assign your array of Washingtons (the return value of your action) to the variable `washingtons`.
# MAGIC
# MAGIC ** Bonus **
# MAGIC
# MAGIC Repeat the exercise for the Adams
# MAGIC 0. Filter the result to the **en** Wikipedia project.
# MAGIC 0. Find all the articles where the name of the article ends with **_Adams** (presumably "John Adams", "John Quincy Adams", etc)
# MAGIC 0. Return all records as an array to the Driver.
# MAGIC 0. Total the requests for the Adamas and assign it to the variable `totalAdams`.
# MAGIC 0. Assign your array of Adamas (the return value of your action) to the variable `adams`.
# MAGIC 0. But you cannot do it the same way twice:
# MAGIC    * In the filter, don't use the same conditional method as the one used for the Washingtons.
# MAGIC    * Don't use the same action as used for the Washingtons.
# MAGIC
# MAGIC **Testing**
# MAGIC
# MAGIC Run the last cell to verify that your results are correct.
# MAGIC
# MAGIC **Hints**
# MAGIC * The actions we've explored for extracting data include:
# MAGIC   * `first()`
# MAGIC   * `collect()`
# MAGIC   * `head()`
# MAGIC   * `take(n)`
# MAGIC * The conditional methods used with a `filter(..)` include:
# MAGIC   * equals
# MAGIC   * not-equals
# MAGIC   * starts-with
# MAGIC   * and there are others - remember, the `DataFrames` API is built upon an SQL engine.
# MAGIC * There shouldn't be more than 1000 records for either the Washingtons or the Adams

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Show Your Work

# COMMAND ----------

# ANSWER

from pyspark.sql.functions import *

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

washingtons = (spark.read
  .parquet(parquetDir)
  .filter( col("project") == "en")
  .filter( col("article").endswith("_Washington") )
  #.filter( col("article").like("%\\_Washington") )
  .collect()
  #.take(1000)
)
totalWashingtons = 0

for washington in washingtons:
  totalWashingtons += washington["requests"]

print("Total Washingtons: {0:,}".format( len(washingtons) ))
print("Total Washington Requests: {0:,}".format( totalWashingtons ))

# COMMAND ----------

# ANSWER
# BEST ANSWER - this is how you would do it in production

from pyspark.sql.functions import *  # sum(), count()

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

stats = (spark.read
  .parquet(parquetDir)
  .filter((col("project") == "en") & col("article").endswith("_Washington"))
  .select(sum("requests"), count("*"))
  .first())

totalWashingtons = stats[0]
washingtonCount = stats[1]

print("Total Washingtons: {}".format(washingtonCount) )
print("Total Washingtons Requests: {}".format(totalWashingtons))

# COMMAND ----------

# ANSWER

from pyspark.sql.functions import *

adams = (spark.read
  .parquet(parquetDir)
  .filter( col("project") == "en")
  #.filter( col("article").endswith("_Adams") )
  .filter( col("article").like("%\\_Adams") )
  #.collect()
  .take(1000)
)
totalAdams = 0

for adam in adams:
  totalAdams += adam["requests"]

print("Total Adams: {0:,}".format( len(adams) ))
print("Total Adam Requests: {0:,}".format( totalAdams ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Verify Your Work
# MAGIC Run the following cell to verify that your `DataFrame` was created properly.

# COMMAND ----------

print("Total Washingtons: {0:,}".format( len(washingtons) ))
print("Total Washington Requests: {0:,}".format( totalWashingtons ))

expectedCount = 466
assert len(washingtons) == expectedCount, "Expected " + str(expectedCount) + " articles but found " + str( len(washingtons) )

expectedTotal = 3266
assert totalWashingtons == expectedTotal, "Expected " + str(expectedTotal) + " requests but found " + str(totalWashingtons)

# COMMAND ----------

print("Total Adams: {0:,}".format( len(adams) ))
print("Total Adam Requests: {0:,}".format( totalAdams ))

expectedCount = 235
assert len(adams) == expectedCount, "Expected " + str(expectedCount) + " articles but found " + str( len(adams) )

expectedTotal = 3126
assert totalAdams == expectedTotal, "Expected " + str(expectedTotal) + " requests but found " + str(totalAdams)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
