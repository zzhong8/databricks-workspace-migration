# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Airbnb in San Francicsco
# MAGIC ![Airbnb logo](http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png)<br>
# MAGIC
# MAGIC The dataset we'll be working with is from Airbnb rentals in San Francisco.
# MAGIC
# MAGIC You can find more information here:<br>
# MAGIC http://insideairbnb.com/get-the-data.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Reading the data

# COMMAND ----------

# MAGIC %fs ls mnt/training/airbnb/sf-listings/sf-listings.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Let's examine the file to properly read it.

# COMMAND ----------

dbutils.fs.head("mnt/training/airbnb/sf-listings/sf-listings.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like we have a header and a comma separator. 

# COMMAND ----------

filePath = "mnt/training/airbnb/sf-listings/sf-listings.csv"

rawDF = (spark
         .read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(filePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Hm, the *id* field got picked up as `string`. Let's see what's going on here. *Take a look at the 3rd record*.

# COMMAND ----------

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC It seems our CSV has *multiLine* values. We can use our CSV reader in multiline mode by passing an extra options.

# COMMAND ----------

rawDF = (spark
         .read
         .option("header", "true")
         .option("multiLine", "true")
         .option("inferSchema", "true")
         .csv(filePath))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The *id* field has gotten still picked up as `string`...
# MAGIC
# MAGIC Let's take a closer look and make your guess about what's going wrong here:

# COMMAND ----------

display(rawDF)

# COMMAND ----------

from pyspark.sql.functions import col

display(rawDF.filter(col("id").like("%AirBedAndBreakfast%")).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC What we see is that the quotation marks are escaped as `""`. Let's indicate this to the CSV reader.

# COMMAND ----------

rawDF = (spark
         .read
         .option("header", "true")
         .option("multiLine", "true")
         .option("inferSchema", "true")
         .option("escape",'"')
         .csv(filePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Now this looks much better!

# COMMAND ----------

display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC For our usecase, only keep certain columns from this dataset.

# COMMAND ----------

columnsToKeep = [
  "host_is_superhost",
  "cancellation_policy",
  "instant_bookable",
  "host_total_listings_count",
  "neighbourhood_cleansed",
  "zipcode",
  "latitude",
  "longitude",
  "property_type",
  "room_type",
  "accommodates",
  "bathrooms",
  "bedrooms",
  "beds",
  "bed_type",
  "minimum_nights",
  "number_of_reviews",
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value",
  "price"]

baseDF = rawDF.select(columnsToKeep)

# COMMAND ----------

baseDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Well done! Now we have a DataFrame we can cleanse and use for Machine Learning later.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Data Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fixing the data types

# COMMAND ----------

# MAGIC %md
# MAGIC If you take a look above, you will see that the `price` field got picked up as *string*. Let's see why:

# COMMAND ----------

display(baseDF.select("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's fix this!

# COMMAND ----------

from pyspark.sql.functions import *

fixedPriceDF = (baseDF
                .withColumnRenamed("price", "price_raw")
                .withColumn("price", regexp_replace(col("price_raw"), "[\$,]", "").cast("Double")))

# COMMAND ----------

display(fixedPriceDF.select("price_raw", "price"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting rid of *NULL*s
# MAGIC
# MAGIC The `count` property of the `df.describe()` DataFrame displays the number of non-null values for each column. 

# COMMAND ----------

display(fixedPriceDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ** 1. Cleansing Categorical features**
# MAGIC
# MAGIC There are a few nulls in the categorical feature `zipcode` and `host_is_superhost`. Let's get rid of them.

# COMMAND ----------

noNullsDF = fixedPriceDF.filter(col("zipcode").isNotNull() & col("host_is_superhost").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ** 2. Imputing Nominal Features **
# MAGIC
# MAGIC Let's take our nominal features and replace the null values with the median of the non-null values for each column.
# MAGIC
# MAGIC We will use the `Imputer` Spark ML module for this. The `Imputer` favors `Double` type features. Let's change the type of our nominal columns to `Double`.

# COMMAND ----------

from pyspark.sql.types import *

integerColumns = [x.name for x in baseDF.schema.fields if x.dataType == IntegerType()]
doublesDF = noNullsDF

for c in integerColumns:
  doublesDF = doublesDF.withColumn(c, col(c).cast("double"))

print("Columns converted from Integer to Double:\n - {}".format("\n - ".join(integerColumns)))

# COMMAND ----------

doublesDF.printSchema()

# COMMAND ----------

from pyspark.ml.feature import Imputer

imputer = Imputer()
print(imputer.explainParams())

# COMMAND ----------

imputer.setStrategy("median")

# COMMAND ----------

# MAGIC %md
# MAGIC We will only impute columns that have null values. Let's see which ones are these.

# COMMAND ----------

display(doublesDF.describe())

# COMMAND ----------

imputeCols = [
              "host_total_listings_count",
              "bathrooms",
              "beds", 
              "review_scores_rating",
              "review_scores_accuracy",
              "review_scores_cleanliness",
              "review_scores_checkin",
              "review_scores_communication",
              "review_scores_location",
              "review_scores_value"]

imputer.setInputCols(imputeCols)
imputer.setOutputCols(imputeCols)

imputedDF = imputer.fit(doublesDF).transform(doublesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting rid of extreme values
# MAGIC
# MAGIC Let's take a look at the *min* and *max* values of the `price` column:

# COMMAND ----------

display(imputedDF.select("price").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC %md There are some super-expensive listings. But that's the Data Scientist's job to decide what to do with them. We can certainly filter the "free" AirBnBs though. First see how many there are.

# COMMAND ----------

imputedDF.filter(col("price") == 0).count()

# COMMAND ----------

posPricesDF = imputedDF.filter(col("price") > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the *min* and *max* values of the `minimum_nights` column

# COMMAND ----------

display(posPricesDF.select("minimum_nights").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC A minimum of 100 million nights to stay? There are certainly some extremes here. Let's see these on a barchart.

# COMMAND ----------

display(posPricesDF.groupBy("minimum_nights").count().orderBy(col("count").desc(), col("minimum_nights")))

# COMMAND ----------

# MAGIC %md
# MAGIC One year looks like a reasonable limit here.

# COMMAND ----------

cleanDF = posPricesDF.filter(col("minimum_nights") <= 365)

# COMMAND ----------

# MAGIC %md
# MAGIC OK, our data is cleansed now. Let's save this DataFrame to a table so that our Data Scientist friend can pick it up.

# COMMAND ----------

outputPath = userhome + "/airbnb-cleansed.parquet"
# Remove output folder if it exists
dbutils.fs.rm(outputPath, True)
cleanDF.write.parquet(outputPath);

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
