# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Understanding Parallelization of Machine Learning Algorithms in Apache Spark™
# MAGIC
# MAGIC ##Data Preparation

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The dataset used for this example is Bank marketing. Given a set of features about a customer can we predict whether the person will open a term deposit account.
# MAGIC
# MAGIC Original Source: [UCI Machine Learning Repository 
# MAGIC Bank Marketing Data Set](https://archive.ics.uci.edu/ml/datasets/bank+marketing)
# MAGIC [Moro et al., 2014] S. Moro, P. Cortez and P. Rita. A Data-Driven Approach to Predict the Success of Bank Telemarketing. Decision Support Systems, Elsevier, 62:22-31, June 2014

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Attribute Information:
# MAGIC
# MAGIC ####Input variables:
# MAGIC bank client data:
# MAGIC ```
# MAGIC 1 - age (numeric)
# MAGIC 2 - job : type of job (categorical: 'admin.','blue-collar','entrepreneur','housemaid','management','retired','self-employed','services','student','technician','unemployed','unknown')
# MAGIC 3 - marital : marital status (categorical: 'divorced','married','single','unknown'; note: 'divorced' means divorced or widowed)
# MAGIC 4 - education (categorical: 'basic.4y','basic.6y','basic.9y','high.school','illiterate','professional.course','university.degree','unknown')
# MAGIC 5 - default: has credit in default? (categorical: 'no','yes','unknown')
# MAGIC 6 - housing: has housing loan? (categorical: 'no','yes','unknown')
# MAGIC 7 - loan: has personal loan? (categorical: 'no','yes','unknown')
# MAGIC related with the last contact of the current campaign:
# MAGIC 8 - contact: contact communication type (categorical: 'cellular','telephone') 
# MAGIC 9 - month: last contact month of year (categorical: 'jan', 'feb', 'mar', ..., 'nov', 'dec')
# MAGIC 10 - day_of_week: last contact day of the week (categorical: 'mon','tue','wed','thu','fri')
# MAGIC 11 - duration: last contact duration, in seconds (numeric). Important note: this attribute highly affects the output target (e.g., if duration=0 then y='no'). Yet, the duration is not known before a call is performed. Also, after the end of the call y is obviously known. Thus, this input should only be included for benchmark purposes and should be discarded if the intention is to have a realistic predictive model.
# MAGIC other attributes:
# MAGIC 12 - campaign: number of contacts performed during this campaign and for this client (numeric, includes last contact)
# MAGIC 13 - pdays: number of days that passed by after the client was last contacted from a previous campaign (numeric; 999 means client was not previously contacted)
# MAGIC 14 - previous: number of contacts performed before this campaign and for this client (numeric)
# MAGIC 15 - poutcome: outcome of the previous marketing campaign (categorical: 'failure','nonexistent','success')
# MAGIC social and economic context attributes
# MAGIC 16 - emp.var.rate: employment variation rate - quarterly indicator (numeric)
# MAGIC 17 - cons.price.idx: consumer price index - monthly indicator (numeric) 
# MAGIC 18 - cons.conf.idx: consumer confidence index - monthly indicator (numeric) 
# MAGIC 19 - euribor3m: euribor 3 month rate - daily indicator (numeric)
# MAGIC 20 - nr.employed: number of employees - quarterly indicator (numeric)
# MAGIC
# MAGIC Output variable (desired target):
# MAGIC 21 - y - has the client subscribed a term deposit? (binary: 'yes','no')
# MAGIC ```

# COMMAND ----------

#Run this cell to download the dataset
dbutils.notebook.run("../Utils/01 Get Bank Marketing Dataset",0)

# COMMAND ----------

display(table("bank_marketing"))

# COMMAND ----------

# MAGIC %md ## Feature Engineering in Spark

# COMMAND ----------

input_data = table("bank_marketing")

cols = input_data.columns

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, StringIndexerModel, VectorAssembler

categoricalColumns = ["job", "marital", "education", "default", "housing", "loan", "contact", "month", "poutcome"]
stages = [] # stages in our Pipeline
for categoricalCol in categoricalColumns:
  # Category Indexing with StringIndexer
  stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol+"Index")
  # Use OneHotEncoder to convert categorical variables into binary SparseVectors
  encoder = OneHotEncoder(inputCol=categoricalCol+"Index", outputCol=categoricalCol+"classVec")
  # Add stages.  These are not run here, but will be run all at once later on.
  stages += [stringIndexer, encoder]

# COMMAND ----------

#numericCols = ["age", "balance", "duration", "campaign", "previous", "day"]
numericCols = ["age", "balance", "campaign", "previous", "day"]

assemblerInputs = numericCols + [c + "classVec" for c in categoricalColumns]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

labelIndexer = StringIndexer(inputCol="y", outputCol="label")

stages += [labelIndexer]

# COMMAND ----------

pipeline = Pipeline(stages=stages)
# Run the feature transformations.
#  - fit() computes feature statistics as needed.
#  - transform() actually transforms the features.
pipelineModel = pipeline.fit(input_data)
dataset = pipelineModel.transform(input_data)

# Keep relevant columns
selectedcols = ["label", "features"] + cols
dataset = dataset.select(selectedcols)
display(dataset)

# COMMAND ----------

# MAGIC %md ## Write out the Training and Testing data for future reference

# COMMAND ----------

(trainingData, testData) = dataset.randomSplit([0.8, 0.2], seed = 12)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Set the username
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC spark.conf.set("my.username", username)

# COMMAND ----------

username = spark.conf.get("my.username")
trainingData.write.mode("overwrite").format("delta").save("dbfs:/ml-workshop-datasets/{}/employee/delta/singleNode/trainingData".format(username))
testData.write.mode("overwrite").format("delta").save("dbfs:/ml-workshop-datasets/{}/employee/delta/singleNode/testData".format(username))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data in and convert to Pandas dataframe

# COMMAND ----------

pdf_train = (spark.read
             .format("delta")
             .load("dbfs:/ml-workshop-datasets/{}/employee/delta/singleNode/trainingData".format(username))
             .toPandas())

pdf_test = (spark.read
            .format("delta")
            .load("dbfs:/ml-workshop-datasets/{}/employee/delta/singleNode/testData".format(username))
            .toPandas())

# COMMAND ----------

pdf_train

# COMMAND ----------

# MAGIC %md ### Ready to train your model!
