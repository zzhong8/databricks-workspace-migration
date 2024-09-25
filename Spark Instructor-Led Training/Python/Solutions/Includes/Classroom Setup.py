# Databricks notebook source
# MAGIC %run "./Setup Environment"

# COMMAND ----------

# MAGIC %run "./Dataset Mounts"

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC //*******************************************
# MAGIC // CHECK FOR REQUIRED VERIONS OF SPARK & DBR
# MAGIC //*******************************************
# MAGIC
# MAGIC val dbrVersion = assertDbrVersion(4, 0)
# MAGIC val sparkVersion = assertSparkVersion(2, 3)
# MAGIC
# MAGIC displayHTML(s"""
# MAGIC Checking versions...
# MAGIC   <li>Spark: $sparkVersion</li>
# MAGIC   <li>DBR: $dbrVersion</li>
# MAGIC   <li>Scala: $scalaVersion</li>
# MAGIC   <li>Python: ${spark.conf.get("com.databricks.training.python-version")}</li>
# MAGIC """)
# MAGIC
# MAGIC //*******************************************
# MAGIC // ILT Specific functions
# MAGIC //*******************************************
# MAGIC
# MAGIC // Utility method to count & print the number of records in each partition.
# MAGIC def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
# MAGIC   import org.apache.spark.sql.functions._
# MAGIC   println("Per-Partition Counts:")
# MAGIC   val results = df.rdd                                   // Convert to an RDD
# MAGIC     .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
# MAGIC     .collect()                                           // Return the counts to the driver
# MAGIC
# MAGIC   results.foreach(x => println("* " + x))
# MAGIC }

# COMMAND ----------


#*******************************************
# ILT Specific functions
#*******************************************

# Utility method to count & print the number of records in each partition.
def printRecordsPerPartition(df):
  print("Per-Partition Counts:")
  def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
  results = (df.rdd                   # Convert to an RDD
    .mapPartitions(countInPartition)  # For each partition, count
    .collect()                        # Return the counts to the driver
  )
  # Print out the results.
  for result in results: print("* " + str(result))

None # suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC displayHTML("All done!")
