// Databricks notebook source
// MAGIC %run "./Setup Environment"

// COMMAND ----------

// MAGIC %run "./Dataset Mounts"

// COMMAND ----------


//*******************************************
// CHECK FOR REQUIRED VERIONS OF SPARK & DBR
//*******************************************

val dbrVersion = assertDbrVersion(4, 0)
val sparkVersion = assertSparkVersion(2, 3)

displayHTML(s"""
Checking versions...
  <li>Spark: $sparkVersion</li>
  <li>DBR: $dbrVersion</li>
  <li>Scala: $scalaVersion</li>
  <li>Python: ${spark.conf.get("com.databricks.training.python-version")}</li>
""")

//*******************************************
// ILT Specific functions
//*******************************************

// Utility method to count & print the number of records in each partition.
def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
  import org.apache.spark.sql.functions._
  println("Per-Partition Counts:")
  val results = df.rdd                                   // Convert to an RDD
    .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
    .collect()                                           // Return the counts to the driver

  results.foreach(x => println("* " + x))
}

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC #*******************************************
// MAGIC # ILT Specific functions
// MAGIC #*******************************************
// MAGIC
// MAGIC # Utility method to count & print the number of records in each partition.
// MAGIC def printRecordsPerPartition(df):
// MAGIC   print("Per-Partition Counts:")
// MAGIC   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
// MAGIC   results = (df.rdd                   # Convert to an RDD
// MAGIC     .mapPartitions(countInPartition)  # For each partition, count
// MAGIC     .collect()                        # Return the counts to the driver
// MAGIC   )
// MAGIC   # Print out the results.
// MAGIC   for result in results: print("* " + str(result))
// MAGIC
// MAGIC None # suppress output

// COMMAND ----------

displayHTML("All done!")
