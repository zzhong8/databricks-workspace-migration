// Databricks notebook source
val tags = com.databricks.logging.AttributionContext.current.tags

//*******************************************
// GET VERSION OF APACHE SPARK
//*******************************************

// Get the version of spark
val Array(sparkMajorVersion, sparkMinorVersion, _) = spark.version.split("""\.""")

// Set the major and minor versions
spark.conf.set("com.databricks.training.spark.major-version", sparkMajorVersion)
spark.conf.set("com.databricks.training.spark.minor-version", sparkMinorVersion)

//*******************************************
// GET VERSION OF DATABRICKS RUNTIME
//*******************************************

// Get the version of the Databricks Runtime
val runtimeVersion = tags.collect({ case (t, v) if t.name == "sparkVersion" => v }).head
val runtimeVersions = runtimeVersion.split("""-""")
val (dbrVersion, scalaVersion) = if (runtimeVersions.size == 3) {
  val Array(dbrVersion, _, scalaVersion) = runtimeVersions
  (dbrVersion, scalaVersion.replace("scala", ""))
} else {
  val Array(dbrVersion, scalaVersion) = runtimeVersions
  (dbrVersion, scalaVersion.replace("scala", ""))
}
val Array(dbrMajorVersion, dbrMinorVersion, _) = dbrVersion.split("""\.""")

// Set the the major and minor versions
spark.conf.set("com.databricks.training.dbr.major-version", dbrMajorVersion)
spark.conf.set("com.databricks.training.dbr.minor-version", dbrMinorVersion)

//*******************************************
// GET USERNAME AND USERHOME
//*******************************************

// Get the user's name
val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))

// Get the user's home. Create it if necessary
val userhome = s"dbfs:/user/$username"
// This call doesn't fail if it already exists
val created = dbutils.fs.mkdirs(userhome)

// Set the user's name and home directory
spark.conf.set("com.databricks.training.username", username)
spark.conf.set("com.databricks.training.userhome", userhome)

//**********************************
// VARIOUS UTILITY FUNCTIONS
//**********************************

def assertSparkVersion(expMajor:Int, expMinor:Int):String = {
  val major = spark.conf.get("com.databricks.training.spark.major-version")
  val minor = spark.conf.get("com.databricks.training.spark.minor-version")

  if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor))
    throw new Exception(s"This notebook must be ran on Spark version $expMajor.$expMinor or better, found Spark $major.$minor")

  s"$major.$minor"
}

def assertDbrVersion(expMajor:Int, expMinor:Int):String = {
  val major = spark.conf.get("com.databricks.training.dbr.major-version")
  val minor = spark.conf.get("com.databricks.training.dbr.minor-version")

  if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor))
    throw new Exception(s"This notebook must be ran on Databricks Runtime (DBR) version $expMajor.$expMinor or better, found $major.$minor.")
  
  s"$major.$minor"
}

displayHTML("Successfully created class variables and utility functions.")

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC from __future__ import print_function
// MAGIC from __future__ import division
// MAGIC from pyspark.sql.functions import *
// MAGIC
// MAGIC #**********************************
// MAGIC # VARIOUS UTILITY FUNCTIONS
// MAGIC #**********************************
// MAGIC
// MAGIC def assertSparkVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.spark.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.spark.minor-version")
// MAGIC
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Spark version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC
// MAGIC   return major+"."+minor
// MAGIC
// MAGIC def assertDbrVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Databricks Runtime (DBR) version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC
// MAGIC #**********************************
// MAGIC # INIT VARIOUS VARIABLES
// MAGIC #**********************************
// MAGIC
// MAGIC username = spark.conf.get("com.databricks.training.username")
// MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
// MAGIC
// MAGIC import sys
// MAGIC pythonVersion = spark.conf.set("com.databricks.training.python-version", sys.version[0:sys.version.index(" ")])
// MAGIC
// MAGIC None # suppress output
