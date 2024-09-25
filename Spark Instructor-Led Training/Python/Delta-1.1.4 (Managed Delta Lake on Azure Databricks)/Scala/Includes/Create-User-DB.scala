// Databricks notebook source
val databaseName = {
  val tags = com.databricks.logging.AttributionContext.current.tags
  val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
  val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
  val databaseName   = username.replaceAll("[^a-zA-Z0-9]", "_") + "_dbs"
  spark.conf.set("com.databricks.training.spark.databaseName", databaseName)
  databaseName
}

displayHTML(s"Created user-specific database")

// COMMAND ----------

// MAGIC %python
// MAGIC databaseName = spark.conf.get("com.databricks.training.spark.databaseName")
// MAGIC databaseName = databaseName[:len(databaseName)-1]+"p"
// MAGIC
// MAGIC None #Suppress output

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS `%s`".format(databaseName))
spark.sql("USE `%s`".format(databaseName))

displayHTML("""Using the database <b style="color:green">%s</b>.""".format(databaseName))

