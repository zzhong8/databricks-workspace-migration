// Databricks notebook source
val fileTobeCopied = dbutils.fs.ls("/mnt/market6/production").filter(_.name.contains("Danonewave"))
fileTobeCopied.foreach(file=>{
//   println(s"/mnt/kroger-starbucks-us/lz/test/${file.name}")
  dbutils.fs.cp(file.path, s"/mnt/kroger-danonewave-us/lz/202001211006_97a6a8fa85c3469d92pos21Jan_2080227/${file.name}")
})

// COMMAND ----------

val fileTobeCopied = dbutils.fs.ls("/mnt/artifacts/packages/acosta_alerting/acosta_alerting-latest.egg")
display(fileTobeCopied)

// COMMAND ----------

dbutils.fs.cp("/mnt/artifacts/packages/acosta_alerting-latest.egg", s"/mnt/kroger-danonewave-us/lz/202001211006_97a6a8fa85c3469d92pos21Jan_2080227/${file.name}")

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/artifacts/packages/acosta_alerting-gen2-migration-latest.egg"))

// COMMAND ----------

display(dbutils.fs.ls("/mnt/artifacts/"))

// COMMAND ----------

display(dbutils.fs.mounts)

// COMMAND ----------

dbutils.fs.ls("dbfs:/databricks/egg/")

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks/egg/acosta_alerting-gen2-migration-latest.egg"))
