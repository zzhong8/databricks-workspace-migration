// Databricks notebook source
// MAGIC
// MAGIC %python
// MAGIC course_name = "Databricks Delta"

// COMMAND ----------

// MAGIC %run "./Dataset-Mounts"

// COMMAND ----------

// MAGIC %run "./Test-Library"

// COMMAND ----------

// MAGIC %run "./Create-User-DB"

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC def deleteTables(database) :
// MAGIC   try:
// MAGIC     tables=spark.catalog.listTables(database)
// MAGIC   except:
// MAGIC     return
// MAGIC   
// MAGIC   for table in tables:
// MAGIC       spark.sql("DROP TABLE {}.{}".format(database, table.name))
// MAGIC
// MAGIC def untilStreamIsReady(name):
// MAGIC   queries = list(filter(lambda query: query.name == name, spark.streams.active))
// MAGIC
// MAGIC   if len(queries) == 0:
// MAGIC     print("The stream is not active.")
// MAGIC
// MAGIC   else:
// MAGIC     while (queries[0].isActive and len(queries[0].recentProgress) == 0):
// MAGIC       pass # wait until there is any type of progress
// MAGIC
// MAGIC     if queries[0].isActive:
// MAGIC       queries[0].awaitTermination(5)
// MAGIC       print("The stream is active and ready.")
// MAGIC     else:
// MAGIC       print("The stream is not active.")
// MAGIC
// MAGIC None

// COMMAND ----------


def deleteTables(database:String):Unit = {
  val tables = try { spark.catalog.listTables(database).collect.toSeq} 
  catch { case _: Throwable => Seq[org.apache.spark.sql.catalog.Table]() }
  
  for (table <- tables) {
      spark.sql("DROP TABLE %s.%s".format(database, table.name))
  }
}

def untilStreamIsReady(name:String):Unit = {
  val queries = spark.streams.active.filter(_.name == name)

  if (queries.length == 0) {
    println("The stream is not active.")
  } else {
    while (queries(0).isActive && queries(0).recentProgress.length == 0) {
      // wait until there is any type of progress
    }

    if (queries(0).isActive) {
      queries(0).awaitTermination(5*1000)
      println("The stream is active and ready.")
    } else {
      println("The stream is not active.")
    }
  }
}

displayHTML("""
<div>Declared various utility methods:</div>
<li>Declared <b style="color:green">deleteTables(<i>database:String</i>)</b> for database resets</li>
<li>Declared <b style="color:green">untilStreamIsReady(<i>name:String</i>)</b> to control workflow</li>
<br/>
<div>All done!</div>
""")
