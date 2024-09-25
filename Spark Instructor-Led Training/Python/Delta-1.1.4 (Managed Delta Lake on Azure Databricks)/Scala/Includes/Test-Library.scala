// Databricks notebook source
// MAGIC
// MAGIC %python
// MAGIC def dbTest(id, expected, result):
// MAGIC   try:
// MAGIC     ok = spark.conf.get("com.databricks.training.valid") == "true"
// MAGIC   except:
// MAGIC     ok = False
// MAGIC   if not ok:
// MAGIC     raise Exception("You are not authorized to run this course.")
// MAGIC   assert str(expected) == str(result), "{} does not equal expected {}".format(result, expected)

// COMMAND ----------

def dbTest[T](id: String, expected: T, result: => T, message: String = ""): Unit = {
  val ok = try {
    spark.conf.get("com.databricks.training.valid") == "true"
  }
  catch {
    case _: Exception => false
  }
  if (! ok) throw new Exception("You are not authorized to run this course.")
  assert(result == expected, message)
}
displayHTML("Imported Test Library...") // suppress output
