// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Catalyst
// MAGIC
// MAGIC * Catalyst Anti-Patterns
// MAGIC   * Partially cached DataFrames
// MAGIC   * User defined functions
// MAGIC   * Cartesian products
// MAGIC * Tungsten Encoders' effect on Catalyst Optimization
// MAGIC * Datasets: The DataFrame Query Language vs. Lambdas
// MAGIC * Custom Optimizers

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Catalyst Anti-Patterns
// MAGIC
// MAGIC We will begin by reviewing 3 common anti-patterns that can hurt application performance and prevent Catalyst optimizations:
// MAGIC - Partially cached DFs 
// MAGIC - User defined functions
// MAGIC - Cartesian products

// COMMAND ----------

// MAGIC %md
// MAGIC ### Anti-Pattern: Partially cached DataFrames 
// MAGIC
// MAGIC A partially cached DataFrame is considered an anti-pattern, as re-computation of missing partitions can be expensive. If any of the transformations carried out on missing data were wide (they required a shuffle where data was moved from one stage to another), all the work on the missing partition would have to be redone.
// MAGIC
// MAGIC To attempt to avoid this scenario, default settings for caching a `DataFrame` or `Dataset` have been altered compared to the older settings used for `RDD`s. By default the `StorageLevel.MEMORY_AND_DISK` level is used for a DF's `cache()` function.

// COMMAND ----------

throw new Exception("Please do not run this command during class.")

import org.apache.spark.sql.types._
val peopleSchema = StructType(List(
  StructField("id", IntegerType, true),
  StructField("firstName", StringType, true),
  StructField("middleName", StringType, true),
  StructField("lastName", StringType, true),
  StructField("gender", StringType, true),
  StructField("birthDate", TimestampType, true),
  StructField("ssn", StringType, true),
  StructField("salary", IntegerType, true)
))
// code used to generate screenshot #1 below (partial cache).

val people = spark
  .read//.option("inferSchema", "true")
  .option("header", "true")
  .option("delimiter", ":")
  .schema(peopleSchema)
  .csv("/mnt/training/dataframes/people-with-header-10m.txt")

val people2 = people.union(people)
val people3 = people2.union(people2)
val people4 = people3.union(people3)
people4.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY).count()

// COMMAND ----------

people4.unpersist()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/partially-cached-df.png" style="min-width: 800px; max-width: 1200px" alt="Partial DF Caching"/><br/>
// MAGIC
// MAGIC Any time further processing needs to be carried out on a partially cached DataFrame as above, it will cause re-computation that would involve pulling data from the initial data source. A simple solution is to use a `StorageLevel` that will save any data that doesn't fit in memory on to disk instead, such as `StorageLevel.MEMORY_AND_DISK_SER`. Note that the default implementation of `cache()`, for DataFrames, properly ensures that the entire DataFrame will be cached, even if the data has to spill to local disk.
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/fully-cached-df.png" style="min-width: 800px; max-width: 1200px" alt="Full DF Caching"/><br/>
// MAGIC
// MAGIC Accessing data from disk is slower than memory, but, in this case, the disk is _local_ to the node, so it's likely to be faster than reading from the original data source. Plus, it's better to avoid re-computation, especially if any shuffling (due to wide transformations) is involved.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Anti-Pattern: User defined functions
// MAGIC
// MAGIC UDFs require deserialization of data stored under the Tungsten format. The data needs to be available as an object in an executor so the UDF function can be applied to it. 
// MAGIC
// MAGIC Below is an example of a UDF that would require a column value to be deserialized from Tungsten's format to an object, to allow the UDF to operate on it.

// COMMAND ----------

import org.apache.spark.sql.functions.udf

val upperUDF = udf { s: String => s.toUpperCase }
val lowerUDF = udf { s: String => s.toLowerCase }

val udfDF = spark.read.parquet("dbfs:/mnt/training/dataframes/people-10m.parquet")
                 .select(upperUDF($"firstName"), lowerUDF($"middleName"), upperUDF($"lastName"), lowerUDF($"gender"))
udfDF.count

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC But:
// MAGIC
// MAGIC - Numerous utility functions for DataFrames and Datasets are already available in spark. 
// MAGIC - These functions are located in the <a href="https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/sql/functions.html">functions package</a> in spark under `org.apache.spark.sql.functions`
// MAGIC
// MAGIC Using built in function usage is preferred over coding UDFs:
// MAGIC - As built-in functions are integrated with Catalyst, they can be optimized in ways in which UDFs cannot. 
// MAGIC - Built-in functions can benefit from code-gen and can also manipulate our dataset even when it's serialized using the Tungsten format without `serialization / deserialization` overhead.
// MAGIC - Python UDFs carry extra overhead as they require additional serialization from the driver vm to the executor's JVM. 
// MAGIC     - A Hive UDF can be used instead of a python UDF. This will avoid serialization overheads. 
// MAGIC  
// MAGIC Below is an example of using the built in `lower` and `upper` functions. 

// COMMAND ----------

import org.apache.spark.sql.functions.{lower, upper}

val noUDF = spark.read.parquet("dbfs:/mnt/training/dataframes/people-10m.parquet")
                 .select(upper($"firstName"), lower($"middleName"), upper($"lastName"), lower($"gender"))
noUDF.count

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Anti-Pattern: Cartesian Products
// MAGIC
// MAGIC Put simply, a Cartesian product is a set that contains all possible combinations of elements from two other sets. 
// MAGIC
// MAGIC Related to Spark SQL this can be a table that contains all combinations of rows from two other tables, that were joined together.
// MAGIC
// MAGIC Cartesian products are problematic as they are a sign of an expensive computation.
// MAGIC
// MAGIC First, let's force the broadcast join threshold very low, just to ensure no side of the join is broadcast. (We do this for demo purposes.)

// COMMAND ----------

val previousThreshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)

// COMMAND ----------

val numbDF = (1 to 3).toDF("n1")
val numbDF2 = (4 to 6).toDF("n2")

val cartesianDF = numbDF.crossJoin(numbDF2)
cartesianDF.explain()

// COMMAND ----------

cartesianDF.show(100)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's reset the `spark.sql.autoBroadcastJoinThreshold` value to its default.

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previousThreshold)
println(s"Restored broadcast join threshold to ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")

// COMMAND ----------

// MAGIC %md
// MAGIC `BroadcastNestedLoopJoin` is the result of a Cartesian product that contains a DataFrame small enough to be broadcasted.
// MAGIC `CartesianProduct` would be the result of a Cartesian join where neither DataFrame is small enough to be broadcasted.
// MAGIC
// MAGIC **Don't run an action that requires accessing all partitions on the below Cartesian product**. It can take a very long time!
// MAGIC
// MAGIC `first()` is fine, because it's only going to operate on the first partition.

// COMMAND ----------

val ipDF = spark.read.parquet("/mnt/training/ip-geocode.parquet")
println("ipDF.count: " + ipDF.count)

val cartesianDF2 = ipDF.crossJoin(ipDF)
// explain is fine, but actions would take a very long time.
cartesianDF2.explain()
// show / first can be ok as it only requires one partition, but count will be problematic.
cartesianDF2.first()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Let's work through an example use-case. Our dataset contains a JSON file of IP address ranges allocated to various countries (`country_ip_ranges.json`), as well as various IP addresses of interesting transactions (`transaction_ips.json`). We want to work out the country code of a transaction's IP address. This can be accomplished using a ranged query to check if the transaction's address falls between one of the known ranges.
// MAGIC
// MAGIC First, let's inspect our dataset.

// COMMAND ----------

val ipRangesDF = spark.read.json("dbfs:/mnt/training/dataframes/country_ip_ranges.json")
val transactionDF = spark.read.json("dbfs:/mnt/training/dataframes/transaction_ips.json")

ipRangesDF.show(5)
transactionDF.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Next we want to run the query and check what range contains the transactions' IP addresses, and, thus, the country where the transaction occurred.

// COMMAND ----------

// TODO

val ipByCountry = transactionDF.join(ipRangesDF, transactionDF(<<FILL_IN>>) >= ipRangesDF(<<FILL_IN>>) && transactionDF(<<FILL_IN>>) <= ipRangesDF(<<FILL_IN>>))
ipByCountry.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Since we are expecting only the first partition using `show()` the result is rendered almost instantly. We can review what type of join resulted from the operation using the `explain()` function to render the physical plan.

// COMMAND ----------

ipByCountry.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC As expected, the resulting join was a `CartesianProduct`. One easy optimization is to pick the smaller of the two DataFrames and hint at broadcasting it, to instead achieve a `BroadcastNestedLoopJoin` instead.

// COMMAND ----------

// TODO

val ipByCountry = <<FILL_IN>>
ipByCountry.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Let's review the type of join resulting from the broadcast hint.

// COMMAND ----------

ipByCountryBroadcasted.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Tungsten Encoders' effect on Catalyst Optimization
// MAGIC
// MAGIC The Domain Specific Language (DSL) used by DataFrames and DataSets allows for data manipulation without having to deserialize that data from the Tungsten format. 
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/tuning/dsl-lambda.png" alt="Lambda serialization overhead"/><br/>
// MAGIC
// MAGIC The advantage of this is that we avoid any *serialization / deserialization* overhead. <br/>
// MAGIC DataSets give users the ability to carry out data manipulation through lambdas which can be very powerful, especially with semi-structured data. The **downside** of lambda is that they can't directly work with the Tungsten format, thus deserialization is required adding an overhead to the process.
// MAGIC
// MAGIC Avoiding frequent jumps between DSL and closures would mean that the *serialization / deserialization* to and from Tungsten format would be reduced, leading to a performance gain.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Datasets: The DataFrame Query Language vs. Lambdas
// MAGIC
// MAGIC The Dataset API gives you the option to use both the DataFrame query language _and_ RDD-like lambda transformations.
// MAGIC
// MAGIC **NOTE**: The Dataset API is not available in Python, so this section is in Scala.

// COMMAND ----------

case class Person(firstName: String, middleName: String, lastName: String, gender: String, birthDate: String, ssn: String, salary: String)
val personDS = spark.read.parquet("dbfs:/mnt/training/dataframes/people-10m.parquet/").as[Person]
// DataFrame query DSL
println(personDS.filter($"firstName" === "Nell").count)

// COMMAND ----------

// Dataset, with a lambda
println(personDS.filter(x => x.firstName == "Nell").count)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC - Advantages of using lambdas:
// MAGIC     - Good for semi-structured data
// MAGIC     - Very powerful
// MAGIC - Disadvantages:
// MAGIC     - Catalyst can't interpret lambdas until runtime. 
// MAGIC     - Lambdas are opaque to Catalyst. Since it doesn't know what a lambda is doing, it can't move it elsewhere in the processing.
// MAGIC     - Jumping between lambdas and the DataFrame query API can hurt performance.
// MAGIC     - Working with lambdas means that we need to `deserialize` from Tungsten's format to an object and then reserialize back to Tungsten format when the lambda is done.
// MAGIC     
// MAGIC If you _have_ to use lambdas, chaining them together can help.
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/tuning/interleaved-lambdas.png" alt="Interleaved Lambdas" style="border: 1px solid #cccccc; margin: 20px"/>
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/tuning/chained-lambdas.png" alt="Chained Lambdas" style="border: 1px solid #cccccc; margin: 20px"/><br/>

// COMMAND ----------

// define the year 40 years ago for the below query
import java.util.Calendar
val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40

// COMMAND ----------

// TODO
// Rewrite this to use pure DSL and avoid closures.
personDS.filter(x => x.birthDate.split("-")(0).toInt > earliestYear) // everyone above 40
        .filter($"salary" > 80000) // everyone earning more than 80K
        .filter(x => x.lastName.startsWith("J")) // last name starts with J
        .filter($"firstName".startsWith("D")) // first name starts with D
        .count()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Custom Optimizers
// MAGIC
// MAGIC As we know Catalyst is the component of spark that optimizes our code. This is achieved through rules that help spark understand that, for example, changing execution order of transformations might lead to better performance overall. The typical example of this scenario is filtering data as early as possible before processing it with spark. 
// MAGIC
// MAGIC Custom optimizations can be added to Catalyst by extending `Rule`. Additional conditions for operations can be specified. 
// MAGIC
// MAGIC One example of an optimization could be making **multiplication by 0** directly return 0 and avoiding the arithmetic operation all together. Before we create an optimization, lets review the physical plan of a DataFrame.
// MAGIC
// MAGIC **NOTE**: Custom optimizers are not currently available in Python, so this section is entirely in Scala.

// COMMAND ----------

val df = spark.read.parquet("dbfs:/mnt/training/ssn/names.parquet")
val dfRegular = df.select($"year" * 0)
dfRegular.explain()
dfRegular.show(5)

// COMMAND ----------

import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.IntegerType

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Next we need to extend Rule and implement our own `apply` function. The `apply` function takes a `LogicalPlan` of operations as the parameter and can operate on various operations. 
// MAGIC
// MAGIC One example of these operations is `Multiply` which takes two parameters:
// MAGIC  1. `left` is an <a href="https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala#L212">`AttributeReference`</a> which holds metadata and acts as a unqie way of identifying the data
// MAGIC  2. `right` is the `Literal` that is being applied during the operation, aka the number we're multiplying by.
// MAGIC  
// MAGIC  The logic of the rule is to check if the `Literal == 0` and if so, to just return a 0.

// COMMAND ----------

object MultiplyBy0Rule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case Multiply(left,right) if right.asInstanceOf[Literal].value.asInstanceOf[Int] == 0 => {
        println("Multiplication by 0 detected. Optimization Applied.")
        // return 0, we need to cast our 0 into a literal using an in-built spark function called Cast.
        Cast(Literal(0), IntegerType)
      }
    }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Catalyst needs to be made aware of the new rule. This can be done via the `SparkSession`'s `experimental` functionality.

// COMMAND ----------

spark.experimental.extraOptimizations = Seq(MultiplyBy0Rule)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Finally, lets review the physical plan that is generated after the optimization is applied when **multiplying by 0**.

// COMMAND ----------

val df = spark.read.parquet("dbfs:/mnt/training/ssn/names.parquet")
val dfZero = df.select($"year" * 0)
dfZero.explain()
dfZero.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC  - No optimization:   `*Project [(year#399 * 0) AS (year * 0)#405]`
// MAGIC  - With rule applied: `*Project [cast(0 as int) AS (year * 0)#421]`
// MAGIC
// MAGIC Notice that in the optimized version, 0 is just selected as a literal, without carrying out the arithmetic.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Try it yourself. (Optional)
// MAGIC
// MAGIC Complete the below rule to override the effect of division by 0 by adding a console or log message to let the user know this arithmetic problem occurred.

// COMMAND ----------

// TODO

import org.apache.spark.sql.catalyst.expressions.Divide
import org.apache.spark.sql.types.NullType

object DivideBy0Rule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      // check that we are dividing by 0 by inspecting the value of the Literal (the right param)
      case Divide(left,right) if <<FILL_IN>> => {
        // print to console that division by 0 occured. 
        <<FILL_IN>>
        // return a null, default spark behavior when dividing by 0.
      }
    }
  }
}

// add the new rule to the extra optimizations.
// verify by checking the physical plan of the DF generated.
<TODO>

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Further Reading
// MAGIC
// MAGIC <a href="https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html">Deep Dive into Spark SQL’s Catalyst Optimizer</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
