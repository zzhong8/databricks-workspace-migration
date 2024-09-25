// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC
// MAGIC # Structured Streaming 
// MAGIC ## Kafka Examples
// MAGIC
// MAGIC We have another server that reads Wikipedia edits in real time, with a multitude of different languages. 
// MAGIC
// MAGIC **What you will learn:**
// MAGIC * About Kafka
// MAGIC * How to establish a connection with Kafka
// MAGIC * More examples 
// MAGIC * More visualizations

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Our Kafka Server
// MAGIC
// MAGIC The following function reads the Wikipedia edit data from a Kafka server. 
// MAGIC
// MAGIC The Kafka server is fed by a separate TCP server that reads the Wikipedia edits, in real time, from the various language-specific IRC channels to which Wikimedia posts them. 
// MAGIC
// MAGIC That server parses the IRC data, converts the results to JSON, and sends the JSON to:
// MAGIC * any directly connected TCP clients. TCP clients receive data for _all_ languages the server is monitoring.
// MAGIC * a Kafka server, with the edits segregated by language. That is, Kafka topic "en" corresponds to edits for en.wikipedia.org.
// MAGIC
// MAGIC In general, using Kafka is the better choice, as it provides guarantees of end-to-end fault-tolerance. 
// MAGIC
// MAGIC For details on reading structured streams from Kafka, see 
// MAGIC [The Structured Streaming + Kafka Integration Guide](https://people.apache.org//~pwendell/spark-nightly/spark-branch-2.1-docs/latest/structured-streaming-kafka-integration.html).
// MAGIC
// MAGIC **NOTE**: The Kafka server must be Kafka broker version 0.10.0 or higher.

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Data
// MAGIC
// MAGIC Reading from Kafka returns a `DataFrame` with the following fields:
// MAGIC * `key`           - the data key (we don't need this)
// MAGIC * `value`         - the data, in binary format. This is our JSON payload. We'll need to cast it to STRING.
// MAGIC * `topic`         - the topic. In this case, the topic is the same as the "wikipedia" field, so we don't need it.
// MAGIC * `partition`     - partition. This server only has one partition, so we don't need this information.
// MAGIC * `offset`        - the offset value. We're not using it.
// MAGIC * `timestamp`     - the timestamp. We'll keep this, as it's useful for windowing
// MAGIC * `timestampType` - not needed
// MAGIC
// MAGIC Of these, the only two columns we want to keep are `timestamp` and `value`.

// COMMAND ----------

import org.apache.spark.sql.functions._ 
spark.conf.set("spark.sql.shuffle.partitions", 8)

val editsDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")
  .option("subscribe", "en")
  .load()
  .select($"timestamp", $"value".cast("STRING").as("value"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Required Options
// MAGIC
// MAGIC When consuming from a Kafka source, you _must_ specify:
// MAGIC
// MAGIC 1. The Kafka bootstrap servers (option "kafka.bootstrap.servers")
// MAGIC 2. Some indication of the topics you want to consume.
// MAGIC
// MAGIC There are three mutually-exclusive ways to define #2:
// MAGIC
// MAGIC | Option             | Value                                          | Description                            | Example |
// MAGIC | ------------------ | ---------------------------------------------- | -------------------------------------- | ------- |
// MAGIC | "subscribe"        | A comma-separated list of topics               | A list of topics to which to subscribe | `"topic1"`, or `"topic1,topic2,topic3"`
// MAGIC | "assign"           | A JSON string indicating topics and partitions | Specific topic-partitions to consume.  | `{"topic1": [1,3], "topic2": [2,5]}`
// MAGIC | "subscribePattern" | A (Java) regular expression                    | A pattern to match desired topics      | `"e[ns]"`, `"topic[123]"`
// MAGIC
// MAGIC
// MAGIC Note that we're using the "subscribe" option in the above code to select the topics we're interested in consuming. We've selected only the "en" topic here, corresponding to edits for the English Wikipedia. If we wanted to consume multiple topics (multiple Wikipedia languages, in our case), we could just specify them as a comma-separate list:
// MAGIC
// MAGIC ```.option("subscribe", "en,es,it,fr,de,eo")```
// MAGIC
// MAGIC #### But what about partitions?
// MAGIC
// MAGIC As it happens, the Kafka server from which we're reading only has a single partition, so we don't have to worry about partitions in this notebook.
// MAGIC
// MAGIC #### Other options
// MAGIC
// MAGIC There are other, optional, arguments you can give the Kafka source. See [the integration guide](http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream) for details.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at our schema:

// COMMAND ----------

editsDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### The JSON
// MAGIC
// MAGIC Spark 2.1 has a [SQL function][functions] called `from_json()` that takes an input column of JSON data and produces a structured output column. To call that function, we need to:
// MAGIC
// MAGIC * import the appropriate package
// MAGIC * provide a schema for the JSON
// MAGIC
// MAGIC Let's connect to the TCP server (not the Kafka server) and take a look at one line of the JSON, to see how to create the schema.
// MAGIC
// MAGIC [functions]: https://people.apache.org/~pwendell/spark-nightly/spark-branch-2.1-docs/latest/api/scala/index.html#org.apache.spark.sql.functions$

// COMMAND ----------

import java.net.{InetAddress, Socket}
import scala.io.Source

val addr = InetAddress.getByName("server1.databricks.training")
val socket = new Socket(addr, 9002)
val jsonString = Source.fromInputStream(socket.getInputStream).getLines.next
socket.close()

// Pretty print the JSON using Jackson, because Jackson is already installed
// on Databricks.
val mapper = new org.codehaus.jackson.map.ObjectMapper
val prettyJSON = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(
  mapper.readTree(jsonString)
)
println(prettyJSON)

// COMMAND ----------

// MAGIC %md
// MAGIC We can hand-craft the schema, for sure. But, having read a line of JSON data, let's have Spark infer a schema for us, just to see what we have. The easiest way to do that is to:
// MAGIC
// MAGIC * create an RDD from the string (via `sc.parallelize`),
// MAGIC * create a DataFrame from the RDD,
// MAGIC * extract the schema, and
// MAGIC * print it in a code-friendly, cut-and-paste-ready form.

// COMMAND ----------

import org.apache.spark.sql.types._

// Let's craft a simple function to print the schema in code-friendly form.
// Don't worry if this doesn't make sense. It's not really relevant to streaming.
def prettySchema(schema: StructType, indentation: Int = 2): String = {
  def prettyStruct(st: StructType, indentationLevel: Int): String = {
    val indentSpaces = " " * (indentationLevel * indentation)
    val prefix = s"${indentSpaces}StructType(List(\n"
    val fieldIndentSpaces = " " * ((indentationLevel + 1) * indentation)
    val fieldStrings: Seq[String] = for (field <- st.fields) yield {
      val fieldPrefix = s"""${fieldIndentSpaces}StructField("${field.name}", """

      val fieldType = field.dataType match {
        case st2: StructType =>  s"${prettyStruct(st2, indentationLevel + 1)}"
        case _               =>  s"${field.dataType}"
      }
      
      s"$fieldPrefix$fieldType, ${field.nullable})"
    }
    val fields = fieldStrings.mkString(",\n")
    s"$prefix$fields\n$indentSpaces))"
  }
  
  prettyStruct(schema, 0)
}

val inferredSchema = spark.read.json(sc.parallelize(Array(jsonString))).schema
println(prettySchema(inferredSchema))

// COMMAND ----------

// MAGIC %md
// MAGIC That looks pretty good. However, if the record we read has no geocoding (which is likely), then it won't be quite right. If the record has not been geocoded, then `geocoding.latitude` and
// MAGIC `geocoding.longitude` will be null, and Spark will infer that they must be strings. They aren't. We can cut and paste the printed schema from the previous cell and tweak it, if that's the case, to get
// MAGIC the correct schema. (We've already done that for you here.)

// COMMAND ----------

val schema = StructType(List(
  StructField("channel", StringType, true),
  StructField("comment", StringType, true),
  StructField("delta", IntegerType, true),
  StructField("flag", StringType, true),
  StructField("geocoding", StructType(List(
    StructField("city", StringType, true),
    StructField("country", StringType, true),
    StructField("countryCode2", StringType, true),
    StructField("countryCode3", StringType, true),
    StructField("stateProvince", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true)
  )), true),
  StructField("isAnonymous", BooleanType, true),
  StructField("isNewPage", BooleanType, true),
  StructField("isRobot", BooleanType, true),
  StructField("isUnpatrolled", BooleanType, true),
  StructField("namespace", StringType, true),
  StructField("page", StringType, true),
  StructField("pageURL", StringType, true),
  StructField("timestamp", StringType, true),
  StructField("url", StringType, true),
  StructField("user", StringType, true),
  StructField("userURL", StringType, true),
  StructField("wikipediaURL", StringType, true),
  StructField("wikipedia", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### What _are_ these fields, anyway?
// MAGIC
// MAGIC We'll be keeping some of the fields and discarding others. Here's a description of the various fields.
// MAGIC
// MAGIC
// MAGIC #### The fields we're keeping:
// MAGIC
// MAGIC Here are the fields we're keeping from the incoming data:
// MAGIC
// MAGIC * `geocoding` (`OBJECT`): Added by the server, this field contains IP address geocoding information for anonymous edits.
// MAGIC   We're keeping some of it.
// MAGIC * `isAnonymous` (`BOOLEAN`): Whether or not the change was made by an anonymous user.
// MAGIC * `namespace` (`STRING`): The page's namespace. See <https://en.wikipedia.org/wiki/Wikipedia:Namespace> Some examples:
// MAGIC     * "special": This is a special page (i.e., its name begins with "Special:")
// MAGIC     * "user": the page is a user's home page.
// MAGIC     * "talk": a discussion page about a particular real page
// MAGIC     * "category": a page about a category
// MAGIC     * "file": a page about an uploaded file
// MAGIC     * "media": a page about some kind of media
// MAGIC     * "template": a template page
// MAGIC * `pageURL` (`STRING`): The URL of the page that was edited.
// MAGIC * `page`: (`STRING`): The printable name of the page that was edited
// MAGIC * `timestamp` (`STRING`): The time the edit occurred, in [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) format
// MAGIC * `user` (`STRING`): The user who made the edit or, if the edit is anonymous, the IP address associated with the anonymous editor.
// MAGIC * `wikipedia` (`STRING`): The short name of the Wikipedia that was edited (e.g., "en" for the English Wikipedia, "es" for the Spanish Wikipedia, etc.).
// MAGIC
// MAGIC #### The fields we're ignoring:
// MAGIC
// MAGIC * `channel` (`STRING`): The Wikipedia IRC channel, e.g., "#en.wikipedia"
// MAGIC * `comment` (`STRING`): The comment associated with the change (i.e., the commit message).
// MAGIC * `delta` (`INT`): The number of lines changes, deleted, and/or added.
// MAGIC * `flag`: Various flags about the edit.
// MAGIC     * "m" means the user marked the edit as "minor".
// MAGIC     * "N" means the page is new.
// MAGIC     * "b" means the page was edited by a 'bot.
// MAGIC     * "!" means the page is unpatrolled. (See below.)
// MAGIC * `isNewPage` (`BOOLEAN`): Whether or not the edit created a new page.
// MAGIC * `isRobot` (`BOOLEAN`): Whether the edit was made by a robot (`true`) or a human (`false`).
// MAGIC * `isUnpatrolled` (`BOOLEAN`): Whether or not the article is patrolled. See <https://en.wikipedia.org/wiki/Wikipedia:New_pages_patrol/Unpatrolled_articles>
// MAGIC * `url` (`STRING`): The URL of the edit diff.
// MAGIC * `userUrl` (`STRING`): The Wikipedia profile page of the user, if the edit is not anonymous.
// MAGIC * `wikipediaURL` (`STRING`): The URL of the Wikipedia edition containing the article.

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have the schema, we can parse the JSON in the `value` column. We'll produce a new column called `json`.

// COMMAND ----------

val jsonEdits = editsDF.select($"timestamp", from_json($"value", schema).as("json"))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the resulting schema.

// COMMAND ----------

jsonEdits.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC We can now access the data we want using column names like "json.wikipedia" and "json.user". But, we don't need all the columns; we're only keeping some of them.
// MAGIC Let's select only the ones we want. We might as well give them simpler names, while we're at it.
// MAGIC
// MAGIC Also note the use of the `unix_timestamp()` function. This function uses a Java [SimpleDateFormat](http://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html)
// MAGIC date string to parse a string to a numeric timestamp representing the number of seconds since January 1, 1970. We can then cast that value to a timestamp.

// COMMAND ----------

val editsFinalDF = jsonEdits
  .select($"json.wikipedia".as("wikipedia"),
          $"json.isAnonymous".as("isAnonymous"),
          $"json.namespace".as("namespace"),
          $"json.page".as("page"),
          $"json.pageURL".as("pageURL"),
          $"json.geocoding".as("geocoding"),
          unix_timestamp($"json.timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").as("timestamp"),
          $"json.user".as("user"))

// COMMAND ----------

// MAGIC %md
// MAGIC Our schema is now much simpler.

// COMMAND ----------

editsFinalDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Mapping Anonymous Edits
// MAGIC
// MAGIC Okay, let's figure out who's editing the English Wikipedia anonymously. Anonymous edits are tagged with the user's IP address, rather than a login name. The server automatically geocodes such records, and we can use that geocoding information to figure out the countries associated with the editors.
// MAGIC
// MAGIC We're going to start the stream and buffer it somewhere. Then, we'll do batch analysis on it. There are several places we can write our incoming stream data, and we _have_ to write it somewhere. The typical pattern for starting the stream is:
// MAGIC
// MAGIC `editsFinalDF.writeStream.format("...").start()`
// MAGIC
// MAGIC There are several supported formats, or _output sinks_, as discussed in the [Output Sinks](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks)
// MAGIC section of the [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html):
// MAGIC
// MAGIC * File
// MAGIC * Console (i.e., dump to the screen), which is used primarily for debugging.
// MAGIC * An in-memory "table", also used primarily for debugging.
// MAGIC * A "foreach sink", which allows you to write your own custom output sink by extending a `ForeachWriter` class.
// MAGIC
// MAGIC We want to dump the data somewhere and do some analysis on it.
// MAGIC
// MAGIC We could write to Parquet, but it's difficult to pick up changes to Parquet files. For instance, suppose we have the following situation:
// MAGIC
// MAGIC `val df = spark.read.parquet("/path/to/our/parquet/file")`
// MAGIC
// MAGIC At the point where we create `df`, Spark queries the file to obtain metadata—the schema, primarily. But since the metadata also contains the
// MAGIC number of records in the Parquet file, Spark reads that, too. As a result, it will never see additional data appended to the Parquet file,
// MAGIC unless we recreate the `df` variable.
// MAGIC
// MAGIC So, we'll append the data to an in-memory table, for now.
// MAGIC
// MAGIC ### Filtering the data
// MAGIC
// MAGIC First, let's extract only the anonymous edits, and ones where the namespace is `article` (we're not interested in edits to discussion pages or users' home pages). We'll discard records for which geocoding failed.

// COMMAND ----------

val anonDF = editsFinalDF
  .filter($"namespace" === "article")
  .filter($"isAnonymous" === true)
  .filter(! isnull($"geocoding.countryCode3"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC ### Visualizing Anonymous Editors' Locations
// MAGIC
// MAGIC ![World Map](http://i.imgur.com/66KVZZ3.png)
// MAGIC
// MAGIC We can aggregate the anonymous edits by country, over a window, to see who's editing the English Wikipedia over time. Even better, we can use Databricks' built-in world map to plot those edits on a map, to see where the English Wikipedia is being edited. The map wants a 3-letter ISO country code; fortunately, we have one handy.
// MAGIC
// MAGIC We'll aggregate the the counts across a tumbling 1-second window, but we'll grab a snapshot of the current window every half-second. We'll see the graph update in real time.
// MAGIC
// MAGIC <img src="http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/structured-streaming-world-graph-plot-options-1.png" style="float: right; height: 400px"/>
// MAGIC
// MAGIC Once the display starts updating, change it to use the world graph, and set the plot options as shown on the right.
// MAGIC
// MAGIC
// MAGIC **NOTE**: It'll take awhile for each display cycle to update.

// COMMAND ----------

display(
  anonDF
    .groupBy( window($"timestamp", "1 second", "500 milliseconds"), 
              $"geocoding.countryCode3")
    .count()
    .select($"countryCode3", $"count")
)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/labs.png) Structured Streaming Lab
// MAGIC It's time to put what we learned to practice.
// MAGIC
// MAGIC Go ahead and open the notebook [Structured Streaming 4 - Lab]($./Structured Streaming 4 - Lab) and complete the exercises.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
