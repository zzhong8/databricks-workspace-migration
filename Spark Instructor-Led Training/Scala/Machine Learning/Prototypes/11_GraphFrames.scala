// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #GraphFrames
// MAGIC
// MAGIC **Data Source**
// MAGIC * 2015 FBS college football games
// MAGIC * Type: CSV
// MAGIC
// MAGIC **Technical Accomplishments:**
// MAGIC * Import & Attach Libraries
// MAGIC * Create a GraphFrame
// MAGIC * Explore various algorithms:
// MAGIC   * In & Out Degrees
// MAGIC   * Triangle Count
// MAGIC   * Label Propagation
// MAGIC   * Shortest Paths
// MAGIC   * Page Rank

// COMMAND ----------

// MAGIC %md
// MAGIC In this notebok, we are going to do some graph analysis to predict college football rankings from 2015. 
// MAGIC
// MAGIC The most referenced ranking site for college sports is the [AP Poll](http://collegefootball.ap.org/poll). The AP Poll ranks the top 25 NCAA teams in Division I college football. The rankings from the AP Poll are very subjective, as it relies solely on polling 65 sportswriters and broadcasters from across the nation, and not purely on the data collected from the games. 
// MAGIC
// MAGIC Thus, in this lab we are going to create a more objective way to analyze football and rank football teams. We are going to use the popular GraphFrames library in Spark.
// MAGIC
// MAGIC [GraphFrames Python docs](https://graphframes.github.io/api/python/graphframes.html)  
// MAGIC [GraphFrames Scala docs](https://graphframes.github.io/api/scala/index.html#org.graphframes.package)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) The GraphFrames library
// MAGIC
// MAGIC While maintained as part of the Apache Spark project, the `GraphFrames` library is not bundled with the default distribution of spark.
// MAGIC
// MAGIC Because of that, we will need to first attach the library to our cluster and restart the cluster.
// MAGIC
// MAGIC Before we do, we can see evidence of this by attempting to import the library in the following cell:

// COMMAND ----------

import org.graphframes.GraphFrame

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Import the GraphFrames library
// MAGIC
// MAGIC 1. On the left edge of the window, click the "Workspace" button.  You'll see a file browser appear.  (Note: "Home" goes to your home folder, whereas "Workspace" goes to the most recently used location in the file browser.)  
// MAGIC    <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Click+Workspace+Button.png"></img>
// MAGIC
// MAGIC 2. At the top of the file browser, you'll see "Workspace" with a downward arrow next to it.  Click on the arrow.  
// MAGIC    <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Workspace+down+arrow.png" alt="Screenshot placeholder"></img>
// MAGIC
// MAGIC 3. In the popup menu, select Create -> Library.  You'll then see the "New Library" screen.  
// MAGIC    <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Create-+Library.png" alt="Screenshot placeholder" style="border:1px solid black"></img>
// MAGIC
// MAGIC 4. Install the [Spark GraphFrames library](https://spark-packages.org/package/graphframes/graphframes):
// MAGIC    * **Source**: Maven Coordinate
// MAGIC    * **Coordinate**: `graphframes:graphframes:0.5.0-spark2.1-s_2.11` (NOTE: This assumes your cluster is running Scala 2.11)
// MAGIC    * **Click**: Create Library  
// MAGIC    <img src="" alt="Screenshot placeholder" style="border:1px solid black"></img>
// MAGIC
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/301/GraphFramesLibrary.png" style="height: 400px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Loading the GraphFrames library
// MAGIC
// MAGIC Once imported, you will need to restart the cluster for the libraries to be loaded (distributed to each of the clusters).
// MAGIC
// MAGIC You can verify that the library is loaded by running the following cell:

// COMMAND ----------

import org.graphframes.GraphFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Looking at the data
// MAGIC
// MAGIC Now that we have successfully installed the GraphFrames library and attached it to our cluster, let's start by reading in our dataset. It consists of all of the FBS college football games (including post-season) played in the 2015 season. 
// MAGIC
// MAGIC NOTE: FBS is the highest division of college football, and consists of 128 teams.

// COMMAND ----------

val df = spark.read.option("header", true).csv("dbfs:/mnt/training/301/football_2015.csv")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC The schema of our dataframe:
// MAGIC
// MAGIC - Wk: Week in season
// MAGIC - Date: Date of game
// MAGIC - Time: Time of game
// MAGIC - Day: Day of week
// MAGIC - Winner: Team that won
// MAGIC - Pts_W: Winning team points 
// MAGIC - Location: Home or Away of winning team
// MAGIC - Loser: Team that lost
// MAGIC - Pts_L: Losing team points
// MAGIC - Rank_W: Rank of winning team before game
// MAGIC - Rank_L: Rank of losing team before game
// MAGIC
// MAGIC NOTE: Only the top 25 teams for a given week have a ranking. 

// COMMAND ----------

// MAGIC %md
// MAGIC Let's do some basic analysis of our data. Which teams that started ranked won their first game?
// MAGIC
// MAGIC Steps:
// MAGIC - Filter our dataFrame for games occuring during week 1
// MAGIC - Find the teams with a non-null entry for `Rank_W`
// MAGIC - Select the `Winner` and `Rank_W` columns to get the name of the school along with their associated rank

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
display(df.filter(<FILL_IN>).filter(<FILL_IN>).select(<FILL_IN>))

// COMMAND ----------

// MAGIC %md
// MAGIC Which teams that started ranked lost their first game?

// COMMAND ----------

display(df.filter($"Wk" === 1).filter($"Rank_L".isNotNull).select($"Loser", $"Rank_L"))

// COMMAND ----------

// MAGIC %md
// MAGIC To analyze this data like a graph, we need to convert it into a set of vertices and edges. In this lab, our vertices will be all of the college football teams, and the edges will be the relation between two teams (i.e. W/L).
// MAGIC
// MAGIC To represent the vertices, we need a DataFrame containing all of the (distinct) college football teams. 
// MAGIC
// MAGIC Steps:
// MAGIC
// MAGIC 1) Create a DataFrame containing all of the teams that won games called `dfWin` (HINT: Use `select`)
// MAGIC
// MAGIC 2) Create a DataFrame containing all of the teams that lost games called `dfLose` (HINT: Use `select`)
// MAGIC
// MAGIC 3) Union these two DataFrames, and rename the resulting column `id`. Then call `distinct` to remove duplicates, and assign the result to `vertices`.

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
val dfWin = <FILL_IN>
val dfLose = <FILL_IN>
val vertices = <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC To create the edges, rename the `Loser` and `Winner` columns as `src` and `dst`, respectively. If you want to add any metadata to our edges, you can create a third column (optional).
// MAGIC
// MAGIC Here, we are using the convention that a `src` team lost to the `dst` team (i.e. directed edge from src -> dst).
// MAGIC
// MAGIC
// MAGIC Hints: 
// MAGIC - Either use `col()` to select a literal column name and `alias()` to rename that column OR use a `selectExpr()`

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
import org.apache.spark.sql.functions._

val edges = df.select(<FILL_IN>, <FILL_IN>)

// COMMAND ----------

// MAGIC %md
// MAGIC We are going to initialize our graph using the `vertices` and `edges` dataframes that we just created above. Then, we will cache the graph because we will be referencing it frequently.

// COMMAND ----------

import org.graphframes.GraphFrame

val g = GraphFrame(vertices, edges)
g.cache

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) inDegrees
// MAGIC
// MAGIC We are going to start by looking at the number of `inDegrees` for each football team. 
// MAGIC
// MAGIC The intuition here is that each inDegree represents a Win in our graph, so the total number of inDegrees is equivalent to the total number of wins that team had (includes post-season). 
// MAGIC
// MAGIC We sort by the number of inDegrees in decreasing order to find the teams that won the most number of games.

// COMMAND ----------

display(g.inDegrees.sort($"inDegree".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) outDegrees
// MAGIC
// MAGIC If we want to find the teams that lost the most often, we need to compute its `outDegrees`. 
// MAGIC
// MAGIC The regular college season has 12 games (unless they play Hawaii), so although a team can win more than 12 games due to the post-season, a team cannot lose more than 12 games. We see that Kansas and Central Florida both lost 12 games, thus they lost every single game in the 2015 season.
// MAGIC
// MAGIC **Note**: We cannot simply sort the inDegrees in reverse order, because some teams never won a game.

// COMMAND ----------

display(g.outDegrees.sort($"outDegree".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Calculating Win Percentage
// MAGIC
// MAGIC To find the percentage of wins per school, we need to divide the number of inDegrees (wins) by the number of inDegrees + outDegrees (total number of games). 
// MAGIC
// MAGIC Steps:
// MAGIC - Create a new DataFrame by joining the outDegrees and inDegrees of each school
// MAGIC - Divide the inDegrees (wins) by the sum of inDegrees and outDegrees (total number of games), and rename the column `winPercentage`
// MAGIC - Sort in descending order to find the teams with the highest winning percentage

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
val winLossDF = (<FILL_IN>).join(<FILL_IN>)
val winPercentageDF = winLossDF.selectExpr(<FILL_IN>)

display(winPercentageDF.sort($"winPercentage".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Triangle Count
// MAGIC
// MAGIC We are going to run the TriangleCount algorithm to see how many triangles each team in our graph forms. A triangle forms when team A plays team B, team B plays team C, and team C plays team A. 
// MAGIC
// MAGIC We need to call `distinct` on the result, or else we will get doubles for every team (because each team occurs in the dst and src columns of our graph).

// COMMAND ----------

import org.graphframes.lib._

display(g.triangleCount.run().distinct()) // NOTE: Do not need to call run when using Python

// COMMAND ----------

// MAGIC %md
// MAGIC Look at the resulting triangleCount for each team. Is it interesting that some teams don't form any triangles? Notably, all of the teams that don't form triangles are not playing [FBS football](https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_FBS_football_programs). Thus, they are unlikely to be playing many FBS football teams, therefore not forming any triangles.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Label Propagation
// MAGIC
// MAGIC Based on the structure of the graph, we are going to group schools together using the `labelPropagation` algorithm. Roughly speaking, the label corresponds to the conferences/geographic regions of the teams.
// MAGIC
// MAGIC How label propagation works: Each node in the network is initially assigned to its own community. At every iteration, nodes send their community affiliation to all neighbors and update their state to the mode community affiliation of incoming messages.
// MAGIC
// MAGIC It is very inexpensive computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial solutions (all nodes are identified into a single community).
// MAGIC
// MAGIC You can verify the solution found by label is by looking at the grouping of the West Coast schools (label 0 when using 5 iterations). 
// MAGIC
// MAGIC Be patient - this cell takes roughly 2-3 minutes to run on CE.

// COMMAND ----------

val label_prop = g.labelPropagation.maxIter(5).run()
display(label_prop.distinct.orderBy("label"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Shortest Paths
// MAGIC
// MAGIC We can also compute the shortest paths between one school and another school by running the `shortestPaths` algorithm. `shortestPaths` computes the shortest paths to the given set of landmark vertices, where landmarks are specified by vertex ID.  
// MAGIC
// MAGIC The returned DataFrame contains all the original vertex information as well as one additional column: distances (MapType[vertex ID type, IntegerType]): For each vertex v, a map containing the shortest-path distance to each reachable landmark vertex.
// MAGIC
// MAGIC In this example, we are going to find the shortest paths between your favorite college football team (landmark vertex) and all the other vertices in our graph.
// MAGIC
// MAGIC **NOTE:** This implementation of shortest paths takes edge direction into account. 

// COMMAND ----------

// TODO
val shortest_path = g.shortestPaths.landmarks(Seq("<FILL_IN>")).run()
display(shortest_path)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Page Rank
// MAGIC
// MAGIC Now that we have explored our data and covered a few basic graph algorithms, we are ready to use PageRank to rank our college football teams. 
// MAGIC
// MAGIC The PageRank Algorithm was developed at Google by Larry Page and Sergey Brin, and is how the Google Search Engine orders search results. PageRank measures the importance of each vertex in a graph. The basic idea is that a source is more important if it has lots of incoming links/edges (relative to its number of outgoing links/edges). 
// MAGIC
// MAGIC ![pageRank](https://upload.wikimedia.org/wikipedia/commons/thumb/f/fb/PageRanks-Example.svg/400px-PageRanks-Example.svg.png)
// MAGIC
// MAGIC In this diagram, vertex B has a high PageRank score because it has many incoming links. Vertex C also has a high PageRank score because vertex B is important, and it points to C.
// MAGIC
// MAGIC Run PageRank with `maxIter` set to 8, and set `resetProbability` to 0.1 (the probability of resetting to a random vertex). `resetProbability` helps avoid getting stuck in infinite loops in our graph.
// MAGIC
// MAGIC Python docs: [PageRank](https://graphframes.github.io/api/python/graphframes.html#graphframes.GraphFrame.pageRank)
// MAGIC
// MAGIC Scala docs: [PageRank](https://graphframes.github.io/api/scala/index.html#org.graphframes.lib.PageRank)

// COMMAND ----------

// TODO: Replace <FILL_IN> with appropriate code
val results = g.pageRank.<FILL_IN>.run()
display(results.vertices.distinct().sort($"pagerank".desc).limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's compare how these rankings with the ones from the AP Poll.
// MAGIC
// MAGIC ![football](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/AP_Poll.png)

// COMMAND ----------

// MAGIC %md
// MAGIC We can see that our PageRank algorithm and the AP Poll picked 6/10 of the same teams to be in the top 10. What is interesting is that Ole Miss (Mississippi) is ranked 1st according to PageRank, but 10th according to the AP Poll. Perhaps this is because Ole Miss had beat Alabama (their only loss).
// MAGIC
// MAGIC Alternatively, what if we wanted to find the weakest teams? Then we would reverse the direction of the edges directions reversed, and compute the PageRank of the resulting graph (think of this as inverse PageRank). 

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
