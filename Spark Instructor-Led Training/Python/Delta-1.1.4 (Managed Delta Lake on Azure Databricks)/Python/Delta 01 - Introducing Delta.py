# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Course Overview and Setup
# MAGIC
# MAGIC Databricks&reg; provides an Apache Spark&trade; as-a-service workspace environment, making it easy to manage clusters and explore data interactively.
# MAGIC
# MAGIC ## Databricks Delta
# MAGIC
# MAGIC Databricks&reg; Delta is a transactional storage layer designed specifically to harness the power of Apache Spark and Databricks DBFS. The core abstraction of Databricks Delta is an optimized Spark table that stores your data as Parquet files in DBFS and maintains a transaction log that efficiently tracks changes to the table.
# MAGIC
# MAGIC ** The course is composed of the following lessons:**  
# MAGIC 1. Course Overview and Setup
# MAGIC 2. Create Table
# MAGIC 3. Append Table
# MAGIC 4. Upsert Table
# MAGIC 5. Streaming 
# MAGIC 6. Optimization
# MAGIC 4. Databricks Delta Architecture
# MAGIC 5. Capstone Project 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## In this lesson you:
# MAGIC * Learn about when to use Databricks Delta
# MAGIC * Log into Databricks
# MAGIC * Create a notebook inside your home folder in Databricks
# MAGIC * Create, or attach to, a Spark cluster
# MAGIC * Import the course files into your home folder
# MAGIC * <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure to go through the lessons in order because they build upon each other
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Secondary Audience: Data Analysts, and Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/q6wgvu9noh?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/q6wgvu9noh?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # The Challenge with Data Lakes
# MAGIC ### Or, it's not a Data Lake, it's a Data CESSPOOL
# MAGIC
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/cesspool.jpg" style="height: 200px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC Image: iStock/Alexmia
# MAGIC
# MAGIC A <b>Data Lake</b>: 
# MAGIC * Is a storage repository that inexpensively stores a vast amount of raw data in its native format.
# MAGIC * Consists of current and historical data dumps in various formats including XML, JSON, CSV, Parquet, etc.
# MAGIC * May contain operational relational databases with live transactional data.
# MAGIC * In effect, it's a dumping ground of amorphous data.
# MAGIC
# MAGIC To extract meaningful information out of a Data Lake, we need to resolve problems like:
# MAGIC * Schema enforcement when new tables are introduced 
# MAGIC * Table repairs when any new data is inserted into the data lake
# MAGIC * Frequent refreshes of metadata 
# MAGIC * Bottlenecks of small file sizes for distributed computations
# MAGIC * Difficulty re-sorting data by an index (i.e. userID) if data is spread across many files and partitioned by i.e. eventTime

# COMMAND ----------

# MAGIC %md
# MAGIC # The Solution: Databricks Delta
# MAGIC
# MAGIC Databricks Delta is a unified data management system that brings reliability and performance (10-100x faster than Spark on Parquet) to cloud data lakes.  Delta's core abstraction is a Spark table with built-in reliability and performance optimizations.
# MAGIC
# MAGIC You can read and write data stored in Databricks Delta using the same familiar Apache Spark SQL batch and streaming APIs you use to work with Hive tables or DBFS directories. Databricks Delta provides the following functionality:<br><br>
# MAGIC
# MAGIC * <b>ACID transactions</b> - Multiple writers can simultaneously modify a data set and see consistent views.
# MAGIC * <b>DELETES/UPDATES/UPSERTS</b> - Writers can modify a data set without interfering with jobs reading the data set.
# MAGIC * <b>Automatic file management</b> - Data access speeds up by organizing data into large files that can be read efficiently.
# MAGIC * <b>Statistics and data skipping</b> - Reads are 10-100x faster when statistics are tracked about the data in each file, allowing Delta to avoid reading irrelevant information.

# COMMAND ----------

# MAGIC %md
# MAGIC # Up and Running with Databricks
# MAGIC
# MAGIC Before we continue with Databricks Delta, a little digression on setting up your Databricks account. 
# MAGIC
# MAGIC You may wish to skip this section if you already have Databricks up and running.
# MAGIC
# MAGIC Create a notebook and Spark cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** This step requires you to navigate Databricks while doing this lesson.  We recommend you <a href="" target="_blank">open a second browser window</a> when navigating Databricks to view these instructions in one window while navigating in the other.
# MAGIC
# MAGIC ### Step 1
# MAGIC Databricks notebooks are backed by clusters, or networked computers that work together to process your data. Create a Spark cluster (*if you already have a running cluster, skip to **Step 2** *):
# MAGIC 1. In your new window, click the **Clusters** button in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-4.png" style="height: 200px"/></div><br/>
# MAGIC 2. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 3. Name your cluster. Use your name or initials to easily differentiate your cluster from your coworkers.
# MAGIC 4. Select the cluster type. We recommend the latest runtime and Scala **2.11**.
# MAGIC 5. Specify your cluster configuration.
# MAGIC   * For clusters created on a **Community Edition** shard the default values are sufficient for the remaining fields.
# MAGIC   * For all other environments, refer to your company's policy on creating and using clusters.</br></br>
# MAGIC 6. Right click on **Cluster** button on left side and open a new tab. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-2.png" style="height: 300px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Check with your local system administrator to see if there is a recommended default cluster at your company to use for the rest of the class. This could save you some money!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC Create a new notebook in your home folder:
# MAGIC 1. Click the **Home** button in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/home.png" style="height: 200px"/></div><br/>
# MAGIC 2. Right-click on your home folder.
# MAGIC 3. Select **Create**.
# MAGIC 4. Select **Notebook**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-notebook-1.png" style="height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 5. Name your notebook `First Notebook`.<br/>
# MAGIC 6. Set the language to **Python**.<br/>
# MAGIC 7. Select the cluster to which to attach this notebook.  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If a cluster is not currently running, this option will not exist.
# MAGIC 8. Click **Create**.
# MAGIC <div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2b.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="float:left">&nbsp;&nbsp;&nbsp;or&nbsp;&nbsp;&nbsp;</div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="clear:both"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC
# MAGIC Now that you have a notebook, use it to run code.
# MAGIC 1. In the first cell of your notebook, type `1 + 1`. 
# MAGIC 2. Run the cell, click the run icon and select **Run Cell**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also run a cell by typing **Ctrl-Enter**.

# COMMAND ----------

1 + 1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Attach and Run
# MAGIC
# MAGIC If your notebook was not previously attached to a cluster you might receive the following prompt: 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-2.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC
# MAGIC If you click **Attach and Run**, first make sure you attach to the correct cluster.
# MAGIC
# MAGIC If it is not the correct cluster, click **Cancel** and follow the steps in the the next cell, **Attach & Detach**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Attach & Detach
# MAGIC
# MAGIC If your notebook is detached you can attach it to another cluster:  
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <br/>
# MAGIC If your notebook is attached to a cluster you can:
# MAGIC * Detach your notebook from the cluster
# MAGIC * Restart the cluster
# MAGIC * Attach to another cluster
# MAGIC * Open the Spark UI
# MAGIC * View the Driver's log files
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/detach-from-cluster.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC * To create a notebook click the down arrow on a folder and select **Create Notebook**.
# MAGIC * To import notebooks click the down arrow on a folder and select **Import**.
# MAGIC * To attach to a spark cluster select **Attached/Detached**, directly below the notebook title.
# MAGIC * Create clusters using the **Clusters** button on the left sidebar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC
# MAGIC **Question:** What is Databricks Delta?<br>
# MAGIC **Answer:** Databricks Delta is a mechanism of effectively managing the flow of data (<b>data pipeline</b>) to and from a <b>Data Lake</b>.
# MAGIC
# MAGIC **Question:** What are some of the pain points of existing data pipelines?<br>
# MAGIC **Answer:** 
# MAGIC * Introduction of new tables requires schema creation 
# MAGIC * Whenever any new data is inserted into the data lake, table repairs are required
# MAGIC * Metadata must be frequently refreshed
# MAGIC * Small file sizes become a bottleneck for distributed computations
# MAGIC * If data is sorted by, say,  `eventTime`, it can be computationally expensive to sort the data by a different column, say, `userID`
# MAGIC
# MAGIC **Question:** How do you create a notebook?  
# MAGIC **Answer:** Sign into Databricks, select the **Home** icon from the sidebar, right-click your home-folder, select **Create**, and then **Notebook**. In the **Create Notebook** dialog, specify the name of your notebook and the default programming language.
# MAGIC
# MAGIC **Question:** How do you create a cluster?  
# MAGIC **Answer:** Select the **Clusters** icon on the sidebar, click the **Create Cluster** button, specify the specific settings for your cluster and then click **Create Cluster**.
# MAGIC
# MAGIC **Question:** How do you attach a notebook to a cluster?  
# MAGIC **Answer:** If you run a command while detached, you may be prompted to connect to a cluster. To connect to a specific cluster, open the cluster menu by clicking the **Attached/Detached** menu item and then selecting your desired cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Next Steps
# MAGIC
# MAGIC This course is available in Python and Scala.  Start the next lesson, **02-Create**.
# MAGIC 1. Click the **Home** icon in the left sidebar
# MAGIC 2. Select your home folder
# MAGIC 3. Select the folder **Delta-Version #**
# MAGIC 4. Open the notebook **02-Create** in either the Python or Scala folder
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/Delta/course-import-2.png" style="margin-bottom: 5px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; width: auto; height: auto; max-height: 350px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Additional Topics & Resources
# MAGIC **Q:** Where can I find documentation on Databricks Delta?  
# MAGIC **A:** See <a href="https://docs.databricks.com/delta/delta-intro.html#what-is-databricks-delta" target="_blank">Databricks Delta Guide</a>.
# MAGIC
# MAGIC **Q:** Are there additional docs I can reference to find my way around Databricks?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>.
# MAGIC
# MAGIC **Q:** Where can I learn more about the cluster configuration options?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Spark Clusters on Databricks</a>.
# MAGIC
# MAGIC **Q:** Can I import formats other than .dbc files?  
# MAGIC **A:** Yes, see <a href="https://docs.databricks.com/user-guide/notebooks/index.html#importing-notebooks" target="_blank">Importing notebooks</a>.
# MAGIC
# MAGIC **Q:** Can I use browsers other than Chrome or Firefox?  
# MAGIC **A:** Databricks is tested for Chrome and Firefox.  It does work on Internet Explorer 11 and Safari however, it is possible some user-interface features may not work properly.
# MAGIC
# MAGIC **Q:** Can I install the courseware notebooks into a non-Databricks distribution of Spark?  
# MAGIC **A:** No, the files that contain the courseware are in a Databricks specific format (DBC).
# MAGIC
# MAGIC **Q:** Do I have to have a paid Databricks subscription to complete this course?  
# MAGIC **A:** No, you can sign up for a free <a href="https://databricks.com/try-databricks" target="_blank">Community Edition</a> account from Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
