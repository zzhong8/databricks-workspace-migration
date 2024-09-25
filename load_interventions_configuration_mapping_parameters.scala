// Databricks notebook source
// MAGIC %md 
// MAGIC # This code is to load  SQL server tables for RSV NARS Load 

// COMMAND ----------

dbutils.widgets.text("N_PREVIOUS_DAYS", "100", "N_PREVIOUS_DAYS")
dbutils.widgets.text("adlsname", "", "adlsname")


// COMMAND ----------

val adlsname=dbutils.widgets.get("adlsname")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Repair source NARS tables before start of the process 

// COMMAND ----------

// %sql
// msck repair table nars_raw.tag;
// msck repair table nars_raw.objtag;
// msck repair table nars_raw.dpch;
// msck repair table nars_raw.aich;
// msck repair table nars_raw.aiai;

// COMMAND ----------

// MAGIC %md
// MAGIC ##  Define time format 

// COMMAND ----------

import java.text.SimpleDateFormat
import java.util.Calendar 
 /**
* Get current timestamp in format yyyy-MM-dd HH:mm:ss.SSS zzz
*
* @return Current timestamp in format yyyy-MM-dd HH:mm:ss.SSS zzz
*/

def CurrentTimestampWithTZ(): String = {
  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS zzz")
  timestampFormat.format(Calendar.getInstance().getTime)
}

def Log(msg: String): Unit = {
  println(f"${CurrentTimestampWithTZ}:- ${msg}")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Create JDBC SQL Server External tables

// COMMAND ----------

val SCOPE = "key-vault-secrets"
val SQL_SERVER_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
 val SQL_SERVER_JDBC_HOST_NAME = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-server-jdbc-host-name")
 val SQL_SERVER_JDBC_PORT = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-server-jdbc-port")
 val SQL_SERVER_JDBC_USER = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-server-jdbc-user")
 val SQL_SERVER_JDBC_PASSWORD = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-server-jdbc-password")
 val SQL_SERVER_DB_NAME = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-server-jdbc-db-name")

val SQL_SERVER_JDBC_URL = s"jdbc:sqlserver://${SQL_SERVER_JDBC_HOST_NAME}:${SQL_SERVER_JDBC_PORT};database=${SQL_SERVER_DB_NAME}"

val GO_LIVE_BATCH_TS_OPPORTUNITES = "20200701000000"
val GO_LIVE_BATCH_TS_DLA = "20200701000000"
val CURRENT_TS = spark.sql("SELECT cast(CURRENT_TIMESTAMP as timestamp) as ts").rdd.map(r => r(0).toString).collect.toList.head

val OBJECTIVE_GO_LIVE_MAP: Map[String, String] = Map("DLA" -> GO_LIVE_BATCH_TS_DLA, "Opportunity" -> GO_LIVE_BATCH_TS_OPPORTUNITES)

val SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM = "interventions_retailer_client_config"
val SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM = "interventions_response_mapping"
val SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM = "interventions_parameters"

val N_PREVIOUS_DAYS = dbutils.widgets.get("N_PREVIOUS_DAYS")

// COMMAND ----------

// MAGIC %md
// MAGIC # Define DDLs for Intervention SQL Server tables

// COMMAND ----------

// DDL for Retailer Client Configuration table

val SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_DDL =
  """|IF OBJECT_ID (N'dbo.interventions_retailer_client_config') IS NULL
     |CREATE TABLE interventions_retailer_client_config
     |(
     |    mdm_country_id bigint NOT NULL,
     |    mdm_country_nm VARCHAR(200) NOT NULL,
     |    mdm_holding_id bigint NOT NULL,
     |    mdm_holding_nm VARCHAR(200) NOT NULL,
     |    mdm_banner_id bigint NULL,
     |    mdm_banner_nm VARCHAR(200) NULL,
     |    mdm_client_id bigint NOT NULL,
     |    mdm_client_nm VARCHAR(200) NOT NULL,
     |    active_flg BIT NOT NULL,
     |    epos_datavault_db_nm VARCHAR(200) NOT NULL,
     |    alertgen_im_db_nm VARCHAR(200) NOT NULL,
     |    audit_create_ts DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
     |    audit_update_ts DATETIME2 DEFAULT NULL
     |)
     |""".stripMargin

// DDL for Intervention Mapping table

val SQL_SERVER_INTERVENTION_MAPPING_TABLE_DDL =
  """|IF OBJECT_ID (N'dbo.interventions_response_mapping') IS NULL
     |CREATE TABLE interventions_response_mapping
     |(
     |    mdm_country_id bigint NOT NULL, 
     |    mdm_country_nm VARCHAR(200) NOT NULL, 
     |    objective_typ VARCHAR(200) NOT NULL, 
     |    standard_response_cd VARCHAR(200) DEFAULT NULL, 
     |    standard_response_text VARCHAR(5000) DEFAULT NULL, 
     |    nars_response_text VARCHAR(5000) NOT NULL,
     |    audit_create_ts DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
     |    audit_update_ts DATETIME2 DEFAULT NULL
     |)
     |""".stripMargin

// DDL for Intervention Parameter table

val SQL_SERVER_INTERVENTION_PAREMETER_TABLE_DDL =
  """|IF OBJECT_ID (N'dbo.interventions_parameters') IS NULL
     |CREATE TABLE interventions_parameters
     |(
     |    mdm_country_id bigint NOT NULL, 
     |    mdm_country_nm VARCHAR(200) NOT NULL, 
     |    mdm_holding_id bigint NOT NULL, 
     |    mdm_holding_nm VARCHAR(200) NOT NULL, 
     |    mdm_banner_id bigint NULL, 
     |    mdm_banner_nm VARCHAR(200) NULL, 
     |    mdm_client_id bigint NOT NULL, 
     |    mdm_client_nm VARCHAR(200) NOT NULL, 
     |    objective_typ VARCHAR(200) NOT NULL,
     |    standard_response_cd VARCHAR(200) DEFAULT NULL, 
     |    standard_response_text VARCHAR(5000) DEFAULT NULL, 
     |    intervention_rank INT NULL,
     |    intervention_group VARCHAR(1000) NULL,
     |    intervention_start_day INT NULL,
     |    intervention_end_day INT NULL,
     |    actionable_flg BIT NULL,
     |    audit_create_ts DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
     |    audit_update_ts DATETIME2 DEFAULT NULL
     |)
     |""".stripMargin

// A dictionary mapping table to corresponding DDL
val SQL_SERVER_TABLE_TO_DDL_MAP = Map(SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM -> SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_DDL,
                                      SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM -> SQL_SERVER_INTERVENTION_MAPPING_TABLE_DDL,
                                      SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM -> SQL_SERVER_INTERVENTION_PAREMETER_TABLE_DDL)

// COMMAND ----------

// MAGIC %md
// MAGIC # Create Intervention SQL Server tables if they do not exist already

// COMMAND ----------

import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement

def createInterventionSQLServerTables(): Unit = {
  var connection:java.sql.Connection = null
  
  // Create DDL for each table
  for((table, ddl) <- SQL_SERVER_TABLE_TO_DDL_MAP){
    Log(f"Start creating SQL Server table '${table}' if does not exist already")

    try{

      connection = DriverManager.getConnection(SQL_SERVER_JDBC_URL, SQL_SERVER_JDBC_USER, SQL_SERVER_JDBC_PASSWORD)
      var statement: java.sql.Statement = connection.createStatement

      Log("Running DDL: \n" + ddl)
      var resultSet = statement.execute(ddl)
      Log(f"DONE creating SQL Server table '${table}'\n\n")
      
    }
    catch {
        case e: Throwable => e.printStackTrace
        connection.close()
    } 
  }//For Loop Ends
  
  connection.close()
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Take backup of the SQL Server Tables on ADLS

// COMMAND ----------

def takeBackupInterventionSQLServerTables(): Unit = {
  val tables: List[String] = List(SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM, SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM, SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM)
  val baseBackupPath = f"abfss://data@${adlsname}.dfs.core.windows.net/informationmart/acosta_retail_report/intervention_conf_sql_server_backup"

  for(table <- tables){
    Log(f"Start taking backup of table ${table} at location ${baseBackupPath}/${table}")

    spark.sql(f"select * from acosta_retail_analytics_im.${table}")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "\t")
      .option("header", "true")
      .csv(f"${baseBackupPath}/${table}")
    
    
    Log(f"DONE taking backup of table ${table} at location ${baseBackupPath}/${table}")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Initialize Intervention Retailer Client Configuration table if it is empty
// MAGIC With the list of holdings, retailers and clients known at golive in 2020 July

// COMMAND ----------

def initSQLServerRetailerClientConfigurationTable(): Unit = {

  var connection: java.sql.Connection = null

  // Prepare Insert SQL Query
  val sql =
    """|INSERT INTO dbo.interventions_retailer_client_config
       |VALUES
       |(30,'uk',3257,'AcostaRetailUK',7743,'Asda',16343,'GeneralMillsUK',1,'asda_generalmills_uk_dv','asda_generalmills_uk_retail_alert_im', CURRENT_TIMESTAMP, NULL),
       |(30,'uk',3257,'AcostaRetailUK',7744,'Morrisons',16343,'GeneralMillsUK',1,'morrisons_generalmills_uk_dv','morrisons_generalmills_uk_retail_alert_im', CURRENT_TIMESTAMP, NULL),
       |(30,'uk',3257,'AcostaRetailUK',7745,'Sainsburys',16343,'GeneralMillsUK',1,'sainsburys_generalmills_uk_dv','sainsburys_generalmills_uk_retail_alert_im', CURRENT_TIMESTAMP, NULL),
       |(30,'uk',3257,'AcostaRetailUK',7746,'Tesco',16343,'GeneralMillsUK',1,'tesco_generalmills_uk_dv','tesco_generalmills_uk_retail_alert_im', CURRENT_TIMESTAMP, NULL),
       |(1,'us',71,'Wal-Mart',null,null,13429,'NestleWatersN.A',1,'walmart_nestlewaters_us_dv','walmart_nestlewaters_us_retail_alert_im', CURRENT_TIMESTAMP, NULL),
       |(1,'us',71,'Wal-Mart',null,null,15599,'Snyder''s-Lance,Inc',1,'walmart_campbellssnack_us_dv','walmart_campbellssnack_us_retail_alert_im', CURRENT_TIMESTAMP, NULL),
       |(1,'us',91,'KrogerCo.',null,null,13054,'NestleCoffeePartners',1,'kroger_starbucks_us_dv','kroger_starbucks_us_retail_alert_im', CURRENT_TIMESTAMP, NULL),
       |(1,'us',91,'KrogerCo.',null,null,882,'DanoneU.S.LLC',1,'kroger_danonewave_us_dv','kroger_danonewave_us_retail_alert_im', CURRENT_TIMESTAMP, NULL);
       |""".stripMargin

  // Execute Insert SQL Query
  try {

    val connection = DriverManager.getConnection(SQL_SERVER_JDBC_URL, SQL_SERVER_JDBC_USER, SQL_SERVER_JDBC_PASSWORD)
    var statement: java.sql.Statement = connection.createStatement

    // Get count from the table
    var count = 0
    val countSql = f"select count(*) as cnt from ${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM}"
    Log("Running Query: ${countSql}")
    val rs = statement.executeQuery(countSql)
    while (rs.next()) {
      count = rs.getInt(1)
    }
    Log(f"Number of records in table '${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM}' = ${count}")
    
    if(count == 0){
      Log("Warning: No records found in table '${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM}', making an initial entry now...")
      Log("Running Query: \n" + sql)
      var resultSet = statement.executeUpdate(sql)
      Log("Output: " + resultSet)
      Log(f"DONE Running Query")      
    }
    else{
      Log(f"Table '${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM}' contains more than 0 records, no action required to initialize")
    }

    // Close the connection
    connection.close()
  }
  catch {
    case e: Throwable => e.printStackTrace
      connection.close()
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Create External tables in Spark over JDBC to connect to SQL Server

// COMMAND ----------

def createExternalSQLServerTables(): Unit = {
  
  // Get list of all the Intervention SQL Server Tables that we need to create
  val ivTableList = List(SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM, SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM, SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM)

  // Loop through all the tables, DROP and CREATE them
  for(table <- ivTableList){
    Log(f"Preparing DDL for Table '${table}'")
    
    // Prepare DDL
    val ddl = f"""
    CREATE TABLE acosta_retail_analytics_im.${table}
    USING org.apache.spark.sql.jdbc
    OPTIONS (
      url "${SQL_SERVER_JDBC_URL}",
      dbtable "${table}",
      user "${SQL_SERVER_JDBC_USER}",
      password "${SQL_SERVER_JDBC_PASSWORD}"
    )"""

    Log(f"Executing DDL for Table '${table}'")
    Log(f"DROP TABLE IF EXISTS ${table}")
    Log(ddl)
    
    // Execute DDL
    spark.sql(f"DROP TABLE IF EXISTS acosta_retail_analytics_im.${table}")
    spark.sql(ddl)
    
    Log(f"Executed DDL and Created Table '${table}'\n\n\n")
  }  
  
  // IF the backup directory does not exist, then take the backup
  // This is more likely only for the first run.
  
  val cnt = dbutils.fs.ls(f"abfss://data@${adlsname}.dfs.core.windows.net/informationmart/acosta_retail_report/").map(_.name).filter(_ == "intervention_conf_sql_server_backup/").size
  if(cnt == 0){
    Log("No Backup directory found, taking the backup now")
    takeBackupInterventionSQLServerTables
  }
}



// COMMAND ----------

// MAGIC %md
// MAGIC # Create Audit Last Processed Batch TS table

// COMMAND ----------

def createAuditLastProcessedBatchTsTable(): Unit = {
  val ddl =
    f"""create table if not exists acosta_retail_analytics_im.audit_interventions_last_processed_batch_ts 
       |(
       |  table_name string,
       |  batch_id string,
       |  batch_ts bigint,
       |  audit_create_ts timestamp,
       |  audit_update_ts timestamp
       |)
       |using delta
       |location 'abfss://data@${adlsname}.dfs.core.windows.net/informationmart/acosta_retail_report/audit_interventions_last_processed_batch_ts' 
  """.stripMargin
  Log(f"Running SQL: ${ddl}")
  spark.sql(ddl)
  Log("DONE")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Set Last Processed Batch details for the given table

// COMMAND ----------

def setAuditLastProcessedTS(table: String, batchId: String, batchTs: Long): Unit = {

  // Prepare Merge Query
  val sql =
    f"""merge into acosta_retail_analytics_im.audit_interventions_last_processed_batch_ts as TGT
       |using
       |(
       | select '${table}' as table_name, '${batchId}' as batch_id, '${batchTs}' as batch_ts
       |) as SRC
       |on
       |TGT.table_name = SRC.table_name
       |when matched then
       |update set batch_id = SRC.batch_id, batch_ts = SRC.batch_ts, TGT.audit_update_ts = CURRENT_TIMESTAMP
       |when not matched then
       |INSERT (table_name, batch_id, batch_ts, audit_create_ts, audit_update_ts) values(SRC.table_name, SRC.batch_id, SRC.batch_ts, current_timestamp, NULl)""".stripMargin

  // Set SparkSQL config to avoid an error/warning
  spark.sql("set spark.sql.crossJoin.enabled=true")

  // Run the query
  Log(f"Running Query: ${sql}")
  spark.sql(sql)

  // Show the output for reference --DB name added
  val sqlCount = f"select * from acosta_retail_analytics_im.audit_interventions_last_processed_batch_ts where table_name = '${table}'"
  Log(f"Running Query to get counts: ${sqlCount}")
  spark.sql(sqlCount).collect.foreach(println)
  Log("DONE")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Get Last Processed Batch details for the given table

// COMMAND ----------

def getAuditLastProcessedTS(table: String): (String, Long) = {
  // Prepare Query
  val sql =
    f"""select
      |batch_id, 
      |batch_ts 
      |from 
      |acosta_retail_analytics_im.audit_interventions_last_processed_batch_ts
      |where table_name = '${table}'
  """.stripMargin
  Log(f"Running Query: ${sql}")
  val df = spark.sql(sql)

  // If there is no record found then make a dummy entry with July 01, 2020 Batch TS
  // The reason is that all the Engage clients for Value Measureement are onboarded in July 2020.
  if (df.count == 0) {
    Log(f"Warning: No entry found for the table '${table} in Audit table 'audit_interventions_last_processed_batch_ts', makding a dummy entry'")
    setAuditLastProcessedTS(table, batchId = "dummy", batchTs = 20200701000000L)
  }

  spark.sql(sql).rdd.map(r => (r(0).toString, r(1).toString.toLong)).collect.toList.head

}

// COMMAND ----------

// MAGIC %md
// MAGIC # Check if history load required for the mapping table

// COMMAND ----------

def checkIfHistoryPullRequired(objectiveType: String): Long = {
  
  /**
  ** Part 1 - History Management: When the given Objective Type does not exist in the Mapping table
  **/
  // If there is no record found for the given obective in the SQL Server Intervention Mapping table
  // then, set the Min Batch TS to the GO live date of the objective
  
  var minBatchTs_part1 = -1L
  Log(f"Get number of records from table '${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM}'")
  //DB NAME ADDED
  val countSQL = f"SELECT COUNT(*) FROM acosta_retail_analytics_im.${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM}  WHERE OBJECTIVE_TYP IN (${objectiveType})"
  Log(f"Running SQL: ${countSQL}")
  var count = spark.sql(countSQL).rdd.map(r => r(0).toString.toLong).collect.toList.head
  
  // History Pull Required: Return Value = GO_LIVE_BATCH_TS_OPPORTUNITES
  if (count == 0) {
    val t = OBJECTIVE_GO_LIVE_MAP(objectiveType.replace("'", "")).toLong
    Log(f"Count is 0, hence fetching data since Batch TS = '${t}' like a History Load(Part 1)")
    minBatchTs_part1 = OBJECTIVE_GO_LIVE_MAP(objectiveType.replace("'", "")).toLong
  }
  
  // History Pull NOT Required: Return Value = -1
  else {
    Log(f"No Objective History load required(Part 1)")
  }

  /**
  ** Part 2 - History Management: When the given Objective Type has new Retailer-Client configuration found compared to last run
  **/
  
  val pathInterventionConfigBackup = f"abfss://data@${adlsname}.dfs.core.windows.net/informationmart/acosta_retail_report/intervention_conf_sql_server_backup/${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM}"

  val dfBackup = spark
          .read
          .option("delimiter", "\t")
          .option("header", "true")
          .format("csv")
          .load(pathInterventionConfigBackup)
          .createOrReplaceTempView(f"tmpvw_backup_${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM}")

  val sql = f"""
  select
    mdm_country_id,
    mdm_holding_id,
    coalesce(mdm_banner_id, -1),
    mdm_client_id
  from acosta_retail_analytics_im.interventions_retailer_client_config
  where active_flg = 1
  minus
  select
    mdm_country_id,
    mdm_holding_id,
    coalesce(mdm_banner_id, -1),
    mdm_client_id
  from tmpvw_backup_interventions_retailer_client_config
  where active_flg =true
  """
  val df = spark.sql(sql).cache
  // Get the new configuration values to print in the log


  // Get the count of new configuration values
  val count1 = df.count

  // If the count is more than 0 then set the minBatchTs value to current date - N days(based on the config)
  var minBatchTs_part2 = -1L
  if(count1 > 0){
  
    Log(f"Seems there are new configuration entries compared to the last run(Part 2)")

    minBatchTs_part2 = spark.sql(f"select cast(concat(replace(date_add(current_date, -${N_PREVIOUS_DAYS}), '-', ''), '000000') as bigint)").rdd.map(r=>r(0).toString.toLong).collect.toList.head

    // If N previous months is less than GO Live DLA date then set Min Batch TS to GO Live DLA date
    if(OBJECTIVE_GO_LIVE_MAP(objectiveType.replace("'", "")).toLong > minBatchTs_part2){
      minBatchTs_part2 = OBJECTIVE_GO_LIVE_MAP(objectiveType.replace("'", "")).toLong
    }
    Log(f"Now Min Batch Ts would be: ${minBatchTs_part2}")
  }
  else{
    Log(f"No new configuration entries found compared to the last run, hence no history load required(Part 2)")
  }

  // Find the min batch out of part 1 and part 2
  var minBatchTs = -1L
  
  // Part 1 history load is required. The minBatchTs_part1 would be the GO Live of Respective Objective
  if(minBatchTs_part1 != -1){
    minBatchTs = minBatchTs_part1

    // Return
    minBatchTs
  }
  // When Part 1 history load is NOT required
  else{
    // If Part 2 history load is required, The minBatchTs_part1 would be the pars N day
    if(minBatchTs_part2 != -1){
      // Return
      minBatchTs = minBatchTs_part2
    }
  }
  minBatchTs
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Get Min-Max values for Batch ID and Batch TS for the given table
// MAGIC Here Min Batch ID and Batch TS values would be used to find the incremental data, the Max values would be used to update back to the Audit Last Processed table to be ready for the next run/cycle

// COMMAND ----------

def getMinMaxBatchIdAndTs(table: String, objectiveType: String): (String, Long, String, Long) = {

  Log(f"Getting Min-Max BatchID and Batch TS for table '${table}'")


  // Get last processed batch details for given table from audit table = audit_interventions_last_processed_batch_ts
  Log(f"Get last processed batch details for table ${table}")
  val (lastBatchId, lastBatchTs) = getAuditLastProcessedTS(table)
  Log(f"DONE Getting last processed batch details for table ${table}")

  // Get the Minimum and Maximum batch that has arrived after the last processed batch
  // Minimum BatchTs is used to get the new/incremental answers from the given table
  // and the Maximum BatchTs is used to update the audit table at the end of the successful run
  // as a checkpoint for the next run/cycle

  val df = spark.sql(
    f"""SELECT
       |    MIN(BATCH_ID), 
       |    MIN(BATCH_TS), 
       |    MAX(BATCH_ID), 
       |    MAX(BATCH_TS),
       |    count(*)
       |FROM
       |    nars_raw.AICH 
       |WHERE
       |    BATCH_TS > ${lastBatchTs} -- Get the incremental data since the last process
       |HAVING
       |    count(*)> 0""").cache

  // If no new batch found then skip the process
  if (df.count == 0) {
    Log(f"No More batches to process after last processed Batch TS = '${lastBatchTs}' for ${table}, skipping the process now...")
    return ("", -2, "", -2)
  }
  else{
    // Read required fields from the dataframe
    val (minBatchId, minBatchTs, maxBatchId, maxBatchTs) = df.rdd.map(r=> (r(0).toString, r(1).toString, r(2).toString, r(3).toString)).collect.toList.head
    
    // Logging output
    Log(f"Table = '${table}\nMin Batch ID = ${minBatchId},\nMin Batch TS = ${minBatchTs},\nMax Batch ID = ${maxBatchId},\nMax Batch TS = ${maxBatchTs}")
    //Return
    (minBatchId, minBatchTs.toLong, maxBatchId, maxBatchTs.toLong)   
  }

}

// COMMAND ----------

def getMinMaxBatchIdAndTsP360(table: String, objectiveType: String): (String, Long, String, Long) = {

  Log(f"Getting Min-Max BatchID and Batch TS for table '${table}'")


  // Get last processed batch details for given table from audit table = audit_interventions_last_processed_batch_ts
  Log(f"Get last processed batch details for table ${table}")
  val (lastBatchId, lastBatchTs) = getAuditLastProcessedTS(table)
  Log(f"DONE Getting last processed batch details for table ${table}")

  // Get the Minimum and Maximum batch that has arrived after the last processed batch
  // Minimum BatchTs is used to get the new/incremental answers from the given table
  // and the Maximum BatchTs is used to update the audit table at the end of the successful run
  // as a checkpoint for the next run/cycle

  val df = spark.sql(
    f"""SELECT
       |    MIN(BATCH_ID), 
       |    SUBSTRING(MIN(BATCH_ID),1,14) as min_batch_ts , 
       |    MAX(BATCH_ID), 
       |    SUBSTRING(MAX(BATCH_ID),1,14) as max_batch_ts ,
       |    count(*)
       |FROM
       |   premium360_analytics_im.vw_fact_answer_alerts
       |WHERE
       |    BATCH_ID > "${lastBatchId}" -- Get the incremental data since the last process
       |HAVING
       |    count(*)> 0""").cache

    val (minBatchId, minBatchTs, maxBatchId, maxBatchTs) = df.rdd.map(r=> (r(0).toString, r(1).toString, r(2).toString, r(3).toString)).collect.toList.head
    
    // Logging output
    Log(f"Table = '${table}\nMin Batch ID = ${minBatchId},\nMin Batch TS = ${minBatchTs},\nMax Batch ID = ${maxBatchId},\nMax Batch TS = ${maxBatchTs}")
    //Return
    (minBatchId, minBatchTs.toLong, maxBatchId, maxBatchTs.toLong)   
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Helper method to load the Intervention Mapping table for the given objective type view

// COMMAND ----------

def loadInterventionMappingTable(sourceView: String): Unit = {
  // Prepare SQL and load new responses to mapping table --DB NAME ADDED
  val mappingInsertSQL =
    f"""
       |insert into acosta_retail_analytics_im.${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM}
       |  select distinct
       |    mdm_country_id,
       |    mdm_country_nm,
       |    objective_typ,
       |    NULL as standard_response_cd,
       |    NULL as standard_response_text,
       |    nars_response_text,
       |    audit_create_ts,
       |    audit_update_ts
       |  from ${sourceView} src
       |  where
       |    not exists
       |    (
       |      select 1 from acosta_retail_analytics_im.${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM} tgt
       |      where
       |        tgt.mdm_country_id = src.mdm_country_id and
       |        tgt.mdm_country_nm = src.mdm_country_nm and
       |        tgt.objective_typ = src.objective_typ and
       |        tgt.nars_response_text = src.nars_response_text
       |    )
       |""".stripMargin
  Log(f"Start loading table '${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM}' from source view = '${sourceView}'")
  Log(f"Running Query: ${mappingInsertSQL}")
  spark.sql(mappingInsertSQL)
  Log(f"DONE loading table '${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM}' from source view = '${sourceView}'")

}

// COMMAND ----------

// MAGIC %md
// MAGIC # Helper method to load the Intervention Parameter table for the given objective type view

// COMMAND ----------

def loadInterventionParameterTable(objectiveType: String , currentTs: String): Unit = {
  // Prepare SQL and load new responses to parameter table--DB NAME ADDED
  val parameterInsertSQL = f"""
      |insert into acosta_retail_analytics_im.${SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM}
      |SELECT DISTINCT
      |    conf.mdm_country_id as mdm_country_id, 
      |    conf.mdm_country_nm as mdm_country_nm, 
      |    conf.mdm_holding_id, 
      |    conf.mdm_holding_nm, 
      |    conf.mdm_banner_id, 
      |    conf.mdm_banner_nm, 
      |    conf.mdm_client_id, 
      |    conf.mdm_client_nm,
      |    map.objective_typ,
      |    map.standard_response_cd as standard_response_cd,
      |    map.standard_response_text as standard_response_text,
      |    NULL as intervention_rank,
      |    NULL as intervention_group,
      |    NULL as intervention_start_day,
      |    NULL as intervention_end_day,
      |    NULL as actionable_flg,
      |    CAST('${currentTs}' as timestamp) as audit_create_ts,
      |    NULL as audit_update_ts
      |FROM
      |    acosta_retail_analytics_im.${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM} conf,
      |    acosta_retail_analytics_im.${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM} map
      |
      |WHERE
      |conf.active_flg = 1 AND -- Get data for only active clients
      |conf.mdm_country_id = map.mdm_country_id AND
      |map.standard_response_text is NOT NULL AND -- Get only those records where standard response is already assigned
      |map.objective_typ ="${objectiveType}"
      |
      | -- Take off the records that already exist in the target table(de-dup logic)
      |and not exists
      |(
      |      select 1 from acosta_retail_analytics_im.${SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM} tgt
      |      where
      |      tgt.mdm_country_id = conf.mdm_country_id and
      |      tgt.mdm_holding_id = conf.mdm_holding_id and
      |      coalesce(tgt.mdm_banner_id, -1) = coalesce(conf.mdm_banner_id, -1) and
      |      tgt.mdm_client_id  = conf.mdm_client_id and
      |      coalesce(tgt.standard_response_cd, '') = coalesce(map.standard_response_cd, '') and
      |      tgt.objective_typ = "${objectiveType}" and
      |      tgt.standard_response_text = map.standard_response_text
      |)
      |""".stripMargin
  Log(f"Start loading table '${SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM}' for objective type ='${objectiveType}'" )
  Log(f"Running Query: ${parameterInsertSQL}")
  spark.sql(parameterInsertSQL)
  Log(f"DONE loading table '${SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM}' for objective type ='${objectiveType}'")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Audit method to get counts of loaded data and update the Audit Last Processed table

// COMMAND ----------

def audit(srcTable: String, currentTs: String, maxBatchId: String, maxBatchTs: Long, interventionParameterTable: String, lastProcessedUpdateFlg: Boolean): Unit = {
  // Get count of the new Responses
  Log(f"Start getting count of newly inserted records into table '${interventionParameterTable}'")
  //DB NAME ADDED
  val count = spark.sql(f"SELECT COUNT(*) FROM acosta_retail_analytics_im.${interventionParameterTable} where audit_create_ts = '${currentTs}'").rdd.map(r=>r(0).toString.toLong).collect.toList.head
  Log(f"DONE getting count of newly inserted records into table '${interventionParameterTable}', count = ${count}")
  
  Log(f"Loaded new NARS responses to table '${interventionParameterTable}', Load Record Count = '${count}'")
  
  // Update the Batch details to Audit Last processed TS table only if this flag is true
  if(lastProcessedUpdateFlg){
    Log(f"Start updating last process batch details with BatchID = ${maxBatchId} and BatchTS = ${maxBatchTs} for table '${srcTable}'")
    setAuditLastProcessedTS(srcTable, maxBatchId, maxBatchTs.toLong)
    Log(f"DONE updating last process batch details with BatchID = ${maxBatchId} and BatchTS = ${maxBatchTs} for table '${srcTable}'")  
  }
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Load UK Opportunity Objective data to both Intervention Mapping and Parameter tables

// COMMAND ----------

def loadUKOpportunityInterventionData(): Unit = {
  
  Log(f"DONE loading Intervention MappingData: Opportunity")
  
  // Local variables
  val NARS_TAG_UK_OPPORTUNITIES = "Opportunity"
  val tagOpportunitiesFilterList: List[String] = List(NARS_TAG_UK_OPPORTUNITIES)
  val tagOpportunitiesFilterString = tagOpportunitiesFilterList.map(x => "'" + x + "'").mkString(",")
  val CURRENT_TS = spark.sql("SELECT cast(CURRENT_TIMESTAMP as timestamp) as ts").rdd.map(r => r(0).toString).collect.toList.head
  val baseNarsTable = "aich"

  // Get Min, Max Batch ID and Batch TS from base NARS table
  var (minBatchId, minBatchTs, maxBatchId, maxBatchTs) = getMinMaxBatchIdAndTs(baseNarsTable, tagOpportunitiesFilterString) // gettting value from incrremental
     
  val tmpMinBatchId = checkIfHistoryPullRequired(tagOpportunitiesFilterString)

  // If History Pull required then set the minBatchTs to GO_LIVE_BATCH_TS_OPPORTUNITES so that
  // we pull data from beginning/go live
  if (tmpMinBatchId != -1) { // if histiry required then set it to go liove date else moveas is
    minBatchTs = tmpMinBatchId
  }
  
  // Get latest Batch for Tag table as its a full refresh table
  Log("Getting Max Batch ID and TS for TAG table as it is a full refresh table")
  
   if (minBatchTs == -2) {// if no new sales dates are required 
     Log("We don't have new batch to process, so loading paramters table for OPPORTUNITIES  before exiting  the process")
    loadInterventionParameterTable(NARS_TAG_UK_OPPORTUNITIES,CURRENT_TS)
    Log("No new date to process. Skipping the other command")
     return
  }
  else // Processing incremental batches 
  {    
  val qry = f"""
     |SELECT 
     |  MAX(BATCH_ID) AS BATCH_ID, 
     |  MAX(BATCH_TS) 
     |FROM
     |  nars_raw.TAG 
     |WHERE
     |  BATCH_TS >= ${GO_LIVE_BATCH_TS_OPPORTUNITES}
     |""".stripMargin
  Log(f"Running Query: ${qry}")
  val (tagMaxBatchId, tagMaxBatchTs) = spark.sql(qry).rdd.map(r=> (r(0).toString, r(1).toString)).collect.toList.head
  
  Log("DONE Getting Max Batch ID and TS for TAG table as it is a full refresh table")

  // Prepare SQL to identify UK Opportunity Responses-DB NAME ADDED
  val sqlOpportunities =
     f"""|
        |with objective_tag_grp_id as ( 
        |  select
        |    distinct QUESTION_GROUP_ID from nars_raw.AIAI 
        |     join 
        |     (SELECT ot.obj_id, row_number() over (partition by source,obj_id,ot.tag_id  order by ot.batch_ts desc ) rn FROM nars_raw.OBJTAG ot 
        |      JOIN
        | (SELECT  max(tag_id) tag_id from nars_raw.TAG t WHERE t.NAME = '${NARS_TAG_UK_OPPORTUNITIES}'  and t.batch_id = '${tagMaxBatchId}' and t.batch_ts = ${tagMaxBatchTs}
        |  ) t ON ot.TAG_ID = t.TAG_ID 
        |     where ot.batch_ts >= ${GO_LIVE_BATCH_TS_OPPORTUNITES}
        |) OT ON AIAI.QUESTION_ID = OT.OBJ_ID 
        |      AND rn = 1
        |      )
        | select distinct
        |    c.countryid as mdm_country_id,
        |    case when c.countryid = 1 then 'us' when c.countryid = 30 then 'uk' when c.countryid = 2 then 'ca' else null end as mdm_country_nm,
        |    conf.mdm_holding_id,
        |    conf.mdm_holding_nm,
        |    conf.mdm_banner_id,
        |    conf.mdm_banner_nm,
        |    conf.mdm_client_id,
        |    conf.mdm_client_nm,
        |    '${NARS_TAG_UK_OPPORTUNITIES}' as objective_typ,
        |    aich.mchoice as nars_response_text,
        |    '${CURRENT_TS}' as audit_create_ts,
        |    NULL as audit_update_ts
        |from
        |      nars_raw.aich,
        |      mdm_raw.hp_client c,
        |      acosta_retail_analytics_im.${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM} conf
        |where
        |      aich.batch_ts >= ${minBatchTs} and
        |      aich.QUESTION_GROUP_ID IN ( select * from objective_tag_grp_id ) and
        |      aich.client_id = c.clientid and
        |
        |      -- Get data for configured Country and Clients only
        |      conf.mdm_client_id = c.clientid and
        |      conf.mdm_country_id = c.countryid and
        |      conf.active_flg = 1 -- Get data for only active clients
        |""".stripMargin
  Log(f"Running Query: ${sqlOpportunities}")
  spark.sql(sqlOpportunities).cache.createOrReplaceTempView("tmpvw_mapping_uk_opportunities")
  Log("Identified new NARS responses, preparing to insert them to table '${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM}' and '${SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM}'")
  
  // Load SQL Server table - Intervention Mapping
  loadInterventionMappingTable("tmpvw_mapping_uk_opportunities")
  
  // If there were no note batches to process then get the max batch id and ts again from the base table
  if(maxBatchTs == -2){
    
    Log("Seems there are no new batches to process, hence retrieving Max Batch ID ad Batch TS again to be updated in the Audit Last Process Batch table")
    val (tmpMaxBatchId, tmpMaxBatchTs) = spark.sql(f"select max(batch_id), max(batch_ts) from nars_raw.aich where batch_ts >= ${GO_LIVE_BATCH_TS_OPPORTUNITES}").rdd.map(r=>(r(0).toString, r(1).toString.toLong)).collect.toList.head
    maxBatchId = tmpMaxBatchId
    maxBatchTs = tmpMaxBatchTs
    
    Log(f"Found Max Batch ID = ${maxBatchId} and Max Batch Ts = ${maxBatchTs}")
  }  
  
  // Get audit counts and update audit table for next run/cycle
  audit(baseNarsTable, CURRENT_TS, maxBatchId, maxBatchTs, interventionParameterTable=SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM, lastProcessedUpdateFlg=false)
  
  /**************************************************
  * Load SQL Server Intervention Parameter Table
  **************************************************/
  loadInterventionParameterTable(NARS_TAG_UK_OPPORTUNITIES,CURRENT_TS)
  
  // Get audit counts and update audit table for next run/cycle
  audit(baseNarsTable, CURRENT_TS, maxBatchId, maxBatchTs, interventionParameterTable=SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM, lastProcessedUpdateFlg=true)
  
  Log(f"DONE loading Intervention MappingData: Opportunity")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Load DLA Objective data to both Intervention Mapping and Parameter tables

// COMMAND ----------

def loadDLAInterventionData(): Unit = {
  Log("Start loading Intervention MappingData: DLA")

  // Define scalar variables
  val NARS_TAG_DLA = "DLA"
  val p360_TAG_DLA = "Data Led Alerts"
  val tagDLAFilterList: List[String] = List(NARS_TAG_DLA)
  val tagDLAFilterString = tagDLAFilterList.map(x => "'" + x + "'").mkString(",")
  val CURRENT_TS = spark.sql("SELECT cast(CURRENT_TIMESTAMP as timestamp) as ts").rdd.map(r => r(0).toString).collect.toList.head
  val baseNarsTable = "dpch"
  val basep360Table = "vw_fact_answer_alerts"

  //    Get Min, Max Batch ID and Batch TS from base p360 table
  var (minBatchIdP360, minBatchTsP360, maxBatchIdP360, maxBatchTsP360) = getMinMaxBatchIdAndTsP360(basep360Table, tagDLAFilterString) 

  
  // Get Min, Max Batch ID and Batch TS from base NARS table
  var (minBatchId, minBatchTs, maxBatchId, maxBatchTs) = getMinMaxBatchIdAndTs(baseNarsTable, tagDLAFilterString)
  val tmpMinBatchId = checkIfHistoryPullRequired(tagDLAFilterString)

  // If History Pull required then set the minBatchTs to GO_LIVE_BATCH_TS_OPPORTUNITES so that
  // we pull data from beginning/go live
  if (tmpMinBatchId != -1) {
    minBatchTs = tmpMinBatchId
  }
  
  if (minBatchTs == -2) {// if no new sales dates are required 
    Log("We don't have new batch to process, so loading paramters table for DLA  before exiting  the process")
    loadInterventionParameterTable(NARS_TAG_DLA,CURRENT_TS)
    Log("No new date to process. Skipping the other command")
    return
  }
  else // Processing incremental batches 
  { 
  // Prepare SQL to identify UK Opportunity Responses--DB NAME ADDED
  val sqlDLA =
    f"""
      |with p360 as 
      |(Select 
      |shelfcode as epos_retailer_item_id, 
      |q.question_type as objective_typ,
      |call_completed_date as call_date,
      |fa.client_id as client_id,
      |call_id,
      |product_country_id,
      |store_id,
      |response_id,
      |batch_id,
      |response as nars_response_text
      |From premium360_analytics_im.vw_fact_answer_alerts fa
      |Inner join premium360_analytics_im.vw_dimension_question q
      |on fa.question_id = q.question_id
      |where fa.source in ('P360_DP') 
      |and fa.BATCH_ID >="${minBatchIdP360}") ,   
      |store as
      |(
      |select
      |  s.country_code as mdm_country_id,
      |  h.holdingid as mdm_holding_id,
      |  h.description as mdm_holding_nm,
      |  s.banner_id as mdm_banner_id,
      |  s.banner_description as mdm_banner_nm,
      |  s.store_acosta_number as nars_store_id,
      |  customer_store_number
      | 
      |from
      |  mdm_raw.vw_lookup_stores s
      |  left join mdm_raw.hs_division as d on s.division_id = d.divisionid
      |  left join mdm_raw.hs_holding as h on d.holdingid = h.holdingid
      |)
      |select distinct
      |    c.countryid as mdm_country_id,
      |    case when c.countryid = 1 then 'us' when c.countryid = 30 then 'uk' when c.countryid = 2 then 'ca' else null end as mdm_country_nm,
      |    conf.mdm_holding_id,
      |    conf.mdm_holding_nm,
      |    conf.mdm_banner_id,
      |    conf.mdm_banner_nm,
      |    conf.mdm_client_id,
      |    conf.mdm_client_nm,
      |    p360.store_id as acosta_store_number,
      |    p360.objective_typ as objective_typ,
      |    p360.nars_response_text as nars_response_text,
      |    '${CURRENT_TS}' as audit_create_ts,
      |    NULL as audit_update_ts
      |from
      |      p360,
      |      mdm_raw.hp_client c,
      |      acosta_retail_analytics_im.interventions_retailer_client_config conf,
      |      store
      |
      |where
      |      p360.objective_typ in ("${p360_TAG_DLA}") and
      |      p360.client_id = c.clientid and
      |
      |    -- Get data for configured Country and Clients only
      |      conf.mdm_client_id = c.clientid and
      |      conf.mdm_country_id = c.countryid and
      |      conf.active_flg = 1 and -- Get data for only active clients
      |
      |    -- Below conditions will make sure we get only those holdings and banners for which p360 MCHOICE are set up
      |      conf.mdm_country_id = store.mdm_country_id and
      |      p360.store_id = store.nars_store_id and 
      |      conf.mdm_holding_id = store.mdm_holding_id 
      |         
      |      union 
      |select distinct
      |    c.countryid as mdm_country_id,
      |    case when c.countryid = 1 then 'us' when c.countryid = 30 then 'uk' when c.countryid = 2 then 'ca' else null end as mdm_country_nm,
      |    conf.mdm_holding_id,
      |    conf.mdm_holding_nm,
      |    conf.mdm_banner_id,
      |    conf.mdm_banner_nm,
      |    conf.mdm_client_id,
      |    conf.mdm_client_nm,
      |    dpch.store_id as acosta_store_number,
      |    dpch.type as objective_typ,
      |    dpch.mchoice as nars_response_text,
      |    '${CURRENT_TS}' as audit_create_ts,
      |    NULL as audit_update_ts
      |from
      |      nars_raw.dpch,
      |      mdm_raw.hp_client c,
      |      acosta_retail_analytics_im.${SQL_SERVER_RETAILER_CLIENT_CONFIG_TABLE_NM} conf,
      |      store
      |
      |where
      |      dpch.batch_ts >= ${minBatchTs} and
      |      dpch.type in (${tagDLAFilterString}) and
      |      dpch.client_id = c.clientid and
      |
      |    -- Get data for configured Country and Clients only
      |      conf.mdm_client_id = c.clientid and
      |      conf.mdm_country_id = c.countryid and
      |      conf.active_flg = 1 and -- Get data for only active clients
      |
      |    -- Below conditions will make sure we get only those holdings and banners for which DPCH MCHOICE are set up
      |      conf.mdm_country_id = store.mdm_country_id and
      |      dpch.store_id = store.nars_store_id and 
      |      conf.mdm_holding_id = store.mdm_holding_id
      |""".stripMargin
  Log(f"Running Query: ${sqlDLA}")
  spark.sql(sqlDLA).cache.createOrReplaceTempView("tmpvw_mapping_dla")
  Log("Identified new NARS responses, preparing to insert them to tableS '${SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM}' and '${SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM}'")
  
  /**************************************************
  * Load SQL Server Intervention Mapping Table
  **************************************************/
  loadInterventionMappingTable("tmpvw_mapping_dla")
    
  if(maxBatchTs == -2){
    Log("Seems there are no new batches to process, hence retrieving Max Batch ID ad Batch TS again to be updated in the Audit Last Process Batch table")
    val (tmpMaxBatchId, tmpMaxBatchTs) = spark.sql(f"select max(batch_id), max(batch_ts) from nars_raw.aich where batch_ts >= ${GO_LIVE_BATCH_TS_OPPORTUNITES}").rdd.map(r=>(r(0).toString, r(1).toString.toLong)).collect.toList.head
    maxBatchId = tmpMaxBatchId
    maxBatchTs = tmpMaxBatchTs
    
    Log(f"Found Max Batch ID = ${maxBatchId} and Max Batch Ts = ${maxBatchTs}")
  }  
  
  
  // Get audit counts and update audit table for next run/cycle
  audit(baseNarsTable, CURRENT_TS, maxBatchId, maxBatchTs, interventionParameterTable=SQL_SERVER_INTERVENTION_MAPPING_TABLE_NM, lastProcessedUpdateFlg=false)
  
  /**************************************************
  * Load SQL Server Intervention Parameter Table
  **************************************************/
  loadInterventionParameterTable(NARS_TAG_DLA,CURRENT_TS)
  loadInterventionParameterTable(p360_TAG_DLA,CURRENT_TS)
  
  // Get audit counts and update audit table for next run/cycle
  audit(baseNarsTable, CURRENT_TS, maxBatchId, maxBatchTs, interventionParameterTable=SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM, lastProcessedUpdateFlg=true)
    
    
          
  // Get audit counts and update audit table for next run/cycle
  audit(basep360Table, CURRENT_TS, maxBatchIdP360, maxBatchTsP360, interventionParameterTable=SQL_SERVER_INTERVENTION_PAREMETER_TABLE_NM, lastProcessedUpdateFlg=true)
  
  
  Log("Start loading Intervention MappingData: DLA")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Main Section Starts Here

// COMMAND ----------

// MAGIC %md
// MAGIC ###Temporary Work around for Gen2 deployment
// MAGIC #### Please update to work with SPARK3.0 if modifying this notebook and remove this dependency.

// COMMAND ----------

spark.sql("SET spark.sql.storeAssignmentPolicy = Legacy")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Create Intervention SQL Server tables if they do not exist already

// COMMAND ----------

createInterventionSQLServerTables

// COMMAND ----------

// MAGIC %md
// MAGIC # Initialize Retailer Client Configuration Table if its empty

// COMMAND ----------

initSQLServerRetailerClientConfigurationTable

// COMMAND ----------

// MAGIC %md
// MAGIC ### Create Audit Last Processed Batch table if not exists

// COMMAND ----------

createAuditLastProcessedBatchTsTable

// COMMAND ----------

// MAGIC %md
// MAGIC ### Create External SQL Server tables for Intervention Configurations tables if not exist

// COMMAND ----------

createExternalSQLServerTables

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Opportunity NARS Responses to interventions_response_mapping

// COMMAND ----------

loadUKOpportunityInterventionData

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load DLA NARS Responses to interventions_response_mapping

// COMMAND ----------

loadDLAInterventionData

// COMMAND ----------

// MAGIC %md
// MAGIC # Get the backup of all the Intervention SQL Server tables after the processing is done

// COMMAND ----------

takeBackupInterventionSQLServerTables
