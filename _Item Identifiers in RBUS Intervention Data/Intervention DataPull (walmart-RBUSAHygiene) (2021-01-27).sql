-- Databricks notebook source
-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC sql = "select * from walmart_rbusahygiene_us_retail_alert_im.vw_gc_ds_input_rsv_measurement where InterventionDate like '2020-06%'"
-- MAGIC
-- MAGIC df_IV = spark.sql(sql)
-- MAGIC df_IV.cache().createOrReplaceTempView("adv_analytics_IV")

-- COMMAND ----------

select * from adv_analytics_IV

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC #THIS NEEDS TO BE CUSTOMIZED TO ACTUALLY PULL DATA
-- MAGIC
-- MAGIC sql = "SELECT \
-- MAGIC           \"walmart_rbusahygiene_us\" AS RETAIL_CLIENT \
-- MAGIC           , 950 AS ParentChainId \
-- MAGIC           , s.SALES_DT \
-- MAGIC           , s.HUB_ORGANIZATION_UNIT_HK \
-- MAGIC           , s.HUB_RETAILER_ITEM_HK \
-- MAGIC           , s.POS_ITEM_QTY \
-- MAGIC           , s.POS_AMT \
-- MAGIC           , s.ON_HAND_INVENTORY_QTY \
-- MAGIC           , ri.RETAILER_ITEM_ID \
-- MAGIC           , ou.ORGANIZATION_UNIT_NUM \
-- MAGIC           , d.BASELINE_POS_ITEM_QTY \
-- MAGIC         FROM walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary s \
-- MAGIC         INNER JOIN walmart_rbusahygiene_us_dv.hub_retailer_item ri \
-- MAGIC           ON s.HUB_RETAILER_ITEM_HK = ri.HUB_RETAILER_ITEM_HK \
-- MAGIC         INNER JOIN walmart_rbusahygiene_us_dv.hub_organization_unit ou \
-- MAGIC           ON s.HUB_ORGANIZATION_UNIT_HK = ou.HUB_ORGANIZATION_UNIT_HK \
-- MAGIC         LEFT JOIN walmart_rbusahygiene_us_retail_alert_im.vw_drfe_forecast_baseline_unit d \
-- MAGIC         /* This could be replaced with expected baseline table */ \
-- MAGIC           ON s.SALES_DT = d.SALES_DT \
-- MAGIC           AND s.HUB_ORGANIZATION_UNIT_HK = d.HUB_ORGANIZATION_UNIT_HK \
-- MAGIC           AND s.HUB_RETAILER_ITEM_HK = d.HUB_RETAILER_ITEM_HK"
-- MAGIC
-- MAGIC dfEPOS = spark.sql(sql)
-- MAGIC dfEPOS.cache().createOrReplaceTempView("adv_analytics_IV_epos_sales")

-- COMMAND ----------

select * from adv_analytics_IV_epos_sales

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ivs_with_epos
AS SELECT 
          iv.CallfileVisitId
          , iv.ParentChainName
          , iv.ChainName
          , iv.ChainRefExternal
          , iv.RefExternal
          , iv.InterventionId
          , iv.InterventionDate
          , iv.InterventionGroup
          , iv.Intervention
          , iv.DiffStart
          , iv.DiffEnd
          , iv.Duration
          , s.SALES_DT
          , s.POS_ITEM_QTY
          , s.BASELINE_POS_ITEM_QTY
        FROM adv_analytics_IV iv
        LEFT JOIN adv_analytics_IV_epos_sales s
          ON iv.ChainRefExternal = s.ORGANIZATION_UNIT_NUM
          AND iv.RefExternal = s.RETAILER_ITEM_ID
        WHERE
          (DATE_ADD(CAST(iv.InterventionDate AS DATE), CAST(SUBSTRING(iv.DiffStart, 9, 2) AS INTEGER)) <= s.SALES_DT
          AND DATE_ADD(CAST(iv.InterventionDate AS DATE), CAST(SUBSTRING(iv.DiffEnd, 9, 2) AS INTEGER)) >= s.SALES_DT)
          OR (CAST(iv.InterventionDate AS DATE) = SALES_DT)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC df_ivs_with_epos = spark.sql('select * from ivs_with_epos')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_ivs_with_epos.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_ivs_with_epos.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_ivs_with_epos_head = df_ivs_with_epos.head(100)
-- MAGIC
-- MAGIC display(df_ivs_with_epos_head)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC
-- MAGIC df_ivs_with_epos.write.mode('overwrite').format('delta').save('/mnt/processed/temp/rbusahygiene_walmart_inv_epos_2020-06')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # PATHS (new!)
-- MAGIC PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/rbusahygiene-interventions-2020-06'
-- MAGIC
-- MAGIC df_ivs_with_epos.coalesce(1)\
-- MAGIC     .write.format('com.databricks.spark.csv')\
-- MAGIC     .option('header', 'true')\
-- MAGIC     .mode('overwrite')\
-- MAGIC     .save('{}'.format(PATH_RESULTS_OUTPUT))  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Hello world")

-- COMMAND ----------

select * from ivs_with_epos limit 50

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # PATHS (new!)
-- MAGIC PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/rbusahygiene-interventions-2020-07'
-- MAGIC
-- MAGIC df_iv_filtered.coalesce(1)\
-- MAGIC     .write.format('com.databricks.spark.csv')\
-- MAGIC     .option('header', 'true')\
-- MAGIC     .mode('overwrite')\
-- MAGIC     .save('{}'.format(PATH_RESULTS_OUTPUT))  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # df_iv = spark.sql('select ParentChainName, Intervention, InterventionDate, InterventionGroup from walmart_rbusahygiene_us_retail_alert_im.vw_gc_ds_input_rsv_measurement where InterventionDate')
-- MAGIC
-- MAGIC # display(df_iv)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # from pyspark.sql import functions as pyf
-- MAGIC
-- MAGIC # print(df_iv.dtypes)
-- MAGIC
-- MAGIC # df_iv_filtered = df_iv.where((pyf.col("InterventionDate") < pyf.lit("2019-07-01")) & (pyf.col("InterventionDate") >= pyf.lit("2018-07-01")))
-- MAGIC
-- MAGIC # print(df_iv_filtered.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # display(df_iv_filtered)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # df_iv_groups = df_iv_filtered.groupBy('ParentChainName', 'InterventionGroup').agg(pyf.count("*").alias('intervention_count')).orderBy(["ParentChainName", "InterventionGroup"], ascending=True)
-- MAGIC
-- MAGIC # display(df_iv_groups)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # # PATHS (new!)
-- MAGIC # PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/nestlecereals-interventions-jul-18-to-jun-19'
-- MAGIC
-- MAGIC # df_iv_filtered.coalesce(1)\
-- MAGIC #     .write.format('com.databricks.spark.csv')\
-- MAGIC #     .option('header', 'true')\
-- MAGIC #     .mode('overwrite')\
-- MAGIC #     .save('{}'.format(PATH_RESULTS_OUTPUT))  
-- MAGIC
-- MAGIC # # PATHS (new!)
-- MAGIC # PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/nestlecereals-interventions-by-retailer-and-group'
-- MAGIC
-- MAGIC # df_iv_groups.coalesce(1)\
-- MAGIC #     .write.format('com.databricks.spark.csv')\
-- MAGIC #     .option('header', 'true')\
-- MAGIC #     .mode('overwrite')\
-- MAGIC #     .save('{}'.format(PATH_RESULTS_OUTPUT))  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC df_iv = spark.sql('select * from adv_analytics_IV where ParentChainName = \'Walmart - Parent\'')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(df_iv.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # print(df_iv.count())
-- MAGIC
-- MAGIC df_iv.write.mode('overwrite').format('delta').save('/mnt/processed/temp/intervention_data-rbusahygiene-walmart')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_iv = spark.read.format('delta').load('/mnt/processed/temp/intervention_data-rbusahygiene-walmart')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC retailer_item_ids_df_iv = df_iv.select("RefExternal").distinct()
-- MAGIC
-- MAGIC retailer_item_ids_df_iv.count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Collect SalesData for Callfile Interventions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC #THIS NEEDS TO BE CUSTOMIZED TO ACTUALLY PULL DATA
-- MAGIC
-- MAGIC sql = "SELECT \
-- MAGIC           \"walmart_rbusahygiene_us\" AS RETAIL_CLIENT \
-- MAGIC           , 950 AS ParentChainId \
-- MAGIC           , s.SALES_DT \
-- MAGIC           , s.HUB_ORGANIZATION_UNIT_HK \
-- MAGIC           , s.HUB_RETAILER_ITEM_HK \
-- MAGIC           , s.POS_ITEM_QTY \
-- MAGIC           , s.POS_AMT \
-- MAGIC           , s.ON_HAND_INVENTORY_QTY \
-- MAGIC           , ri.RETAILER_ITEM_ID \
-- MAGIC           , ou.ORGANIZATION_UNIT_NUM \
-- MAGIC           , d.BASELINE_POS_ITEM_QTY \
-- MAGIC         FROM walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary s \
-- MAGIC         INNER JOIN walmart_rbusahygiene_us_dv.hub_retailer_item ri \
-- MAGIC           ON s.HUB_RETAILER_ITEM_HK = ri.HUB_RETAILER_ITEM_HK \
-- MAGIC         INNER JOIN walmart_rbusahygiene_us_dv.hub_organization_unit ou \
-- MAGIC           ON s.HUB_ORGANIZATION_UNIT_HK = ou.HUB_ORGANIZATION_UNIT_HK \
-- MAGIC         LEFT JOIN walmart_rbusahygiene_us_retail_alert_im.vw_drfe_forecast_baseline_unit d \
-- MAGIC         /* This could be replaced with expected baseline table */ \
-- MAGIC           ON s.SALES_DT = d.SALES_DT \
-- MAGIC           AND s.HUB_ORGANIZATION_UNIT_HK = d.HUB_ORGANIZATION_UNIT_HK \
-- MAGIC           AND s.HUB_RETAILER_ITEM_HK = d.HUB_RETAILER_ITEM_HK"
-- MAGIC
-- MAGIC dfEPOS = spark.sql(sql)
-- MAGIC dfEPOS.cache().createOrReplaceTempView("adv_analytics_IV_epos_sales")

-- COMMAND ----------

select * from adv_analytics_IV_epos_sales limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Interventions Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #don't worry about this table it's garbage.
-- MAGIC
-- MAGIC #sql = "DROP TABLE IF EXISTS " + Team_Name + "_" + ctry_cd + "_team_alerts_im.tmp_team_roi_interventions";
-- MAGIC #spark.sql(sql);
-- MAGIC
-- MAGIC sql = "SELECT \
-- MAGIC           iv.CallfileId \
-- MAGIC           , iv.InterventionCount \
-- MAGIC           , iv.Campaign \
-- MAGIC           , iv.PeriodName \
-- MAGIC           , iv.ParentChainName \
-- MAGIC           , iv.ChainName \
-- MAGIC           , iv.UserId \
-- MAGIC           , iv.Username \
-- MAGIC           , iv.ChainRefExternal \
-- MAGIC           , iv.OutletId \
-- MAGIC           , iv.OutletName \
-- MAGIC           , iv.ProductBrand \
-- MAGIC           , iv.ProductId \
-- MAGIC           , iv.ProductName \
-- MAGIC           , iv.InterventionId \
-- MAGIC           , iv.InterventionDate \
-- MAGIC           , iv.InterventionNo \
-- MAGIC           , iv.InterventionGroup \
-- MAGIC           , iv.Intervention \
-- MAGIC           , iv.InterventionCount AS InternvetionTotal \
-- MAGIC           , iv.InterventionCost \
-- MAGIC           , CONCAT(CAST(\"Diff Day \" AS VARCHAR(10)), CAST(iv.DayStart AS VARCHAR(10))) AS DiffStart \
-- MAGIC           , CONCAT(CAST(\"Diff Day \" AS VARCHAR(10)), CAST(iv.DayEnd AS VARCHAR(10))) AS DiffEnd \
-- MAGIC           , ABS(iv.DayEnd - iv.DayStart) + 1 Duration \
-- MAGIC           , s.SALES_DT \
-- MAGIC           , s.POS_ITEM_QTY \
-- MAGIC           , s.BASELINE_POS_ITEM_QTY \
-- MAGIC           , s.UNIT_PRICE_AMT \
-- MAGIC         FROM adv_analytics_IV iv \
-- MAGIC         LEFT JOIN adv_analytics_IV_epos_sales s \ 
-- MAGIC           ON iv.ChainRefExternal = s.ORGANIZATION_UNIT_NUM \
-- MAGIC           AND iv.RefExternal = s.RETAILER_ITEM_ID \
-- MAGIC         WHERE \
-- MAGIC           DATE_ADD(CAST(iv.InterventionDate AS DATE), DayStart) <= s.SALES_DT \
-- MAGIC           AND DATE_ADD(CAST(iv.InterventionDate AS DATE), DayEnd) >= s.SALES_DT";
-- MAGIC
-- MAGIC dfIVdata = spark.sql(sql);
-- MAGIC
-- MAGIC #WE NEED TO REPLICATE THE TEMP TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Below query pulls IVs attached to the epos

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME (BELOW)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC df_ivs_with_epos = spark.sql('select * from ivs_with_epos')
-- MAGIC
-- MAGIC df_ivs_with_epos.write.mode('overwrite').format('delta').save('/mnt/processed/temp/rbusahygiene_walmart_inv_epos')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_epos = spark.read.format('delta').load('/mnt/processed/training/data-walmart-rbusahygiene-us-2020-06-17/data_vault')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC retailer_item_ids_epos = df_epos.select("RETAILER_ITEM_ID").distinct()
-- MAGIC
-- MAGIC retailer_item_ids_epos.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_ivs_with_epos = spark.read.format('delta').load('/mnt/processed/temp/rbusahygiene_walmart_inv_epos')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC retailer_item_ids = df_ivs_with_epos.select("RefExternal").distinct()
-- MAGIC
-- MAGIC retailer_item_ids.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_ivs_with_epos.select("SALES_DT").distinct())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Below query fails to pull ePOS with IV's attached
-- MAGIC This doesn't work yet. Why? It's doing two things at once so it can only get the interventions days due to the where clause. We need to use the above table joined to POS data instead.

-- COMMAND ----------

SELECT 
          iv.CallfileId
          , iv.InterventionCount
          , iv.Campaign
          , iv.PeriodName
          , iv.ParentChainName
          , iv.ChainName
          , iv.UserId
          , iv.Username
          , iv.ChainRefExternal
          , iv.OutletId
          , iv.OutletName
          , iv.ProductBrand
          , iv.ProductId
          , iv.ProductName
          , iv.InterventionId
          , iv.InterventionDate
          , iv.InterventionNo
          , iv.InterventionGroup
          , iv.Intervention
          , iv.InterventionCount AS InterventionTotal
          , iv.InterventionCost
          , CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(iv.DayStart AS VARCHAR(10))) AS DiffStart
          , CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(iv.DayEnd AS VARCHAR(10))) AS DiffEnd
          , ABS(iv.DayEnd - iv.DayStart) + 1 Duration
          , s.SALES_DT
          , s.POS_ITEM_QTY
          , s.BASELINE_POS_ITEM_QTY
        FROM adv_analytics_IV iv
        RIGHT JOIN adv_analytics_IV_epos_sales s
          ON iv.ChainRefExternal = s.ORGANIZATION_UNIT_NUM
          AND iv.RefExternal = s.RETAILER_ITEM_ID
        WHERE
          DATE_ADD(CAST(iv.InterventionDate AS DATE), DayStart) <= s.SALES_DT
          AND DATE_ADD(CAST(iv.InterventionDate AS DATE), DayEnd) >= s.SALES_DT

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sql = "SELECT DISTINCT CallfileId FROM " + Team_Name + "_" + ctry_cd + "_team_alerts_im.tmp_team_roi_interventions";
-- MAGIC # REPLACE THIS WITH A TEMP TABLE OR DF REFERENCE
-- MAGIC dfPartitions = spark.sql(sql);
-- MAGIC
-- MAGIC #for row in dfPartitions.rdd.collect():
-- MAGIC #  sql = "INSERT OVERWRITE TABLE " + Team_Name + "_" + ctry_cd + "_team_alerts_im.team_roi_interventions PARTITION (CallfileId = " + #str(row["CallfileId"]) + ") \
-- MAGIC   SELECT \
-- MAGIC           InterventionCount \
-- MAGIC           , Campaign \
-- MAGIC           , PeriodName \
-- MAGIC           , ParentChainName \
-- MAGIC           , ChainName \
-- MAGIC           , UserId \
-- MAGIC           , Username \
-- MAGIC           , ChainRefExternal \
-- MAGIC           , OutletId \
-- MAGIC           , OutletName \
-- MAGIC           , ProductBrand \
-- MAGIC           , ProductId \
-- MAGIC           , ProductName \
-- MAGIC           , InterventionId \
-- MAGIC           , InterventionDate \
-- MAGIC           , InterventionNo \
-- MAGIC           , InterventionGroup \
-- MAGIC           , Intervention \
-- MAGIC           , InternvetionTotal \
-- MAGIC           , InterventionCost \
-- MAGIC           , DiffStart \
-- MAGIC           , DiffEnd \
-- MAGIC           , Duration \
-- MAGIC           , SALES_DT \
-- MAGIC           , POS_ITEM_QTY \
-- MAGIC           , BASELINE_POS_ITEM_QTY \
-- MAGIC           , UNIT_PRICE_AMT \
-- MAGIC           , current_timestamp \
-- MAGIC         FROM " + Team_Name + "_" + ctry_cd + "_team_alerts_im.tmp_team_roi_interventions \
-- MAGIC         WHERE \
-- MAGIC           CallfileId = " + str(row["CallfileId"]);
-- MAGIC
-- MAGIC   spark.sql(sql);
-- MAGIC   
-- MAGIC   #select from temp table
