-- Databricks notebook source
-- MAGIC %md
-- MAGIC - Company ID is the **client**, found in table bobv2.Company
-- MAGIC - Chain ID is the **retailer**, found in bobv2.Chain
-- MAGIC     Chain ID = 
-- MAGIC - RefExternal = retailer item id
-- MAGIC - ChainRefExternal = Org Unit Number
-- MAGIC - Intervention = Intervention
-- MAGIC - Intervention Group = Uplift or Recovery (expected to be below baseline)

-- COMMAND ----------

--select * from bobv2.Company

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("CompanyId", "", "CompanyId");
-- MAGIC dbutils.widgets.text("Team_Name", "", "Team_Name");
-- MAGIC dbutils.widgets.text("recordSourceCode", "", "recordSourceCode");
-- MAGIC dbutils.widgets.text("ctry_cd", "", "ctry_cd");

-- COMMAND ----------

select * from bobv2.Campaign where CompanyID = '559'

-- COMMAND ----------

select * from bobv2.Company

-- COMMAND ----------

select * from bobv2.Campaign

-- COMMAND ----------

select * from bobv2.chain

-- COMMAND ----------

select * from bobv2.chain where FullName like '%almart%'

-- COMMAND ----------

select * from bobv2.callfile

-- COMMAND ----------

select * from bobv2.callfile where campaignid = '4429'

-- COMMAND ----------

select * from bobv2.callfile where campaignid = '4567'

-- COMMAND ----------

select * from bobv2.callfile where CampaignId = '5013'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC import datetime
-- MAGIC import pyspark
-- MAGIC from pyspark.sql.types import ArrayType
-- MAGIC from pyspark.sql.types import DateType
-- MAGIC from pyspark.sql.functions import UserDefinedFunction
-- MAGIC from pyspark.sql.functions import min
-- MAGIC
-- MAGIC def generate_date_series(start, stop):
-- MAGIC     return [start + datetime.timedelta(days = x) for x in range(0, (stop - start).days + 1)];
-- MAGIC
-- MAGIC spark.udf.register("generate_date_series", generate_date_series, ArrayType(DateType()) );
-- MAGIC
-- MAGIC Team_Name = dbutils.widgets.get("Team_Name");
-- MAGIC CompanyId = dbutils.widgets.get("CompanyId");
-- MAGIC recordSourceCode = dbutils.widgets.get("recordSourceCode");
-- MAGIC ctry_cd = dbutils.widgets.get("ctry_cd");

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(Team_Name)
-- MAGIC print(CompanyId)
-- MAGIC print(recordSourceCode)
-- MAGIC print(ctry_cd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Determine Callfiles to update and IV dates

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC
-- MAGIC sql = "SELECT \
-- MAGIC               CallfileId \
-- MAGIC               , date_add(Min_SurveyResponseDate, DayStart) AS StartDate \
-- MAGIC               , date_add(Max_SurveyResponseDate, DayEnd) AS EndDate \
-- MAGIC           FROM (SELECT \
-- MAGIC                       cf.CallfileId \
-- MAGIC                       , MIN(CAST(sr.VisitDate AS DATE)) AS Min_SurveyResponseDate \
-- MAGIC                       , MAX(CAST(sr.VisitDate AS DATE)) AS Max_SurveyResponseDate \
-- MAGIC                   FROM BOBv2.Campaign ca \
-- MAGIC                   INNER JOIN BOBv2.Callfile cf \
-- MAGIC                     ON ca.CampaignId = cf.CampaignId \
-- MAGIC                   INNER JOIN BOBv2.CallfileVisit cfv \
-- MAGIC                       ON cf.CallfileId = cfv.CallfileId \
-- MAGIC                   INNER JOIN BOBv2.SurveyResponse sr \
-- MAGIC                       ON cfv.CallfileVisitId = sr.CallfileVisitId \
-- MAGIC                   WHERE \
-- MAGIC                     ca.CompanyId = 559 \
-- MAGIC                   GROUP BY \
-- MAGIC                       cf.CallfileId) cf \
-- MAGIC           CROSS JOIN (SELECT Min(DayStart) AS DayStart, MAX(DayEnd) AS DayEnd \
-- MAGIC                           FROM BOBv2.reachetl_interventions_parameternew \
-- MAGIC                           WHERE CompanyId = 559) ivp" 
-- MAGIC
-- MAGIC dfCallFiles = spark.sql(sql)
-- MAGIC
-- MAGIC strCallFiles = ""
-- MAGIC for row in dfCallFiles.rdd.collect():
-- MAGIC   if len(strCallFiles) > 0:
-- MAGIC     strCallFiles = strCallFiles + ", " + str(row["CallfileId"])
-- MAGIC   else:
-- MAGIC     strCallFiles = str(row["CallfileId"])
-- MAGIC
-- MAGIC startdate = dfCallFiles.agg({"StartDate" : "min"}).collect()[0][0]
-- MAGIC enddate = dfCallFiles.agg({"EndDate" : "max"}).collect()[0][0]
-- MAGIC
-- MAGIC sql = "SELECT explode(generate_date_series(CAST('" + startdate.strftime("%Y-%m-%d") + "' AS DATE), CAST('" + enddate.strftime("%Y-%m-%d") + "' AS DATE))) AS IVDate"
-- MAGIC
-- MAGIC dfIVDates = spark.sql(sql)
-- MAGIC
-- MAGIC strIVDates = ""
-- MAGIC for row in dfIVDates.rdd.collect():
-- MAGIC   if len(strIVDates) > 0:
-- MAGIC     strIVDates = strIVDates + ", \"" +row["IVDate"].strftime("%Y-%m-%d") + "\""
-- MAGIC   else:
-- MAGIC     strIVDates = "\"" + row["IVDate"].strftime("%Y-%m-%d") + "\""

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dfIVDates)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Collect Callfile Interventions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC #sql = "DROP TABLE IF EXISTS " + Team_Name + "_" + ctry_cd + "_team_alerts_im.tmp_callfile_interventions";
-- MAGIC #spark.sql(sql);
-- MAGIC
-- MAGIC sql = "SELECT \
-- MAGIC           * \
-- MAGIC         FROM (SELECT \
-- MAGIC                 ROW_NUMBER() OVER (PARTITION BY CallfileId, OutletId, ProductId, InterventionDate ORDER BY InterventionRank) InterventionNo \
-- MAGIC                 , COUNT(*) OVER (PARTITION BY CallfileId, OutletId, ProductId, InterventionDate) InterventionCount \
-- MAGIC                 , * \
-- MAGIC               FROM (SELECT DISTINCT \
-- MAGIC                       cfv.OutletId \
-- MAGIC                       , ca.FullName AS Campaign \
-- MAGIC                       , cf.PeriodName \
-- MAGIC                       , cfv.CallfileId \
-- MAGIC                       , cfv.CallfileVisitId \
-- MAGIC                       , COALESCE(pcm.ProductId, psism.ProductId, 0) AS ProductId \
-- MAGIC                       , CAST(r.RefExternal AS VARCHAR(1000)) AS RefExternal \
-- MAGIC                       , nvl(sis.VariantText, '') AS TableProduct \
-- MAGIC                       , sr.VisitDate AS InterventionDate \
-- MAGIC                       , sio.SurveyItemOptionId \
-- MAGIC                       , REPLACE(REPLACE(sio.VariantText, CHAR(10), ''), CHAR(13), '') AS VariantText \
-- MAGIC                       , sir.ResponseValue \
-- MAGIC                       , ivp.Intervention \
-- MAGIC                       , ivp.InterventionRank \
-- MAGIC                       , ivp.InterventionCost \
-- MAGIC                       , nvl(ivp.InterventionGroup, \"\") AS InterventionGroup \
-- MAGIC                       , ivp.DayStart \
-- MAGIC                       , ivp.DayEnd \
-- MAGIC                       , REPLACE(REPLACE(o.FullName, CHAR(10), ''), CHAR(13), '') AS OutletName \
-- MAGIC                       , o.ChainRefExternal \
-- MAGIC                       , c.FullName AS ChainName \
-- MAGIC                       , pc.FullName AS ParentChainName \
-- MAGIC                       , cfv.UserId \
-- MAGIC                       , REPLACE(REPLACE(CONCAT(u.NameFirst, \" \", u.NameSurname), CHAR(10), ''), CHAR(13), '') AS Username \
-- MAGIC                       , pr.FullName AS ProductName \
-- MAGIC                       , pb.FullName AS ProductBrand \
-- MAGIC                       , pr.ManufacturerId \
-- MAGIC                       , sir.SurveyItemResponseId as InterventionId\
-- MAGIC                     FROM BOBv2.CallfileVisit cfv \
-- MAGIC                     INNER JOIN BOBv2.Callfile cf \
-- MAGIC                       ON cfv.CallfileId = cf.CallfileId \
-- MAGIC                     INNER JOIN BOBv2.Campaign ca \
-- MAGIC                       ON cf.CampaignId = ca.CampaignId \
-- MAGIC                     INNER JOIN BOBv2.SurveyResponse sr \
-- MAGIC                       ON cfv.CallfileVisitId = sr.CallfileVisitId \
-- MAGIC                     INNER JOIN BOBv2.SurveyItemResponse sir \
-- MAGIC                       ON sr.SurveyResponseId = sir.SurveyResponseId \
-- MAGIC                     LEFT JOIN BOBv2.SurveyItemOption sio \
-- MAGIC                       ON sir.SurveyItemOptionId = sio.SurveyItemOptionId \
-- MAGIC                     INNER JOIN BOBv2.SurveyItem si \
-- MAGIC                       ON sir.SurveyItemId = si.SurveyItemId \
-- MAGIC                     INNER JOIN BOBv2.QuestionVariant qv \
-- MAGIC                       ON si.QuestionVariantId = qv.QuestionVariantId \
-- MAGIC                     LEFT JOIN BOBv2.SurveyItem psi \
-- MAGIC                       ON si.ParentSurveyItemId = psi.SurveyItemId \
-- MAGIC                     INNER JOIN bobv2.reachetl_interventions_parameternew ivp \
-- MAGIC                       ON ivp.CompanyId = " + CompanyId + " \
-- MAGIC                       AND nvl(sir.SurveyItemOptionId, 0) = ivp.SurveyItemOptionId \
-- MAGIC                       AND ((si.QuestionVariantId IS NOT NULL \
-- MAGIC                             AND psi.QuestionVariantId IS NOT NULL \
-- MAGIC                             AND array_contains(split(ivp.ChildQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true \
-- MAGIC                             AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(psi.QuestionVariantId AS VARCHAR(100))) = true) \
-- MAGIC                            OR (si.QuestionVariantId IS NOT NULL \
-- MAGIC                                AND psi.QuestionVariantId IS NULL \
-- MAGIC                                AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true)) \
-- MAGIC                     LEFT JOIN BOBv2.SurveyItemSubject sis \
-- MAGIC                       ON sir.SurveyItemSubjectId = sis.SurveyItemSubjectId \
-- MAGIC                     LEFT JOIN BOBv2.ProductSurveyItemSubjectMap psism \
-- MAGIC                       ON sis.SurveyItemSubjectId = psism.SurveyItemSubjectId \
-- MAGIC                     LEFT JOIN BOBv2.ProductControlMap pcm \
-- MAGIC                       ON sir.ProductControlMapId = pcm.ProductControlMapId \
-- MAGIC                     INNER JOIN BOBv2.Outlet o \
-- MAGIC                       ON cfv.OutletId = o.OutletId \
-- MAGIC                     INNER JOIN BOBv2.Chain c \
-- MAGIC                       ON o.ChainId = c.ChainId \
-- MAGIC                     LEFT JOIN BOBv2.Chain pc \
-- MAGIC                       ON c.ParentChainId = pc.ChainId \
-- MAGIC                     INNER JOIN BOBv2.User u \
-- MAGIC                       ON cfv.UserId = u.UserId \
-- MAGIC                     LEFT JOIN BOBv2.Product pr \
-- MAGIC                       ON COALESCE(pcm.ProductId, psism.ProductId, 0) = pr.ProductId \
-- MAGIC                     LEFT JOIN BOBv2.ProductBrand pb \
-- MAGIC                       ON pr.ProductBrandId = pb.ProductBrandId \
-- MAGIC                     LEFT JOIN (SELECT \
-- MAGIC                                   p.ProductId \
-- MAGIC                                   , COALESCE(c.ParentChainId, c.ChainId) AS ParentChainId \
-- MAGIC                                   , RTRIM(LTRIM(REPLACE(pmc.RefExternal, CHAR(9), ''))) AS RefExternal \
-- MAGIC                                 FROM BOBv2.Product p \
-- MAGIC                                 INNER JOIN BOBv2.ProductMapChain pmc \
-- MAGIC                                   ON p.ProductId = pmc.ProductId \
-- MAGIC                                 INNER JOIN BOBv2.Chain c \
-- MAGIC                                   ON pmc.ChainId = c.ChainId \
-- MAGIC                                 WHERE \
-- MAGIC                                   p.CompanyId = " + CompanyId + ") r \
-- MAGIC                       ON COALESCE(pcm.ProductId, psism.ProductId, 0) = r.ProductId \
-- MAGIC                       AND COALESCE(pc.ChainId,c.ChainId) = r.ParentChainId \
-- MAGIC                     WHERE \
-- MAGIC                       cfv.CallfileId IN (" + strCallFiles + ") \
-- MAGIC                       AND ca.CampaignId IN (4429, 4567, 5013) \
-- MAGIC                       AND cfv.IsDeleted = 0 \
-- MAGIC                       AND sr.IsDeleted = 0 \
-- MAGIC                       AND sir.IsDeleted = 0 \
-- MAGIC                       AND sir.ResponseValue IS NOT NULL) a) i \
-- MAGIC         WHERE \
-- MAGIC           InterventionNo = 1";
-- MAGIC
-- MAGIC dfIV = spark.sql(sql);
-- MAGIC dfIV.createOrReplaceTempView("adv_analytics_IV")

-- COMMAND ----------

select * from adv_analytics_IV
--this looks good! we can probably reduce from here to get to the good stuff for a skinny table, or use this directly...

-- COMMAND ----------

select count(*) from adv_analytics_IV

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # df_iv = spark.sql('select ParentChainName, Intervention, InterventionDate, InterventionGroup from adv_analytics_IV where InterventionDate')
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
-- MAGIC df_iv.write.mode('overwrite').format('delta').save('/mnt/processed/temp/intervention_data-rbusahealth-walmart')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_iv = spark.read.format('delta').load('/mnt/processed/temp/intervention_data-rbusahealth-walmart')

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
-- MAGIC           \"walmart_rbusahealth_us\" AS RETAIL_CLIENT \
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
-- MAGIC         FROM walmart_rbusahealth_us_dv.vw_sat_link_epos_summary s \
-- MAGIC         INNER JOIN walmart_rbusahealth_us_dv.hub_retailer_item ri \
-- MAGIC           ON s.HUB_RETAILER_ITEM_HK = ri.HUB_RETAILER_ITEM_HK \
-- MAGIC         INNER JOIN walmart_rbusahealth_us_dv.hub_organization_unit ou \
-- MAGIC           ON s.HUB_ORGANIZATION_UNIT_HK = ou.HUB_ORGANIZATION_UNIT_HK \
-- MAGIC         LEFT JOIN walmart_rbusahealth_us_retail_alert_im.vw_drfe_forecast_baseline_unit d \
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

CREATE OR REPLACE TEMPORARY VIEW ivs_with_epos
AS SELECT 
          iv.CallfileId
          , iv.CallfileVisitId
          , iv.InterventionCount
          , iv.Campaign
          , iv.PeriodName
          , iv.ParentChainName
          , iv.ChainName
          , iv.UserId
          , iv.Username
          , iv.ChainRefExternal
          , iv.RefExternal
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
        LEFT JOIN adv_analytics_IV_epos_sales s
          ON iv.ChainRefExternal = s.ORGANIZATION_UNIT_NUM
          AND iv.RefExternal = s.RETAILER_ITEM_ID
        WHERE
          (DATE_ADD(CAST(iv.InterventionDate AS DATE), DayStart) <= s.SALES_DT
          AND DATE_ADD(CAST(iv.InterventionDate AS DATE), DayEnd) >= s.SALES_DT)
          OR (CAST(iv.InterventionDate AS Date) = SALES_DT)

-- COMMAND ----------

select * from ivs_with_epos limit 50

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # RUN ME
-- MAGIC df_ivs_with_epos = spark.sql('select * from ivs_with_epos')
-- MAGIC
-- MAGIC df_ivs_with_epos.write.mode('overwrite').format('delta').save('/mnt/processed/temp/rbusahealth_walmart_inv_epos')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_epos = spark.read.format('delta').load('/mnt/processed/training/data-walmart-rbusahealth-us-2020-06-17/data_vault')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC retailer_item_ids_epos = df_epos.select("RETAILER_ITEM_ID").distinct()
-- MAGIC
-- MAGIC retailer_item_ids_epos.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_ivs_with_epos = spark.read.format('delta').load('/mnt/processed/temp/rbusahealth_walmart_inv_epos')

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
