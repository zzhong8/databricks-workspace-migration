# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# RUN ME
import datetime
import pyspark
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DateType
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import min

# COMMAND ----------

df_rbusahygiene = spark.sql('select distinct RefExternal from walmart_rbusahygiene_us_retail_alert_im.vw_gc_ds_input_rsv_measurement')

display(df_rbusahygiene)

# COMMAND ----------

# sqlWalmartRbusahealthUsTempRSVmeasurementView = """
# select 
# CallfileVisitId,
# ParentChainName,
# ChainName,
# ChainRefExternal,
# RefExternal,
# InterventionId,
# InterventionDate,
# InterventionGroup,
# Intervention,
# DiffStart,
# DiffEnd,
# Duration
# FROM(
# select
#    cfv.CallfileVisitId as CallfileVisitId,
#    COALESCE(pc.FullName,c.FullName) AS ParentChainName, ---If Parent name is null then assign chain name 
#    c.FullName AS ChainName,
#    o.ChainRefExternal as ChainRefExternal,
#    CAST(r.RefExternal AS VARCHAR(1000)) AS RefExternal,
#    sir.SurveyItemResponseId as InterventionId,
#    sr.VisitDate AS InterventionDate,
#    nvl(ivp.InterventionGroup, "") AS InterventionGroup,
#    ivp.Intervention as Intervention,
#    CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayStart AS VARCHAR(10))) AS DiffStart,
#    CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayEnd AS VARCHAR(10))) AS DiffEnd,
#    ABS(ivp.DayEnd - ivp.DayStart) + 1 Duration ,  ---Absolute value of Duration
#    ROW_NUMBER() OVER (PARTITION BY cfv.CallfileId, o.OutletId, COALESCE(pcm.ProductId, psism.ProductId, 0), sr.VisitDate ORDER BY ivp.InterventionRank) InterventionNo
# from
#    BOBv2.CallfileVisit cfv 
#    INNER JOIN
#       BOBv2.vw_BOBv2_Outlet o 
#       ON cfv.OutletId = o.OutletId --Join Outlet Bobv2
#    inner join
#       BOBv2.Chain c 
#       on o.ParentChainId = c.ChainId ---Join Chain for ParentChainName
#    INNER JOIN
#       BOBv2.SurveyResponse sr 
#       ON cfv.CallfileVisitId = sr.CallfileVisitId 
#    INNER JOIN
#       BOBv2.SurveyItemResponse sir 
#       ON sr.SurveyResponseId = sir.SurveyResponseId 
#    LEFT JOIN
#       BOBv2.Chain pc 
#       ON c.ParentChainId = pc.ChainId ----Self Join Chain for ChainName 
#    INNER JOIN
#       BOBv2.SurveyItem si 
#       ON sir.SurveyItemId = si.SurveyItemId 
#    LEFT JOIN
#       BOBv2.SurveyItem psi 
#       ON si.ParentSurveyItemId = psi.SurveyItemId 
#    INNER JOIN
#       bobv2.reachetl_interventions_parameternew ivp 
#       ON nvl(sir.SurveyItemOptionId, 0) = ivp.SurveyItemOptionId 
#       -----Interventions Join----------------------
#       AND 
#       (
# (si.QuestionVariantId IS NOT NULL 
#          AND psi.QuestionVariantId IS NOT NULL 
#          AND array_contains(split(ivp.ChildQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true 
#          AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(psi.QuestionVariantId AS VARCHAR(100))) = true) 
#          OR 
#          (
#             si.QuestionVariantId IS NOT NULL 
#             AND psi.QuestionVariantId IS NULL 
#             AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true
#          )
#       )
#    LEFT JOIN
#       BOBv2.SurveyItemSubject sis 
#       ON sir.SurveyItemSubjectId = sis.SurveyItemSubjectId 
#    LEFT JOIN
#       BOBv2.ProductControlMap pcm 
#       ON sir.ProductControlMapId = pcm.ProductControlMapId 
#    LEFT JOIN
#       BOBv2.ProductSurveyItemSubjectMap psism 
#       ON sis.SurveyItemSubjectId = psism.SurveyItemSubjectId 
#       ---------- Join Product for RefExternal------------
#    Left join
#       (
#          SELECT
#             p.ProductId,
#             COALESCE(c.ParentChainId, c.ChainId) AS ParentChainId,
#             RTRIM(LTRIM(REPLACE(pmc.RefExternal, CHAR(9), ''))) AS RefExternal 
#          FROM
#             BOBv2.Product p 
#             INNER JOIN
#                BOBv2.ProductMapChain pmc 
#                ON p.ProductId = pmc.ProductId 
#             INNER JOIN
#                BOBv2.Chain c 
#                ON pmc.ChainId = c.ChainId 
#          WHERE
#             p.CompanyId =559 and COALESCE(c.ParentChainId, c.ChainId)=950 
#     AND pmc.Active = 1
#     AND ((pmc.IsDeleted IS NULL) OR (pmc.IsDeleted = 0))
#       )
#       r 
#       ON COALESCE(pcm.ProductId, psism.ProductId, 0) = r.ProductId 
# where
#    ivp.CompanyId =559 ---filter on Each CompanyId 
#    AND cfv.IsDeleted = 0 
#    and sr.IsDeleted = 0 
#    AND sir.IsDeleted = 0 
#    and sir.ResponseValue IS NOT NULL
#     and o.ParentChainId=950   ---Filter on Parent chain id 
#  ) final 
# ----Remove duplicate in case of multiple Intervention on InterventionRank 
# Where InterventionNo = 1
# """

# COMMAND ----------

sqlWalmartRbusahealthUsTempRSVmeasurementView = """
select 
RefExternal
FROM(
select
   cfv.CallfileVisitId as CallfileVisitId,
   COALESCE(pc.FullName,c.FullName) AS ParentChainName, ---If Parent name is null then assign chain name 
   c.FullName AS ChainName,
   o.ChainRefExternal as ChainRefExternal,
   CAST(r.RefExternal AS VARCHAR(1000)) AS RefExternal,
   sir.SurveyItemResponseId as InterventionId,
   sr.VisitDate AS InterventionDate,
   nvl(ivp.InterventionGroup, "") AS InterventionGroup,
   ivp.Intervention as Intervention,
   CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayStart AS VARCHAR(10))) AS DiffStart,
   CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayEnd AS VARCHAR(10))) AS DiffEnd,
   ABS(ivp.DayEnd - ivp.DayStart) + 1 Duration ,  ---Absolute value of Duration
   ROW_NUMBER() OVER (PARTITION BY cfv.CallfileId, o.OutletId, COALESCE(pcm.ProductId, psism.ProductId, 0), sr.VisitDate ORDER BY ivp.InterventionRank) InterventionNo
from
   BOBv2.CallfileVisit cfv 
   INNER JOIN
      BOBv2.vw_BOBv2_Outlet o 
      ON cfv.OutletId = o.OutletId --Join Outlet Bobv2
   inner join
      BOBv2.Chain c 
      on o.ParentChainId = c.ChainId ---Join Chain for ParentChainName
   INNER JOIN
      BOBv2.SurveyResponse sr 
      ON cfv.CallfileVisitId = sr.CallfileVisitId 
   INNER JOIN
      BOBv2.SurveyItemResponse sir 
      ON sr.SurveyResponseId = sir.SurveyResponseId 
   LEFT JOIN
      BOBv2.Chain pc 
      ON c.ParentChainId = pc.ChainId ----Self Join Chain for ChainName 
   INNER JOIN
      BOBv2.SurveyItem si 
      ON sir.SurveyItemId = si.SurveyItemId 
   LEFT JOIN
      BOBv2.SurveyItem psi 
      ON si.ParentSurveyItemId = psi.SurveyItemId 
   INNER JOIN
      bobv2.reachetl_interventions_parameternew ivp 
      ON nvl(sir.SurveyItemOptionId, 0) = ivp.SurveyItemOptionId 
      -----Interventions Join----------------------
      AND 
      (
(si.QuestionVariantId IS NOT NULL 
         AND psi.QuestionVariantId IS NOT NULL 
         AND array_contains(split(ivp.ChildQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true 
         AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(psi.QuestionVariantId AS VARCHAR(100))) = true) 
         OR 
         (
            si.QuestionVariantId IS NOT NULL 
            AND psi.QuestionVariantId IS NULL 
            AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true
         )
      )
   LEFT JOIN
      BOBv2.SurveyItemSubject sis 
      ON sir.SurveyItemSubjectId = sis.SurveyItemSubjectId 
   LEFT JOIN
      BOBv2.ProductControlMap pcm 
      ON sir.ProductControlMapId = pcm.ProductControlMapId 
   LEFT JOIN
      BOBv2.ProductSurveyItemSubjectMap psism 
      ON sis.SurveyItemSubjectId = psism.SurveyItemSubjectId 
      ---------- Join Product for RefExternal------------
   Left join
      (
         SELECT
            p.ProductId,
            COALESCE(c.ParentChainId, c.ChainId) AS ParentChainId,
            RTRIM(LTRIM(REPLACE(pmc.RefExternal, CHAR(9), ''))) AS RefExternal 
         FROM
            BOBv2.Product p 
            INNER JOIN
               BOBv2.ProductMapChain pmc 
               ON p.ProductId = pmc.ProductId 
            INNER JOIN
               BOBv2.Chain c 
               ON pmc.ChainId = c.ChainId 
         WHERE
            p.CompanyId =559 and COALESCE(c.ParentChainId, c.ChainId)=950 
    AND pmc.Active = 1
    AND ((pmc.IsDeleted IS NULL) OR (pmc.IsDeleted = 0))
      )
      r 
      ON COALESCE(pcm.ProductId, psism.ProductId, 0) = r.ProductId 
where
   ivp.CompanyId =559 ---filter on Each CompanyId 
   AND cfv.IsDeleted = 0 
   and sr.IsDeleted = 0 
   AND sir.IsDeleted = 0 
   and sir.ResponseValue IS NOT NULL
    and o.ParentChainId=950   ---Filter on Parent chain id 
 ) final 
----Remove duplicate in case of multiple Intervention on InterventionRank 
Where InterventionNo = 1
"""

# COMMAND ----------

spark.sql(sqlWalmartRbusahealthUsTempRSVmeasurementView).createOrReplaceTempView("walmart_rbusahealth_us_tmp_vw_gc_ds_input_rsv_measurement")

# COMMAND ----------

df_rbusahealth = spark.sql('select distinct RefExternal from walmart_rbusahealth_us_tmp_vw_gc_ds_input_rsv_measurement')

display(df_rbusahealth)

# COMMAND ----------

sqlWalmartRbusahygieneUsTempRSVmeasurementView = """
select 
RefExternal
FROM(
select
   cfv.CallfileVisitId as CallfileVisitId,
   COALESCE(pc.FullName,c.FullName) AS ParentChainName, ---If Parent name is null then assign chain name 
   c.FullName AS ChainName,
   o.ChainRefExternal as ChainRefExternal,
   CAST(r.RefExternal AS VARCHAR(1000)) AS RefExternal,
   sir.SurveyItemResponseId as InterventionId,
   sr.VisitDate AS InterventionDate,
   nvl(ivp.InterventionGroup, "") AS InterventionGroup,
   ivp.Intervention as Intervention,
   CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayStart AS VARCHAR(10))) AS DiffStart,
   CONCAT(CAST("Diff Day" AS VARCHAR(10)), CAST(ivp.DayEnd AS VARCHAR(10))) AS DiffEnd,
   ABS(ivp.DayEnd - ivp.DayStart) + 1 Duration ,  ---Absolute value of Duration
   ROW_NUMBER() OVER (PARTITION BY cfv.CallfileId, o.OutletId, COALESCE(pcm.ProductId, psism.ProductId, 0), sr.VisitDate ORDER BY ivp.InterventionRank) InterventionNo
from
   BOBv2.CallfileVisit cfv 
   INNER JOIN
      BOBv2.vw_BOBv2_Outlet o 
      ON cfv.OutletId = o.OutletId --Join Outlet Bobv2
   inner join
      BOBv2.Chain c 
      on o.ParentChainId = c.ChainId ---Join Chain for ParentChainName
   INNER JOIN
      BOBv2.SurveyResponse sr 
      ON cfv.CallfileVisitId = sr.CallfileVisitId 
   INNER JOIN
      BOBv2.SurveyItemResponse sir 
      ON sr.SurveyResponseId = sir.SurveyResponseId 
   LEFT JOIN
      BOBv2.Chain pc 
      ON c.ParentChainId = pc.ChainId ----Self Join Chain for ChainName 
   INNER JOIN
      BOBv2.SurveyItem si 
      ON sir.SurveyItemId = si.SurveyItemId 
   LEFT JOIN
      BOBv2.SurveyItem psi 
      ON si.ParentSurveyItemId = psi.SurveyItemId 
   INNER JOIN
      bobv2.reachetl_interventions_parameternew ivp 
      ON nvl(sir.SurveyItemOptionId, 0) = ivp.SurveyItemOptionId 
      -----Interventions Join----------------------
      AND 
      (
(si.QuestionVariantId IS NOT NULL 
         AND psi.QuestionVariantId IS NOT NULL 
         AND array_contains(split(ivp.ChildQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true 
         AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(psi.QuestionVariantId AS VARCHAR(100))) = true) 
         OR 
         (
            si.QuestionVariantId IS NOT NULL 
            AND psi.QuestionVariantId IS NULL 
            AND array_contains(split(ivp.ParentQuestionVariantId, ','), CAST(si.QuestionVariantId AS VARCHAR(100))) = true
         )
      )
   LEFT JOIN
      BOBv2.SurveyItemSubject sis 
      ON sir.SurveyItemSubjectId = sis.SurveyItemSubjectId 
   LEFT JOIN
      BOBv2.ProductControlMap pcm 
      ON sir.ProductControlMapId = pcm.ProductControlMapId 
   LEFT JOIN
      BOBv2.ProductSurveyItemSubjectMap psism 
      ON sis.SurveyItemSubjectId = psism.SurveyItemSubjectId 
      ---------- Join Product for RefExternal------------
   Left join
      (
         SELECT
            p.ProductId,
            COALESCE(c.ParentChainId, c.ChainId) AS ParentChainId,
            RTRIM(LTRIM(REPLACE(pmc.RefExternal, CHAR(9), ''))) AS RefExternal 
         FROM
            BOBv2.Product p 
            INNER JOIN
               BOBv2.ProductMapChain pmc 
               ON p.ProductId = pmc.ProductId 
            INNER JOIN
               BOBv2.Chain c 
               ON pmc.ChainId = c.ChainId 
         WHERE
            p.CompanyId =560 and COALESCE(c.ParentChainId, c.ChainId)=950 
    AND pmc.Active = 1
    AND ((pmc.IsDeleted IS NULL) OR (pmc.IsDeleted = 0))
      )
      r 
      ON COALESCE(pcm.ProductId, psism.ProductId, 0) = r.ProductId 
where
   ivp.CompanyId =560 ---filter on Each CompanyId 
   AND cfv.IsDeleted = 0 
   and sr.IsDeleted = 0 
   AND sir.IsDeleted = 0 
   and sir.ResponseValue IS NOT NULL
    and o.ParentChainId=950   ---Filter on Parent chain id 
 ) final 
----Remove duplicate in case of multiple Intervention on InterventionRank 
Where InterventionNo = 1
"""

# COMMAND ----------

spark.sql(sqlWalmartRbusahygieneUsTempRSVmeasurementView).createOrReplaceTempView("walmart_rbusahygiene_us_tmp_vw_gc_ds_input_rsv_measurement")

# COMMAND ----------

df_rbusahygiene = spark.sql('select distinct RefExternal from walmart_rbusahygiene_us_tmp_vw_gc_ds_input_rsv_measurement')

display(df_rbusahygiene)

# COMMAND ----------

# #temporary

# df_asda_nestlecore = spark.sql('select distinct RefExternal from asda_nestlecore_uk_retail_alert_im.vw_gc_ds_input_rsv_measurement')

# display(df_asda_nestlecore)

# COMMAND ----------

retailer_item_ids_df_iv_rbusahealth = spark.read.format('delta').load('/mnt/processed/temp/intervention_data-rbusahealth-walmart-retailer_item_ids')

retailer_item_ids_df_joined_rbusahealth = spark.read.format('delta').load('/mnt/processed/temp/rbusahealth_walmart_inv_epos-retailer_item_ids')

# COMMAND ----------

display(retailer_item_ids_df_iv_rbusahealth)

# COMMAND ----------

retailer_item_ids_df_iv_rbusahealth.count()

# COMMAND ----------

display(retailer_item_ids_df_joined_rbusahealth)

# COMMAND ----------

retailer_item_ids_df_joined_rbusahealth.count()

# COMMAND ----------

non_joins_rbusahealth = retailer_item_ids_df_iv_rbusahealth\
    .join(retailer_item_ids_df_joined_rbusahealth, retailer_item_ids_df_iv_rbusahealth.RefExternal == retailer_item_ids_df_joined_rbusahealth.RefExternal, 'left_anti')

# COMMAND ----------

display(non_joins_rbusahealth)

# COMMAND ----------

non_joins_rbusahealth.count()

# COMMAND ----------

retailer_item_ids_df_iv_rbusahygiene = spark.read.format('delta').load('/mnt/processed/temp/intervention_data-rbusahygiene-walmart-retailer_item_ids')

retailer_item_ids_df_joined_rbusahygiene = spark.read.format('delta').load('/mnt/processed/temp/rbusahygiene_walmart_inv_epos-retailer_item_ids')

# COMMAND ----------

display(retailer_item_ids_df_iv_rbusahygiene)

# COMMAND ----------

retailer_item_ids_df_iv_rbusahygiene.count()

# COMMAND ----------

display(retailer_item_ids_df_joined_rbusahygiene)

# COMMAND ----------

retailer_item_ids_df_joined_rbusahygiene.count()

# COMMAND ----------

non_joins_rbusahygiene = retailer_item_ids_df_iv_rbusahygiene\
    .join(retailer_item_ids_df_joined_rbusahygiene, retailer_item_ids_df_iv_rbusahygiene.RefExternal == retailer_item_ids_df_joined_rbusahygiene.RefExternal, 'left_anti')

# COMMAND ----------

display(non_joins_rbusahygiene)

# COMMAND ----------

non_joins_rbusahygiene.count()

# COMMAND ----------

joins_rbusahygiene = retailer_item_ids_df_iv_rbusahygiene\
    .join(retailer_item_ids_df_joined_rbusahygiene, retailer_item_ids_df_iv_rbusahygiene.RefExternal == retailer_item_ids_df_joined_rbusahygiene.RefExternal, 'inner')

# COMMAND ----------

display(joins_rbusahygiene)

# COMMAND ----------

joins_rbusahygiene.count()

# COMMAND ----------

sql = "SELECT \
          * \
        FROM BOBv2.ProductMapChain";

df_ProductMapChain = spark.sql(sql);

# COMMAND ----------

df_ProductMapChain.count()

# COMMAND ----------

# from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf

# COMMAND ----------

non_joins_rbusahygiene_df_ProductMapChain_joined = non_joins_rbusahygiene\
    .join(df_ProductMapChain, 'RefExternal', 'inner')\
    .where((pyf.col("ChainId") == 950))

# & (pyf.col("Active") == pyf.lit("true"))

# COMMAND ----------

non_joins_rbusahygiene_df_ProductMapChain_joined.dtypes

# COMMAND ----------

display(non_joins_rbusahygiene_df_ProductMapChain_joined)

# COMMAND ----------

non_joins_rbusahygiene_df_ProductMapChain_joined.count()

# COMMAND ----------

df1 = non_joins_rbusahygiene_df_ProductMapChain_joined.select('RefExternal', 'ProductId', 'ChainId', 'IsPrimary', 'LastChanged', 'Active')

# COMMAND ----------

display(df1)

# COMMAND ----------

non_joins_rbusahealth_df_ProductMapChain_joined = non_joins_rbusahealth\
    .join(df_ProductMapChain, 'RefExternal', 'inner')\
    .where((pyf.col("ChainId") == 950))

# COMMAND ----------

df2 = non_joins_rbusahealth_df_ProductMapChain_joined.select('RefExternal', 'ProductId', 'ChainId', 'IsPrimary', 'LastChanged', 'Active')

# COMMAND ----------

df2.count()

# COMMAND ----------

display(df2)

# COMMAND ----------


