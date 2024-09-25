# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17683 -- Premier Foods UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17683 -- Premier Foods UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct mdm_banner_nm, objective_typ, standard_response_text from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17683 -- Premier Foods UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC order by
# MAGIC mdm_banner_nm, objective_typ, standard_response_text
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct 
# MAGIC mdm_banner_nm, objective_typ, standard_response_text, 
# MAGIC intervention_start_day, intervention_end_day, actionable_flg
# MAGIC from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17683 -- Premier Foods UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC order by
# MAGIC mdm_banner_nm, objective_typ, standard_response_text
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda

# COMMAND ----------

client_id = 17688
country_id = 30
holding_id = 3257
banner_id = 7743 # Asda

sql_all_measured_asda_kenvue_uk_rep_responses = """
select * from acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 17688 -- Nestle UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7743 -- Asda
"""
df_all_measured_asda_kenvue_uk_rep_responses = spark.sql(sql_all_measured_asda_kenvue_uk_rep_responses).cache()

print(df_all_measured_asda_kenvue_uk_rep_responses.count())

df_all_measured_asda_kenvue_uk_rep_responses.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/{client_id}-{country_id}-{holding_id}-{banner_id}-ds_intervention_summary')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE', total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17688   -- Kenvue UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id = 7743    -- Asda
# MAGIC    AND standard_response_text IN ('Display Built', 'Display Maintained', 'Yes - I built/maintained a display')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE'
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17688   -- Kenvue UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id = 7743    -- Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'TRUE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17688   -- Kenvue UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id = 7743    -- Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17688   -- Kenvue UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id = 7743    -- Asda
# MAGIC    AND is_complete = 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC SALES_DT, count(1)
# MAGIC FROM
# MAGIC retail_alert_sainsburys_johnsonandjohnson_uk_im.loess_forecast_baseline_unit
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC SALES_DT, count(1)
# MAGIC FROM
# MAGIC retail_alert_tesco_johnsonandjohnson_uk_im.loess_forecast_baseline_unit
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC SALES_DT, count(1)
# MAGIC FROM
# MAGIC horizon_sainsburys_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC SALES_DT, count(1)
# MAGIC FROM
# MAGIC tescopartnertoolkit_tesco_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT DESC

# COMMAND ----------

country_id = 30  # UK
client_id = 17688 # Kenvue UK
holding_id = 3257 # AcostaRetailUK
banner_id = 7743  # Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by
# MAGIC objective_typ,
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC nars_response_text = 'Book Stock Error Corrected'
# MAGIC order by
# MAGIC objective_typ,
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct nars_response_text from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where nars_response_text not like '%,%'
# MAGIC order by nars_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC order by
# MAGIC objective_typ,
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC mdm_client_id,
# MAGIC mdm_banner_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC WEEKOFYEAR('2022-06-26'),
# MAGIC WEEKOFYEAR('2022-06-27'),
# MAGIC WEEKOFYEAR('2022-12-04'),
# MAGIC WEEKOFYEAR(DATE_ADD(CAST('2022-12-04' AS DATE), 1)),
# MAGIC WEEKOFYEAR('2022-12-05')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, mdm_banner_nm, count(1) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC group by
# MAGIC mdm_banner_id, mdm_banner_nm
# MAGIC order by
# MAGIC mdm_banner_id, mdm_banner_nm

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC epos_retailer_item_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT b.shelfcode, b.item_level_id, b.item_dimension_id, b.item_entity_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars a
# MAGIC join acosta_retail_analytics_im.vw_fact_question_response b on a.response_id = b.response_id
# MAGIC where
# MAGIC a.mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC a.mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC a.mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC a.epos_retailer_item_id is null
# MAGIC order by
# MAGIC b.shelfcode

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_dimension_question

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_dimension_store

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select banner_description, count(1)
# MAGIC from rex_dv.sat_service_request r
# MAGIC left join premium360_analytics_im.vw_lookup_store s
# MAGIC on r.siteid =  s.site_id
# MAGIC left join acosta_retail_analytics_im.vw_dimension_store m
# MAGIC on s.mdm_store_id = m.store_id
# MAGIC --and m.banner_description = 'Tesco'
# MAGIC where clientid = 3898
# MAGIC Group by banner_description

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select a.question_id, c.question_category, c.question_code, c.question, a.response_id, a.response_type_code, a.response_type, a.response from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7743) b
# MAGIC on a.store_id = b.store_id join
# MAGIC (select question_id, question_category, question_code, question from vw_dimension_question
# MAGIC where client_id = 17688) c 
# MAGIC on a.question_id = c.question_id
# MAGIC and call_completed_date = '2023-01-05'
# MAGIC order by response_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select a.question_id, c.question_category, c.question_code, c.question, a.response_id, a.response_type_code, a.response_type, a.response from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id join
# MAGIC (select question_id, question_category, question_code, question from vw_dimension_question
# MAGIC where client_id = 17688) c 
# MAGIC on a.question_id = c.question_id
# MAGIC and call_completed_date = '2023-01-05'
# MAGIC order by response_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_completed_date, count(1) from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id join
# MAGIC (select distinct question_id from vw_dimension_question
# MAGIC where client_id = 17688) c 
# MAGIC on a.question_id = c.question_id
# MAGIC and call_completed_date >= '2023-01-01'
# MAGIC group by call_completed_date
# MAGIC order by call_completed_date%sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_completed_date, count(1) from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id join
# MAGIC (select distinct question_id from vw_dimension_question
# MAGIC where client_id = 17688) c 
# MAGIC on a.question_id = c.question_id
# MAGIC and call_completed_date >= '2023-01-01'
# MAGIC group by call_completed_date
# MAGIC order by call_completed_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC standard_response_text = 'Promo Recharge'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct 
# MAGIC mdm_banner_id, mdm_banner_nm, standard_response_cd, standard_response_text, nars_response_text, 
# MAGIC intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg
# MAGIC FROM 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC order by
# MAGIC standard_response_text, nars_response_text, mdm_banner_id, mdm_banner_nm, standard_response_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT standard_response_cd, standard_response_text, nars_response_text, 
# MAGIC intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg, count(*)
# MAGIC FROM 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC group by
# MAGIC standard_response_cd, standard_response_text, nars_response_text, 
# MAGIC intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg
# MAGIC order by
# MAGIC nars_response_text, standard_response_text, standard_response_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2023-02-28'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC order by
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC group by 
# MAGIC call_week
# MAGIC ORDER BY
# MAGIC call_week

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC group by 
# MAGIC call_week
# MAGIC ORDER BY
# MAGIC call_week

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_id, mdm_holding_nm FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC order by mdm_holding_id, mdm_holding_nm

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct call_date FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by call_date

# COMMAND ----------

# Get the list of completely measured responses by week
df_sql_query_acosta_retail_analytics_im = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  --mdm_banner_nm,
  mdm_client_nm,
  --mdm_country_id,
  --mdm_client_id,
  --mdm_holding_id,
  --mdm_banner_id,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  --SUM(total_qintervention_effect),
  SUM(total_impact),
  --SUM(total_qimpact),
  WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2022-06-02' AND
      call_date <= '2022-12-31'
  ) AND -- calls happened between 2022-06-02 and 2022-12-31
  load_ts like '2023-%' -- loaded in 2023
  GROUP BY
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  call_week
  ORDER BY
  call_week
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)
display(df_acosta_retail_analytics_im)

# Kroger Wildcat
# Results from 2022-06-02 and 2022-12-31

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im = """
  SELECT *
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  -- mdm_banner_id = {banner_id} AND
  (
      call_date >= '2023-01-01' AND
      call_date <= '2023-02-28'
  ) -- calls happened between 2023-01-01 and 2023-02-28
  ORDER BY
  mdm_banner_id,
  objective_typ,
  call_date,
  response_id
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im).cache()

# COMMAND ----------

df_acosta_retail_analytics_im.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/kenvue-uk-measurable-interventions-20220101-to-20230228')

# COMMAND ----------

print(f'Intervention Count = {df_acosta_retail_analytics_im.count():,}')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date
