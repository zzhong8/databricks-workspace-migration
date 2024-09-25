# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

dbutils.widgets.text('country_id', '-1', 'Country ID')
dbutils.widgets.text('client_id', '-1', 'Client ID')
dbutils.widgets.text('holding_id', '-1', 'Holding ID')
dbutils.widgets.text('banner_id', '-1', 'Banner ID')

input_names = ('country_id', 'client_id', 'holding_id', 'banner_id')

country_id, client_id, holding_id, banner_id = [process_notebook_inputs(dbutils.widgets.get(s)) for s in input_names]

print('Country ID =', country_id)
print('Client ID =', client_id)
print('Holding ID =', holding_id)
print('Banner ID =', banner_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC distinct
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_nm,
# MAGIC mdm_country_id,
# MAGIC mdm_holding_id,
# MAGIC mdm_banner_id,
# MAGIC mdm_client_id
# MAGIC FROM 
# MAGIC acosta_retail_report_im.ds_intervention_summary

# COMMAND ----------

# %sql
# SELECT DISTINCT
# objective_typ,
# nars_response_text,
# standard_response_text,
# standard_response_cd
# FROM acosta_retail_report_im.ds_intervention_summary
# ORDER BY
# standard_response_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN acosta_retail_report_im.ds_intervention_summary

# COMMAND ----------

df_standard_responses_sql_query = """
  SELECT DISTINCT
  -- objective_typ,
  -- nars_response_text,
  standard_response_text,
  standard_response_cd,
  measurement_duration
  FROM acosta_retail_report_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id}
  ORDER BY
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_standard_responses = spark.sql(df_standard_responses_sql_query)

display(df_standard_responses)

# COMMAND ----------

df_intervention_parameters_sql_query = """
  SELECT DISTINCT
  standard_response_text,
  intervention_group,
  intervention_start_day,
  intervention_end_day
  FROM acosta_retail_report_im.interventions_parameters
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id}
  ORDER BY
  standard_response_text
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_intervention_parameters = spark.sql(df_intervention_parameters_sql_query)

display(df_intervention_parameters)

# COMMAND ----------

df_standard_interventions = df_standard_responses.join(df_intervention_parameters, 'standard_response_text')

display(df_standard_interventions)

# COMMAND ----------

df_total_qintervention_effect_by_standard_response_sql_query = """
  SELECT
  standard_response_text,
  standard_response_cd,
  COUNT(total_qintervention_effect),
  -- SUM(total_qintervention_effect),
  AVG(total_qintervention_effect)
  FROM 
  acosta_retail_report_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id}
  GROUP BY
  standard_response_text, standard_response_cd
  ORDER BY
  standard_response_cd, standard_response_text
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_total_qintervention_effect_by_standard_response = spark.sql(df_total_qintervention_effect_by_standard_response_sql_query)

display(df_total_qintervention_effect_by_standard_response)

# COMMAND ----------

df_total_qintervention_effect_by_standard_intervention = df_total_qintervention_effect_by_standard_response.join(df_intervention_parameters, 'standard_response_text')

display(df_total_qintervention_effect_by_standard_intervention)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_RCM_requested_inventory_correction_sql_query = """
SELECT
store_acosta_number,
epos_organization_unit_num,
epos_retailer_item_id,
--objective_typ,
call_id,
response_id,
--nars_response_text,
standard_response_text,
--standard_response_cd,
--measurement_duration,
--is_complete,
total_intervention_effect,
total_qintervention_effect,
total_impact,
total_qimpact,
--load_ts,
call_date
FROM 
acosta_retail_report_im.ds_intervention_summary
WHERE
mdm_client_id = {client_id}
AND
standard_response_text = 'RCM Requested Inventory Correction'
ORDER BY 
total_qintervention_effect DESC
LIMIT 10
""".format(client_id=client_id)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_RCM_requested_inventory_correction = spark.sql(df_top_intervenions_by_total_qintervention_effect_RCM_requested_inventory_correction_sql_query)

display(df_top_intervenions_by_total_qintervention_effect_RCM_requested_inventory_correction)

# COMMAND ----------

df_sql_query = """
SELECT
store_acosta_number,
epos_organization_unit_num,
epos_retailer_item_id,
--objective_typ,
call_id,
response_id,
--nars_response_text,
standard_response_text,
--standard_response_cd,
--measurement_duration,
--is_complete,
total_intervention_effect,
total_qintervention_effect,
total_impact,
total_qimpact,
--load_ts,
call_date
FROM 
acosta_retail_report_im.ds_intervention_summary
WHERE
mdm_client_id = {client_id}
AND
epos_retailer_item_id <> 554764517
ORDER BY 
total_qintervention_effect DESC
LIMIT 10
""".format(client_id=client_id)

# COMMAND ----------

df = \
spark.sql(df_sql_query)

display(df)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_non_RCM_requested_inventory_correction_sql_query = """
SELECT
store_acosta_number,
epos_organization_unit_num,
epos_retailer_item_id,
--objective_typ,
call_id,
response_id,
--nars_response_text,
standard_response_text,
--standard_response_cd,
--measurement_duration,
--is_complete,
total_intervention_effect,
total_qintervention_effect,
total_impact,
total_qimpact,
--load_ts,
call_date
FROM 
acosta_retail_report_im.ds_intervention_summary
WHERE
mdm_client_id = {client_id}
AND
standard_response_text <> 'RCM Requested Inventory Correction'
ORDER BY 
total_qintervention_effect DESC
LIMIT 10
""".format(client_id=client_id)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_non_RCM_requested_inventory_correction = \
spark.sql(df_top_intervenions_by_total_qintervention_effect_non_RCM_requested_inventory_correction_sql_query)

display(df_top_intervenions_by_total_qintervention_effect_non_RCM_requested_inventory_correction)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_facing_corrected_sql_query = """
SELECT
store_acosta_number,
epos_organization_unit_num,
epos_retailer_item_id,
--objective_typ,
call_id,
response_id,
--nars_response_text,
standard_response_text,
--standard_response_cd,
--measurement_duration,
--is_complete,
total_intervention_effect,
total_qintervention_effect,
total_impact,
total_qimpact,
--load_ts,
call_date
FROM 
acosta_retail_report_im.ds_intervention_summary
WHERE
mdm_client_id = {client_id}
AND
standard_response_text = 'Facing Corrected'
ORDER BY 
total_qintervention_effect DESC
LIMIT 10
""".format(client_id=client_id)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_facing_corrected = \
spark.sql(df_top_intervenions_by_total_qintervention_effect_facing_corrected_sql_query)

display(df_top_intervenions_by_total_qintervention_effect_facing_corrected)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_display_build_store_approved_sql_query = """
SELECT
store_acosta_number,
epos_organization_unit_num,
epos_retailer_item_id,
--objective_typ,
call_id,
response_id,
--nars_response_text,
standard_response_text,
--standard_response_cd,
--measurement_duration,
--is_complete,
total_intervention_effect,
total_qintervention_effect,
total_impact,
total_qimpact,
--load_ts,
call_date
FROM 
acosta_retail_report_im.ds_intervention_summary
WHERE
mdm_client_id = {client_id}
AND
standard_response_text = 'Display Build - Store Approved'
ORDER BY 
total_qintervention_effect DESC
LIMIT 10
""".format(client_id=client_id)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_display_build_store_approved = \
spark.sql(df_top_intervenions_by_total_qintervention_effect_display_build_store_approved_sql_query)

display(df_top_intervenions_by_total_qintervention_effect_display_build_store_approved)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_display_build_home_office_approved_sql_query = """
SELECT
store_acosta_number,
epos_organization_unit_num,
epos_retailer_item_id,
--objective_typ,
call_id,
response_id,
--nars_response_text,
standard_response_text,
--standard_response_cd,
--measurement_duration,
--is_complete,
total_intervention_effect,
total_qintervention_effect,
total_impact,
total_qimpact,
--load_ts,
call_date
FROM 
acosta_retail_report_im.ds_intervention_summary
WHERE
mdm_client_id = {client_id}
AND
standard_response_text = 'Display Build - Home Office Approved'
ORDER BY 
total_qintervention_effect DESC
LIMIT 10
""".format(client_id=client_id)

# COMMAND ----------

df_top_intervenions_by_total_qintervention_effect_display_build_home_office_approved = \
spark.sql(df_top_intervenions_by_total_qintervention_effect_display_build_home_office_approved_sql_query)

display(df_top_intervenions_by_total_qintervention_effect_display_build_home_office_approved)

# COMMAND ----------

PATH_RAW_RESULTS = f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-processed'

print(PATH_RAW_RESULTS)

# COMMAND ----------

# Filtering of data set
df_raw_results = spark.read.format('delta').load(PATH_RAW_RESULTS)

# COMMAND ----------

df_raw_results.printSchema()

# COMMAND ----------

print(f'N results = {df_raw_results.cache().count():,}')

# COMMAND ----------

retail_item_id_list_path = f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-retail_item_id-list.csv'

retail_item_id_list_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(retail_item_id_list_path)

retail_item_id_list = retail_item_id_list_df_info.select('retailer_item_id')

retail_item_ids = retail_item_id_list.rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

for retail_item_id in retail_item_ids:
    print(retail_item_id)

# COMMAND ----------

for retail_item_id in retail_item_ids:
    # print(retail_item_id)
  
    df_results = df_raw_results.where(pyf.col("RETAILER_ITEM_ID") == pyf.lit(retail_item_id))
    print(df_results.count())
    
    retail_item_id_path = f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-processed-retail_item_id={retail_item_id}'
    
    # Write data
    df_results.write.format('csv')\
    .mode('overwrite')\
    .option('header', 'true')\
    .save(retail_item_id_path)    

# COMMAND ----------

RETAILER = 'walmart'
CLIENT = 'nestlewaters'
COUNTRY_CODE = 'us'

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

data_vault_data1 = data_vault_data.where((pyf.col("SALES_DT") < pyf.lit("2021-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-09-01")))

data_vault_data1 = data_vault_data1.drop("UNIT_PRICE", "ROW_ORIGIN", "PRICE_ORIGIN", "TRAINING_ROLE")

data_vault_data1.cache()

# COMMAND ----------

for retail_item_id in retail_item_ids:
    # print(retail_item_id)
  
    df_data_vault_data = data_vault_data1.where(pyf.col("RETAILER_ITEM_ID") == pyf.lit(retail_item_id))
    print(df_data_vault_data.count())
    
    retail_item_id_epos_path = f'/mnt/processed/hugh/ivm-analysis/{client_id}-{country_id}-{holding_id}-{banner_id}-epos-retail_item_id={retail_item_id}'
    
    # Write data
    df_data_vault_data.coalesce(1).write.format('csv')\
    .mode('overwrite')\
    .option('header', 'true')\
    .save(retail_item_id_epos_path) 

# COMMAND ----------


