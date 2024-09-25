# Databricks notebook source
from pprint import pprint

import numpy as np
import pandas as pd

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from acosta.measurement import process_notebook_inputs

import acosta

print(acosta.__version__)

# COMMAND ----------

# Inputs get required inputs
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

# country_id = 30 # UK
# client_id = 16320 # Nestle UK
# banner_id = 7743 # Asda
# holding_id = 3257 # AcostaRetailUK

# country_id = 1  # US
# client_id = 12407 # JOHNSON & JOHNSON US
# holding_id = 71 # Walmart
# banner_id = -1  # default

# country_id = 1  # US
# client_id = 882 # DanoneU.S.LLC
# client_id = 13054 # Starbucks
# holding_id = 91 # Kroger
# banner_id = -1  # default

# COMMAND ----------

client_config = spark.sql(f'''
    SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config
    WHERE
    mdm_country_id = {country_id} AND
    mdm_client_id = {client_id} AND
    mdm_holding_id = {holding_id} AND
    coalesce(mdm_banner_id, -1) = {banner_id}
''')
assert client_config.cache().count() == 1

# Create config dict
config_dict = client_config.toPandas().T
config_dict = dict(zip(config_dict.index, config_dict[0]))
pprint(config_dict)

# COMMAND ----------

if (config_dict['alertgen_im_db_nm'] == "retail_alert_kroger_danonewave_us_im"):
    config_dict['alertgen_im_db_nm'] = "kroger_danonewaves_us_retail_alert_im"

if (config_dict['alertgen_im_db_nm'] == "retail_alert_kroger_starbucks_us_im"):
    config_dict['alertgen_im_db_nm'] = "kroger_starbucks_us_retail_alert_im"

# COMMAND ----------

df_sql_query_acosta_retail_alert_im = """
    SELECT * from
    {alertgen_im_db_nm}.loess_forecast_baseline_unit
""".format(alertgen_im_db_nm=config_dict['alertgen_im_db_nm'])

# COMMAND ----------

df_acosta_retail_alert_im = spark.sql(df_sql_query_acosta_retail_alert_im)

print(f'{df_acosta_retail_alert_im.cache().count():,}')

# COMMAND ----------

display(df_acosta_retail_alert_im)

# COMMAND ----------

df_acosta_retail_alert_im_unique_days = df_acosta_retail_alert_im.select("SALES_DT").distinct()

df_acosta_retail_alert_im_num_days = df_acosta_retail_alert_im_unique_days.count()

print(df_acosta_retail_alert_im_num_days)

# COMMAND ----------

display(df_acosta_retail_alert_im_unique_days)

# COMMAND ----------

df_sql_query = """
    SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320 -- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    objective_typ = 'Data Led Alerts'
    and
    call_date >= '2022-06-01'
    and
    actionable_flg is not NULL
"""

# COMMAND ----------

df = spark.sql(df_sql_query)

df.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/nestle-uk-measurable-interventions-20220601-to-20221201')

# COMMAND ----------

df_sql_query_walmart = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from retail_alert_walmart_johnsonandjohnson_us_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-12-04'
    order by SALES_DT desc
"""

# COMMAND ----------

df_walmart = spark.sql(df_sql_query_walmart)

df_walmart.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart_johnsonandjohnson_us_drfe_forecast_baseline_unit-20221204')
