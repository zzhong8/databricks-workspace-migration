# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
import datetime

from pyspark.sql import Window
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf

import acosta

from acosta.alerting.preprocessing import read_pos_data
from acosta.alerting.helpers import check_path_exists
from acosta.measurement import process_notebook_inputs

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

# COMMAND ----------

# mdm_country_id = 1 # US
# mdm_client_id = 16540 # wildcat
# mdm_holding_id = 91 # kroger

# COMMAND ----------

def read_rep_response_data(country_id, client_id, holding_id, sql_context):
    """
    Reads in Rep Response data from acosta_retail_analytics_im info-mart and returns a DataFrame

    :param string mdm_country_id: the two character name of the country code to pull
    :param string mdm_client_id: the name of the client to pull
    :param string mdm_holding_id: the name of the retailer to pull

    :return DataFrame:
    """

    sql_statement = """
        SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
        where
        mdm_country_id = \'{country_id}\'
        and
        mdm_client_id = \'{client_id}\'
        and
        mdm_holding_id = \'{holding_id}\'
        and
        epos_retailer_item_id is not NULL
    """

    sql_statement = sql_statement.format(country_id=country_id, client_id=client_id, holding_id=holding_id)

    return sql_context.sql(sql_statement)

# COMMAND ----------

rep_response_data = read_rep_response_data(country_id, client_id, holding_id, sqlContext).repartition('call_date')

# COMMAND ----------

measureable_rep_response_data = rep_response_data.where(pyf.col("actionable_flg") == pyf.lit("true"))

# COMMAND ----------

non_measureable_rep_response_data = rep_response_data.where(pyf.col("actionable_flg") != pyf.lit("true"))

# COMMAND ----------

count_measureable_rep_responses_by_date = measureable_rep_response_data.select("call_date", "response_id").orderBy("call_date").groupBy("call_date").count()

display(count_measureable_rep_responses_by_date)

# COMMAND ----------

count_non_measureable_rep_responses_by_date = non_measureable_rep_response_data.select("call_date", "response_id").orderBy("call_date").groupBy("call_date").count()

display(count_non_measureable_rep_responses_by_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- wildcat
# MAGIC and
# MAGIC mdm_holding_id = 91 -- kroger
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_id,
# MAGIC objective_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- wildcat
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date
