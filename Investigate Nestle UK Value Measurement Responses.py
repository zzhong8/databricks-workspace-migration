# Databricks notebook source
from pprint import pprint

import numpy as np
import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_report_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC standard_response_text like
# MAGIC (
# MAGIC '%onfectionery%'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC standard_response_text like
# MAGIC (
# MAGIC '%onfectionery%'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC standard_response_text like
# MAGIC (
# MAGIC '%onfectionery%'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC standard_response_text like
# MAGIC (
# MAGIC '%onfectionery%'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC standard_response_text in
# MAGIC (
# MAGIC 'In-aisle promo bay - Confectionery',
# MAGIC 'In-aisle promo bay - Crisps',
# MAGIC 'Dumpbin - inc FlatpackedDumpbin',
# MAGIC 'Half Shipper - pre-filled unit'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where standard_response_text in
# MAGIC (
# MAGIC 'In-aisle promo bay - Confectionery'
# MAGIC 'In-aisle promo bay - Crisps'
# MAGIC 'Dumpbin - inc FlatpackedDumpbin'
# MAGIC 'Half Shipper - pre-filled unit'
# MAGIC )
# MAGIC
