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

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaUK
# MAGIC and
# MAGIC mdm_client_id = 17742 -- BrownForman

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaUK
# MAGIC and
# MAGIC mdm_client_id = 17742 -- BrownForman

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaUK
# MAGIC and
# MAGIC mdm_client_id = 17742 -- BrownForman
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda
# MAGIC order by mdm_banner_id, response_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_asda_brownforman_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from retail_alert_asda_brownforman_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retaillink_asda_brownforman_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select mdm_banner_id, max(call_date) from
# MAGIC (WITH t AS (
# MAGIC     SELECT
# MAGIC         *
# MAGIC     FROM
# MAGIC         acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC     WHERE
# MAGIC         mdm_country_id = 30
# MAGIC         and mdm_client_id = 17742
# MAGIC         and mdm_holding_id = 3257
# MAGIC         -- and coalesce(mdm_banner_id, -1) = 7744
# MAGIC        -- and call_date >= '2023-12-17'
# MAGIC )
# MAGIC SELECT
# MAGIC     a.*
# MAGIC FROM
# MAGIC     t as a
# MAGIC     INNER JOIN (
# MAGIC         SELECT
# MAGIC             epos_organization_unit_num,
# MAGIC             epos_retailer_item_id,
# MAGIC             call_id,
# MAGIC             standard_response_text,
# MAGIC             count(*) AS count_responses,
# MAGIC             MIN(response_id) AS min_response_id
# MAGIC         FROM
# MAGIC             t
# MAGIC         GROUP BY
# MAGIC             epos_organization_unit_num,
# MAGIC             epos_retailer_item_id,
# MAGIC             call_id,
# MAGIC             standard_response_text
# MAGIC         HAVING
# MAGIC             count_responses > 1
# MAGIC     ) b ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC     AND a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC     AND a.call_id = b.call_id
# MAGIC     AND a.standard_response_text = b.standard_response_text
# MAGIC     AND a.response_id = b.min_response_id
# MAGIC UNION
# MAGIC SELECT
# MAGIC     c.*
# MAGIC FROM
# MAGIC     t as c
# MAGIC     INNER JOIN (
# MAGIC         SELECT
# MAGIC             epos_organization_unit_num,
# MAGIC             epos_retailer_item_id,
# MAGIC             call_id,
# MAGIC             standard_response_text,
# MAGIC             count(*) AS count_responses,
# MAGIC             MIN(response_id) AS min_response_id
# MAGIC         FROM
# MAGIC             t
# MAGIC         GROUP BY
# MAGIC             epos_organization_unit_num,
# MAGIC             epos_retailer_item_id,
# MAGIC             call_id,
# MAGIC             standard_response_text
# MAGIC         HAVING
# MAGIC             count_responses = 1
# MAGIC     ) d ON c.epos_organization_unit_num = d.epos_organization_unit_num
# MAGIC     AND c.epos_retailer_item_id = d.epos_retailer_item_id
# MAGIC     AND c.call_id = d.call_id
# MAGIC     AND c.standard_response_text = d.standard_response_text
# MAGIC ORDER BY
# MAGIC     epos_organization_unit_num,
# MAGIC     epos_retailer_item_id,
# MAGIC     call_id,
# MAGIC     standard_response_text,
# MAGIC     response_id)
# MAGIC group by mdm_banner_id

# COMMAND ----------


