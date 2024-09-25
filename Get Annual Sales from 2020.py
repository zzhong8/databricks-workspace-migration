# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.alerting.preprocessing.functions import get_pos_data
from acosta.measurement import required_columns, process_notebook_inputs

import acosta

print(acosta.__version__)

# COMMAND ----------

client_config = spark.sql(f'''
  SELECT epos_datavault_db_nm FROM acosta_retail_analytics_im.interventions_retailer_client_config
''')

# Create config dict
config_dict = client_config.toPandas()
pprint(config_dict)

# COMMAND ----------

for index, row in config_dict.iterrows():
    if index >= 4:
        if index 
  
        # Load POS data (with the nextgen processing function)
        df_pos = get_pos_data(row['epos_datavault_db_nm'], spark).repartition('SALES_DT')

        df_pos = df_pos.fillna({
            'ON_HAND_INVENTORY_QTY': 0,
            'RECENT_ON_HAND_INVENTORY_QTY': 0,
            'RECENT_ON_HAND_INVENTORY_DIFF': 0
        })

        df_pos = df_pos.where((pyf.col("SALES_DT") < pyf.lit("2021-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-01-01")))

        print(row['epos_datavault_db_nm'], df_pos.select("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").distinct().count(), df_pos.select('POS_ITEM_QTY').groupBy().sum().collect(), df_pos.select('POS_AMT').groupBy().sum().collect())

# COMMAND ----------

# # Load POS data (with the nextgen processing function)
# df_pos = get_pos_data(config_dict['epos_datavault_db_nm'], spark)

# df_pos = df_pos.fillna({
#     'ON_HAND_INVENTORY_QTY': 0,
#     'RECENT_ON_HAND_INVENTORY_QTY': 0,
#     'RECENT_ON_HAND_INVENTORY_DIFF': 0
# })

# COMMAND ----------

client_config2 = spark.sql(f'''
  SELECT alertgen_im_db_nm FROM acosta_retail_analytics_im.interventions_retailer_client_config
''')

# Create config dict
config_dict2 = client_config2.toPandas()
pprint(config_dict2)

# COMMAND ----------

client_config3 = spark.sql(f'''
  SELECT distinct mdm_holding_nm, mdm_holding_id, mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary
''')

# Create config dict
config_dict3 = client_config3.toPandas()
pprint(config_dict3)

# COMMAND ----------

country_id = 1  # US

banner_id = -1  # default

# COMMAND ----------

for index, row in config_dict3.iterrows():
  
    client_nm = row['mdm_client_nm'] # e.g. Minute Maid
    holding_nm = row['mdm_holding_nm'] # e.g. Walmart  
  
    client_id = row['mdm_client_id']
    holding_id = row['mdm_holding_id']
  
    df_sql_query_acosta_retail_analytics_im = """
        SELECT
        (
            SELECT
            COUNT(total_intervention_effect)
            FROM 
            acosta_retail_analytics_im.ds_intervention_summary
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2020-11%'
            )
        )
        as COUNT1, 
        
        (
            SELECT
            COUNT(total_intervention_effect)
            FROM 
            acosta_retail_analytics_im.ds_intervention_summary
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2020-12%'
            ) 
        )
        as COUNT2,

        (
            SELECT
            COUNT(total_intervention_effect)
            FROM 
            acosta_retail_analytics_im.ds_intervention_summary
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2021-01%'
            )
        )
        as COUNT3,
        
        (
            SELECT
            COUNT(total_intervention_effect)
            FROM 
            acosta_retail_analytics_im.ds_intervention_summary
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2021-02%'
            )
        )
        as COUNT4
    """.format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)
    
    df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)

    print(holding_nm, client_nm, df_acosta_retail_analytics_im.collect())

# COMMAND ----------

for index, row in config_dict3.iterrows():
  
    client_nm = row['mdm_client_nm'] # e.g. Minute Maid
    holding_nm = row['mdm_holding_nm'] # e.g. Walmart  
  
    client_id = row['mdm_client_id']
    holding_id = row['mdm_holding_id']
  
    df_sql_query_acosta_retail_analytics_im = """
        SELECT
        (
            SELECT
            COUNT(call_id)
            FROM 
            acosta_retail_analytics_im.vw_ds_intervention_input_nars
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2020-11%'
            )
        )
        as COUNT1, 
        
        (
            SELECT
            COUNT(call_id)
            FROM 
            acosta_retail_analytics_im.vw_ds_intervention_input_nars
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2020-12%'
            ) 
        )
        as COUNT2,

        (
            SELECT
            COUNT(call_id)
            FROM 
            acosta_retail_analytics_im.vw_ds_intervention_input_nars
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2021-01%'
            )
        )
        as COUNT3,
        
        (
            SELECT
            COUNT(call_id)
            FROM 
            acosta_retail_analytics_im.vw_ds_intervention_input_nars
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2021-02%'
            )
        )
        as COUNT4
    """.format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)
    
    df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)

    print(holding_nm, client_nm, df_acosta_retail_analytics_im.collect())

# COMMAND ----------

for index, row in config_dict3.iterrows():
  
    client_nm = row['mdm_client_nm'] # e.g. Minute Maid
    holding_nm = row['mdm_holding_nm'] # e.g. Walmart  
  
    client_id = row['mdm_client_id']
    holding_id = row['mdm_holding_id']
  
    df_sql_query_acosta_retail_analytics_im = """
        SELECT
        (
            SELECT
            COUNT(call_id)
            FROM 
            acosta_retail_analytics_im.vw_ds_intervention_input_nars
            WHERE
            mdm_country_id = {country_id} AND
            mdm_client_id = {client_id} AND
            mdm_holding_id = {holding_id} AND
            coalesce(mdm_banner_id, -1) = {banner_id} AND
            (
                call_date like '2021-03%'
            )
        )
        as COUNT5
    """.format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)
    
    df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)

    print(holding_nm, client_nm, df_acosta_retail_analytics_im.collect())

# COMMAND ----------


