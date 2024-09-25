# Databricks notebook source
import numpy as np
import pandas as pd 
from scipy import stats

import matplotlib.pyplot as graph
import seaborn as sns

import datetime as dt
import os

from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf

# Acosta.Alerting package imports
import acosta
from acosta.alerting.helpers import check_path_exists

# COMMAND ----------

def print_shape(a):
    print(' : '.join([f'{x:,}' for x in a.shape]))

# COMMAND ----------

# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_asda.csv'
product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_morrisons.csv'

check_path_exists(product_number_list_path, 'csv', 'raise')

product_number_list_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(product_number_list_path)

product_number_list = product_number_list_df_info.select('ProductNumber')

product_numbers_1 = product_number_list.rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_80%+_asda.csv'
product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_58%+_morrisons.csv'

check_path_exists(product_number_list_path, 'csv', 'raise')

product_number_list_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(product_number_list_path)

product_number_list = product_number_list_df_info.select('ProductNumber')

product_numbers_2 = product_number_list.rdd.flatMap(lambda x: x).collect()

product_numbers = product_numbers_1 + product_numbers_2

# COMMAND ----------

print(product_numbers)

# COMMAND ----------

# # PATHS (new!)
# PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-tesco-intervention-measurement-results'

# results = spark.createDataFrame(df_results)

# results.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('{}/retailer_item_id={}'.format(PATH_RESULTS_OUTPUT, product_number))

# COMMAND ----------

# PATH_RESULTS_INPUT = '/mnt/artifacts/hugh/kraftheinz-asda-intervention-measurement-results'
PATH_RESULTS_INPUT = '/mnt/artifacts/hugh/kraftheinz-morrisons-intervention-measurement-results'

dfs = []
for product_number in product_numbers:
    file_load_path = '{}/retailer_item_id={}'.format(PATH_RESULTS_INPUT, product_number)
  
    # If cate intervention measurements have been computed for this product
    if check_path_exists(file_load_path, 'csv', 'ignore'):  
        print(product_number)

        spark_df_raw = spark.read.format('csv').option('header', 'true').load(file_load_path)

        df_raw = spark_df_raw.toPandas()

        columns_to_sum = []
        
        candidate_columns_to_sum = ['intervention_effect_display local',
                                    'intervention_effect_display corporate',
                                    'intervention_effect_placed pos',
                                    'intervention_effect_order placed',
                                    'intervention_effect_inventory correction',
                                    'intervention_effect_product packout',
                                    'intervention_effect_shelf changes']

        for candidate_column_to_sum in candidate_columns_to_sum:
            if candidate_column_to_sum in df_raw.columns:
                df_raw[candidate_column_to_sum] = pd.to_numeric(df_raw[candidate_column_to_sum])
                
                columns_to_sum.append(candidate_column_to_sum)
                
        df_raw['intervention_effect'] = df_raw[columns_to_sum].sum(axis=1)        

        columns_to_drop = ['CallfileVisitId',
                           'InterventionStnd', 
                           'DiffDay', 
                           'baseline',
                           'expected', 
                           'counterfactual', 
                           'raw_ite_inventory correction',
                           'raw_ite_display local',
                           'raw_ite_order placed',
                           'raw_ite_display corporate',
                           'raw_ite_product packout',
                           'raw_ite_placed pos',
                           'raw_ite_shelf changes',
                           'intervention_effect_display local',
                           'intervention_effect_display corporate',
                           'intervention_effect_placed pos',
                           'intervention_effect_order placed',
                           'intervention_effect_inventory correction',
                           'intervention_effect_product packout',
                           'intervention_effect_shelf changes']

        for column_to_drop in columns_to_drop:
            if column_to_drop in df_raw.columns:
                df_raw = df_raw.drop(column_to_drop, axis = 1)

        for column in df_raw.columns:
            if (column != 'SALES_DT'):
                df_raw[column] = pd.to_numeric(df_raw[column])
            
        df_raw['SALES_DT'] = pd.to_datetime(df_raw['SALES_DT']).dt.date
          
        df = df_raw
        
        df_pivot_table = df.groupby(['SALES_DT', 'ORGANIZATION_UNIT_NUM', 'is_intervention']).sum()

#         col_list = list(df_pivot_table)
#         col_list.remove('POS_ITEM_QTY')

#         df_pivot_table['intervention_effect'] = df_pivot_table[col_list].sum(axis=1)

        df_output = df_pivot_table[['intervention_effect', 'POS_ITEM_QTY']]
        df_output['product_number'] = product_number
        dfs.append(df_output)
        
#         break;

dfs = pd.concat(dfs)

dfs.reset_index(drop=False, inplace=True)

# PATHS (new!)
# PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-tesco-intervention-measurement-summary'
# PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-asda-intervention-measurement-by-store'
PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-morrisons-intervention-measurement-by-store'

results = spark.createDataFrame(dfs)

display(results)

# COMMAND ----------

results.count()

# COMMAND ----------

results = results.where((col("SALES_DT") < lit("2019-07-01")) & (col("SALES_DT") >= lit("2018-07-01")))

results_filtered = results.select('ORGANIZATION_UNIT_NUM', 'is_intervention')

# COMMAND ----------

df_valid_stores_pivot_table = results_filtered.groupby(['ORGANIZATION_UNIT_NUM']).sum()

df_valid_stores_pivot_table.count()

# COMMAND ----------

df_valid_stores_pivot_table =  df_valid_stores_pivot_table.where(col("sum(is_intervention)") > 0)

df_valid_stores_pivot_table.count()

# COMMAND ----------

df_valid_stores_pivot_table = df_valid_stores_pivot_table[['ORGANIZATION_UNIT_NUM']]

# COMMAND ----------

results = results.join(
    df_valid_stores_pivot_table,
    ['ORGANIZATION_UNIT_NUM'],
    'inner'
)

results.count()

# COMMAND ----------

results = results.withColumn('year', year('SALES_DT'))
results = results.withColumn('month', month('SALES_DT'))

results = results.drop('SALES_DT')

results.columns

# COMMAND ----------

results = results.withColumn('retailer', lit('tesco'))

# COMMAND ----------

results.count()

# COMMAND ----------

aggregated_results = results.groupby(['ORGANIZATION_UNIT_NUM', 'is_intervention', 'product_number', 'year', 'month', 'retailer']).sum('intervention_effect', 'POS_ITEM_QTY')

# COMMAND ----------

display(aggregated_results)

# COMMAND ----------

aggregated_results.count()

# COMMAND ----------

aggregated_results.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(PATH_RESULTS_OUTPUT)

# COMMAND ----------


