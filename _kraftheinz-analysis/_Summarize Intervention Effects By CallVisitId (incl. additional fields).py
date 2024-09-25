# Databricks notebook source
import numpy as np
import pandas as pd 
from scipy import stats

import matplotlib.pyplot as graph
import seaborn as sns

import datetime as dt
import os

# Acosta.Alerting package imports
import acosta
from acosta.alerting.helpers import check_path_exists

# COMMAND ----------

def print_shape(a):
    print(' : '.join([f'{x:,}' for x in a.shape]))

# COMMAND ----------

# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_tesco.csv'
product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_morrisons.csv'

check_path_exists(product_number_list_path, 'csv', 'raise')

product_number_list_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(product_number_list_path)

product_number_list = product_number_list_df_info.select('ProductNumber')

product_numbers = product_number_list.rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_80%+_tesco.csv'

# check_path_exists(product_number_list_path, 'csv', 'raise')

# product_number_list_df_info = spark.read.format('csv')\
#     .options(header='true', inferSchema='true')\
#     .load(product_number_list_path)

# product_number_list = product_number_list_df_info.select('ProductNumber')

# product_numbers_2 = product_number_list.rdd.flatMap(lambda x: x).collect()

# product_numbers = product_numbers_1 + product_numbers_2

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

# PATH_RESULTS_INPUT = '/mnt/artifacts/hugh/kraftheinz-tesco-intervention-measurement-results'
PATH_RESULTS_INPUT = '/mnt/artifacts/hugh/kraftheinz-morrisons-intervention-measurement-results'

dfs = []
for product_number in product_numbers:
    file_load_path = '{}/retailer_item_id={}'.format(PATH_RESULTS_INPUT, product_number)
  
    # If cate intervention measurements have been computed for this product
    if check_path_exists(file_load_path, 'csv', 'ignore'):  
        print(product_number)

        spark_df_raw = spark.read.format('csv').option('header', 'true').load(file_load_path)

        df_raw = spark_df_raw.toPandas()

        columns_to_drop = ['ORGANIZATION_UNIT_NUM',
                           'CallfileVisitId',
                           'is_intervention', 
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
                           'raw_ite_shelf changes']

        for column_to_drop in columns_to_drop:
            if column_to_drop in df_raw.columns:
                df_raw = df_raw.drop(column_to_drop, axis = 1)

        for column in df_raw.columns:
            if (column != 'SALES_DT' and column != 'InterventionStnd'):
                df_raw[column] = pd.to_numeric(df_raw[column])
            
        df_raw['SALES_DT'] = pd.to_datetime(df_raw['SALES_DT']).dt.date
          
        df = df_raw
        
        df_pivot_table = df.groupby(['SALES_DT', 'InterventionStnd']).sum()

        col_list = list(df_pivot_table)
        col_list.remove('POS_ITEM_QTY')

        df_pivot_table['intervention_effect'] = df_pivot_table[col_list].sum(axis=1)

        df_output = df_pivot_table[['intervention_effect', 'POS_ITEM_QTY']]
        df_output['product_number'] = product_number
        dfs.append(df_output)

dfs = pd.concat(dfs)

dfs.reset_index(drop=False, inplace=True)

# PATHS (new!)
# PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-tesco-intervention-measurement-summary'
PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-morrisons-intervention-measurement-summary'

results = spark.createDataFrame(dfs)
results.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(PATH_RESULTS_OUTPUT)

# COMMAND ----------


