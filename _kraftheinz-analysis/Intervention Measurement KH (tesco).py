# Databricks notebook source
import copy
import uuid
import numpy as np
import pandas as pd
from scipy import stats

import matplotlib.pyplot as graph
import seaborn as sns

import sklearn.linear_model as lm
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelBinarizer
# from sklearn_pandas import DataFrameMapper

from catboost import CatBoostRegressor, CatBoostClassifier

from pygam.utils import b_spline_basis

from datetime import timedelta
from time import time

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

from datetime import datetime as dtm

# Acosta.Alerting package imports
import acosta
from acosta.alerting.helpers import check_path_exists
from acosta.alerting.helpers.features import get_lag_column_name

print(acosta.__version__)
# print(pyarrow.__version__)

# from causalgraphicalmodels import CausalGraphicalModel

graph.style.use('fivethirtyeight')

def print_shape(a):
    print(' : '.join([f'{x:,}' for x in a.shape]))

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE  = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())

# COMMAND ----------

# MAGIC %md # Intervention Measurement (Kraft Heinz)

# COMMAND ----------

intervention_date_windows_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/kraft_heinz_intervention_date_windows.csv'
intervention_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/KHZ-All_Interventions_1819.csv'

check_path_exists(intervention_date_windows_path, 'csv', 'raise')

# COMMAND ----------

spark_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(intervention_date_windows_path)

# COMMAND ----------

cols = spark_df_info.columns

# cols

# COMMAND ----------

spark_df_intervention_info = spark.read.format('csv')\
    .options(header='false', inferSchema='true')\
    .load(intervention_path)

# COMMAND ----------

spark_df_intervention_info = spark_df_intervention_info.toDF(*cols)

# COMMAND ----------

# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
df_intervention_info = spark_df_intervention_info.toPandas()

# COMMAND ----------

# display(df_intervention_info.sample(5))

# COMMAND ----------

if ITEM:
    df_info = df_intervention_info[df_intervention_info['ProductNumber']==int(ITEM)]

# COMMAND ----------

df_info['Date&Time'] = pd.to_datetime(df_info['Date&Time'])
df_info['ChainRefExternal'] = pd.to_numeric(df_info['ChainRefExternal'])

# display(df_info.head())

# COMMAND ----------

# Tweak the diff day range for pack out
df_info = df_info[df_info['DiffDay'] >= -2]

df_info_pack_out = df_info[df_info['InterventionStnd'] == 'Product Pack-Out']

df_info_pack_out['DiffDay'] += 3
df_info_pack_out['Date&Time'] += timedelta(days=3)

df_info_pack_out_2 = df_info_pack_out.copy(deep=True)

df_info_pack_out_2['DiffDay'] += 3
df_info_pack_out_2['Date&Time'] += timedelta(days=3)

# COMMAND ----------

# do same but attach it to the dataframe
df_info['IsNegDiffDay'] = (df_info['DiffDay'] < 0).astype(int)
# df_info.loc[df_info['DiffDay'] < 0, 'DiffDay'] = 0

# do same but attach it to the dataframe
df_info_pack_out['IsNegDiffDay'] = (df_info_pack_out['DiffDay'] < 0).astype(int)
df_info_pack_out_2['IsNegDiffDay'] = (df_info_pack_out_2['DiffDay'] < 0).astype(int)

# COMMAND ----------

df_info = df_info.append(df_info_pack_out, ignore_index=True)

df_info = df_info.append(df_info_pack_out_2, ignore_index=True)

# COMMAND ----------

# PATHS (new!)
PATH_RESULTS_INPUT = '/mnt/processed/measurement_csv/retailer={retailer}/client={client}/country_code={country_code}/run_id={run_id}/retailer_item_id={retailer_item_id}'.format(
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE,
    run_id=RUN_ID,
    retailer_item_id=ITEM
)

print(PATH_RESULTS_INPUT)

# COMMAND ----------

# Filtering of data set
df_features = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT)

# COMMAND ----------

# display(df_features)

# COMMAND ----------

df = df_features.toPandas()

# COMMAND ----------

df['SALES_DT'] = pd.to_datetime(df['SALES_DT'])
df['ORGANIZATION_UNIT_NUM'] = pd.to_numeric(df['ORGANIZATION_UNIT_NUM'])

# Filter valid date range
print('Valid date range', df_info['Date&Time'].min(), df_info['Date&Time'].max())
df = df[(df['SALES_DT'] >= df_info['Date&Time'].min()) & (df['SALES_DT'] <= df_info['Date&Time'].max())]

# print_shape(df)

# COMMAND ----------

# df.info()

# COMMAND ----------

# df_info.info()

# COMMAND ----------

# Merge dataframes and (potentially) filter intervention type
df_merged = df.merge(df_info, left_on=['SALES_DT', 'ORGANIZATION_UNIT_NUM'], right_on=['Date&Time', 'ChainRefExternal'], how='left')
df_merged['is_intervention'] = (~df_merged['InterventionStnd'].isna()).astype(int)
df_merged.loc[df_merged['is_intervention'] == 0, 'InterventionStnd'] = 'None'

# df_merged.loc[df_merged['IsNegDiffDay'] == 1, 'InterventionStnd'] = 'None'
# df_merged.loc[df_merged['IsNegDiffDay'] == 1, 'DiffDay'] = -99

df_merged.loc[df_merged['DiffDay'].isna(), 'DiffDay'] = -99

# Remove all other kinds of interventions
# df_merged = df_merged[df_merged['InterventionStnd'].isin(set(['Display - Local', 'Display - Corporate', 'None']))]

# df_merged = df_merged[~df_merged['InterventionStnd'].isin(set(['Product Pack-Out']))]

display(df_merged[['SALES_DT', 'ORGANIZATION_UNIT_NUM', 'DiffDay', 'InterventionStnd', 'is_intervention', 'POS_ITEM_QTY']].sample(10))
print_shape(df_merged)
# [['SALES_DT', 'ORGANIZATION_UNIT_NUM', 'Date&Time', 'ChainRefExternal', 'POS_ITEM_QTY', 'DiffDay', 'InterventionStnd']]

# COMMAND ----------

df_merged_product_pack_out = df_merged[df_merged['InterventionStnd'].isin(set(['Product Pack-Out']))]

# COMMAND ----------

df_merged_product_pack_out_negdiffday = df_merged_product_pack_out[df_merged_product_pack_out['DiffDay'] < 0]
df_merged_product_pack_out_negdiffday = df_merged_product_pack_out_negdiffday[df_merged_product_pack_out_negdiffday['DiffDay'] > -99]

# COMMAND ----------

# MAGIC %md # Fit Causal Models
# MAGIC
# MAGIC NOTE: Try `Display - Corporate` and `Display - Local`

# COMMAND ----------

# Setup
cat_features = ['DOW', 'ORGANIZATION_UNIT_NUM']
model_kwargs = dict(
    iterations=10000,
    learning_rate=0.5,
    loss_function='RMSE',
    cat_features=cat_features,
    use_best_model=True,
    early_stopping_rounds=50,
    verbose=False
)
model_propensity_kwargs = copy.deepcopy(model_kwargs)
model_propensity_kwargs['loss_function'] = 'CrossEntropy'

lags_to_include = [f'LAG_UNITS_{i}' for i in range(1, 8)]
lags_quesi_loess = [f'LAG_UNITS_{i * 7}' for i in range(1, 4)]
dynamic_intercepts = [c for c in df.columns if 'TIME_OF_YEAR' in c]

predictor_cols = []
# predictor_cols += ['RECENT_ON_HAND_INVENTORY_QTY', 'PRICE', 'SNAPINDEX', 'NONSNAPINDEX']
predictor_cols += ['ORGANIZATION_UNIT_NUM']
# predictor_cols += lags_quesi_loess
predictor_cols += dynamic_intercepts
predictor_cols += ['WEEK_SEASONALITY', 'YEAR_SEASONALITY']
predictor_cols += ['DOW']
predictor_cols += ['IsNegDiffDay']
# predictor_cols += lags_to_include[:-1]

# df_mapper = {x: None for x in predictor_cols}
# for feat in cat_features:
#     df_mapper[feat] = LabelBinarizer(sparse_output=True)

# display(list(df_mapper.items()))
# df_mapper = DataFrameMapper(list(df_mapper.items()), sparse=True)
# print(df_mapper)

# COMMAND ----------

# MAGIC %md ## S Multitask Learner
# MAGIC
# MAGIC The causal diagram shows we can make an S learner that controls for every single intervention type

# COMMAND ----------

x_train, x_val, y_train, y_val = train_test_split(
    df_merged[predictor_cols + ['InterventionStnd']], df_merged['POS_ITEM_QTY'], 
    test_size=0.1, 
    stratify=df_merged['ORGANIZATION_UNIT_NUM']
#     stratify=df_merged[['InterventionStnd', 'ORGANIZATION_UNIT_NUM']]
)

# print_shape(x_train)
# print_shape(x_val)

# COMMAND ----------

s_multi_model_kwargs = copy.deepcopy(model_kwargs)
s_multi_model_kwargs['cat_features'] += ['InterventionStnd']
# display(s_multi_model_kwargs)

# Model fitting
fs_multi_model = CatBoostRegressor(**s_multi_model_kwargs)
fs_multi_model.fit(x_train, y_train, eval_set=(x_val, y_val))
fs_multi_model.shrink(fs_multi_model.best_iteration_)

# COMMAND ----------

# graph.figure(figsize=(8, 8))
# graph.barh(x_train.columns.tolist(), fs_multi_model.feature_importances_)
# graph.xlabel('Feature Importance')
# graph.show()

# COMMAND ----------

# Compute results
x_baseline = df_merged[predictor_cols + ['InterventionStnd']].copy()
x_baseline['InterventionStnd'] = 'None'

intervention_dict = {}
for intervention_type in df_merged['InterventionStnd'].unique():
    if intervention_type == 'None':
        continue
    df_i = df_merged[predictor_cols + ['InterventionStnd']].copy()
    df_i['InterventionStnd'] = intervention_type
    intervention_dict[intervention_type] = df_i
    
# Create results csv
df_results = df_merged[['SALES_DT', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY', 'CallfileVisitId', 'is_intervention', 'DiffDay', 'InterventionStnd']]
df_results['baseline'] = np.maximum(fs_multi_model.predict(x_baseline), 0)
df_results['expected'] = np.maximum(fs_multi_model.predict(df_merged[predictor_cols + ['InterventionStnd']]), 0)
df_results['counterfactual'] = np.nan
                                 
for intervention_type, x_intervention in intervention_dict.items():
    intervention_type_as_str = intervention_type.lower().replace('-', '').replace('  ', ' ')
    raw_ite_col_name = f'raw_ite_{intervention_type_as_str}'
    ie_col_name = f'intervention_effect_{intervention_type_as_str}'
    
    df_results[raw_ite_col_name] = np.maximum(fs_multi_model.predict(x_intervention), 0) - df_results['baseline']
    df_results[ie_col_name] = np.maximum(df_results[raw_ite_col_name] * (df_results['InterventionStnd'] == intervention_type), 0)
                                 
# results.to_csv(f'cate-s-multitask-kraft-heinz-item-{df_info["ProductNumber"]}.csv', index=False)

# PATHS (new!)
PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-tesco-intervention-measurement-results'

# COMMAND ----------

results = spark.createDataFrame(df_results)

# COMMAND ----------

results.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('{}/retailer_item_id={}'.format(PATH_RESULTS_OUTPUT, ITEM))               
               
# display(df_results.query('is_intervention == 0').head(20))
# display(df_results.query('is_intervention == 1').head(20))

# COMMAND ----------

# for name, dfi in df_results.groupby('InterventionStnd'):
#     dfi = dfi.query('is_intervention == 1')
    
#     try:
#         graph.figure(figsize=(12, 5))
#         graph.title(name.title())
#         sns.pointplot(x=dfi['DiffDay'], y=dfi['expected'] - dfi['baseline'], label='Intervention')
#         graph.axhline(0, linestyle='--', color='black')
#         graph.show()

#     except ValueError:
#         continue

# COMMAND ----------

# # intervention_date_windows_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/kraft_heinz_intervention_date_windows.csv'
# # intervention_path = '/mnt/artifacts/hugh/kraftheinz-intervention-results/KHZ-All_Interventions_1819.csv'

# # PATHS (new!)
# PATH_RESULTS_OUTPUT = '/mnt/processed/pos_csv/retailer={retailer}/client={client}/country_code={country_code}/run_id={run_id}'.format(
#     retailer=RETAILER,
#     client=CLIENT,
#     country_code=COUNTRY_CODE,
#     run_id=RUN_ID
# )

# results.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('{}/retailer_item_id={}'.format(PATH_RESULTS_OUTPUT, ITEM))

# # results.to_csv(f'cate-s-multitask-kraft-heinz-item-{df_info["ProductNumber"].unique()[0]}.csv', index=False)
