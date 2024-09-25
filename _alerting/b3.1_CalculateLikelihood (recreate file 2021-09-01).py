# Databricks notebook source
import uuid
import warnings

import numpy as np
import numpy.linalg as la
import pandas as pd
from scipy import stats
import datetime

# import pysftp

import pyspark.sql.functions as pyf
from pyspark.sql import Window
import pyspark.sql.types as pyt

from sklearn.metrics import r2_score, mean_squared_error
from sklearn.utils import resample

from acosta.alerting.helpers import universal_encoder, universal_decoder
from acosta.alerting.preprocessing.functions import get_params_from_database_name

from expectation.functions import get_pos, pivot_pos, resample_to_weekly_data
   
    
def join_matched_stores_to_pos(df_matched_stores, df_input, frequency):
    """
    Example of use
    >>> wow = join_matched_stores_to_pos(df_similar_stores, df_full_pos, 'weekly')
    """
    print('pivot_pos running')
    df_pivot = pivot_pos(
        df_input,
        frequency,
        2
    )
    
    print('joining matched stores with POS')
    df_matched_stores = df_matched_stores.join(
        df_pivot,
        (df_matched_stores['ORGANIZATION_UNIT_NUM']==df_pivot['ORGANIZATION_UNIT_NUM']) \
        & (df_matched_stores['RETAILER_ITEM_ID']==df_pivot['RETAILER_ITEM_ID']),
        how='left'
    ).drop(
        df_pivot['RETAILER_ITEM_ID']
    ).drop(
        df_pivot['ORGANIZATION_UNIT_NUM']
    )

    return df_matched_stores
  
    
def parse_widget_or_raise(key, raise_error=True):
    x = str(dbutils.widgets.get(key)).strip().lower()
    if raise_error and x == '':
        raise ValueError(f'Could not find key {key}')
    else:
        return x

# COMMAND ----------

dbutils.widgets.text('database', 'market6_kroger_danonewave_us_dv', 'Database Name')
dbutils.widgets.dropdown('frequency', 'daily', ['daily', 'weekly'], 'Frequency')
dbutils.widgets.text('sftp_folder', 'CORE TEAM TEST', 'SFTP Folder')
dbutils.widgets.dropdown('logic', 'A(LSV)', ['A(LSV)', 'B(ZSCORE&LSV)'], 'Logic')
dbutils.widgets.dropdown('cap_lsv', 'YES', ['YES', 'NO'], 'CAP LSV')
dbutils.widgets.dropdown('split_run_test', 'NO', ['YES', 'NO'], 'Split-Run Testing')
dbutils.widgets.dropdown('save_only_on_blob_storage', 'NO', ['YES', 'NO'], 'Save Only on Blob Storage')
dbutils.widgets.dropdown('include_inv_cleanup_alerts', 'NO', ['YES', 'NO'], 'Inventory Cleanup Alerts')

dbutils.widgets.text('output_filename', '' , 'Output Filename')


required_inputs = ('database', 'frequency', 'sftp_folder', 
                   'logic', 'cap_lsv', 'split_run_test', 
                   'save_only_on_blob_storage', 'output_filename',
                  'include_inv_cleanup_alerts')

database_name, frequency, sftp_folder, logic, cap_lsv,\
split_run_test, save_only_on_blob_storage, output_filename, include_inv_cleanup_alerts  = [parse_widget_or_raise(key) for key in required_inputs]

source, country_code, client, retailer = get_params_from_database_name(database_name).values()

#TODO: Get data from database on this dictionary - or get them from widgets as inputs
config_dict = {
    'nestlewaters': {'company_id': 567, 'parent_id': 950, 'manufacturer_id': 199, 'client_id': 13429},
    'danonewave': {'company_id': 603, 'parent_id': 955, 'manufacturer_id': None, 'client_id': 882},
    'nestlecore': {'company_id': 609, 'parent_id': 16, 'manufacturer_id': None, 'client_id': 16320}
}

print(client)
print(country_code)
print(retailer)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load and Preprocess Data 

# COMMAND ----------

#TODO: if the file not exist (new client) run the other notebook first
df_store_matches = spark.read.format('delta')\
    .load(f'/mnt/processed/alerting/cache/{client}-{country_code}-{retailer}-matched_stores/') 
df_store_matches = df_store_matches.withColumn('ORGANIZATION_UNIT_NUM', pyf.col('ORGANIZATION_UNIT_NUM'))
print(f'N = {df_store_matches.cache().count():,}')

# COMMAND ----------

#filter out uncovered kroger stores
def get_org_unit_num(sql_context, company_id, parent_id):
    """
    For kroger only
    """
    df_chain = sql_context.sql(
        f'select ChainId from BOBv2.chain WHERE parentchainid = {parent_id}'
    )
    df_outlet = sql_context.sql('select * from BOBv2.outlet')
    df_outlet = df_outlet.join(
        df_chain,
        ['ChainId'],
        'left_semi'
    ).select('OutletId', 'ChainRefExternal')    
    df = sql_context.sql('select * from BOBv2.vw_BOBv2_DailyCallfileVisit')
    df = df.filter(f'CompanyId == {company_id}')
    df = df.join(df_outlet, ['OutletId'])
    return df


if retailer == 'kroger':
    df_org_num = get_org_unit_num(
      spark, 
      config_dict[client]['company_id'],
      config_dict[client]['parent_id'],
    )
     
    print(f'N = {df_org_num.cache().count():,}')


    df_store_matches = df_store_matches.join(df_org_num ,
          df_store_matches['TEST_ORGANIZATION_UNIT_NUM'] == df_org_num['ChainRefExternal'],
          how = 'left_semi')

    print(f'N = {df_store_matches.cache().count():,}')

    
if retailer == 'tesco' and client == 'nestlecore':
    df_valid_stores = spark.read\
        .options(header=True, inferSchema=True)\
        .csv('/mnt/processed/alerting/fieldTest/covered_stores/Tesco_dimension_store.csv')\
        .withColumn('ORGANIZATION_UNIT_NUM', pyf.col('StoreID'))
    
    print(f'N = {df_valid_stores.cache().count():,}')


    df_store_matches = df_store_matches.join(df_valid_stores ,
          df_store_matches['TEST_ORGANIZATION_UNIT_NUM'] == df_valid_stores['ORGANIZATION_UNIT_NUM'],
          how = 'left_semi')
    
    print(f'N = {df_store_matches.cache().count():,}')

# COMMAND ----------

df_full_pos = get_pos(database_name, spark, n_days = 90)

# Check if the data is not older than one day
max_date = df_full_pos.agg({'SALES_DT': 'max'}).collect()[0][0]

# !!NOTE: datetime.date.today() returns the current UTC date and time ,it is better to covert all times to local time to aviod date issues if you want to run jobs after 7:00 pm EST
if max_date < (datetime.date.today() - datetime.timedelta(days=1)):
    raise ValueError(f'The maximum date is: {max_date} and Today date is {datetime.date.today()}. The last POS data is older than one day.')

print(f'N = {df_full_pos.cache().count():,}')

# COMMAND ----------

active_pairs_inPastTwoMonths = df_full_pos\
        .groupBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'])\
        .agg(pyf.max('SALES_DT').alias('Last_sale'))\
        .filter(pyf.col('Last_sale') > pyf.date_sub(pyf.current_date(), 60))

print(f'N = {active_pairs_inPastTwoMonths.cache().count():,}')

df_full_pos = df_full_pos.join(active_pairs_inPastTwoMonths ,
       ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'],
        how = 'left_semi')
print(f'N = {df_full_pos.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Deal with Null Sales

# COMMAND ----------

def add_missing_dates(df):
    
    df['SALES_DT'] = pd.to_datetime(df['SALES_DT'])
    df.index = df['SALES_DT'] 
    ix = pd.date_range(start = df['SALES_DT'].min(), end = max_date) #pd.to_datetime('today') - pd.Timedelta(1, unit='D') 
    df = df.reindex(ix) 
    df['SALES_DT'] = df.index
    
    fillna_dict = {}
    cols = ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID']
    for col in cols:
        fillna_dict[col] = df[col].mode().iloc[0]
    df = df.fillna(fillna_dict)

    return df


def count_trailing_nulls(array: np.ndarray):
    reversed_array = array[::-1]

    n = np.argmax(~np.isnan(reversed_array))
    if n == 0 and np.all(np.isnan(reversed_array)):
        n = len(reversed_array)
    return n


def count_trailing_nulls_vectorized(array: np.ndarray):
    result = []
    for i, _ in enumerate(array):
        if i == 0:
            result.append(1 if np.isnan(array[i]) else 0)
        else:
            result.append(count_trailing_nulls(array[:i+1]))
        
    return np.array(result)


def count_max_trailing_nulls_vectorized(array: np.ndarray):
    result = count_trailing_nulls_vectorized(array)
    result_max_null_days= []
    for i, n in enumerate(result[:-1]):
        if n == 0:
            result_max_null_days.append(n)
        elif result[i+1] > result[i]:
            pass
        else:
            result_max_null_days.append(n)
    
    result_max_null_days.append(result[-1])
        
    return np.array(result_max_null_days)

# COMMAND ----------

def inactive_items_check(dfi):
    dfi = add_missing_dates(dfi)
    dfi['INACTIVE_CHECK'] = False
        
    for col in ['POS_ITEM_QTY']:
        dfi[col] = pd.to_numeric(dfi[col] , downcast='float')
        
    if np.isnan(dfi['POS_ITEM_QTY'][-61:-1]).all():
        dfi['INACTIVE_CHECK'] = True

    elif np.isnan(dfi['POS_ITEM_QTY'][-15:-1]).all():

        nulls = count_max_trailing_nulls_vectorized(dfi['POS_ITEM_QTY'])        
        last = nulls[-1]
        nulls = nulls[:-1]
        max_nulls_in_row, std_nulls = (max(nulls),  np.std(nulls))

        inactive_thrsh = max_nulls_in_row + std_nulls   
        if last > inactive_thrsh:
            dfi['INACTIVE_CHECK'] = True
    
    return dfi[schema_cols]

# COMMAND ----------

if retailer == 'kroger':  
    schema = pyt.StructType([
        pyt.StructField('SALES_DT', pyt.DateType()),
        pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.StringType()),
        pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
        pyt.StructField('INACTIVE_CHECK', pyt.BooleanType()),
        ])
    schema_cols = [col.name for col in schema]
    print(schema_cols)

    #Inactive items
    df = df_full_pos.groupby('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').applyInPandas(
        inactive_items_check,
        schema=schema
    )
    print(f'N results = {df.cache().count():,}')
    
    df = df.filter(pyf.col('INACTIVE_CHECK') == False)
    print(f'N actives = {df.cache().count():,}')
    
    df = df.join(
        df_full_pos,
        ['SALES_DT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
        how='left'
    ).drop(df_full_pos['ORGANIZATION_UNIT_NUM'])\
    .drop(df_full_pos['RETAILER_ITEM_ID'])\
    .drop(df_full_pos['SALES_DT'])\
    .drop(df_full_pos['YEAR_WEEK'])
    
    df = resample_to_weekly_data(df, 'SALES_DT', spark)

    print(f'N = {df.cache().count():,}')
    
    #condition 1: finding unknown dates
    unique_date = df.groupby('SALES_DT').count()
    
    df_stats = unique_date.select(
        pyf.mean(pyf.col('COUNT')).alias('mean'),
        pyf.stddev(pyf.col('COUNT')).alias('std')
    ).collect()

    mean = df_stats[0]['mean']
    std = df_stats[0]['std']
    threshold = int(mean - 2*(std))
    
    unique_date = unique_date.withColumn('DATE_CHECK', \
                          pyf.when(unique_date['COUNT'] <= threshold, True) \
                          .otherwise(False))
    
    
    #condition 2: the number of item per store
    unique_item = df.groupBy(
        ['SALES_DT', 'ORGANIZATION_UNIT_NUM'])\
    .agg({'RETAILER_ITEM_ID': 'count'})
    
    def stddev_pop_w(col, w):
        """Builtin stddev doesn't support windowing
        You can easily implement sample variant as well
        """
        return pyf.sqrt(pyf.avg(col * col).over(w) - pyf.pow(pyf.avg(col).over(w), 2))

    w = Window().partitionBy('ORGANIZATION_UNIT_NUM')
    unique_item= (unique_item
        .withColumn('stddev', stddev_pop_w(pyf.col("count(RETAILER_ITEM_ID)"), w))
        .withColumn('mean', pyf.avg("count(RETAILER_ITEM_ID)").over(w)))
    
    
    unique_item = unique_item.withColumn('ITEM_CHECK',\
                          pyf.when(unique_item['count(RETAILER_ITEM_ID)'] <= (unique_item['mean']-2*unique_item['stddev']), True) \
                          .otherwise(False))
    
    
    #Condition 3: number of store per item
    unique_store = df.groupBy(['SALES_DT', 'RETAILER_ITEM_ID']).agg({'ORGANIZATION_UNIT_NUM': 'count'})
    
    
    w = Window().partitionBy('RETAILER_ITEM_ID')
    unique_store= (unique_store
        .withColumn('stddev', stddev_pop_w(pyf.col('count(ORGANIZATION_UNIT_NUM)'), w))
        .withColumn('mean', pyf.avg('count(ORGANIZATION_UNIT_NUM)').over(w)))
    
    unique_store = unique_store.withColumn('STORE_CHECK', \
                              pyf.when(unique_store['count(ORGANIZATION_UNIT_NUM)'] <= (unique_store['mean']-2*unique_store['stddev']), True) \
                              .otherwise(False))
    
    df = df.join(
        unique_date,
        ['SALES_DT'],
        how='left'
    ).drop(unique_date['count'])

    df = df.join(
        unique_item,
        ['SALES_DT','ORGANIZATION_UNIT_NUM'],
        how='left'
    ).drop(unique_item['count(RETAILER_ITEM_ID)'])\
    .drop(unique_item['stddev'])\
    .drop(unique_item['mean'])

    df = df.join(
        unique_store,
        ['SALES_DT','RETAILER_ITEM_ID'],
        how='left'
    ).drop(unique_store['count(ORGANIZATION_UNIT_NUM)'])\
    .drop(unique_store['stddev'])\
    .drop(unique_store['mean'])
    
    df = df.withColumn('DATA_QUALITY',\
                              pyf.when(((pyf.col('INACTIVE_CHECK')==False) & (pyf.col('DATE_CHECK')==False)\
                                        & (pyf.col('ITEM_CHECK')==False) & (pyf.col('STORE_CHECK')==False)), True)\
                              .otherwise(False)
                      )

    df = df.withColumn('POS_AMT', pyf.when((df.POS_AMT.isNull() & df['DATA_QUALITY']==True), 0).otherwise(df.POS_AMT))
    df = df.withColumn('POS_ITEM_QTY', pyf.when((df.POS_ITEM_QTY.isNull() & df['DATA_QUALITY']==True), 0).otherwise(df.POS_ITEM_QTY))
    
    
    schema = pyt.StructType([
        pyt.StructField('SALES_DT', pyt.DateType()),
        pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.StringType()),
        pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
        pyt.StructField('POS_ITEM_QTY', pyt.FloatType()),
        pyt.StructField('POS_AMT', pyt.FloatType()),
        pyt.StructField('PRICE', pyt.FloatType()),
        pyt.StructField('ON_HAND_INVENTORY_QTY', pyt.IntegerType()),
        pyt.StructField('YEAR_WEEK', pyt.DateType()),

    ])

    schema_cols = [col.name for col in schema]
    print(schema_cols)

    def distributed_data_quality_check(dfi):    
        # check 1 to 1
        hash_keys = {col: dfi[col].unique()[0] for col in (['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'])}

        # Coerce all date columns
        for col in ['PRICE', 'ON_HAND_INVENTORY_QTY', 'POS_ITEM_QTY', 'POS_AMT']:
            dfi[col] = pd.to_numeric(dfi[col] , downcast='float')


        dfi = dfi.sort_values(by = 'SALES_DT')
        dfi['PRICE'] = np.where(pd.isnull(dfi.PRICE) &  dfi.DATA_QUALITY==True, dfi['PRICE'].ffill(), dfi['PRICE'])
        #define a variable that calculate invetory quantity by the end of the day
        dfi['PM_INVENTORY_QTY'] = dfi['ON_HAND_INVENTORY_QTY'] - dfi['POS_ITEM_QTY']
        dfi['PM_INVENTORY_QTY'] = dfi[['PM_INVENTORY_QTY']].apply(lambda x: x.ffill())
        dfi['ON_HAND_INVENTORY_QTY'] = np.where(pd.isnull(dfi.ON_HAND_INVENTORY_QTY) & dfi.DATA_QUALITY==True, dfi['PM_INVENTORY_QTY'], dfi['ON_HAND_INVENTORY_QTY'])

        # Set ID columns
        for col, val in hash_keys.items():
            dfi[col] = val

        # Check all required columns exist
        for col in schema_cols:
            assert col in dfi.columns, f'Required column {col} not in dfi'

        return dfi[schema_cols]

    df_full_pos = df.groupby('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').applyInPandas(
        distributed_data_quality_check,
        schema=schema
    )
    print(f'N results = {df_full_pos.cache().count():,}')

# COMMAND ----------

df_pos = join_matched_stores_to_pos(df_store_matches, df_full_pos, frequency)
print(f'N = {df_pos.cache().count():,}')

# COMMAND ----------

# Filter out combinations that will cause index errors
label_data_as_safe_schema = pyt.StructType(
    list(df_pos.schema) + [pyt.StructField('is_safe', pyt.BooleanType())]
)
def label_data_as_safe_udf(dfi):
    assert dfi['RETAILER_ITEM_ID'].nunique() == 1, 'Check groupby code' + assertion_message
    assert dfi['TEST_ORGANIZATION_UNIT_NUM'].nunique() == 1, 'Check data preprocessing process' + assertion_message
    
    test_store_num = dfi['TEST_ORGANIZATION_UNIT_NUM'].unique()[0]
    n_stores = dfi['ORGANIZATION_UNIT_NUM'].nunique()
    
    n_store_cond_passed = n_stores > 1
    test_store_is_present = test_store_num in set(dfi['ORGANIZATION_UNIT_NUM'].values)
    # test_store_is_present = dfi['ORGANIZATION_UNIT_NUM'].isin(test_store).any()
    
    is_safe_test_store = n_store_cond_passed and test_store_is_present

    # Process data
    data = dfi.drop(columns=['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'DISTANCE'])
    data = data.sort_values(by='ORGANIZATION_UNIT_NUM')
    data = data.dropna(axis='columns', how='all')
    data = data.fillna(0)
    data.index = data['ORGANIZATION_UNIT_NUM']
    
    # Check if is safe
    is_safe_data = True
    try:
        x = data.drop(columns='ORGANIZATION_UNIT_NUM', index=test_store_num).values.T
        y = data.drop(columns='ORGANIZATION_UNIT_NUM').loc[test_store_num].values
        last_obs = y[-1]
    except:
        is_safe_data = False
    
    # Add flag
    dfi['is_safe'] = is_safe_data and is_safe_test_store
    return dfi

# Run and filter
df_pos = df_pos.groupby('RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM').applyInPandas(label_data_as_safe_udf, schema=label_data_as_safe_schema)
print(f'N_1 = {df_pos.cache().count():,}')

df_filtered_out = df_pos.filter('is_safe == False')

df_pos = df_pos.filter('is_safe == True')
df_pos = df_pos.drop('is_safe')
print(f'N_f = {df_pos.cache().count():,}')

# COMMAND ----------

try:
    df_filtered_out.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', True)\
    .option('overwriteSchema', True)\
    .save(f'/mnt/processed/alerting/cache/{client}-{country_code}-{retailer}-{frequency}-debug-filtered-out')
except:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC # Estimate Probabilities

# COMMAND ----------

# Expectation Model
def poisson_geometric_expectation(obs, rate: float, n_zero_days: int):
    prob_zero_sales = stats.poisson(rate).pmf(0)
    prob_nonzero_sales = 1 - prob_zero_sales
    
    # Only the poisson distribution is required to compute the probability of non-zero sales
    prob_given_nonzero = stats.poisson(rate).cdf(obs)
    # Both the poisson and the geometric distribution is required to compute the probability of observed sales
    prob_given_zero = stats.geom(p=prob_nonzero_sales).sf(n_zero_days)
    
    if isinstance(obs, np.ndarray):
        selector = obs > 0
        prob = prob_given_zero.copy()
        prob[selector] = prob_given_nonzero[selector]
    else:
        prob = prob_given_nonzero if obs > 0 else prob_given_zero

    return prob

  
def count_trailing_zeros(array: np.ndarray):
    reversed_array = array[::-1]

    n = np.argmax(reversed_array != 0)
    if n == 0 and np.all(reversed_array == 0):
        n = len(reversed_array)
    return n


def count_trailing_zeros_vectorized(array: np.ndarray):
    result = []
    for i, _ in enumerate(array):
        if i == 0:
            result.append(1 if np.isclose(array[i], 0) else 0)
        else:
            result.append(count_trailing_zeros(array[:i+1]))
    return np.array(result)
  
    
def compute_lost_sales(obs, rate:float, n_zero_days: int):
    if isinstance(obs, np.ndarray):
        selector = obs > 0
        lost_sales = np.zeros_like(obs)
        lost_sales[selector] = (rate - obs)[selector]
        lost_sales[~selector] = rate[~selector]
    else:
        lost_sales = rate - obs if obs > 0 else rate
    return np.maximum(lost_sales, 0)


def get_regression_coefs(data, target):
    return la.pinv(data.T @ data) @ data.T @ target

# COMMAND ----------

fit_udf_schema = pyt.StructType([
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('TEST_ORGANIZATION_UNIT_NUM', pyt.StringType()),
    pyt.StructField('DATE', pyt.DateType()),
    pyt.StructField('LAST_OBS', pyt.FloatType()),
    pyt.StructField('LAST_RATE', pyt.FloatType()),
    pyt.StructField('LAST_LOST_SALES', pyt.FloatType()),
    pyt.StructField('N_DAYS_LAST_SALE', pyt.IntegerType()),
    pyt.StructField('PROB', pyt.FloatType()),
    pyt.StructField('ZSCORE', pyt.FloatType()),
    pyt.StructField('R2', pyt.FloatType()),
    pyt.StructField('RMSE', pyt.FloatType()),
    pyt.StructField('IS_FITTED', pyt.BooleanType()),
    pyt.StructField('COEFS', pyt.StringType()),
    pyt.StructField('HISTORICAL_PROBS', pyt.StringType()),
    pyt.StructField('HISTORICAL_LOST_SALES', pyt.StringType())
])
fit_udf_cols = [col.name for col in fit_udf_schema]


def fit_udf(dfi):
    assertion_message = f'| Item = {dfi["RETAILER_ITEM_ID"].unique()[0]} & TestStore = {dfi["TEST_ORGANIZATION_UNIT_NUM"].unique()[0]}'
    assert dfi['RETAILER_ITEM_ID'].nunique() == 1, 'Check groupby code' + assertion_message
    assert dfi['TEST_ORGANIZATION_UNIT_NUM'].nunique() == 1, 'Check data preprocessing process' + assertion_message
    test_store_num = dfi['TEST_ORGANIZATION_UNIT_NUM'].unique()[0]

    # Process data
    data = dfi.drop(columns=['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'DISTANCE'])
    data = data.sort_values(by='ORGANIZATION_UNIT_NUM')
    data = data.dropna(axis='columns', how='all')
    data = data.fillna(0)
    data.index = data['ORGANIZATION_UNIT_NUM']
    
    x = data.drop(columns='ORGANIZATION_UNIT_NUM', index=test_store_num).values.T
    y = data.drop(columns='ORGANIZATION_UNIT_NUM').loc[test_store_num].values
    
    dates = pd.to_datetime(data.drop(columns='ORGANIZATION_UNIT_NUM').columns)
    
    last_obs = y[-1]
    coefs = None
    rates = None
    n_days_since_last_sale = None
    last_rate = None
    prob = None
    last_lost_sales = None
    zscore = None
    r2 = None
    rmse = None
    historical_probs = None
    historical_lost_sales = None
    
    # Fit Models
    try:
        # Estimate coefficients
        coefs = get_regression_coefs(x, y)
        
        # Estimate other quantities
        # TODO: Decide which method to use after testing with Clorox
        rates = []
        for random_state_i in range(100):  # This method took 1.4min (not much slower so we might as well do it for robustness)
            x_resampled, y_resampled = resample(x, y, random_state=random_state_i)
            rates_i = np.maximum(x @ get_regression_coefs(x_resampled, y_resampled), 0)
            rates.append(rates_i)
        rates = np.median(np.array(rates), axis=0)
        # rates = np.maximum(x @ coefs, 0)  # Old way (lol took 0.8min tho. quite fast)
        last_rate = rates[-1]
        
        n_days_since_last_sale = count_trailing_zeros(y.flatten())
        
        # Compute probabilities
        historical_probs = poisson_geometric_expectation(
            obs=y,
            rate=rates,
            n_zero_days=count_trailing_zeros_vectorized(y.flatten())
        )
        prob = historical_probs[-1]
        zscore = -stats.norm().ppf(prob)  # Larger represent more extreme observations
        
        historical_probs = pd.DataFrame(historical_probs, columns=['prob'])
        historical_probs['date'] = dates
        
        # Compute lost items
        historical_lost_sales = compute_lost_sales(
            obs=y,
            rate=rates,
            n_zero_days=count_trailing_zeros_vectorized(y.flatten())
        )
        last_lost_sales = historical_lost_sales[-1]
        historical_lost_sales = pd.DataFrame(historical_lost_sales, columns=['lost_sales'])
        historical_lost_sales['date'] = dates
        
        # Metrics
        r2 = r2_score(y, rates)
        rmse = np.sqrt(mean_squared_error(y, rates))
    except Exception as e:
        import traceback
        print(f'=== ERROR OCCURRED fitting {assertion_message} ===')
        print(e)
        traceback.print_exc()
    
    # Generate results
    df_results = dfi[['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM']].drop_duplicates().astype(str)
    df_results['DATE'] = dates[-1]
    df_results['COEFS'] = universal_encoder(coefs)
    df_results['R2'] = r2
    df_results['RMSE'] = rmse
    df_results['LAST_OBS'] = last_obs
    df_results['LAST_RATE'] = last_rate
    df_results['LAST_LOST_SALES'] = last_lost_sales
    df_results['N_DAYS_LAST_SALE'] = n_days_since_last_sale
    df_results['PROB'] = prob
    df_results['ZSCORE'] = zscore
    df_results['IS_FITTED'] = coefs is not None
    df_results['HISTORICAL_PROBS'] = universal_encoder(historical_probs, compress=True)
    df_results['HISTORICAL_LOST_SALES'] = universal_encoder(historical_lost_sales, compress=True)
    
    # Done!
    return df_results[fit_udf_cols]

df_prepared = df_pos.groupby('RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM').applyInPandas(fit_udf, schema=fit_udf_schema)
print(f'N = {df_prepared.cache().count():,}')
print(f"N fitted = {df_prepared.select('IS_FITTED').filter('IS_FITTED == true').count():,}")

# COMMAND ----------

# Calculate Lost Sales Value
df_prepared = df_prepared.join(
    df_full_pos.selectExpr(
        'RETAILER_ITEM_ID', 
        'ORGANIZATION_UNIT_NUM AS TEST_ORGANIZATION_UNIT_NUM', 
        'SALES_DT AS DATE', 
        'PRICE',
        'ON_HAND_INVENTORY_QTY',
    ),
    ['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'DATE'],
    how='inner'
).drop(df_full_pos['ORGANIZATION_UNIT_NUM'])

df_prepared = df_prepared.withColumn(
    'LostSalesValue', 
    df_prepared['LAST_LOST_SALES'] * df_prepared['PRICE']
)

print(f'N = {df_prepared.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare Final List

# COMMAND ----------

# Add Universal Product Code and ORGANIZATION_UNIT_NUM. 

def get_cap_values(sql_context, company_id, manufacturer_id):
    df = spark.sql(
            f'select * from BOBV2.vw_bobv2_caps where CompanyId = {company_id}'
    )
    if manufacturer_id is not None:
        df = df.filter(f'ManufacturerId == {manufacturer_id}')

    df = df.select(
            'CapType', 
            'Lkp_productGroupId', 
            'CapValue'
    ).drop_duplicates()
#     .filter(pyf.col('CapType').isin(['OSA LSV Minimum', 'OSARows'])) 
    return df


def get_product_dim(sql_context, company_id, parent_id, manufacturer_id):
    df_itemId_productId = sql_context.sql(
        'select * from BOBv2.vw_BOBv2_Product'
    )

    if manufacturer_id is not None:
        df_itemId_productId = df_itemId_productId.filter(
            f'ManufacturerId == {manufacturer_id}'
        )
        
    df_itemId_productId = df_itemId_productId.filter(
        f'CompanyId == {company_id} and ParentChainId == {parent_id}' 
    ).selectExpr('ProductId', 'RefExternal as RETAILER_ITEM_ID', 'Lkp_productGroupId')
    
    df_productId_upc = sql_context.sql(
        'select * from BOBv2.Product'
    ).filter(
        f'CompanyId == {company_id}'
    ).select(
      'ProductId',
      'UniversalProductCode'
    ).dropna(subset='UniversalProductCode')

    df_link_item_id_upc = df_itemId_productId.join(
        df_productId_upc,
        ['ProductId']
    ).drop(df_itemId_productId['ProductId'])
    return df_link_item_id_upc
     

def reformat_retailer_item_id(df):
    """
    Dataplatform code to join two tables on key RETAILER_ITEM_ID is to add leading zero to length of 20 and take the the first 20
    Characters from right.
    """
    df = df.withColumn(
        'ITEM_ID', 
        pyf.lpad(
            pyf.ltrim(pyf.rtrim('RETAILER_ITEM_ID')), 20, '0'
        ).substr(-20, 20)) 
    return df

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Sort, filter and add alert sub types
# MAGIC Filter out all LSV =< 0 and sort them at store level by LSV and cut based on CapValue from BobV2.
# MAGIC
# MAGIC CapValue = The number of alerts

# COMMAND ----------

df_final = df_prepared.filter('LostSalesValue > 0')
print(f'N after filtering LSV\n -> {df_final.cache().count():,}')

df_final = df_final.filter(pyf.col('Date') >= pyf.date_sub(pyf.current_date(), 1))
print(f'N after filtering Date\n -> {df_final.cache().count():,}')


if df_final.count() == 0:
    raise ValueError('There is no LSV > 0')
    

df_item_upc = get_product_dim(
    spark, 
    config_dict[client]['company_id'],
    config_dict[client]['parent_id'],
    config_dict[client]['manufacturer_id']
) 

df_final = reformat_retailer_item_id(df_final).join(
    reformat_retailer_item_id(df_item_upc).drop('RETAILER_ITEM_ID'),
    ['ITEM_ID']
).drop('ITEM_ID')

print(f'N = {df_final.cache().count():,}')

df_caps_value = get_cap_values(
    spark, 
    config_dict[client]['company_id'],
    config_dict[client]['manufacturer_id']
)


if df_caps_value.filter(~pyf.col('Lkp_productGroupId').isNull()).count() == 0:
    osa_lsv_minimum = df_caps_value.filter(
        'CapType == "OSA LSV Minimum"'
    ).select(
        'CapValue'
    ).collect()[0][0]
    
    df_final = df_final.withColumn('CapValue', pyf.lit(osa_lsv_minimum))

else:
    df_final = df_final.join(
        df_caps_value.filter('CapType == "OSA LSV Minimum"'),
        ['Lkp_productGroupId'],
        how='inner'
    ).drop('CapType')

print(f'N = {df_final.cache().count():,}')



# COMMAND ----------

def cut_by_max_alert_num_logicB(df, maximum_num_alerts, confidence_level_list ):
    for c in confidence_level_list:
        df = df.withColumn(
            f'CONFIDENCE_{int(c*100)}', 
            pyf.when(
                pyf.col('PROB') <= (1 - c), 
                pyf.lit(c)
            ).otherwise(pyf.lit(None).cast(pyt.FloatType()))
        )

    cols = [pyf.col(c) for c in df.columns if c.startswith('CONFIDENCE')]
    df = df.withColumn('CONFIDENCE', pyf.coalesce(*(cols)))


    win_sepc = Window.partitionBy(
        'TEST_ORGANIZATION_UNIT_NUM',
        'CONFIDENCE'
    ).orderBy(pyf.col('LostSalesValue').desc())

    df = df.withColumn(
        'lsv_rank', 
        pyf.row_number().over(win_sepc)
    )

    win_spec = Window.partitionBy(
        'TEST_ORGANIZATION_UNIT_NUM'
    ).orderBy(pyf.col('lsv_rank')).orderBy(pyf.col('CONFIDENCE').desc())

    df = df.withColumn(
        'row_number', 
        pyf.row_number().over(win_spec)
    ).drop(*[c for c in df.columns if c.startswith('CONFIDENCE')])
    
    df = df.filter(
        df['row_number'] <= maximum_num_alerts
    ).drop('row_number', 'lsv_rank')
    return df


def cut_by_max_alert_num_logicA(df, maximum_num_alerts):
    win_spec = Window.partitionBy(
        ['TEST_ORGANIZATION_UNIT_NUM']
    ).orderBy(pyf.col('LostSalesValue').desc())

    df = df.withColumn(
        'row_number', 
        pyf.row_number().over(win_spec)
    )

    df = df.filter(
        df['row_number'] <= maximum_num_alerts
    ).drop('row_number')

    return df
  
    
def cut_by_max_alert_num_WeekCover(df, maximum_num_alerts):
    win_spec = Window.partitionBy(
        ['ORGANIZATION_UNIT_NUM']
    ).orderBy(pyf.col('WeeksCover').desc())

    df = df.withColumn(
        'row_number', 
        pyf.row_number().over(win_spec)
    )

    df = df.filter(
        df['row_number'] <= maximum_num_alerts
    ).drop('row_number')

    return df

# COMMAND ----------

if cap_lsv == 'yes':
    df_final = df_final.filter(pyf.col('LostSalesValue') >= pyf.col('CapValue'))
    print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Muting repeated Alerts

# COMMAND ----------

def get_rep_response(client_id, start_date, end_date, rep_id_list = None, store_list = None):

    batch_ts_s = int(start_date.replace('-','') +'000000')
    batch_ts_e = int(end_date.replace('-','') +'235959')

    query = f"""

      select 

          b.Client_ID,
          b.TYPE,
          b.STORE_TDLINX_ID,
          b.STOREINFO,
          b.ANSWER,
          b.STARTDATE,
          b.ENDDATE,
          b.STORE_ID,
          b.RETAILER_ID,
          b.CALL_DATE, 
          b.CALL_ID,
          b.PRIORITY,
          b.ANSWERTYPE,
          b.ACTIONCOMPLETED,
          b.batch_ts,
          b.PRODUCT_ID,
          b.SHELFCODE,

          a.DATE_EXECUTED,
          a.DURATION,
          a.EXEC_DURATION,
          a.DONE_STATUS,
          a.DONE_EMPNO

    from
        (select * , ROW_NUMBER() OVER(PARTITION BY store_id , product_id , call_id ORDER BY batch_ts desc) as r 
         from nars_raw.dpau as dpau
         where    dpau.client_id = \'{client_id}\'
                  and dpau.batch_ts >= \'{batch_ts_s}\'
                  and dpau.batch_ts <= \'{batch_ts_e}\'
                  and dpau.type = 'DLA'
                  and dpau.answer is not null
                  ) as b    

        join   
          (select call_id, date_executed, extract_date, duration,
          done_status, exec_duration, DONE_EMPNO,
          ROW_NUMBER() OVER(PARTITION BY call_id ORDER BY extract_date desc) as rn 
          from nars_raw.agac
              ) as a

        on b.call_id = a.call_id
        where 
          a.rn = 1
          and b.r = 1
          and a.date_executed >= \'{start_date}\'
          and a.date_executed <= \'{end_date}\'         

    """

    df = spark.sql(query)
    df = df.dropDuplicates()

    if rep_id_list is not None:
        df = df.filter(pyf.col('DONE_EMPNO').isin(rep_id_list))

    if store_list is not None:
        df = df.filter(pyf.col('STORE_TDLINX_ID').isin(store_list))

    return df

# COMMAND ----------

if retailer == 'kroger' or retailer == 'tesco':

#         a csv file should be read that provids information about mute time of each alert
        window_df = spark.read.option("header",True) \
         .csv(f'/mnt/processed/alerting/fieldTest/mute_window_size.csv')

        response_window = window_df.select('nars_response_text', 'mute_window_size')
        response_window= response_window.withColumn('nars_response_text', pyf.regexp_replace('nars_response_text', '\W+', ' '))
        response_window = response_window.withColumn('nars_response_text',pyf.trim(pyf.lower(pyf.col('nars_response_text'))))


        client_id = config_dict[client]['client_id']
        end = datetime.date.today()
        start = end - datetime.timedelta(14)
        start_date= start.strftime("%Y-%m-%d")
        end_date= end.strftime("%Y-%m-%d")
        # Historical data for two weeks interval is saved to a dataframe for fast searching and controling mute window for each alert.
        Hist_Data = get_rep_response(client_id, start_date, end_date)
        Hist_Data = Hist_Data.select('Client_ID','RETAILER_ID', 'STARTDATE', 'STORE_ID', 'PRODUCT_ID', 'SHELFCODE','ANSWER')
        
#       for kroger we need to run a function to filter retailer_id and make product_id consistance
        if retailer == 'kroger': 
            retailer_id = 0
            Hist_Data = Hist_Data.filter(pyf.col('RETAILER_ID')==retailer_id)
            Hist_Data = Hist_Data.withColumn('PRODUCT_ID', pyf.lpad(Hist_Data.PRODUCT_ID,13, '0'))
            store = 'STORE_ID'
            product = 'PRODUCT_ID'
             
            
        
        if retailer =='tesco':
            store_id_match = df_valid_stores.select('AcostaNo', 'StoreID')
            store_id_match = store_id_match.withColumnRenamed('StoreID', 'ORGANIZATION_UNIT_NUM')
            store_id_match = store_id_match.withColumn('ORGANIZATION_UNIT_NUM', store_id_match['ORGANIZATION_UNIT_NUM'].cast(pyf.StringType()))
            store_id_match = store_id_match.withColumn('AcostaNo', store_id_match['AcostaNo'].cast(pyf.StringType()))
            Hist_Data = Hist_Data.join(store_id_match, Hist_Data['STORE_ID']==store_id_match['AcostaNo'], how= 'left').drop(store_id_match['AcostaNo'])
            store = 'ORGANIZATION_UNIT_NUM'
            product = 'SHELFCODE'
        
        
        

        Hist_Data = Hist_Data.filter(Hist_Data.ANSWER.isNotNull())
        Hist_Data = Hist_Data.filter(Hist_Data.ANSWER !='No Response')
        window = Window.partitionBy(store, product).orderBy(pyf.col('STARTDATE').desc()) #keep the last response
        selected_response = Hist_Data.withColumn('row_number',pyf.row_number().over(window)).filter(pyf.col('row_number')==1).drop('row_number')
        selected_response = selected_response.withColumn('ANSWER',pyf.regexp_replace(pyf.col('ANSWER'), '\W+', ' '))
        selected_response = selected_response.withColumn('ANSWER',pyf.trim(pyf.lower(pyf.col('ANSWER'))))
        
        
        
        
        
        df_final = df_final.join(
            selected_response,
            (df_final['TEST_ORGANIZATION_UNIT_NUM']==selected_response[store]) \
            & (df_final['RETAILER_ITEM_ID']==selected_response[product]),
            how='left'
            ).drop(selected_response[store])
        print(f'N results = {df_final.cache().count():,}')
        
        
        
        
        df_final = df_final.join(
        response_window,
        df_final['ANSWER']== response_window['nars_response_text'],
        how='left'
        ).drop(
          response_window['nars_response_text']
        )
        print(f'N results = {df_final.cache().count():,}')
        
        
        df_final = df_final.withColumn('MUTE_ALERT', \
                              pyf.when(pyf.datediff(df_final['DATE'],df_final['STARTDATE']) <= df_final['mute_window_size'], 1) \
                              .otherwise(0))
        
        
        df_final= df_final.withColumn('Days_Difference',\
                                                pyf.datediff(df_final['DATE'],df_final['STARTDATE']))
        
        
        df_final.coalesce(1).write.format('csv')\
            .mode('overwrite')\
            .option('overwriteSchema', True)\
            .option('header', 'true')\
            .save(f'/mnt/processed/alerting/fieldTest/mutewindow/{client}-{country_code}-{retailer}-{frequency}/{datetime.date.today()}')

        df_final = df_final.filter('MUTE_ALERT ==0')
        df_final = df_final.drop('Client_ID','RETAILER_ID','STARTDATE','STORE_ID','PRODUCT_ID','SHELFCODE','ANSWER','mute_window_size','MUTE_ALERT', 'Days_Difference')
        print(f'N results = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## logic A/B

# COMMAND ----------

maximum_num_alerts = float(
    df_caps_value.filter(
        'CapType == "OSARows"'
    ).select('CapValue').collect()[0][0]
)

if logic == 'a(lsv)':
    df_final = cut_by_max_alert_num_logicA(df_final, maximum_num_alerts)
    
elif logic == 'b(zscore&lsv)':
    confidence_level_list = [0.95, 0.9, 0.85, 0.75, 0]
    
    df_final = cut_by_max_alert_num_logicB(
        df_final, maximum_num_alerts, 
        confidence_level_list
    )
else:
    raise ValueError('logic is not valid')
    
print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alert Type Business Rules
# MAGIC Applying arbitirary business rules to the label alert type.
# MAGIC
# MAGIC For now we have two out of three sub types. 
# MAGIC - **Availability (Phantom Inventory/Book Stock Error)**: If the first day of zero sales shows inventory more than zero
# MAGIC - **Slow Sales**: everything else

# COMMAND ----------

df_final = df_final.withColumn(
    'Issue', 
    pyf.when(
        (pyf.col('LAST_OBS') == 0) & (pyf.col('N_DAYS_LAST_SALE') <= 1), 
        pyf.lit('Availability (Phantom Inventory/Book Stock Error)')
    ).otherwise(pyf.lit('Slow Sales'))
)
print(f'{df_final.cache().count():,}')

# COMMAND ----------

if retailer == 'kroger':
    df_final = df_final.join(
        df_org_num, 
        df_final['TEST_ORGANIZATION_UNIT_NUM'] == df_org_num['ChainRefExternal']
    ).drop('TEST_ORGANIZATION_UNIT_NUM')
    df_final = df_final.withColumnRenamed('notes', 'TEST_ORGANIZATION_UNIT_NUM')
     
print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare CSV

# COMMAND ----------

# Columns to add to the file
columns_spec = [
    'TARSection', # put 2
    'Id', # starts from 1
    'cast(ORGANIZATION_UNIT_NUM as int) ORGANIZATION_UNIT_NUM', 
    'cast(UniversalProductCode as double) UniversalProductCode', 
    'LostSalesValue', 
    'PromotionName', # leave empty
    'Issue', # leave empty
    'cast(Inventory as decimal(15, 2)) as Inventory', 
    'WeeksCover', # put 0
    'DaysSincePromo', # leave empty
    'DateGenerated', # date of running the notebook
    'LastDateIncluded', # date of lost sales value
    'AlertType', # put 'On Shelf Availibility'
    'CapValue', # This value is not in the end report. We will delete it
    'RETAILER_ITEM_ID' # This value is not in the end report. We will delete it
]

# COMMAND ----------

# Add the remaining columns
df_final = df_final.withColumn('TARSection', pyf.lit(2))
df_final = df_final.withColumn('Id', pyf.monotonically_increasing_id())
df_final = df_final.withColumnRenamed('TEST_ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_NUM')
df_final = df_final.withColumn('PromotionName', pyf.lit(''))
df_final = df_final.withColumnRenamed('ON_HAND_INVENTORY_QTY', 'Inventory')
df_final = df_final.withColumn('WeeksCover', pyf.lit(0))
df_final = df_final.withColumn('DaysSincePromo', pyf.lit(''))
df_final = df_final.withColumn('DateGenerated', pyf.current_date())
df_final = df_final.withColumnRenamed('DATE', 'LastDateIncluded')
df_final = df_final.withColumn('AlertType', pyf.lit('On Shelf Availability'))

print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Results

# COMMAND ----------

df_final.coalesce(1).write.format('csv')\
    .mode('overwrite')\
    .option('overwriteSchema', True)\
    .option('header', 'true')\
    .save(f'/mnt/processed/alerting/fieldTest/results/{client}-{country_code}-{retailer}-{frequency}/{datetime.date.today()}')

# COMMAND ----------

# df_final = spark.read.format('csv')\
#     .options(header='true', inferSchema='true')\
#     .load(f'/mnt/processed/alerting/fieldTest/results/{client}-{country_code}-{retailer}-{frequency}/{datetime.date.today()}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add Inventory Cleanup Alerts

# COMMAND ----------

if include_inv_cleanup_alerts == 'yes':
    inventory_cleanup_alerts = spark.sql(f'''
            select 
                a.* , 
                b.ORGANIZATION_UNIT_NUM , 
                c.RETAILER_ITEM_ID
            from tesco_nestlecore_uk_retail_alert_im.alert_inventory_cleanup a
            join
            tesco_nestlecore_uk_dv.hub_organization_unit b
            on a.HUB_ORGANIZATION_UNIT_HK = b.HUB_ORGANIZATION_UNIT_HK
            join tesco_nestlecore_uk_dv.hub_retailer_item c
            ON a.HUB_RETAILER_ITEM_HK = c.HUB_RETAILER_ITEM_HK
            where  SALES_DT = "{max_date}"
    ''')

    inventory_cleanup_alerts = reformat_retailer_item_id(inventory_cleanup_alerts).join(
        reformat_retailer_item_id(df_item_upc).drop('RETAILER_ITEM_ID'),
        ['ITEM_ID']
    ).drop('ITEM_ID')

    
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn('TARSection', pyf.lit(2))
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn('Id', pyf.monotonically_increasing_id())
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn('PromotionName', pyf.lit(''))
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumnRenamed('ON_HAND_INVENTORY_QTY', 'Inventory')
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumnRenamed('WEEKS_COVER_NUM', 'WeeksCover')
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn('DaysSincePromo', pyf.lit(''))
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn('DateGenerated', pyf.current_date())
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumnRenamed('SALES_DT', 'LastDateIncluded')
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumnRenamed('ALERT_MESSAGE_DESC', 'Issue')
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumnRenamed('ALERT_TYPE_NM', 'AlertType')
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumnRenamed('LOST_SALES_AMT', 'LostSalesValue')
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn('Issue', pyf.lit('Excess Stock'))
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn('CapValue', pyf.lit(0.0))

    inventory_cleanup_alerts = inventory_cleanup_alerts.selectExpr(columns_spec)
    
    inv_cleanup_minimum_stock = float(
        df_caps_value.filter(
            'CapType == "Inv Cleanup Minimum Stock"'
        ).select('CapValue').collect()[0][0]
    )
    
    inv_cleanup_minimum_weeks_cover = float(
        df_caps_value.filter(
            'CapType == "Inv Cleanup Weeks Cover Max"'
        ).select('CapValue').collect()[0][0]
    )
    
    inventory_cleanup_alerts = inventory_cleanup_alerts \
        .filter(pyf.col('WeeksCover') >= inv_cleanup_minimum_weeks_cover) \
        .filter(pyf.col('Inventory') >= inv_cleanup_minimum_stock)
    
    maximum_num_alerts_inv_cleanup = float(
        df_caps_value.filter(
            'CapType == "InvCleanUpRows"'
        ).select('CapValue').collect()[0][0]
    )
    
    inventory_cleanup_alerts = cut_by_max_alert_num_WeekCover(inventory_cleanup_alerts,
                                                              maximum_num_alerts_inv_cleanup)
    
    inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn(
        'LostSalesValue', 
        pyf.when(
            pyf.col('LostSalesValue').isNotNull(), 
            pyf.col('LostSalesValue')
        ).otherwise(pyf.lit(0.0))
    )

    print(f'N Inventory Cleanup = {inventory_cleanup_alerts.cache().count():,}')
    
    df_final = df_final.selectExpr(columns_spec)

    df_final = df_final.union(inventory_cleanup_alerts)
    print(f'N All Alerts = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare Test file

# COMMAND ----------

# Select stores for field test
if split_run_test == 'yes':
    df_test_stores = spark.read.format('csv')\
        .options(header='true', inferSchema='true')\
        .load(f'/mnt/processed/alerting/fieldTest/experiment_design/{retailer}_{client}_{country_code}_store_assignment.csv')

    df_test_stores = df_test_stores.filter('experiment_group == "test"')
    
    if 'Store#' in df_test_stores.schema.names:
        df_test_stores = df_test_stores.withColumn(
            'StoreID', df_test_stores['Store#'])\
        .drop('Store#')
    
    df_final = df_final.join(
        df_test_stores,
        df_final['ORGANIZATION_UNIT_NUM']==df_test_stores['StoreID'],
        how='left_semi'
    )
    
df_final = df_final.selectExpr(columns_spec)
print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

# Add row id to table
# Add a increasing data column 
df_final = df_final.orderBy(
  ['ORGANIZATION_UNIT_NUM', 
   'RETAILER_ITEM_ID', 
   'LostSalesValue'], 
  ascending=False
)
df_final = df_final.drop('Id')
df_final = df_final.withColumn('Idx', pyf.monotonically_increasing_id())

# Create the window specification
window_spec = Window.orderBy('Idx')

# Use row number with the window specification
df_final = df_final.withColumn(
    'Id', 
    pyf.row_number().over(window_spec)
).drop('Idx')

df_final = df_final.selectExpr(columns_spec)
print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

df_final_pd = df_final.toPandas()
# TODO: it is hacky, change it for later versions
# df_final_pd.loc[:, 'UniversalProductCode'] = df_final_pd['UniversalProductCode'].astype('int')

# Set the minimum LSV to the velue defined by client
lsv_values = df_final_pd['LostSalesValue']
lsv_order = lsv_values.copy()
lsv_order[lsv_order > df_final_pd['CapValue']] = 0
lsv_order /= 1000

transformed_lsv_values = np.maximum(lsv_values, df_final_pd['CapValue'].astype('float')+lsv_order)
df_final_pd.loc[:, 'LostSalesValue'] = np.round(transformed_lsv_values, 2)
df_final_pd = df_final_pd[df_final.columns].drop(['CapValue','RETAILER_ITEM_ID'], axis = 1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop CSV file in the SFTP server

# COMMAND ----------

if save_only_on_blob_storage == 'yes':
    
     spark.createDataFrame(df_final_pd).coalesce(1).write.format('csv')\
    .mode('overwrite')\
    .option('overwriteSchema', True)\
    .option('header', 'true')\
    .save(f'/mnt/processed/alerting/fieldTest/results/{client}-{country_code}-{retailer}-{frequency}-report/{datetime.date.today()}')
    
else:
    host_name = 'sftpc.acosta.com'
    user_name = 'wm_dataprism_paas'
    pass_word = '5^4rT%[q7PqSn1'

#     if client == 'danonewave':
#         file_name = f'danonewave_drt_us_alerts'
#     else:
#         file_name = f'{client}_{retailer}_drt_us_alerts'

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None


    sftp = pysftp.Connection(
      host=host_name, 
      username=user_name, 
      password = pass_word, 
      cnopts=cnopts, 
      log=True)
    print ('Connection succesfully established ... ')

    sftp.cwd(sftp_folder)
    print(sftp.pwd)

    # 32768 is a value of SFTPFile.MAX_REQUEST_SIZE
    with sftp.open(f'/{sftp_folder}/{output_filename}.csv', 'w', bufsize=32768) as f:
        df_final_pd.to_csv(f, index=False)

    print('After adding the file:')
    files_list = sftp.listdir()
    print(files_list)
    
    sftp.close()
    
    if f'{output_filename}.csv' not in files_list:
        raise ValueError('SFTP transfer failed')

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance Summary Stats

# COMMAND ----------

import matplotlib.pyplot as graph
import seaborn as sns

# COMMAND ----------

display(df_prepared.describe())

# COMMAND ----------

sns.distplot(df_prepared.select('R2').toPandas()['R2'])
graph.axvline(0)
display(graph.show())
graph.close()

# COMMAND ----------

sns.distplot(df_prepared.select('RMSE').toPandas()['RMSE'])
display(graph.show())
graph.close()

# COMMAND ----------

sns.distplot(df_final_pd['LostSalesValue'])
display(graph.show())
graph.close()

# COMMAND ----------

df_final_pd.describe()

# COMMAND ----------

display(df_final.groupBy('ORGANIZATION_UNIT_NUM').count().select(pyf.max('count'), pyf.min('count')))

# COMMAND ----------

len(df_final_pd.ORGANIZATION_UNIT_NUM.unique())
