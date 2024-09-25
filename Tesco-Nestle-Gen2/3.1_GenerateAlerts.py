# Databricks notebook source
import numpy as np
import pandas as pd
from scipy import stats
import datetime
import re
import mlflow
import pytz

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from sklearn.metrics import r2_score, mean_squared_error
from sklearn.ensemble import GradientBoostingRegressor

from acosta.alerting.preprocessing.functions import get_params_from_database_name

from expectation.functions import pivot_pos, get_pos_prod, get_price
from expectation.model import get_latest_file_path
from expectation import parse_widget_or_raise

# COMMAND ----------

dbutils.widgets.text('database_name', ' ', 'Database Name')
dbutils.widgets.dropdown('frequency', 'daily', ['daily', 'weekly'], 'Frequency')
dbutils.widgets.text('sftp_folder', 'CORE TEAM TEST', 'SFTP Folder')
dbutils.widgets.dropdown('logic',  'B(ZSCORE&LSV)', ['A(LSV)', 'B(ZSCORE&LSV)', 'C(ModelPrediction)'], 'Sorting Logic')
dbutils.widgets.text('output_filename', ' ', 'Output Filename')
dbutils.widgets.text('version', 'A', 'Version')

dbutils.widgets.dropdown('cap_lsv', 'YES', ['YES', 'NO'], 'CAP LSV')
dbutils.widgets.dropdown('split_run_test', 'NO', ['YES', 'NO'], 'Split-Run Testing')
dbutils.widgets.dropdown('save_on_blob_storage', 'NO', ['YES', 'NO'], 'Save on Blob Storage')
dbutils.widgets.dropdown('include_inv_cleanup_alerts', 'NO', ['YES', 'NO'], 'Inventory Cleanup Alerts')
dbutils.widgets.dropdown('fill_missing_dates', 'YES', ['YES', 'NO'], 'Fill Missing Dates')
dbutils.widgets.dropdown('remove_inactive_items', 'YES', ['YES', 'NO'], 'Remove Inactive Items')
dbutils.widgets.dropdown('save_raw_alerts_into_table', 'NO', ['YES', 'NO'], 'Save Raw Alerts into Table')
dbutils.widgets.dropdown('save_on_globalconnect', 'NO', ['YES', 'NO'], 'Save on GlobalConnect')
dbutils.widgets.dropdown('save_on_ftps', 'NO', ['YES', 'NO'], 'Save on Remote Server')


dbutils.widgets.text('company_id', ' ', 'Company Id')
dbutils.widgets.text('parent_chain_id', ' ', 'Parent Chain Id')
dbutils.widgets.text('manufacturer_id', '0', 'Manufacturer Id')
dbutils.widgets.text('n_days', '90', 'Number of Historical Days')
dbutils.widgets.text('confidence_level', '95', 'Confidence Level')
dbutils.widgets.text('min_days_per_item', '60', 'Minimum Days Between First and Last Sales')

required_str_inputs = (
    'database_name', 'frequency', 'sftp_folder',
    'logic', 'output_filename', 'version'
)
str_parsed = [parse_widget_or_raise(dbutils.widgets.get(key)) for key in required_str_inputs]
database_name, frequency, sftp_folder, logic, output_filename, version = str_parsed

required_bool_inputs = (
    'cap_lsv',
    'split_run_test',
    'save_on_blob_storage',
    'include_inv_cleanup_alerts',
    'fill_missing_dates',
    'remove_inactive_items',
    'save_raw_alerts_into_table',
    'save_on_ftps'
)
bool_parsed = [parse_widget_or_raise(dbutils.widgets.get(key)) == 'yes' for key in required_bool_inputs]
cap_lsv, split_run_test, save_on_blob_storage,\
include_inv_cleanup_alerts, fill_missing_dates,\
remove_inactive_items, save_raw_alerts_into_table, save_on_ftps = bool_parsed

required_int_inputs = ('company_id', 'parent_chain_id', 'manufacturer_id',
                       'n_days', 'confidence_level', 'min_days_per_item')
int_parsed = [int(parse_widget_or_raise(dbutils.widgets.get(key))) for key in required_int_inputs]
company_id, parent_chain_id, manufacturer_id, n_days, confidence_level, min_days_per_item = int_parsed

manufacturer_id = manufacturer_id if manufacturer_id != 0 else None
_, country_code, client, retailer = get_params_from_database_name(database_name).values()
print(f'client: {client}, country_code: {country_code}, retailer: {retailer}')

# COMMAND ----------

# MAGIC %run "./3.0_PreprocessingModules"

# COMMAND ----------

# MAGIC %run "./3.2_PostprocessingModules"

# COMMAND ----------

# MAGIC %md
# MAGIC # Load and Preprocess Data

# COMMAND ----------

time_zone = 'Europe/London' if country_code == 'uk' else 'America/Toronto'
local_time = pytz.timezone(time_zone)
today_date = datetime.datetime.now(local_time).strftime('%Y-%m-%d')
today_date = datetime.datetime.strptime(today_date, '%Y-%m-%d').date()

run_similar_store_notebook = False
file_dir = f'/mnt/processed/alerting/cache/{retailer}_{client}_{country_code}/'

try:
    newest_file_path = get_latest_file_path(retailer, client, country_code, file_dir, dbutils)
    latest_date = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", newest_file_path.split('/')[-2]).group(1)
    print('the latest date of matched stores',latest_date)
    latest_date = datetime.datetime.strptime(latest_date, '%Y-%m-%d').date()
    if latest_date < (today_date - datetime.timedelta(days=30)):
        run_similar_store_notebook = True
except:
    run_similar_store_notebook = True

if run_similar_store_notebook:
    dbutils.notebook.run(
        '1.0_CalculateSimilarStores', 3600, {
            'database': database_name,
            'company_id': company_id,
            'parent_chain_id': parent_chain_id,
            'manufacturer_id': dbutils.widgets.get('manufacturer_id'),
            'remove_inactive_items': dbutils.widgets.get('remove_inactive_items'),
            'fill_missing_dates': dbutils.widgets.get('fill_missing_dates'),
            'distance_metric': 'cosine',
            'frequency': 'weekly',
            'dense_dim': 40,
            'latent_dim': 20,
            'total_matches': 5,
            'min_days_per_item': min_days_per_item
        })
    newest_file_path = get_latest_file_path(retailer, client, country_code, file_dir, dbutils)

df_store_matches = spark.read.format('delta').load(newest_file_path)
print(f'N of similar stores = {df_store_matches.cache().count():,}')

# COMMAND ----------

df_full_pos = get_pos_prod(
    database_name,
    spark,
    n_days=90,
    method='Gen2_DLA'
)

# df_full_pos = get_pos(
#     database_name,
#     spark,
#     n_days=n_days,
#     fill_missing_dates=fill_missing_dates,
#     remove_inactive_items=remove_inactive_items
# )

# update POS_ITEM_QTY based on 0 inventory
df_full_pos = df_full_pos.withColumnRenamed('POS_ITEM_QTY', 'POS_ITEM_QTY_ORG')
df_full_pos = df_full_pos.withColumn(
    'POS_INV',
    pyf.when(
        ((df_full_pos['ON_HAND_INVENTORY_QTY'] == 0)\
        & (df_full_pos['POS_ITEM_QTY_ORG'] == 0)), -1
    ).otherwise(df_full_pos['POS_ITEM_QTY_ORG'])
).withColumnRenamed('POS_INV', 'POS_ITEM_QTY')

# Check if the data is not older than one day
max_date = df_full_pos.agg({'SALES_DT': 'max'}).collect()[0][0]

if max_date < (today_date - datetime.timedelta(days=2)):
    raise ValueError(
        f'The max date is: {max_date} and Today date is {today_date}. The last POS data is older than one day.'
    )
print(f'N = {df_full_pos.cache().count():,}')

# COMMAND ----------

#join POS data and similar stores
print('pivot_pos running')
df_pivot = pivot_pos(
    df_full_pos,
    'daily',
    2
)

print('joining matched stores with POS')
df_pos = df_store_matches.join(
    df_pivot,
    (df_store_matches['ORGANIZATION_UNIT_NUM'] == df_pivot['ORGANIZATION_UNIT_NUM'])
     & (df_store_matches['RETAILER_ITEM_ID']== df_pivot['RETAILER_ITEM_ID']),
    how='left'
).drop(df_pivot['RETAILER_ITEM_ID'])\
.drop(df_pivot['ORGANIZATION_UNIT_NUM'])
print(f'N = {df_pos.cache().count():,}')

# Filter out combinations that will cause index errors
label_data_as_safe_schema = pyt.StructType(
    list(df_pos.schema) +
    [pyt.StructField('is_safe', pyt.BooleanType())]
)

# Run and filter
df_pos = df_pos\
    .groupby('RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM')\
    .applyInPandas(label_data_as_safe_udf, schema=label_data_as_safe_schema)
print(f'N_1 = {df_pos.cache().count():,}')


df_pos = df_pos.filter('is_safe is True')
df_pos = df_pos.drop('is_safe')
print(f'N_f = {df_pos.cache().count():,}')

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


def compute_lost_sales(obs, rate:float):
    if isinstance(obs, np.ndarray):
        selector = obs > 0
        lost_sales = np.zeros_like(obs)
        lost_sales[selector] = (rate - obs)[selector]
        lost_sales[~selector] = rate[~selector]
    else:
        lost_sales = rate - obs if obs > 0 else rate
    return np.maximum(lost_sales, 0)

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
    pyt.StructField('ProductId', pyt.IntegerType()),
    pyt.StructField('Lkp_productGroupId', pyt.IntegerType()),
    pyt.StructField('UniversalProductCode', pyt.StringType()),
    pyt.StructField('OutletId', pyt.IntegerType()),
    pyt.StructField('notes', pyt.StringType()),
    pyt.StructField('ChainRefExternal', pyt.StringType()),
])
fit_udf_cols = [col.name for col in fit_udf_schema]


def fit_udf(dfi):
    assertion_message = create_assertion_message(dfi)
    assert dfi['RETAILER_ITEM_ID'].nunique() == 1, 'Check groupby code' + assertion_message
    assert dfi['TEST_ORGANIZATION_UNIT_NUM'].nunique() == 1, 'Check data preprocessing process' + assertion_message
    test_store_num = dfi['TEST_ORGANIZATION_UNIT_NUM'].unique()[0]

    # Process data
    dfi = dfi.drop_duplicates(['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_NUM'])
    cols_to_drop = ['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'DISTANCE', 'ProductId',
                    'Lkp_productGroupId', 'UniversalProductCode', 'OutletId', 'notes', 'ChainRefExternal']
    data = dfi.drop(columns=cols_to_drop)
    data = data.sort_values(by='ORGANIZATION_UNIT_NUM')
    data = data.dropna(axis='columns', how='all')
    data = data.fillna(0)
    data.index = data['ORGANIZATION_UNIT_NUM']

    x = data.drop(columns='ORGANIZATION_UNIT_NUM', index=test_store_num).values.T
    y = data.drop(columns='ORGANIZATION_UNIT_NUM').loc[test_store_num].values

    dates = pd.to_datetime(data.drop(columns='ORGANIZATION_UNIT_NUM').columns)

    last_obs = y[-1]
    n_days_since_last_sale = None
    last_rate = None
    prob = None
    last_lost_sales = None
    z_score = None
    r2 = None
    rmse = None

    # Fit Models
    try:
        clf = GradientBoostingRegressor(random_state=1, n_iter_no_change=3, validation_fraction=0.1)
        clf.fit(x[:-1], y[:-1])
        last_rate = clf.predict(x[-1:])[0]
        n_days_since_last_sale = count_trailing_zeros(y.flatten())

        # Compute probabilities
        if last_rate < 0:
            last_rate = 0.0005
        prob = poisson_geometric_expectation(
            obs=last_obs,
            rate=last_rate,
            n_zero_days=n_days_since_last_sale
        )
        z_score = -stats.norm().ppf(prob)  # Larger represent more extreme observations

        # Compute lost items
        last_lost_sales = compute_lost_sales(
            obs=last_obs,
            rate=last_rate,
        )

        # Metrics
        r2 = r2_score(y[:-1], clf.predict(x[:-1]))
        rmse = np.sqrt(mean_squared_error(y[:-1], clf.predict(x[:-1])))
    except Exception as e:
        import traceback
        print(f'=== ERROR OCCURRED fitting {assertion_message} ===')
        print(e)
        traceback.print_exc()

    # Generate results
    df_results = dfi[['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM']].drop_duplicates().astype(str)
    df_results['DATE'] = dates[-1]
    df_results['R2'] = r2
    df_results['RMSE'] = rmse
    df_results['LAST_OBS'] = last_obs
    df_results['LAST_RATE'] = last_rate
    df_results['LAST_LOST_SALES'] = last_lost_sales
    df_results['N_DAYS_LAST_SALE'] = n_days_since_last_sale
    df_results['PROB'] = prob
    df_results['ZSCORE'] = z_score
    df_results['ProductId'] = dfi['ProductId'].unique()[0]
    df_results['Lkp_productGroupId'] = dfi['Lkp_productGroupId'].unique()[0]
    df_results['UniversalProductCode'] = dfi['UniversalProductCode'].unique()[0]
    df_results['OutletId'] = dfi['OutletId'].unique()[0]
    df_results['notes'] = dfi['notes'].unique()[0]
    df_results['ChainRefExternal'] = dfi['ChainRefExternal'].unique()[0]

    return df_results[fit_udf_cols]

df_prepared = df_pos.groupby('RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM').applyInPandas(fit_udf, schema=fit_udf_schema)
print(f'N = {df_prepared.cache().count():,}')

# COMMAND ----------

# Calculate Lost Sales Value
df_prepared = df_prepared\
    .filter(pyf.col('LAST_LOST_SALES') > 0)\
    .filter(pyf.col('Date') >= pyf.date_sub(pyf.from_utc_timestamp(pyf.current_timestamp(), time_zone), 2))
print(f'N after filtering Date and lost sales\n -> {df_prepared.cache().count():,}')

if df_prepared.count() == 0:
    raise ValueError('There is no LSV > 0 or there is no up to date sales record')

df_price = get_price(database_name, spark)

df_prepared = df_prepared.join(
    df_full_pos.selectExpr(
        'RETAILER_ITEM_ID',
        'ORGANIZATION_UNIT_NUM AS TEST_ORGANIZATION_UNIT_NUM',
        'SALES_DT AS DATE',
        'ON_HAND_INVENTORY_QTY',
    ),
    ['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'DATE'],
    how='inner'
).drop(df_full_pos['ORGANIZATION_UNIT_NUM'])

df_prepared = df_prepared\
     .join(df_price.selectExpr(
        'RETAILER_ITEM_ID',
        'ORGANIZATION_UNIT_NUM AS TEST_ORGANIZATION_UNIT_NUM',
        'SALES_DT AS DATE',
        'PRICE',
    ),
         on=['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'DATE'])

df_prepared = df_prepared.withColumn(
    'LostSalesValue',
    df_prepared['LAST_LOST_SALES'] * df_prepared['PRICE']
)
print(f'N = {df_prepared.cache().count():,}')

df_prepared = df_prepared.withColumn(
    'LostSalesValue',
    pyf.when(
        pyf.col('LostSalesValue').isNotNull(),
        pyf.col('LostSalesValue')
    ).otherwise(pyf.lit(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort, filter and add alert sub types
# MAGIC Filter out all LSV =< 0 and sort them at store level by LSV and cut based on CapValue from BobV2.
# MAGIC
# MAGIC CapValue = The number of alerts

# COMMAND ----------

df_caps_value = get_cap_values(
    spark,
    company_id,
    manufacturer_id
)

if df_caps_value.filter(~pyf.col('Lkp_productGroupId').isNull()).count() == 0:
    osa_lsv_minimum = df_caps_value.filter(
        'CapType == "OSA LSV Minimum"'
    ).select(
        'CapValue'
    ).collect()[0][0]
    df_prepared = df_prepared.withColumn('CapValue', pyf.lit(osa_lsv_minimum))
else:
    df_prepared = df_prepared.join(
        df_caps_value.filter('CapType == "OSA LSV Minimum"'),
        ['Lkp_productGroupId'],
        how='inner'
    ).drop('CapType')

print(f'N = {df_prepared.cache().count():,}')

if cap_lsv:
    df_prepared = df_prepared.filter(pyf.col('LostSalesValue') >= pyf.col('CapValue'))
    print(f'N after LSV threshold = {df_prepared.cache().count():,}')

df_prepared = df_prepared.filter(pyf.col('PROB') <= 1-confidence_level/100)
print(f'N after confidence threshold = {df_prepared.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting logic

# COMMAND ----------

maximum_num_alerts = float(
    df_caps_value.filter(
        'CapType == "OSARows"'
    ).select('CapValue').collect()[0][0]
)

if logic == 'a(lsv)':
    df_final = filter_alerts_by_lsv(
        df_prepared,
         maximum_num_alerts
    )
elif logic == 'b(zscore&lsv)':
    confidence_level_list = [0.95, 0.9, 0.85, 0.75, 0]
    df_final = filter_alerts_by_confidence(
        df_prepared, maximum_num_alerts,
        confidence_level_list
    )

elif logic == 'c(modelprediction)':
    model_dir = '/dbfs/mnt/artifacts/actionable-alert-models/'
    newest_model_path = get_latest_file_path(retailer, client, country_code, model_dir, dbutils)
    loaded_model = mlflow.catboost.load_model(newest_model_path)
    df_prepared = prepare_data_by_model_prediction(df_full_pos, df_prepared, max_date)
    df_final = filter_alerts_by_model_prediction(
        df_prepared,
         maximum_num_alerts
    )
else:
    raise ValueError('logic is not valid')

print(f'N = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Probability Alert Labeling
# MAGIC
# MAGIC For now we have two out of three sub types.
# MAGIC - **Availability (Phantom Inventory/Book Stock Error)**: If the first day of zero sales shows inventory > zero
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

# MAGIC %md
# MAGIC ### Post Processing Steps

# COMMAND ----------

df_final = post_processing_steps(df_final, max_date, df_caps_value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create required files and drop them into specified location in azueus2prdsaretalerts storage account

# COMMAND ----------

# MAGIC %scala
# MAGIC val save_on_globalconnect = dbutils.widgets.get("save_on_globalconnect")
# MAGIC
# MAGIC if(save_on_globalconnect == "YES" ){
# MAGIC
# MAGIC   import java.text.SimpleDateFormat
# MAGIC   import java.util.Calendar
# MAGIC
# MAGIC   val companyId = dbutils.widgets.get("company_id")
# MAGIC   val parentChainId = dbutils.widgets.get("parent_chain_id")
# MAGIC
# MAGIC   // Method: getExportFilePath
# MAGIC   // Derives export absolute path
# MAGIC   def getExportFilePath(): (String, String) = {
# MAGIC
# MAGIC     val timestampFormat = new SimpleDateFormat("yyyyMMdd")
# MAGIC     val currentDt = timestampFormat.format(Calendar.getInstance().getTime)
# MAGIC
# MAGIC     val exportRootDir = f"/mnt/alerts/alertfiles"
# MAGIC     var outputRelativePath =f"/Company627/${currentDt}/walmart/rbusa_drt_us_alerts.csv"
# MAGIC     var outputPath = f"${exportRootDir}/${outputRelativePath}"
# MAGIC
# MAGIC     // Return path
# MAGIC     (outputPath, outputRelativePath)
# MAGIC   }
# MAGIC
# MAGIC   //  Method: getExportEndFileAbsolutePath
# MAGIC   // Derive End File absolute path
# MAGIC
# MAGIC   def getExportEndFileAbsolutePath(): (String, String) = {
# MAGIC
# MAGIC     val timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss")
# MAGIC     val currentTs = timestampFormat.format(Calendar.getInstance().getTime)
# MAGIC     val uuId = java.util.UUID.randomUUID.toString
# MAGIC
# MAGIC     val exportRootDir = f"/mnt/alerts/alerts_end_file"
# MAGIC     var output = f"${exportRootDir}/${currentTs}_${uuId}.csv"
# MAGIC
# MAGIC    (exportRootDir,output)
# MAGIC   }
# MAGIC
# MAGIC
# MAGIC   //  Method: exportAlertsToFile
# MAGIC   // Export data to file
# MAGIC
# MAGIC   def exportAlertsToFile(): Unit = {
# MAGIC
# MAGIC     println("Start the main export process")
# MAGIC     val (exportFile, relativePath) = getExportFilePath()
# MAGIC     val exportTmpFile = exportFile + ".tmp"
# MAGIC
# MAGIC     println(f"Start getting data to export to file '${exportFile}'")
# MAGIC
# MAGIC     // Get DataFrame with data to export
# MAGIC     var dfalert =  spark.sql("select * from df_final_table")
# MAGIC
# MAGIC     val count = dfalert.count
# MAGIC
# MAGIC     // Select only required columns and write to export file
# MAGIC     dfalert.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).option("header", "true").save(exportTmpFile)
# MAGIC
# MAGIC     // Move data file from the folder to single file
# MAGIC     val partFile = dbutils.fs
# MAGIC       .ls(exportTmpFile)
# MAGIC       .filter(_.name.toString.startsWith("part"))
# MAGIC       .map(_.path)
# MAGIC        .head
# MAGIC     println(f"part file name = '${partFile}'")
# MAGIC
# MAGIC     dbutils.fs.mv(partFile, exportFile)
# MAGIC     println(f"Moving file '${partFile}' to '${exportFile}'")
# MAGIC
# MAGIC     dbutils.fs.rm(exportTmpFile, true)
# MAGIC     println(f"Delete file '${exportTmpFile}'. DONE Cleaning.")
# MAGIC
# MAGIC     println(
# MAGIC       f"DONE exporting to file '${exportFile}', exported total '${count}' records."
# MAGIC     )
# MAGIC     println("DONE the main export process")
# MAGIC   }
# MAGIC
# MAGIC   // Method: createAlertsEndFile
# MAGIC   // Create end file with respective content
# MAGIC
# MAGIC   def createAlertsEndFile(): Unit = {
# MAGIC
# MAGIC     val endFile = getExportEndFileAbsolutePath()._2
# MAGIC     val exportRootDir=getExportEndFileAbsolutePath()._1
# MAGIC     val tempendfile=endFile.replace("alerts_end_file","temp")
# MAGIC     val exportTmpFile = tempendfile + ".tmp"
# MAGIC
# MAGIC     println(f"Start getting data to write to end file '${endFile}'")
# MAGIC
# MAGIC     // Get DataFrame with data to write to End File
# MAGIC
# MAGIC     var path =getExportFilePath()._2
# MAGIC     var dfEndFile = spark.sql(f"select '${path}' as Path , ${companyId} as CompanyId , ${parentChainId} as ChainId,count(*) as NumAlerts ,count(distinct OutletId) as NumStores ,count(distinct ProductId) as NumProducts from df_final_table ")
# MAGIC
# MAGIC     dfEndFile.write
# MAGIC       .format("csv")
# MAGIC       .mode(SaveMode.Overwrite)
# MAGIC       .option("header", "true")
# MAGIC       .save(exportTmpFile)
# MAGIC
# MAGIC     // Move data file from the folder to single file
# MAGIC     val partFile = dbutils.fs
# MAGIC       .ls(exportTmpFile)
# MAGIC       .filter(_.name.toString.startsWith("part"))
# MAGIC       .map(_.path)
# MAGIC       .head
# MAGIC     println(f"part file name = '${partFile}'")
# MAGIC     println(f"Moving file '${partFile}' to '${tempendfile}'")
# MAGIC     dbutils.fs.mv(partFile, tempendfile)
# MAGIC     println(f"Copying file '${tempendfile}' to '${exportRootDir}'")
# MAGIC     dbutils.fs.cp(tempendfile, exportRootDir)
# MAGIC     //println(f"Moving file '${partFile}' to '${endFile}'")
# MAGIC
# MAGIC     dbutils.fs.rm(tempendfile, true)
# MAGIC      println(f"Delete file '${tempendfile}'. DONE Cleaning.")
# MAGIC     dbutils.fs.rm(exportTmpFile, true)
# MAGIC     println(f"Delete file '${exportTmpFile}'. DONE Cleaning.")
# MAGIC
# MAGIC     println(f"DONE writing to end file '${endFile}'")
# MAGIC   }
# MAGIC
# MAGIC   getExportFilePath()
# MAGIC   getExportEndFileAbsolutePath()
# MAGIC   exportAlertsToFile()
# MAGIC   createAlertsEndFile()
# MAGIC } else {
# MAGIC    println(f"skip*** Skip***skip***Skip")
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance Summary Stats

# COMMAND ----------

performance_summary(df_prepared, df_final)
