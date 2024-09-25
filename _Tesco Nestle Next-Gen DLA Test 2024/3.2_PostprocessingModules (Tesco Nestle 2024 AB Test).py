# Databricks notebook source
import pyspark.sql.functions as pyf
from pyspark.sql import Window
import pyspark.sql.types as pyt
import pysftp
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# Prepare Final List
def reformat_retailer_item_id(df):
    """
    Data platform code to join two tables on key RETAILER_ITEM_ID is to add leading zero to length of 20 and take the
    first 20 characters from right.
    """
    df = df.withColumn(
        'ITEM_ID',
        pyf.lpad(
            pyf.ltrim(pyf.rtrim('RETAILER_ITEM_ID')),
            20,
            '0'
        ).substr(-20, 20)
    )
    return df

# COMMAND ----------

def prepare_data_by_model_prediction(df_full_pos, df_prepared, max_date):
    # Filter the required POS interval to create feature vector
    df_last_14days = df_full_pos.filter(
        (pyf.col('SALES_DT') >= max_date - datetime.timedelta(13))
        & (pyf.col('SALES_DT') <= max_date))
    print(f'N = {df_last_14days.cache().count():,}')

    # Select required columns of df
    df_temp = df_prepared\
        .select('RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM',
                'DATE', 'ZSCORE', 'ON_HAND_INVENTORY_QTY')\
        .withColumnRenamed('ON_HAND_INVENTORY_QTY', 'INVENTORY')

    df_last_14days = df_temp\
    .join(
        df_last_14days,
        (
            (df_temp['RETAILER_ITEM_ID'] == df_last_14days['RETAILER_ITEM_ID'])
            & (df_temp['TEST_ORGANIZATION_UNIT_NUM'] == df_last_14days['ORGANIZATION_UNIT_NUM'])
        ),
        how='left'
    )\
    .drop(df_temp['RETAILER_ITEM_ID'])\
    .drop(df_temp['TEST_ORGANIZATION_UNIT_NUM'])
    print(f'N = {df_last_14days.cache().count():,}')

    df_feature, x, _ = prepare_actionable_model_input(df_last_14days, mode='prediction')
    print(f'N total alerts = {df_feature.shape}')

    df_feature['EXECUTED_PREDICTION'] = loaded_model.predict(x)
    df_feature['EXECUTED_PROBABILITY'] = loaded_model.predict_proba(x)[:, 1]

    df_prediction = spark.createDataFrame(df_feature)
    df_prepared = df_prepared\
    .join(df_prediction,
        (
        (df_prepared['RETAILER_ITEM_ID'] == df_prediction['RETAILER_ITEM_ID'])
        & (df_prepared['TEST_ORGANIZATION_UNIT_NUM'] == df_prediction['ORGANIZATION_UNIT_NUM'])
        ),
        how='left'
        ).select(df_prepared['*'], df_prediction['EXECUTED_PREDICTION'], df_prediction['EXECUTED_PROBABILITY'])

    df_prepared = df_prepared.withColumn(
        'ALERT_VALUE_PREDICTED',
        (pyf.col('EXECUTED_PROBABILITY') * pyf.col('LostSalesValue'))
    )
    print(df_prepared.groupby('EXECUTED_PREDICTION').count().show())
    return df_prepared

# COMMAND ----------

def filter_alerts_by_lsv(df, maximum_num_alerts):
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


def filter_alerts_by_confidence(df, maximum_num_alerts, confidence_level_list):
    for c in confidence_level_list:
        df = df.withColumn(
            f'CONFIDENCE_{int(c*100)}',
            pyf.when(
                pyf.col('PROB') <= (1 - c),
                pyf.lit(c)
            ).otherwise(pyf.lit(None).cast(pyt.FloatType()))
        )
    cols = [pyf.col(c) for c in df.columns if c.startswith('CONFIDENCE')]
    df = df.withColumn('CONFIDENCE', pyf.coalesce(*cols))

    win_spec = Window.partitionBy(
        'TEST_ORGANIZATION_UNIT_NUM',
        'CONFIDENCE'
    ).orderBy(pyf.col('LostSalesValue').desc())

    df = df.withColumn(
        'lsv_rank',
        pyf.row_number().over(win_spec)
    )
    win_spec = Window.partitionBy(
        'TEST_ORGANIZATION_UNIT_NUM'
    )\
        .orderBy(pyf.col('lsv_rank'))\
        .orderBy(pyf.col('CONFIDENCE').desc())

    df = df.withColumn(
        'row_number',
        pyf.row_number().over(win_spec)
    ).drop(*[c for c in df.columns if c.startswith('CONFIDENCE')])

    df = df.filter(
        df['row_number'] <= maximum_num_alerts
    ).drop('row_number', 'lsv_rank')
    return df


def filter_alerts_by_model_prediction(df, maximum_num_alerts):
    win_spec = Window.partitionBy(
        ['TEST_ORGANIZATION_UNIT_NUM']
    ).orderBy(
        pyf.col('EXECUTED_PREDICTION').desc(),
        pyf.col('ALERT_VALUE_PREDICTED').desc()
    )
    df = df.withColumn(
        'row_number',
        pyf.row_number().over(win_spec)
    )
    df = df.filter(
        df['row_number'] <= maximum_num_alerts
    ).drop('row_number')
    return df


def cut_by_max_alert_num_week_cover(df, maximum_num_alerts):
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

# Prepare CSV: Columns to add to the file
columns_spec = [
    'TARSection',  # put 2
    'Id',
    'ORGANIZATION_UNIT_NUM',
    'notes',
    'cast(UniversalProductCode as double) UniversalProductCode',
    'OutletId',
    'ProductId',
    'cast(LostSalesValue as decimal(15, 2)) as LostSalesValue',
    'PromotionName',  # leave empty
    'Issue',  # leave empty
    'cast(Inventory as decimal(15, 2)) as Inventory',
    'WeeksCover',  # put 0
    'DaysSincePromo',  # leave empty
    'DateGenerated',  # date of running the notebook
    'LastDateIncluded',  # date of lost sales value
    'AlertType',  # put 'On Shelf Availability'
    'CapValue',  # This value is not in the end report. We will delete it
    'RETAILER_ITEM_ID',  # This value is not in the end report. We will delete it
    'ALERT_ID',  # [version][date][monotonic-id] -remove after saving alerts into a table
    'Lkp_productGroupId', #remove later
    'ChainRefExternal'
]

# COMMAND ----------

def create_unique_alert_id(df, version):
    df = df.withColumn('Idx', pyf.monotonically_increasing_id())
    window_spec = Window.orderBy('Idx')
    df = df.withColumn(
        'Id',
        pyf.row_number().over(window_spec)
    ).withColumn('ALERT_ID',
                 pyf.concat(
                     pyf.lit(f'{version.upper()}{today_date.year}{today_date.month}{today_date.day}-'),
                     pyf.col('Id')
                 )
    ).drop('Idx')
    return df

def prepare_csv_file(df):
    # Add the remaining columns
    df = df.orderBy(
      ['TEST_ORGANIZATION_UNIT_NUM',
       'RETAILER_ITEM_ID',
       'LostSalesValue'],
      ascending=False
    )
    df = df\
        .withColumn('TARSection', pyf.lit(2))\
        .withColumnRenamed('TEST_ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_NUM')\
        .withColumn('PromotionName', pyf.lit(''))\
        .withColumnRenamed('ON_HAND_INVENTORY_QTY', 'Inventory')\
        .withColumn('WeeksCover', pyf.lit(0))\
        .withColumn('DaysSincePromo', pyf.lit(''))\
        .withColumn('DateGenerated', pyf.current_date())\
        .withColumnRenamed('DATE', 'LastDateIncluded')\
        .withColumn('AlertType', pyf.lit('On Shelf Availability'))
    return df

# COMMAND ----------

# Save Results
def function_save_raw_alerts_into_table(df):
    raw_alerts_schema = pyt.StructType([
        pyt.StructField('RETAILER', pyt.StringType(), True),
        pyt.StructField('CLIENT', pyt.StringType(), True),
        pyt.StructField('COUNTRY_CODE', pyt.StringType(), True),
        pyt.StructField('RETAILER_ITEM_ID', pyt.StringType(), True),
        pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.StringType(), True),
        pyt.StructField('Lkp_productGroupId', pyt.IntegerType(), True),
        pyt.StructField('UniversalProductCode', pyt.StringType(), True),
        pyt.StructField('ChainRefExternal', pyt.StringType(), True),
        pyt.StructField('ALERT_ID', pyt.StringType(), True),
        pyt.StructField('Id', pyt.IntegerType(), True),
        pyt.StructField('LastDateIncluded', pyt.DateType(), True),
        pyt.StructField('LAST_OBS', pyt.FloatType(), True),
        pyt.StructField('LAST_RATE', pyt.FloatType(), True),
        pyt.StructField('LAST_LOST_SALES', pyt.FloatType(), True),
        pyt.StructField('N_DAYS_LAST_SALE', pyt.IntegerType(), True),
        pyt.StructField('PROB', pyt.FloatType(), True),
        pyt.StructField('ZSCORE', pyt.FloatType(), True),
        pyt.StructField('R2', pyt.FloatType(), True),
        pyt.StructField('RMSE', pyt.FloatType(), True),
        pyt.StructField('PRICE', pyt.FloatType(), True),
        pyt.StructField('Inventory', pyt.IntegerType(), True),
        pyt.StructField('LostSalesValue', pyt.FloatType(), True),
        pyt.StructField('EXECUTED_PREDICTION', pyt.IntegerType(), True),
        pyt.StructField('EXECUTED_PROBABILITY', pyt.FloatType(), True),
        pyt.StructField('ALERT_VALUE_PREDICTED', pyt.FloatType(), True),
        pyt.StructField('Issue', pyt.StringType(), True),
        pyt.StructField('TARSection', pyt.IntegerType(), True),
        pyt.StructField('PromotionName', pyt.StringType(), True),
        pyt.StructField('WeeksCover', pyt.IntegerType(), True),
        pyt.StructField('DaysSincePromo', pyt.StringType(), True),
        pyt.StructField('DateGenerated', pyt.DateType(), True),
        pyt.StructField('AlertType', pyt.StringType(), True),
    ])
    raw_alert_table_cols = [col.name for col in raw_alerts_schema]
    if 'EXECUTED_PREDICTION' not in df.columns:
        df = df\
            .withColumn('EXECUTED_PREDICTION',pyf.lit(None))\
            .withColumn('EXECUTED_PROBABILITY', pyf.lit(None))\
            .withColumn('ALERT_VALUE_PREDICTED', pyf.lit(None))

    df_raw_alerts = df\
        .withColumn('RETAILER', pyf.lit(retailer))\
        .withColumn('CLIENT', pyf.lit(client))\
        .withColumn('COUNTRY_CODE', pyf.lit(country_code))\
        .withColumn('EXECUTED_PREDICTION', df['EXECUTED_PREDICTION'].cast(pyt.IntegerType()))\
        .withColumn('EXECUTED_PROBABILITY', df['EXECUTED_PROBABILITY'].cast(pyt.FloatType()))\
        .withColumn('ALERT_VALUE_PREDICTED', df['ALERT_VALUE_PREDICTED'].cast(pyt.FloatType()))\
        .select(raw_alert_table_cols)

    assert all(
        (a.name, a.dataType) == (b.name, b.dataType)
        for a, b in zip(df_raw_alerts.schema, raw_alerts_schema)
        ), 'Inconsistent Schema'

    df_raw_alerts.write.format("delta")\
        .mode('append')\
        .insertInto('RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS')

# COMMAND ----------

def function_include_inv_cleanup_alerts(max_date, df, df_caps_value):
    df_item_upc = df.select('ProductId', 'RETAILER_ITEM_ID',
                            'Lkp_productGroupId', 'UniversalProductCode')
    df_org_num = df.select('ORGANIZATION_UNIT_NUM', 'OutletId', 'notes',
                           'ChainRefExternal')
    try:
        inventory_cleanup_alerts = globals(
        )[f'{retailer}_{client}_{country_code}_inventory'](max_date)

        inv_cleanup_minimum_stock = float(
            df_caps_value.filter('CapType == "Inv Cleanup Minimum Stock"').
            select('CapValue').collect()[0][0])
        inv_cleanup_minimum_weeks_cover = float(
            df_caps_value.filter('CapType == "Inv Cleanup Weeks Cover Max"').
            select('CapValue').collect()[0][0])
        inventory_cleanup_alerts = inventory_cleanup_alerts\
            .filter(pyf.col('WEEKS_COVER_NUM') >= inv_cleanup_minimum_weeks_cover)\
            .filter(pyf.col('ON_HAND_INVENTORY_QTY') >= inv_cleanup_minimum_stock)

        inventory_cleanup_alerts = reformat_retailer_item_id(
            inventory_cleanup_alerts).join(
                reformat_retailer_item_id(df_item_upc).drop(
                    'RETAILER_ITEM_ID'), ['ITEM_ID']).drop('ITEM_ID')
        inventory_cleanup_alerts = inventory_cleanup_alerts.join(
            df_org_num,
            inventory_cleanup_alerts['ORGANIZATION_UNIT_NUM'] ==
            df_org_num['ORGANIZATION_UNIT_NUM'],
            how='left').drop(df_org_num['ORGANIZATION_UNIT_NUM'])
        inventory_cleanup_alerts = inventory_cleanup_alerts\
            .withColumn('TARSection', pyf.lit(2))\
            .withColumn('PromotionName', pyf.lit(''))\
            .withColumnRenamed('ON_HAND_INVENTORY_QTY', 'Inventory')\
            .withColumnRenamed('WEEKS_COVER_NUM', 'WeeksCover')\
            .withColumn('DaysSincePromo', pyf.lit(''))\
            .withColumn('DateGenerated', pyf.current_date())\
            .withColumnRenamed('SALES_DT', 'LastDateIncluded')\
            .withColumnRenamed('ALERT_TYPE_NM', 'AlertType')\
            .withColumnRenamed('LOST_SALES_AMT', 'LostSalesValue')\
            .withColumn('Issue', pyf.lit('Excess Stock'))\
            .withColumn('CapValue', pyf.lit(0.0))

        if df_caps_value.filter(
                ~pyf.col('Lkp_productGroupId').isNull()).count() == 0:
            osa_lsv_minimum = df_caps_value.filter(
                'CapType == "OSA LSV Minimum"').select(
                    'CapValue').collect()[0][0]

            inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn(
                'CapValue', pyf.lit(osa_lsv_minimum))

        else:
            inventory_cleanup_alerts = inventory_cleanup_alerts.join(
                df_caps_value.filter('CapType == "OSA LSV Minimum"'),
                ['Lkp_productGroupId'],
                how='inner').drop('CapType').drop(
                    inventory_cleanup_alerts['CapValue'])

        inventory_cleanup_alerts = inventory_cleanup_alerts.drop_duplicates(
            subset=['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'])
        inventory_cleanup_alerts = create_unique_alert_id(inventory_cleanup_alerts, version = 'I')\
            .selectExpr(columns_spec)

        maximum_num_alerts_inv_cleanup = float(
            df_caps_value.filter('CapType == "InvCleanUpRows"').select(
                'CapValue').collect()[0][0])
        inventory_cleanup_alerts = cut_by_max_alert_num_week_cover(
            inventory_cleanup_alerts, maximum_num_alerts_inv_cleanup)
        inventory_cleanup_alerts = inventory_cleanup_alerts.withColumn(
            'LostSalesValue',
            pyf.when(
                pyf.col('LostSalesValue').isNotNull(),
                pyf.col('LostSalesValue')).otherwise(pyf.lit(0.0)))
        print(
            f'N Inventory Cleanup = {inventory_cleanup_alerts.cache().count():,}'
        )
        df = df.selectExpr(columns_spec)
        df = df.union(inventory_cleanup_alerts)
        print(f'N All Alerts = {df.cache().count():,}')
    except:
        print(f'There is no inventory function for {retailer} and {client}')
        pass
    return df

# COMMAND ----------

# Prepare Test file - Select stores for field test
def function_split_run_test(df):
    df_test_stores = spark.read.format('csv')\
        .options(header='true', inferSchema='true')\
        .load(f'/mnt/processed/alerting/fieldTest/experiment_design/{retailer}_{client}_{country_code}_store_assignment_2024.csv')

    df_test_stores = df_test_stores.filter('experiment_group == "test"')

    if 'StoreNo' in df_test_stores.schema.names:
        df_test_stores = df_test_stores.withColumnRenamed('StoreNo', 'StoreID')

    if 'STORE NUMBER' in df_test_stores.schema.names:
        df_test_stores = df_test_stores.withColumnRenamed(
            'STORE NUMBER', 'StoreID')

    df = df.join(df_test_stores,
                 df['ORGANIZATION_UNIT_NUM'] == df_test_stores['StoreID'],
                 how='left_semi')
    print(f'N = {df.cache().count():,}')
    return df

# COMMAND ----------

# Add a increasing data column
def prepare_final_file(df):
    df = df.orderBy(
      ['ORGANIZATION_UNIT_NUM',
       'RETAILER_ITEM_ID',
       'LostSalesValue'],
      ascending=False
    ).withColumn(
        'Idx',
        pyf.monotonically_increasing_id()
    )
    # Create the window specification
    window_spec = Window.orderBy('Idx')

    # Use row number with the window specification
    df = df.withColumn(
        'Id',
        pyf.row_number().over(window_spec)
    ).drop('Idx')
    return df

# COMMAND ----------

# Drop CSV file in the SFTP server
def function_save_on_blob_storage(df):
    # Check each column type. If it's nulltype, cast to string type,
    # else keep the original column.

    df = df.select([
        pyf.lit(None).cast('string').alias(i.name)
        if isinstance(i.dataType, pyt.NullType)
        else i.name
        for i in df.schema
    ])
    df.coalesce(1).write\
        .format('csv')\
        .mode('overwrite')\
        .option('overwriteSchema', True)\
        .option('header', 'true')\
        .save(f'/mnt/processed/alerting/fieldTest/results/{client}-{country_code}-{retailer}-report/{today_date}')


def function_save_on_ftps(df_pd):
    host_name = 'sftpc.acosta.com'
    user_name = 'wm_dataprism_paas'
    password = '5^4rT%[q7PqSn1'

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp = pysftp.Connection(
      host=host_name,
      username=user_name,
      password=password,
      cnopts=cnopts,
      log=True)
    print('Connection successfully established...')

    sftp.cwd(sftp_folder)
    print(sftp.pwd)

    # 32768 is a value of SFTPFile.MAX_REQUEST_SIZE
    with sftp.open(f'/{sftp_folder}/{output_filename}.csv', 'w', bufsize=32768) as f:
        df_pd.to_csv(f, index=False)
    print('After adding the file:')
    files_list = sftp.listdir()
    print(files_list)
    sftp.close()

    if f'{output_filename}.csv' not in files_list:
        raise ValueError('SFTP transfer failed')

# COMMAND ----------

def post_processing_steps(df, max_date, df_caps_value):
    df = prepare_csv_file(df)
    df = df.drop_duplicates(subset=['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'])
    df = create_unique_alert_id(df, version=version)
    print(f'N = {df.cache().count():,}')
    #save result
    today_date = datetime.date.today()
    df.coalesce(1).write.format('csv')\
      .mode('overwrite')\
      .option('overwriteSchema', True)\
      .option('header', True)\
      .save(f'/mnt/processed/alerting/fieldTest/results/{client}-{country_code}-{retailer}/{today_date}')

    if save_raw_alerts_into_table:
        function_save_raw_alerts_into_table(df)

    if include_inv_cleanup_alerts:
        df = function_include_inv_cleanup_alerts(max_date, df, df_caps_value)

    try:
        df = globals()[f'{retailer}_{country_code}_functions'](df)
    except:
        pass

    # Select stores for field test
    if split_run_test:
        df = function_split_run_test(df)
    df = prepare_final_file(df)
    df = df.selectExpr(columns_spec)
    print(f'N = {df.cache().count():,}')

    df_to_pd = df.toPandas()
    for col in ['UniversalProductCode', 'OutletId', 'ProductId', 'ORGANIZATION_UNIT_NUM']:
        df_to_pd['OutletId'].fillna(-1, inplace=True)
        df_to_pd.loc[:, col] = df_to_pd[col].astype('int')

    for col in ['UniversalProductCode', 'ORGANIZATION_UNIT_NUM']:
        df_to_pd.loc[:, col] = df_to_pd[col].astype('str')

    for col in ['CapValue', 'LostSalesValue']:
        df_to_pd.loc[:, col] = df_to_pd[col].astype('float')

    # Set the minimum LSV to the value defined by client
    def rescale(x):
        max_value = 2 * x.CapValue.max() if x.LostSalesValue.max() <= x.CapValue.max() else x.LostSalesValue.max()
        return x.CapValue.max() +\
                    (x.LostSalesValue - x.LostSalesValue.min()) *\
                    (max_value - x.CapValue.max())/\
                    (max_value - x.LostSalesValue.min())

    if not cap_lsv:
        g = df_to_pd.groupby('Lkp_productGroupId', as_index=False)
        df_to_pd['LostSalesValue'] = g.apply(rescale).droplevel(0)
        df_to_pd['LostSalesValue'] = df_to_pd['LostSalesValue'].round(2)
    try:
        df_to_pd = globals()[f'{retailer}_{client}_{country_code}_output_adujustment'](df, df_to_pd)
    except:
        df_to_pd = df_to_pd[df.columns].drop(['OutletId', 'ProductId', 'CapValue', 'RETAILER_ITEM_ID', 'ALERT_ID', 'Lkp_productGroupId', 'ChainRefExternal', 'notes'], axis=1)

    # Drop CSV file in the SFTP server
    df_spark = spark.createDataFrame(df_to_pd)
    if save_on_blob_storage:
        function_save_on_blob_storage(df_spark)
    if save_on_ftps:
        function_save_on_ftps(df_to_pd)
    return df_spark

# COMMAND ----------

def performance_summary(df_prepared, df_alerts):
    print('Summary of df_prepared:')
    display(df_prepared.describe())
    fig, axs = plt.subplots(1, 4, figsize=(15,5))

    sns.histplot(df_prepared.select('R2').toPandas()['R2'], kde=True, ax=axs[0])
    sns.histplot(df_prepared.select('RMSE').toPandas()['RMSE'], kde=True, ax=axs[1])
    sns.histplot(df_prepared.select('PROB').toPandas()['PROB'], kde=True, ax=axs[2])
    sns.histplot(df_alerts.select('LostSalesValue').toPandas()['LostSalesValue'], kde=True, ax=axs[3])

    fig.suptitle('Model Performance on Training Data')
    plt.axvline(0)
    display(plt.show())
    plt.close()

    print(f"mean R2: {df_prepared.select('R2').toPandas()['R2'].mean():.2f}, mean RMSE: {df_prepared.select('RMSE').toPandas()['RMSE'].mean():.2f}")

    print('\n Summary of final alerts')
    display(df_alerts.describe())

    try:
        unique_stores = df_alerts.select(pyf.countDistinct('ORGANIZATION_UNIT_NUM')).collect()[0][0]
        unique_products = df_alerts.select(pyf.countDistinct('UniversalProductCode')).collect()[0][0]
        print('\n Min and max number of alerts per ORGANIZATION_UNIT_NUM')
        display(df_alerts.groupBy('ORGANIZATION_UNIT_NUM').count().select(pyf.max('count'), pyf.min('count')))

    except:
        unique_stores = df_alerts.select(pyf.countDistinct('OutletId')).collect()[0][0]
        unique_products = df_alerts.select(pyf.countDistinct('ProductId')).collect()[0][0]
        print('\n Min and max number of alerts per OutletId')
        display(df_alerts.groupBy('OutletId').count().select(pyf.max('count'), pyf.min('count')))

    print(f'Total Alerts = {df_alerts.count():,} \n Number of unique Stores = {unique_stores} \n Number of unique products = {unique_products}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Clients Specific Functions

# COMMAND ----------

def walmart_rbusa_us_output_adujustment(df, df_pd):
    df_pd = df_pd.loc[df_pd['OutletId'] != -1]
    for col in ['DateGenerated', 'LastDateIncluded']:
        df_pd.loc[:, col] = pd.to_datetime(df_pd[col]).dt.strftime('%Y-%m-%d')
#     df_pd.loc[:, 'WeeksCover'] = df_pd['WeeksCover'].astype('float')
    for col in ['WeeksCover', 'LostSalesValue']:
        df_pd.loc[:, col] = df_pd[col].astype('float')
    df_pd = df_pd[df.columns].drop([
        'UniversalProductCode', 'ORGANIZATION_UNIT_NUM', 'CapValue',
        'RETAILER_ITEM_ID', 'ALERT_ID', 'AlertType', 'Lkp_productGroupId',
        'ChainRefExternal', 'notes'
    ],
                                   axis=1)
    print(f'***test***** {df_pd.columns}')
    df_final_spark = spark.createDataFrame(df_pd)
    df_final_spark = df_final_spark.withColumn(
        'LostSalesValue',
        pyf.col('LostSalesValue').cast(pyt.DecimalType(precision=7, scale=2)))
    window_spec = Window.orderBy('OutletId')
    df_final_spark = df_final_spark.withColumn(
        'Id',
        pyf.row_number().over(window_spec))
    df_final_spark = df_final_spark.select('TARSection', 'Id', 'OutletId',
                                           'ProductId', 'LostSalesValue',
                                           'PromotionName', 'Issue',
                                           'Inventory', 'WeeksCover',
                                           'DaysSincePromo', 'DateGenerated',
                                           'LastDateIncluded')
    df_final_spark.createOrReplaceTempView('df_final_table')
    print(df_final_spark.schema)
    return df_pd

# COMMAND ----------

def kroger_us_functions(df):
    df = df.filter(pyf.col('ChainRefExternal').isNotNull()).drop('ORGANIZATION_UNIT_NUM')
    df = df.withColumnRenamed('notes', 'ORGANIZATION_UNIT_NUM')
    df = df.filter(pyf.col('ORGANIZATION_UNIT_NUM')!= '')
    df = df.withColumn('notes', pyf.lit(''))
    return df

# COMMAND ----------

def tesco_nestlecore_uk_inventory(max_date):
    inventory_cleanup_alerts = spark.sql(f'''
        select * from retail_alert_tesco_nestlecore_uk_im.alert_inventory_cleanup
        where SALES_DT = "{max_date}"
    ''')
    return inventory_cleanup_alerts

# COMMAND ----------

def target_wildcat_us_inventory(max_date):
    inventory_cleanup_alerts = spark.sql(f'''
    select * from retail_alert_target_wildcat_us_im.alert_inventory_cleanup
    where SALES_DT = "{max_date}"
    ''')
    return inventory_cleanup_alerts

# COMMAND ----------

def kroger_wildcat_us_inventory(max_date):
    inventory_cleanup_alerts = spark.sql(f'''
        select * from retail_alert_kroger_wildcat_us_im.alert_inventory_cleanup
        where SALES_DT = "{max_date}"
      ''')
    return inventory_cleanup_alerts

# COMMAND ----------

def walmart_rbusa_us_inventory(max_date):
    inventory_cleanup_alerts = spark.sql(f'''
    select * from retail_alert_walmart_rbusa_us_im.alert_inventory_cleanup
    where SALES_DT = "{max_date}"
    ''')
    return inventory_cleanup_alerts
