# Databricks notebook source
import traceback
import datetime
import pandas as pd
import numpy as np

from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import MinMaxScaler

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

import mlflow

from expectation.functions import get_pos_prod, pivot_pos
from expectation.train import train_model
from expectation import mean_absolute_error_with_ignore, parse_widget_or_raise

from acosta.alerting.helpers import check_path_exists
from acosta.alerting.preprocessing.functions import get_params_from_database_name

# COMMAND ----------

dbutils.widgets.text('database_name', ' ', 'Database Name')
dbutils.widgets.dropdown('environment', 'dev', ['dev', 'prod'], 'Environment')
dbutils.widgets.text('company_id', '', 'Company Id')
dbutils.widgets.text('parent_chain_id', '', 'Parent Chain Id')
dbutils.widgets.text('manufacturer_id', '', 'Manufacturer Id')

database_name = parse_widget_or_raise(dbutils.widgets.get('database_name'))
environment = parse_widget_or_raise(dbutils.widgets.get('environment'))
required_int_inputs = (
     'company_id', 'parent_chain_id')
int_parsed = [int(parse_widget_or_raise(dbutils.widgets.get(key))) for key in required_int_inputs]
company_id, parent_chain_id = int_parsed

manufacturer_id = dbutils.widgets.get('manufacturer_id')
manufacturer_id = int(manufacturer_id) if len(manufacturer_id) != 0 else None

_, country_code, client, retailer = get_params_from_database_name(database_name).values()
print(f'client: {client}, country_code: {country_code}, retailer: {retailer}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data for Training

# COMMAND ----------

df_pos = get_pos_prod(
    database_name,
    spark,
    n_days=740,
    method='Gen2_DLA'
)
print(f'{df_pos.cache().count():,}')

# COMMAND ----------

# Import approved product list for this client if available
approved_product_list_reference_path = f'/mnt/artifacts/gen2_dla/approved_product_list/{retailer}_{client}_{country_code}_approved_product_list.csv/'

if check_path_exists(approved_product_list_reference_path, 'csv', 'ignore'):
    approved_product_list = spark\
        .read\
        .options(header=True, inferSchema=True)\
        .csv(approved_product_list_reference_path)
    if 'Item Number' in approved_product_list.columns:
      approved_product_list = approved_product_list.withColumnRenamed('Item Number', 'RETAILER_ITEM_ID')
    if 'ItemNumber' in approved_product_list.columns:
      approved_product_list = approved_product_list.withColumnRenamed('ItemNumber', 'RETAILER_ITEM_ID')
    if 'RETAILER_ITEM_ID' in approved_product_list.columns:
      df_pos = df_pos.join(
          approved_product_list.select('RETAILER_ITEM_ID'),
         'RETAILER_ITEM_ID'
      ).drop(
          'RetailProductCode'
      )
      print(f'{df_pos.cache().count():,}')
    else:
      print('RETAILER_ITEM_ID is not in columns, cannot get approved product list')
else:
    print('Approved product list not found for client')

# COMMAND ----------

# Select stores and items that their last sales from current_day are less than "min_days_per_item"
df_sub = df_pos.filter(
    'POS_ITEM_QTY > 0'
).groupby(
    'RETAILER_ITEM_ID',
    'ORGANIZATION_UNIT_NUM'
).agg( 
  pyf.max(pyf.col('SALES_DT')).alias('MAX_DATE')
)
 
df_sub = df_sub.withColumn(
    'NUM_DAYS_SOLD',
    pyf.datediff(pyf.current_date(),pyf.col("MAX_DATE"))
).filter(pyf.col('NUM_DAYS_SOLD') <= 60)
 
  
df_pos = df_pos.join(
    df_sub.select('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'),
    (
        (df_pos['RETAILER_ITEM_ID'] == df_sub['RETAILER_ITEM_ID']) &
        (df_pos['ORGANIZATION_UNIT_NUM'] == df_sub['ORGANIZATION_UNIT_NUM'])
    ),
    how='left_semi'
)
print(f'{df_pos.cache().count():,}')

# COMMAND ----------

#Keep traited/valid items if applicable
try: 
  db_list = spark.sql('show databases')
  db_list = db_list.filter((pyf.col('databaseName').like(f'dwr_{retailer}_{client[0:4]}%_{country_code}_im')))
  db = db_list.toPandas()['databaseName'].unique().tolist()
  db_name = db[0]
  table_name = f'{db_name}.vw_fact_traited_valid'
  query = f"""select `Store Nbr`, `Item Nbr`  
    from {table_name}
    where `Curr Traited Store Item Comb` = 1 AND `Curr Valid Store Item Comb` = 1 """
  df_valid_store_item  = spark.sql(query)  
  df_pos = df_pos.join(df_valid_store_item,
                     ((df_pos['RETAILER_ITEM_ID'] == df_valid_store_item['Item Nbr']) &
                      (df_pos['ORGANIZATION_UNIT_NUM'] == df_valid_store_item['Store Nbr'])),
                     how='inner'
                    ).select[df_pos['*']]
  print(f'N after removing inactive store and item: {df_pos.cache().count():,}')

except: 
    print('traited/valid Table is not found')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training Autoencoder

# COMMAND ----------

# Prepare data for Neural Network
df_pos = pivot_pos(
    df_pos,
    'weekly',
    2
).fillna(-1)

df_store_item = pd.DataFrame(
    df_pos.select('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM').collect(),
    columns=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
)
print(f'{df_pos.cache().count():,}')

# COMMAND ----------

loss_object_dict = {
    'mean_absolute_error_with_ignore': mean_absolute_error_with_ignore
}

# COMMAND ----------

experiment_name = f'/Users/nafiseh.asghari@mosaic.com/gen2_dla/{retailer}_{client}_{country_code}_NNModel'
mlflow.set_experiment(experiment_name)
mlclient = mlflow.tracking.MlflowClient()
experiment_id = mlclient.get_experiment_by_name(experiment_name).experiment_id

# COMMAND ----------

latent_dim = 20
dense_dim = 40
scaler = MinMaxScaler(feature_range=(0, 1))
inp_x = df_pos.drop('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM').toPandas()
inp_x = pd.DataFrame(scaler.fit_transform(inp_x))
inp_x = inp_x.values.astype('float32')

if inp_x.shape[1] < latent_dim:
    dense_dim = latent_dim = inp_x.shape[1]

elif inp_x.shape[1] < dense_dim:
    dense_dim = inp_x.shape[1]

# COMMAND ----------

train_model({
    'dense_dim': dense_dim,
    'latent_dim': latent_dim,
    'lr': 0.001,
    'batch_size': 512,
    'epochs': 100,
    'data': inp_x,
    'data_tag': 'weekly',
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load best Model

# COMMAND ----------

# Load the most recent run
best_run = mlflow.search_runs(order_by=['start_time desc']).iloc[0]
print(f'RMSE of Best Run: {best_run["metrics.RMSE"]:.2f} and R2 :{best_run["metrics.R2_Score"]:.2f}')
 
encoder = mlflow.keras.load_model(
    best_run.artifact_uri + '/encoder',
    custom_objects=loss_object_dict
)
autoencoder = mlflow.keras.load_model(
    best_run.artifact_uri + '/autoencoder',
    custom_objects=loss_object_dict
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Searching for best matches using KNN

# COMMAND ----------

def explode_stores(pdf):
    df_store_item = pdf.withColumn(
        'TEST_ORGANIZATION_UNIT_NUM',
        pdf['ORGANIZATION_UNIT_NUM']
    ).select(
        'RETAILER_ITEM_ID',
        'TEST_ORGANIZATION_UNIT_NUM'
    )
    schema_list = list(df_store_item.schema) + [pyt.StructField('all_stores', pyt.ArrayType(pyt.StringType()), True)]

    def add_stores_list(pdf: pd.DataFrame) -> pd.DataFrame:
        store_list = list(pdf['TEST_ORGANIZATION_UNIT_NUM'].unique())
        pdf['all_stores'] = [store_list] * pdf.shape[0]
        return pdf

    df_store_item = df_store_item\
        .groupby('RETAILER_ITEM_ID')\
        .applyInPandas(
            add_stores_list,
            schema=pyt.StructType(schema_list)
        )
    cols = [
        'RETAILER_ITEM_ID',
        'TEST_ORGANIZATION_UNIT_NUM',
        pyf.explode(pyf.col('all_stores')).alias('ORGANIZATION_UNIT_NUM')
    ]
    df_store_item = df_store_item.select(cols)
    df_exploded_stores = df_store_item.join(
        pdf, ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'])
    return df_exploded_stores

# COMMAND ----------

df_embedding = pd.DataFrame(encoder.predict(inp_x))
df_embedding = pd.concat([df_store_item, df_embedding], axis=1)
df_embedding = spark.createDataFrame(df_embedding)
df_embedding = explode_stores(df_embedding)
print(f'{df_embedding.cache().count():,}')

# COMMAND ----------

schema_list = list(df_embedding.select(
    'RETAILER_ITEM_ID',
    'TEST_ORGANIZATION_UNIT_NUM',
    'ORGANIZATION_UNIT_NUM'
).schema)
schema_list += [pyt.StructField('DISTANCE', pyt.FloatType(), True)]
schema_cols = [col.name for col in schema_list]

total_matches = 5
def match(pdf):
    n_matches = total_matches + 1
    test_store = pdf['TEST_ORGANIZATION_UNIT_NUM'].unique()[0]
    x_training = pdf.reset_index(drop=True)
    x_test = pdf[pdf['ORGANIZATION_UNIT_NUM'] == test_store]

    cols_to_drop = [
        'RETAILER_ITEM_ID',
        'TEST_ORGANIZATION_UNIT_NUM',
        'ORGANIZATION_UNIT_NUM'
    ]
    try:
        search_space = NearestNeighbors(n_neighbors=n_matches, metric='cosine')
        search_space.fit(
            x_training.drop(columns=cols_to_drop).values
        )
        distance, near_match_indices = search_space.kneighbors(
            x_test.drop(columns=cols_to_drop).values,
            n_matches
        )
        df = x_training.loc[
            near_match_indices[0],
            cols_to_drop
        ]
        df['DISTANCE'] = distance[0]
    except Exception as e:
        print(f'===> Error Store {test_store}: {e}')
        traceback.print_exc()
        df = x_training[cols_to_drop]
        df['DISTANCE'] = None
    return df[schema_cols]

df_similar_stores = df_embedding.groupby(
    'RETAILER_ITEM_ID',
    'TEST_ORGANIZATION_UNIT_NUM'
).applyInPandas(
    match,
    schema=pyt.StructType(schema_list)
)
print(f'{df_similar_stores.cache().count():,}')
main_size_similar_store = df_similar_stores.count()

# COMMAND ----------

display(df_similar_stores.agg({'DISTANCE': 'avg'}).collect())

# COMMAND ----------

print('Number of null/nan values for all columns:')
df_similar_stores.select([pyf.count(pyf.when(pyf.isnan(c)| pyf.col(c).isNull(), c)).alias(c) for c in df_similar_stores.columns]).show()

# COMMAND ----------

print('Number of unique entity for all columns:')
df_similar_stores.agg(*(pyf.countDistinct(pyf.col(c)).alias(c) for c in df_similar_stores.columns)).show()

# COMMAND ----------

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

  
def get_cap_value(company_id, parent_chain_id, manufacturer_id):
  query = f"""select
  v.RefExternal as RETAILER_ITEM_ID,
  v.Lkp_productGroupId,
  c.Lkp_productGroupId as cap_LkpproductGroupId, c.CapType, c.CapValue 
  
  FROM BOBv2.vw_BOBv2_Product AS v
  INNER JOIN BOBv2.Product AS p
  ON p.ProductId = v.ProductId
  AND v.ParentChainId = '{parent_chain_id}'

  INNER JOIN BOBv2.vw_bobv2_caps AS c
          ON  p.CompanyId = c.CompanyId
          AND (CapType = "OSA LSV Minimum" OR CapType = "OSARows")
          AND (v.ManufacturerId = c.ManufacturerId or (c.ManufacturerId IS NULL))
          
  WHERE
  v.CompanyId = '{company_id}' 
  AND (c.ManufacturerId is Null and '{manufacturer_id}'= "None") OR (c.ManufacturerId = '{manufacturer_id}')
  AND p.UniversalProductCode IS NOT NULL
  """

  df_caps_value = spark.sql(query)
  df_caps_value = df_caps_value.drop_duplicates()
  return df_caps_value

# COMMAND ----------

df_caps_value = get_cap_value(company_id, parent_chain_id, manufacturer_id)
df_similar_stores = reformat_retailer_item_id(df_similar_stores
).join(
    reformat_retailer_item_id(df_caps_value.select('Lkp_productGroupId', 'RETAILER_ITEM_ID').drop_duplicates()).drop('RETAILER_ITEM_ID'),
    ['ITEM_ID']
).drop('ITEM_ID')

print(f'N = {df_similar_stores.cache().count():,}')

# COMMAND ----------

if df_caps_value.select('cap_LkpproductGroupId', 'CapType', 'CapValue').drop_duplicates().filter(~pyf.col('cap_LkpproductGroupId').isNull()).count() == 0:
    osa_lsv_minimum = df_caps_value.filter(
        'CapType == "OSA LSV Minimum"'
    ).select(
        'CapValue'
    ).collect()[0][0]

    df_similar_stores = df_similar_stores.withColumn('CapValue', pyf.lit(osa_lsv_minimum)).drop('Lkp_productGroupId')

else:
    df_similar_stores = df_similar_stores.join(
        df_caps_value.selectExpr(
          'cap_LkpproductGroupId', 'CapType', 'CapValue as CAP_VALUE')\
          .drop_duplicates()\
          .filter('CapType == "OSA LSV Minimum"'),
        df_similar_stores['Lkp_productGroupId'] == df_caps_value['cap_LkpproductGroupId'],
        how='inner'
    ).drop('CapType', 'cap_LkpproductGroupId', 'Lkp_productGroupId')


maximum_num_alerts = float(
    df_caps_value.filter(
        'CapType == "OSARows"'
    ).select('CapValue').collect()[0][0]
)
df_similar_stores = df_similar_stores.withColumnRenamed('CapValue', 'CAP_VALUE')
df_similar_stores = df_similar_stores.withColumn('MAX_ALERTS_PER_STORE', pyf.lit(maximum_num_alerts))
print(f'N = {df_similar_stores.cache().count():,}')

# COMMAND ----------

file_dir = f'/mnt{"/prod-ro" if environment == "prod" else ""}/artifacts/gen2_dla/similar_stores/{retailer}_{client}_{country_code}/'
file_name = f'{retailer}_{client}_{country_code}_matched_stores'
file_path = f'{file_dir}{file_name}'
df_similar_stores\
    .write.mode('overwrite').format('delta')\
    .option('overwriteSchema', 'True')\
    .save(f'{file_path}.{datetime.datetime.now().strftime("%Y-%m-%d")}')
