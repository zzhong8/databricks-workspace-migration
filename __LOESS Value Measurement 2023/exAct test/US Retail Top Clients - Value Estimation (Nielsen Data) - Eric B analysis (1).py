# Databricks notebook source
import numpy as np
import matplotlib.pyplot as plt
import pyspark.sql.functions as func

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create & write "velocities" view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW acv AS
# MAGIC select holding_description, avg(swklyvol) as volume from nielsenpos_store_sales_im.vw_dimension_store_tdlinx
# MAGIC group by holding_description 
# MAGIC --where _ like 'Walm%'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW velocities AS 
# MAGIC SELECT 
# MAGIC   --fact.*, 
# MAGIC   fact.store_tdlinx_id,
# MAGIC   fact.upc, 
# MAGIC   td.holding_description,
# MAGIC   acv.volume,
# MAGIC   count(period) as num_weeks,
# MAGIC   avg(fact.unit_sales) as avg_store_sales,
# MAGIC   CAST(
# MAGIC     CASE 
# MAGIC       when sum(fact.dollar_sales)/sum(fact.unit_sales) >= 0.01 then sum(fact.dollar_sales)/sum(fact.unit_sales) 
# MAGIC       else 0 
# MAGIC       end
# MAGIC       AS float)
# MAGIC     AS price,
# MAGIC   CASE
# MAGIC     when upc.be_low = 'CAMPBELL SOUP CO' then 'Campbell\'s'
# MAGIC     when upc.be_low = 'CHATTEM, INC.' then 'Sanofi'  --validated brands match Sanofi scorecard
# MAGIC     when upc.be_low in ('SIMPLY ORANGE JUICE COMPANY','MINUTE MAID COMPANY') then 'Coca Cola'
# MAGIC     when upc.be_low = 'KEN\'S FOODS, INC.' then 'Ken\'s'
# MAGIC     when upc.be_low in ('QUEST NUTRITION','ATKINS NUTRITIONALS INC') then 'Simply Good Foods'
# MAGIC     when upc.be_low = 'HARRY\'S, INC.' then 'Harry\'s'
# MAGIC     when upc.be_low in ('DANNON COMPANY INC., THE','WHITEWAVE FOODS') then 'Danone'
# MAGIC     when upc.be_low = 'BIC CORPORATION' then 'BIC'
# MAGIC     when upc.be_low = 'GEORGIA-PACIFIC CORPORATION' then 'Georgia Pacific'
# MAGIC     when upc.be_low = 'TYSON FOODS INC' then 'Tyson'
# MAGIC     when upc.be_low = 'MARS INCORPORATED' then 'Mars'
# MAGIC     when upc.be_low = 'MORTON INTERNATIONAL INC' then 'Morton Salt'
# MAGIC     when upc.be_low = 'KAO USA INC' then 'Kao'
# MAGIC     when upc.be_low = 'BARILLA AMERICA, INC.' then 'Barilla'
# MAGIC     when upc.be_low = 'NISSIN FOODS USA CO INC' then 'Nissin'
# MAGIC     else '!wut'
# MAGIC     end
# MAGIC     AS client_rollup
# MAGIC FROM 
# MAGIC   nielsenpos_store_sales_im.fact_store_sales as fact
# MAGIC   inner join nielsenpos_store_sales_im.vw_dimension_store_tdlinx as td ON fact.store_tdlinx_id=td.store_tdlinx_id
# MAGIC   inner join acv on td.holding_description = acv.holding_description
# MAGIC   inner join nielsenpos_store_sales_im.vw_upc_nielsen  as upc on fact.upc = upc.upc
# MAGIC   
# MAGIC WHERE
# MAGIC   date(period) >= date('2022-02-08')
# MAGIC   --upc.be_low in ('CAMPBELL SOUP CO','CHATTEM, INC.','SIMPLY ORANGE JUICE COMPANY','MINUTE MAID COMPANY','KEN\'S FOODS, INC.','QUEST NUTRITION','ATKINS NUTRITIONALS INC','HARRY\'S, INC.','DANNON COMPANY INC., THE','WHITEWAVE FOODS','BIC CORPORATION','GEORGIA-PACIFIC CORPORATION','TYSON FOODS INC','MARS INCORPORATED','MORTON INTERNATIONAL INC','KAO USA INC','BARILLA AMERICA, INC.','NISSIN FOODS USA CO INC')
# MAGIC   --upc.be_low = 'BARILLA AMERICA, INC.'
# MAGIC   -- upc.be_low = 'CHATTEM, INC.'
# MAGIC   
# MAGIC GROUP BY client_rollup, fact.store_tdlinx_id, fact.upc, td.holding_description, acv.volume

# COMMAND ----------

# Load, save "velocities" table/view to blob storage (delta format)
df_ps = spark.sql("SELECT * FROM velocities")

path = '/mnt/processed/ari/exact/velocities'
df_ps.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)
print(f'Successfully saved df_v to {path}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data (if previously saved)

# COMMAND ----------

# Load "velocities" table from blob storage (if starting here)
path = '/mnt/processed/ari/exact/velocities'
df_ps = spark.read.format('delta').option("inferSchema", "true").option("header", "true").load(path)

# Convert to Pandas
# df = df_ps.toPandas()
# print(f"df: {len(df):,} rows")

# COMMAND ----------

# Preview df
print(f"velocities dataframe loaded w/ {len(df):,} rows")
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data cleaning

# COMMAND ----------

# Subset df to our clients
all_clients = ['Campbell\'s', 'Sanofi', 'Coca Cola', 'Ken\'s', 'Simply Good Foods', 'Harry\'s', 'Danone', 'BIC', 'Georgia Pacific', 'Tyson', 'Mars', 'Morton Salt', 'Kao', 'Barilla', 'Nissin']

df_all_clients = df[df['client_rollup'].isin(all_clients)].copy()
print(f"df_all_clients: {len(df_all_clients):,} rows")

# Create a dollar_sales column = sales vol * price for $ sales * num weeks
df_all_clients['dollar_sales'] = df_all_clients['avg_store_sales'] * df_all_clients['price'] * df_all_clients['num_weeks']

# Preview data for all our clients
df_all_clients.head()

# COMMAND ----------

##### TEMP - backup df_all_clients ####
df_all_clients_orig = df_all_clients.copy()

# COMMAND ----------

##### TEMP  - restore df_all_clients ####
# df_all_clients = df_all_clients_orig.copy()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter out low-velocity holding/client combos
# MAGIC - Remove holding/client combinations that sell less than $500 per week from df_all_clients

# COMMAND ----------

# Group by holding name & get mean data for each holding/client combo
df_all_clients_sum = df_all_clients.groupby(by=['holding_description', 'client_rollup']).sum()

# Remove holdings (retailers) with near-0 sales velocity
dollar_sales_cutoff = 500
idx_to_remove = df_all_clients_sum[df_all_clients_sum['dollar_sales'] < dollar_sales_cutoff].index  # Low-velocity retailer/client combos
idx_to_keep = [idx for idx in df_all_clients_sum.index if idx not in idx_to_remove]  # High-enough-velocity retailer/client combos

print(f"{len(idx_to_remove):,} holding/client combos with low-velocity")
print(f"{len(idx_to_keep):,} holding/client combos with high-enough-velocity")

# Reindex df_all_clients, subset it by idx_to_keep, and reset its index
len_1 = len(df_all_clients)
df_all_clients_reindexed = df_all_clients.set_index(keys=['holding_description', 'client_rollup'])
df_all_clients_reindexed_keep = df_all_clients_reindexed[df_all_clients_reindexed.index.isin(idx_to_keep)]
df_all_clients = df_all_clients_reindexed_keep.reset_index()  # Overwrite df_all_clients

print(f"df_all_clients reduced from {len_1:,} (all holding/client combos) to {len(df_all_clients):,} (holding/client combos for dollar_sales > ${dollar_sales_cutoff} only)")
df_all_clients.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Focus on majority of good-selling items
# MAGIC - Remove each holding/client combination's bottom-selling SKUs (by dollar_sales)

# COMMAND ----------

# Get list of all UPCs to keep - top 60% selling SKUs
low_quantile, high_quantile = 0.4, 1.0
upcs_to_keep = []

# Get all holding/client combinations
gb_cols = ['holding_description', 'client_rollup', 'upc']
keep_cols = gb_cols + ['dollar_sales']

# Get total dollar_sales for each holding/client/upc group
df_holding_client_upc_sum = df_all_clients[keep_cols].groupby(by=gb_cols).sum()

# Get holding/client groups to iterate over
gb_cols = ['holding_description', 'client_rollup']
holding_client_combo_keys = df_all_clients[gb_cols].groupby(by=gb_cols).groups.keys()

for key_combo in holding_client_combo_keys:
  # Get series for each holidng/client combo
  series_one_combo = df_holding_client_upc_sum.loc[key_combo].loc[:, 'dollar_sales']

  # Get UPCs to keep & add to list
  upcs = series_one_combo[series_one_combo.between(series_one_combo.quantile(low_quantile), series_one_combo.quantile(high_quantile))].index.tolist()
  upcs_to_keep += upcs
  #   print(len(series_one_combo), '-->', len(upcs))  # TEMP: see how many UPCs we're removing
  
# Update df_all_clients to contain only upcs_to_keep
len_1 = len(df_all_clients)
df_all_clients = df_all_clients[df_all_clients['upc'].isin(upcs_to_keep)]

print(f"df_all_clients reduced from {len_1:,} (all UPCS) to {len(df_all_clients):,} (high-enough-velocity UPCs only)")
df_all_clients.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove outlier(s)
# MAGIC - Outlier client: BJ's Wholesale Club

# COMMAND ----------

# Outlier(s) to remove: "BJ's Wholesale Club"
outlier_holding_descriptions = ['BJ\'s Wholesale Club']

df_all_clients_inliers = df_all_clients[~df_all_clients['holding_description'].isin(outlier_holding_descriptions)]
print(f"Outliers removed: {outlier_holding_descriptions}")
print(f"df_all_clients reduced from {len(df_all_clients):,} to {len(df_all_clients_inliers):,} rows")

# COMMAND ----------

# Are there for clients for whom we take an avg of velocity & price
# Based on rate and sale & price is there a central tendency that, if you use it, do you capture the variance well?
# NOW - how much does it matter which product we're touching? ->> bring BRAND into play, use UPC-to-brand mapping
# Break each data point out to brand (1 point -> N points)

# Hypothesis: the client's portfolio is characterized by a single number () across all products
# If you do action X, it's worth Y -> Y is a function of product velocity, price, and constraints on what you can do (max shelf space, etc.)
# Problem: we don't know which item they're touching at an item-level. We don't even know what TYPE of product. Do we need this information?

# Null hypothesis: we have a single line for the client
# OTHER: if we know the brand they touched, we can do a better job -> e.g. for Sanofi, what if we plot price vs. volume for sku for 1 retailer or across retailers.

# Look at skus across retailer, unit sales volume by price (low-selling high-price vs high-selling low-price) - is there a smear of all products, or are they clustered (disjoint portfolio)

# This can be a ML thing, but we're not operationalizing it - the question now is "what makes sense for this project?"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Sales Velocity vs. Sales Volume

# COMMAND ----------

def graph_sales(df, str=''):
  """
  :param df: dataframe with columns "volume" and "dollar_sales" (these should both be averaged over many rows)
  """
  if str:
    title = f'Mean dollar sales vs. volume for all retailers ({str} only)'
  else:
    title = 'Mean dollar sales vs. volume for all retailers (all holding/client combos)'
  
  x, y = df['volume'], df['dollar_sales']
  plt.figure(figsize=(10, 6))
  plt.scatter(x, y)
  plt.xlabel('Sales volume', fontsize=14)
  plt.ylabel('Sales velocity (mean dollar sales)', fontsize=14)
  plt.title(title, fontsize=16)
  plt.show()
  
  return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### All clients

# COMMAND ----------

# Visualize velocity vs. volume (all clients)
df_all_clients_mean = df_all_clients_inliers.groupby(by=['holding_description']).mean()
graph_sales(df_all_clients_mean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Individual clients
# MAGIC - All portfolios, each data point a different retailer ("holding_description")

# COMMAND ----------

# Visualize velocity vs. volume (individual clients)
for client in all_clients:
  df_single_client = df_all_clients_inliers[df_all_clients_inliers['client_rollup'] == client]
  df_single_client_mean = df_single_client.groupby(by=['holding_description']).mean()
  print(client)
  graph_sales(df_single_client_mean, str=client)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Individual clients (by subcategory)
# MAGIC - All portfolios, each data point a different retailer ("holding_description") / "subcategory" (from join on nielsenpos_store_sales_im.vw_upc_nielsen)

# COMMAND ----------

# MAGIC %md
# MAGIC #### CATEGORY

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW upc_to_category AS 
# MAGIC SELECT upc, category
# MAGIC FROM nielsenpos_store_sales_im.vw_upc_nielsen

# COMMAND ----------

# Load category df
df_category_ps = spark.sql("SELECT * FROM upc_to_category")
df_category = df_category_ps.toPandas()

# Merge to get updated df_all_clients_inliers w/ categories
df_all_clients_inliers_c = df_all_clients_inliers.merge(right=df_category, how='left', on='upc')
print(f"df_all_clients_inliers_c produced with {len(df_all_clients_inliers_c ):,} distinct UPCs and {len(df_all_clients_inliers_c ['category'].unique()):,} distinct categories")

df_all_clients_inliers_c.head()

# COMMAND ----------

# EXPORT df_all_clients_inliers_c TO BLOB STORAGE
df_out = spark.createDataFrame(df_all_clients_inliers_c)
path = f'/mnt/processed/ari/exact/df_all_clients_inliers_c'
df_out.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)
print(f'Successfully saved df to {path}')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SUBCATEGORY

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW upc_to_subcategory AS 
# MAGIC SELECT upc, subcategory
# MAGIC FROM nielsenpos_store_sales_im.vw_upc_nielsen

# COMMAND ----------

# Load subcategory df
df_subcategory_ps = spark.sql("SELECT * FROM upc_to_subcategory")
df_subcategory = df_subcategory_ps.toPandas()
# print(f"df_subcategory loaded with {len(df_subcategory):,} distinct UPCs and {len(df_subcategory['subcategory'].unique()):,} distinct subcategories")

# Merge to get updated df_all_clients_inliers w/ subcategories
df_all_clients_inliers_sc = df_all_clients_inliers.merge(right=df_subcategory, how='left', on='upc')
print(f"df_all_clients_inliers_sc produced with {len(df_all_clients_inliers_sc ):,} distinct UPCs and {len(df_all_clients_inliers_sc ['subcategory'].unique()):,} distinct subcategories")

df_all_clients_inliers_sc.head()

# COMMAND ----------

path = '/mnt/processed/ari/exact/df_all_clients_inliers_c'
df_c = spark.read.format('delta').option("inferSchema", "true").option("header", "true").load(path)

# COMMAND ----------

from pyspark.sql.functions import median, col, udf
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import ArrayType, DoubleType, IntegerType

# COMMAND ----------

sales_df = df_c.filter("client_rollup == 'Sanofi'")

# COMMAND ----------

# Define the bucket boundaries
min_sales = sales_df.selectExpr('min(avg_store_sales)').collect()[0][0]
max_sales = sales_df.selectExpr('max(avg_store_sales)').collect()[0][0]
step_size = (max_sales - min_sales) / 10
bucket_boundaries = [min_sales + (i * step_size) for i in range(11)]

# Define the bucketizer
bucketizer = Bucketizer(splits=bucket_boundaries, inputCol='median_sales', outputCol='buckets')

# Define a UDF to convert DenseVector to array of bucket indices
to_array = udf(lambda x: [int(i) for i in x], ArrayType(IntegerType()))

# Apply the bucketizer and UDF to the data
histo_df = sales_df.groupBy('holding_description') \
    .agg(median('avg_store_sales').alias('median_sales')) \
    .select(col('holding_description'), to_array(bucketizer.transform(col('median_sales'))).alias('buckets'))

histo_df.show()

# COMMAND ----------

# EXPORT df_all_clients_inliers_sc TO BLOB STORAGE
df_out = spark.createDataFrame(df_all_clients_inliers_sc)
path = f'/mnt/processed/ari/exact/df_all_clients_inliers_sc'
df_out.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)
print(f'Successfully saved df to {path}')

# COMMAND ----------

# Visualize velocity vs. volume (individual clients/subcategories)
for client in all_clients[:1]:
  df_single_client = df_all_clients_inliers_sc[df_all_clients_inliers_sc['client_rollup'] == client]
  df_single_client_mean = df_single_client.groupby(by=['holding_description', 'subcategory']).mean()
#   display(df_single_client_mean)
  print(client)
  graph_sales(df_single_client_mean, str=client)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Plot sale velocity ("avg_store_sales") vs. unit price ("price") & for different retailers

# COMMAND ----------

df_all_clients_inliers_sc.head()

# COMMAND ----------

# Limit all data to 1 client, small number of retailers
clients = ['Campbell\'s']
retailers = ['Ahold USA', 'Albertsons', 'Target']
df_temp = df_all_clients_inliers_sc[df_all_clients_inliers_sc['client_rollup'].isin(clients)].copy()
df_temp = df_temp[df_temp['holding_description'].isin(retailers)].copy()

print(f"{len(df_temp):,} distinct rows for {len(clients)} client ({clients[0]}) and {len(retailers)} retailers: ({retailers})")

# COMMAND ----------

df_temp.head(3)

# COMMAND ----------

df_temp.sort_values(by=['price'])

# COMMAND ----------

retailer_colors = {'Ahold USA':'blue', 'Albertsons':'red', 'Target':'green'}

xlabel = 'Unit price'
ylabel = 'Velocity (sales per week)'
title = f"{ylabel} vs. {xlabel}"

plt.figure(figsize=(10, 6))
for retailer in retailers:
  df_subset = df_temp[df_temp['holding_description'] == retailer]
  x, y = df_subset['price'], df_subset['avg_store_sales']
  plt.scatter(x, y, marker='.', c=retailer_colors[retailer], label=retailer, alpha=0.5)
plt.xlim(0, 20)
plt.xlabel(xlabel, fontsize=14)
plt.ylabel(ylabel, fontsize=14)
plt.title(title, fontsize=16)
plt.legend(fontsize=14)
plt.show()

# COMMAND ----------

for retailer in retailers:
  plt.figure(figsize=(10, 8))
  df_subset = df_temp[df_temp['holding_description'] == retailer]
  x, y = df_subset['price'], df_subset['avg_store_sales']
  plt.scatter(x, y, marker='.', c=retailer_colors[retailer], label=retailer)
  plt.xlim(0, 20)
  plt.xlabel(xlabel, fontsize=14)
  plt.ylabel(ylabel, fontsize=14)
  plt.title(title, fontsize=16)
  plt.legend(fontsize=14)
  plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Plot median sale velocity ("avg_store_sales") vs. mean unit price ("price") & for different retailer/client/upc groups (combine stores)

# COMMAND ----------

# Limit all data to 1 client, small number of retailers
clients = ['Campbell\'s']
retailers = ['Ahold USA', 'Albertsons', 'Target']
df_temp = df_all_clients_inliers_sc[df_all_clients_inliers_sc['client_rollup'].isin(clients)].copy()
df_temp = df_temp[df_temp['holding_description'].isin(retailers)].copy()

# 3 retailers

print(f"{len(df_temp):,} distinct rows for {len(clients)} client ({clients[0]}) and {len(retailers)} retailers: ({retailers})")

# COMMAND ----------

for retailer in retailers:
  df_subset = df_temp[df_temp['holding_description'] == retailer]
  data = df_subset.groupby(by=['upc']).agg({'avg_store_sales':'median', 'price':'mean'})
  x, y = data['price'], data['avg_store_sales']
  
  plt.figure(figsize=(10, 8))
  plt.scatter(x, y, marker='.', c=retailer_colors[retailer], label=retailer)
  plt.xlim(0, 10)
  plt.xlabel(xlabel, fontsize=14)
  plt.ylabel(ylabel, fontsize=14)
  plt.title(title, fontsize=16)
  plt.legend(fontsize=14)
  plt.show()

# COMMAND ----------



# COMMAND ----------

# Can we predict velocity given ACV (volume) and subcategory sales?
# Linreg for each subcategory

# 1 client, 3 retailers

# Other question: rate of sale (unit movement) vs. avg. price - are these related within each subcategory?
# what's this look like? Smooth? Clumpy?
# GROUP BY UPC graph price, avg_store_sales


df_all_clients_inliers_sc

# COMMAND ----------

len(df_single_client_mean.reset_index()['subcategory'].unique())

# COMMAND ----------

len(df_single_client_mean)

# COMMAND ----------

z = df_all_clients_inliers_sc.groupby(by=['subcategory']).sum()
z.sort_values(by=['dollar_sales']).tail(10)

# COMMAND ----------


df_subset

# COMMAND ----------

# PRICE vs. UNIT VELOCITY by SKU (UPC) within a retailer.

top_5_subcategories = ['JUICE OJ', 'NON DAIRY', 'DRINKS ADES', 'CONDENSED', 'DAIRY']
df_subset = df_all_clients_inliers_sc[df_all_clients_inliers_sc['subcategory'].isin(top_5_subcategories)]

# Visualize velocity vs. volume (individual clients/subcategories)
for client in all_clients[:1]:
  print(client)
  df_single_client = df_subset[df_subset['client_rollup'] == client]
  df_single_client_mean = df_single_client.groupby(by=['holding_description', 'subcategory']).mean()
#   display(df_single_client_mean)
  print(len(df_single_client_mean))
  graph_sales(df_single_client_mean, str=client)

# COMMAND ----------

df_subset.head()

# COMMAND ----------



# COMMAND ----------

# Select top 40 holdings (holding name / ID - ownership group of a retailer) by ACV
# If you're hugging the x-axis, you're probably a problem
# E.g. any holding's mean volume < 0.05 (remove holdings with low volume)
# 0s could be "doesn't carry", "discontinued", "no data for the sku", other error.
# Remove 0s for ALL clients, then consider 0s for specific clients

# Our goal: for a retailer/client, what's the typical rate of sale for a given SKU?
# For top N selling items (e.g. 60% of value sales) what is the rate of sale and associated price for those SKUs? Is it relatively uniform or not?
# Is is safe NOT knowing which SKU we're touching OK because of they all have some central tendency? - Among the SKUs in a protfolio, within a retailer, for a given client, can we show there's consistnet relationship between volume and velocity?

# Among the holdings for which we're going to analyze (filter out 0s first), how varied


# COMMAND ----------

display(df_quartiles)

# COMMAND ----------



# COMMAND ----------

# One client, examine all stores
client = all_clients[0]
df_single_client = df_all_clients[df_all_clients['client_rollup'] == client]
df_single_client_mean = df_single_client.groupby(by=['store_tdlinx_id']).median()  # Group by store (not retailer)
graph_sales(df_single_client_mean, str=client)

# COMMAND ----------

plt.figure(figsize=(12,6))
plt.hist(df_single_client_mean['price'], bins=20)
plt.show()

# COMMAND ----------

z = df_single_client.groupby(by=['store_tdlinx_id']).mean()
len(z)

# COMMAND ----------

df_single_client_mean[df_single_client_mean['price'] > 3.01]

# COMMAND ----------

# # TEMP SAVE TO BLOB STORAGE
# df_out = spark.createDataFrame(df_interventions_all_dates)
# path = f'/mnt/processed/ari/exact/temp_df_interventions_all_dates'
# df_out.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)
# print(f'Successfully saved df to {path}')
