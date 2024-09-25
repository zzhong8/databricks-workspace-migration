# Databricks notebook source
# MAGIC %md
# MAGIC # Exploration
# MAGIC - how should we cut up the data to create our coefficient table? entire-client-portofolio, by category, individual upc
# MAGIC - hierarchical clustering
# MAGIC - start with Nissin
# MAGIC - distribution of upc pricing, sales volume, rate of sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load packages and parameters

# COMMAND ----------

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pyspark.sql.functions as func

from scipy import stats

pd.set_option('display.max_rows', 500)

# COMMAND ----------

path = '/mnt/processed/davis/initial_extract'
p_value = 0.05

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get data

# COMMAND ----------

# Get UPC details
tuple_clients = (
    'CAMPBELL SOUP CO',
    'CHATTEM, INC.',
    'SIMPLY ORANGE JUICE COMPANY', 'MINUTE MAID COMPANY',
    'KEN\'S FOODS, INC.',
    'QUEST NUTRITION','ATKINS NUTRITIONALS INC',
    'HARRY\'S, INC.',
    'DANNON COMPANY INC., THE','WHITEWAVE FOODS',
    'BIC CORPORATION',
    'GEORGIA-PACIFIC CORPORATION',
    'TYSON FOODS INC',
    'MARS INCORPORATED',
    'MORTON INTERNATIONAL INC',
    'KAO USA INC',
    'BARILLA AMERICA, INC.',
    'NISSIN FOODS USA CO INC'
)
query = f"""select 
    distinct upc.upc,
    upc.category,
    upc.subcategory,
    upc.brand,
    upc.department,
    case
        when upc.be_low = 'CAMPBELL SOUP CO' then "Campbell's"
        when upc.be_low = 'CHATTEM, INC.' then 'Sanofi'
        when upc.be_low in ('SIMPLY ORANGE JUICE COMPANY','MINUTE MAID COMPANY') then 'Coca Cola'
        when upc.be_low = "KEN'S FOODS, INC." then "Ken's"
        when upc.be_low in ('QUEST NUTRITION','ATKINS NUTRITIONALS INC') then 'Simply Good Foods'
        when upc.be_low = "HARRY\'S, INC." then "Harry's"
        when upc.be_low in ('DANNON COMPANY INC., THE','WHITEWAVE FOODS') then 'Danone'
        when upc.be_low = 'BIC CORPORATION' then 'BIC'
        when upc.be_low = 'GEORGIA-PACIFIC CORPORATION' then 'Georgia Pacific'
        when upc.be_low = 'TYSON FOODS INC' then 'Tyson'
        when upc.be_low = 'MARS INCORPORATED' then 'Mars'
        when upc.be_low = 'MORTON INTERNATIONAL INC' then 'Morton Salt'
        when upc.be_low = 'KAO USA INC' then 'Kao'
        when upc.be_low = 'BARILLA AMERICA, INC.' then 'Barilla'
        when upc.be_low = 'NISSIN FOODS USA CO INC' then 'Nissin'
        else '!wut'
        end
        AS client_rollup
    from nielsenpos_store_sales_im.vw_upc_nielsen as upc
    where be_low in {tuple_clients}
"""

df_upcs = spark.sql(query).toPandas()
df_upcs.head()

# COMMAND ----------

# %sql
# /* Create the temporary view to store volume */
# CREATE OR REPLACE TEMPORARY VIEW acv AS
# select holding_description, avg(swklyvol) as volume from nielsenpos_store_sales_im.vw_dimension_store_tdlinx
# group by holding_description;

# /* Select the */
# CREATE OR REPLACE TEMPORARY VIEW velocities AS 
# SELECT 
#   fact.store_tdlinx_id,
#   fact.upc, 
#   td.holding_description,
#   acv.volume,
#   count(period) as num_weeks,
#   avg(fact.unit_sales) as avg_store_sales,
#   CAST(
#     CASE 
#       when sum(fact.dollar_sales)/sum(fact.unit_sales) >= 0.01 then sum(fact.dollar_sales)/sum(fact.unit_sales) 
#       else 0 
#       end
#       AS float)
#     AS price,
#   CASE
#     when upc.be_low = 'CAMPBELL SOUP CO' then 'Campbell\'s'
#     when upc.be_low = 'CHATTEM, INC.' then 'Sanofi'
#     when upc.be_low in ('SIMPLY ORANGE JUICE COMPANY','MINUTE MAID COMPANY') then 'Coca Cola'
#     when upc.be_low = 'KEN\'S FOODS, INC.' then 'Ken\'s'
#     when upc.be_low in ('QUEST NUTRITION','ATKINS NUTRITIONALS INC') then 'Simply Good Foods'
#     when upc.be_low = 'HARRY\'S, INC.' then 'Harry\'s'
#     when upc.be_low in ('DANNON COMPANY INC., THE','WHITEWAVE FOODS') then 'Danone'
#     when upc.be_low = 'BIC CORPORATION' then 'BIC'
#     when upc.be_low = 'GEORGIA-PACIFIC CORPORATION' then 'Georgia Pacific'
#     when upc.be_low = 'TYSON FOODS INC' then 'Tyson'
#     when upc.be_low = 'MARS INCORPORATED' then 'Mars'
#     when upc.be_low = 'MORTON INTERNATIONAL INC' then 'Morton Salt'
#     when upc.be_low = 'KAO USA INC' then 'Kao'
#     when upc.be_low = 'BARILLA AMERICA, INC.' then 'Barilla'
#     when upc.be_low = 'NISSIN FOODS USA CO INC' then 'Nissin'
#     else '!wut'
#     end
#     AS client_rollup
# FROM 
#   nielsenpos_store_sales_im.fact_store_sales as fact
#   inner join nielsenpos_store_sales_im.vw_dimension_store_tdlinx as td ON fact.store_tdlinx_id=td.store_tdlinx_id
#   inner join acv on td.holding_description = acv.holding_description
#   inner join nielsenpos_store_sales_im.vw_upc_nielsen  as upc on fact.upc = upc.upc
  
# WHERE
#   date(period) >= date('2022-02-08')
  
# GROUP BY client_rollup, fact.store_tdlinx_id, fact.upc, td.holding_description, acv.volume;

# COMMAND ----------

# # Load, save "velocities" table/view to blob storage (delta format) - Cancelled due to long run time
# df_ps = spark.sql("SELECT * FROM velocities where client_rollup != 'wut'")

# df_ps.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)
# print(f'Successfully saved df_v to {path}')

# # Preview df
# print(f"velocities dataframe loaded w/ {len(df):,} rows")
# display(df_ps)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data
# MAGIC - Generate views on 80% dollar_sales capture, looking at (total client portfolio/category/individual upc) and dollar sales sum greater than $0

# COMMAND ----------

# Load "velocities" table from blob storage (if starting here)
path = '/mnt/processed/ari/exact/velocities'
df_ps = spark.read.format('delta').option("inferSchema", "true").option("header", "true").load(path)
df_ps = df_ps.filter(df_ps.client_rollup != "!wut")

# Convert to Pandas
df_ps = df_ps.toPandas()

# Adding in dollar sales
df_ps['dollar_sales'] = df_ps['avg_store_sales'] * df_ps['price'] * df_ps['num_weeks']
print(f"df: {len(df_ps):,} rows")

df_ps = df_ps.merge(df_upcs.drop(columns='client_rollup'), on='upc')
df_ps = df_ps[df_ps['dollar_sales']>0] # first quartile threshold after removing zeroes
df_ps.head()

# COMMAND ----------

df_stats = df_ps.groupby('client_rollup').agg(
    row_count = ('dollar_sales', 'count'),
    holding_count = ('holding_description', pd.Series.nunique),
    category_count = ('category', pd.Series.nunique),
    subcategory_count = ('subcategory', pd.Series.nunique),
    brand_count = ('brand', pd.Series.nunique)
)
df_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate aggregations

# COMMAND ----------

df_ps['category_holding'] = df_ps.apply(lambda x: x['category'] + '-' + x['holding_description'], axis=1)
df_ps['subcategory_holding'] = df_ps.apply(lambda x: x['subcategory'] + '-' + x['holding_description'], axis=1)
df_ps['brand_holding'] = df_ps.apply(lambda x: x['brand'] + '-' + x['holding_description'], axis=1)

# COMMAND ----------

# By total portfolio 

dollar_sales_threshold = 0.8
col = 'holding_description'
name = 'holding'

df_results_total_portfolio = pd.DataFrame()

dict_agg_total_portfolio = {}

for client in df_ps['client_rollup'].unique():
    df_subset = df_ps[(df_ps['client_rollup']==client) & (df_ps['dollar_sales']>0)]
    df_agg = df_subset.groupby(['holding_description']).agg(
        dollar_sales_mean=('dollar_sales', 'mean'),
        dollar_sales_std=('dollar_sales', 'std'),
        dollar_sales_sum=('dollar_sales', 'sum'),
        unique_stores_count=('store_tdlinx_id', pd.Series.nunique),
        unique_upcs_count=('upc', pd.Series.nunique)
    ).fillna(0).reset_index()
    
    df_agg['dollar_sales_std_to_mean_ratio'] = df_agg['dollar_sales_std']/df_agg['dollar_sales_mean']

    for col_i in ['dollar_sales_sum', 'unique_stores_count', 'unique_upcs_count']:
        new_col = f"""{col_i}_share"""
        df_agg[new_col] = df_agg[col_i]/df_agg[col_i].sum()

    df_agg = df_agg.sort_values(by='dollar_sales_sum_share', ascending=False)
    df_agg['dollar_sales_share_cumsum'] = df_agg['dollar_sales_sum_share'].cumsum()
    
    selected_list = df_agg.loc[df_agg['dollar_sales_share_cumsum']<=dollar_sales_threshold, 'holding_description'].unique()

#   fails to capture Harry's bc one retailer (Target) accounts for ~88% of their sales so had to include this logic
    if len(selected_list) == 0:
        selected_list = [df_agg[col][0]]
        
    dict_agg_total_portfolio[client] = df_agg.loc[df_agg[col].isin(selected_list)]

# COMMAND ----------

# By category
dollar_sales_threshold = 0.8
col = 'category_holding'
name = 'category_holding'

df_results_category_level = pd.DataFrame()
dict_agg_category_level = {}

for client in df_ps['client_rollup'].unique():
    df_subset = df_ps[(df_ps['client_rollup']==client) & (df_ps['dollar_sales']>0)]
    df_agg = df_subset.groupby(['category', 'holding_description', col]).agg(
        dollar_sales_mean=('dollar_sales', 'mean'),
        dollar_sales_std=('dollar_sales', 'std'),
        dollar_sales_sum=('dollar_sales', 'sum'),
        unique_stores_count=('store_tdlinx_id', pd.Series.nunique),
        unique_upcs_count=('upc', pd.Series.nunique)
    ).fillna(0).reset_index()
    
    df_agg['dollar_sales_std_to_mean_ratio'] = df_agg['dollar_sales_std']/df_agg['dollar_sales_mean']

    for col_i in ['dollar_sales_sum', 'unique_stores_count', 'unique_upcs_count']:
        new_col = f"""{col_i}_share"""
        df_agg[new_col] = df_agg[col_i]/df_agg[col_i].sum()

    df_agg = df_agg.sort_values(by='dollar_sales_sum_share', ascending=False)
    df_agg['dollar_sales_share_cumsum'] = df_agg['dollar_sales_sum_share'].cumsum()
    
    selected_list = df_agg.loc[df_agg['dollar_sales_share_cumsum']<=dollar_sales_threshold, col].unique()
    if len(selected_list) == 0:
        selected_list = [df_agg[col][0]]
        
    dict_agg_category_level[client] = df_agg.loc[df_agg[col].isin(selected_list)]

# COMMAND ----------

# By subcategory
dollar_sales_threshold = 0.8
col = 'subcategory_holding'
name = 'subcategory_holding'

df_results_subcategory_level = pd.DataFrame()
dict_agg_subcategory_level = {}

for client in df_ps['client_rollup'].unique():
    df_subset = df_ps[(df_ps['client_rollup']==client) & (df_ps['dollar_sales']>0)]
    df_agg = df_subset.groupby(['subcategory', 'holding_description', col]).agg(
        dollar_sales_mean=('dollar_sales', 'mean'),
        dollar_sales_std=('dollar_sales', 'std'),
        dollar_sales_sum=('dollar_sales', 'sum'),
        unique_stores_count=('store_tdlinx_id', pd.Series.nunique),
        unique_upcs_count=('upc', pd.Series.nunique)
    ).fillna(0).reset_index()
    
    df_agg['dollar_sales_std_to_mean_ratio'] = df_agg['dollar_sales_std']/df_agg['dollar_sales_mean']

    for col_i in ['dollar_sales_sum', 'unique_stores_count', 'unique_upcs_count']:
        new_col = f"""{col_i}_share"""
        df_agg[new_col] = df_agg[col_i]/df_agg[col_i].sum()

    df_agg = df_agg.sort_values(by='dollar_sales_sum_share', ascending=False)
    df_agg['dollar_sales_share_cumsum'] = df_agg['dollar_sales_sum_share'].cumsum()
    
    selected_list = df_agg.loc[df_agg['dollar_sales_share_cumsum']<=dollar_sales_threshold, col].unique()
    if len(selected_list) == 0:
        selected_list = [df_agg[col][0]]
        
    dict_agg_subcategory_level[client] = df_agg.loc[df_agg[col].isin(selected_list)]

# COMMAND ----------

# By brand
dollar_sales_threshold = 0.8
col = 'brand_holding'
name = 'brand_holding'

df_results_brand_level = pd.DataFrame()
dict_agg_brand_level = {}

for client in df_ps['client_rollup'].unique():
    df_subset = df_ps[(df_ps['client_rollup']==client) & (df_ps['dollar_sales']>0)]
    df_agg = df_subset.groupby(['brand', 'holding_description', col]).agg(
        dollar_sales_mean=('dollar_sales', 'mean'),
        dollar_sales_std=('dollar_sales', 'std'),
        dollar_sales_sum=('dollar_sales', 'sum'),
        unique_stores_count=('store_tdlinx_id', pd.Series.nunique),
        unique_upcs_count=('upc', pd.Series.nunique)
    ).fillna(0).reset_index()
    
    df_agg['dollar_sales_std_to_mean_ratio'] = df_agg['dollar_sales_std']/df_agg['dollar_sales_mean']

    for col_i in ['dollar_sales_sum', 'unique_stores_count', 'unique_upcs_count']:
        new_col = f"""{col_i}_share"""
        df_agg[new_col] = df_agg[col_i]/df_agg[col_i].sum()

    df_agg = df_agg.sort_values(by='dollar_sales_sum_share', ascending=False)
    df_agg['dollar_sales_share_cumsum'] = df_agg['dollar_sales_sum_share'].cumsum()
    
    selected_list = df_agg.loc[df_agg['dollar_sales_share_cumsum']<=dollar_sales_threshold, col].unique()
    if len(selected_list) == 0:
        selected_list = [df_agg[col][0]]
        
    dict_agg_brand_level[client] = df_agg.loc[df_agg[col].isin(selected_list)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the aggregation label

# COMMAND ----------

# Instantiation
df_results = pd.DataFrame(
    columns=[
        'total portfolio CoV', 'total portfolio-holding counts',
        'category CoV', 'category-holding counts',
        'subcategory CoV', 'subcategory-holding counts',
        'brand CoV', 'brand-holding counts'
    ]
)
for client in df_ps['client_rollup'].unique():
    df_results.loc[client] = [
        dict_agg_total_portfolio[client]['dollar_sales_std_to_mean_ratio'].mean(),
        dict_agg_total_portfolio[client]['holding_description'].nunique(),
        dict_agg_category_level[client]['dollar_sales_std_to_mean_ratio'].mean(),
        dict_agg_category_level[client].shape[0],
        dict_agg_subcategory_level[client]['dollar_sales_std_to_mean_ratio'].mean(),
        dict_agg_subcategory_level[client].shape[0],
        dict_agg_brand_level[client]['dollar_sales_std_to_mean_ratio'].mean(),
        dict_agg_brand_level[client].shape[0]
    ]
    
df_results_processed = df_results.copy()
aggregation_penalty = 1
    
for col in ['category', 'subcategory', 'brand']:
    new_col = f'{col} reduction from total (%)'
    df_results_processed[new_col] = \
    (df_results_processed['total portfolio CoV'] - df_results_processed[f'{col} CoV'])/((df_results_processed['total portfolio-holding counts'] - df_results_processed[f'{col}-holding counts']*aggregation_penalty))
    df_results_processed[new_col] = df_results_processed[new_col] / df_results_processed['total portfolio CoV'] * 100

df_results_processed = df_results_processed.filter(regex='(total portfolio CoV| reduction)')

df_output = df_results_processed.copy()

# Beginning the logic of label assignment

# Standard deviation evaluation
df_output['evaluation std'] = df_output.iloc[:, 1:].apply(lambda x: np.std(x.dropna()), axis=1).isna()

# Default to total if range of values are positive
df_output['evaluation positives'] = df_output.iloc[:, 1:].apply(lambda x: x>0 ).sum(axis=1)

# Default to total if too many infs
df_output['evaluation infs'] = \
df_output.iloc[:, 1:].apply(lambda x: x==np.inf ).sum(axis=1) + df_output.iloc[:, 1:].apply(lambda x: x==-np.inf ).sum(axis=1)

# Sum evaluation flags
df_output['evaluation sum'] = df_output.filter(regex='evaluation').sum(axis=1)

# Need to select lowest value

df_output.loc[df_output['evaluation sum']<=1, 'label'] = df_output.loc[df_output['evaluation sum']<=1].apply(lambda x: np.min(x), axis=1)

# Keep only simplest flag

for col in df_output.filter(regex='reduction').columns:
    df_output[col] = df_output[col]==df_output['label']
    
labels_list = ((pd.DataFrame(df_output.iloc[:, 1:4].apply(lambda row: pd.Series([False if x and any(row[:i]) else x for i, x in enumerate(row)]), axis=1)))*(df_output.columns[1:4])).apply(lambda x: ''.join(x), axis=1)
labels_list = [i.replace(' reduction from total (%)', '') for i in labels_list]
df_output['label'] = labels_list
df_output.loc[df_output['label']=='', 'label'] = 'total porfolio'
    
df_output[['label']]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnostics - visualize the coefficient of variation descent

# COMMAND ----------

# Create aggregation centric views
for client in df_ps['client_rollup'].unique():
    plt.figure(figsize=(20, 10))
    plt.scatter(
        np.random.normal(1, 0.2, size=len(dict_agg_total_portfolio[client]['dollar_sales_std_to_mean_ratio'].values)),
        dict_agg_total_portfolio[client]['dollar_sales_std_to_mean_ratio'].values,
        color='orange',
        alpha=0.15,
        label='total_portfolio'
    )
    plt.scatter(
        1,
        dict_agg_total_portfolio[client]['dollar_sales_std_to_mean_ratio'].mean(),
        color='orange',
        s=200,
        marker='*'
    )
    plt.scatter(
        np.random.normal(dict_agg_category_level[client]['category'].nunique(), 0.2, len(dict_agg_category_level[client]['dollar_sales_std_to_mean_ratio'].values)),
        dict_agg_category_level[client]['dollar_sales_std_to_mean_ratio'].values,
        color='blue',
        alpha=0.15,
        label='category'
    )
    plt.scatter(
        dict_agg_category_level[client]['category'].nunique(),
        dict_agg_category_level[client]['dollar_sales_std_to_mean_ratio'].mean(),
        color='blue',
        s=200,
        marker='*'
    )
    plt.scatter(
        np.random.normal(dict_agg_subcategory_level[client]['subcategory'].nunique(), 0.2, len(dict_agg_subcategory_level[client]['dollar_sales_std_to_mean_ratio'].values)),
        dict_agg_subcategory_level[client]['dollar_sales_std_to_mean_ratio'].values,
        color='purple',
        alpha=0.15,
        label='subcategory'
    )
    plt.scatter(
        dict_agg_subcategory_level[client]['subcategory'].nunique(),
        dict_agg_subcategory_level[client]['dollar_sales_std_to_mean_ratio'].mean(),
        color='purple',
        s=200,
        marker='*'
    )
    plt.scatter(
        np.random.normal(dict_agg_brand_level[client]['brand'].nunique(), 0.2, len(dict_agg_brand_level[client]['dollar_sales_std_to_mean_ratio'].values)),
        dict_agg_brand_level[client]['dollar_sales_std_to_mean_ratio'].values,
        color='red',
        alpha=0.15,
        label='brand'
    )
    plt.scatter(
        dict_agg_brand_level[client]['brand'].nunique(),
        dict_agg_brand_level[client]['dollar_sales_std_to_mean_ratio'].mean(),
        color='red',
        s=200,
        marker='*'
    )
    plt.scatter(
        np.random.normal(df_ps.loc[df_ps['client_rollup']==client, 'brand_category'].nunique(), 0.2, len(dict_agg_brand_category_level[client]['dollar_sales_std_to_mean_ratio'].values)),
        dict_agg_brand_category_level[client]['dollar_sales_std_to_mean_ratio'].values,
        color='green',
        alpha=0.15,
        label='brand_category'
    )
    plt.scatter(
        df_ps.loc[df_ps['client_rollup']==client, 'brand_category'].nunique(),
        dict_agg_brand_category_level[client]['dollar_sales_std_to_mean_ratio'].mean(),
        color='green',
        s=200,
        marker='*'
    )
    plt.legend(bbox_to_anchor=(1,1))
    plt.xlabel('number of items in aggregation')
    plt.ylabel('dollar_sales_std_to_mean_ratio')
    plt.ylim()
    #     holding_sales = df_ps.loc[(df_ps['holding_description']==holding) & (df_ps['client_rollup']==client), 'dollar_sales'].sum()
    #     total_sales = df_ps.loc[df_ps['client_rollup']==client, 'dollar_sales'].sum()
    plt.title(
        f"{client} scatterplot of dollar_sales_std_to_mean_ratios across holdings")
    plt.show()

# COMMAND ----------


