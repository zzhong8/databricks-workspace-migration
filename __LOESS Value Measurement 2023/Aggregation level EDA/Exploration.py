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
# MAGIC ## Ideas
# MAGIC - hierarchical clustering
# MAGIC - start with Nissin
# MAGIC - distribution of upc pricing, sales volume, rate of sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA
# MAGIC - What % share of dollar_sales is represented?
# MAGIC - How does this look against other clients?
# MAGIC - What do the dollar_sales histograms look like for the chosen aggregation level?
# MAGIC - Is it a good idea to drop the first quartile of dollar sales?

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

# COMMAND ----------

df_ps['client_rollup']

# COMMAND ----------

print(f"""There are {df_subset['holding_description'].nunique()} unique holdings that carry client={client} products""")
print(f"""There are {df_subset['upc'].nunique()} unique items that are sold by client={client}""")
print(f"""There are {df_subset['category'].nunique()} unique categories that are sold by client={client}""")
print(f"""There are {df_subset['subcategory'].nunique()} unique subcategory that are sold by client={client}""")
print(f"""There are {df_subset['department'].nunique()} unique departments that the client={client} is sold under""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nissin and dollar_sales

# COMMAND ----------

df_subset['dollar_sales'].describe()

# COMMAND ----------

df_subset[df_subset['dollar_sales']>0]['dollar_sales'].describe()

# COMMAND ----------

df_subset = df_subset[df_subset['dollar_sales']>0]
print(f"""There are {df_subset['holding_description'].nunique()} unique holdings that carry client={client} products""")
print(f"""There are {df_subset['upc'].nunique()} unique items that are sold by client={client}""")
print(f"""There are {df_subset['category'].nunique()} unique categories that are sold by client={client}""")
print(f"""There are {df_subset['subcategory'].nunique()} unique subcategory that are sold by client={client}""")
print(f"""There are {df_subset['department'].nunique()} unique departments that the client={client} is sold under""")

# COMMAND ----------

# Nissin products - all upcs together

df_results = df_subset.groupby(['holding_description']).agg(mean=('dollar_sales', 'mean'), std=('dollar_sales', 'std'), sum=('dollar_sales', 'sum')).fillna(0).reset_index()
print(f"""There are {df_results.shape[0]} unique all-upcs-together-holding counts""")
print(f"""There are {df_results[df_results['std']<df_results['mean']*.25].shape[0]} all-upcs-together-holding counts where the std is less than 25% of the mean""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25].shape[0]/df_results.shape[0]*100: .2f}% of the all-upcs-together-holding counts""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25]['holding_description'].nunique()/df_results['holding_description'].nunique()*100: .2f}% of the holdings that sell client={client}""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25]['sum'].sum()/df_results['sum'].sum()*100: .5f}% of client={client}'s dollar sales'""")

# COMMAND ----------

# Nissin products - broken up by categories

df_results = df_subset.groupby(['category', 'holding_description']).agg(mean=('dollar_sales', 'mean'), std=('dollar_sales', 'std'), sum=('dollar_sales', 'sum')).fillna(0).reset_index()
print(f"""There are {df_results.shape[0]} unique category-holding counts""")
print(f"""There are {df_results[df_results['std']<df_results['mean']*.25].shape[0]} category-holding counts where the std is less than 25% of the mean""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25].shape[0]/df_results.shape[0]*100: .2f}% of the category-holding counts""")
df_results = df_results.merge(df_subset[['category', 'holding_description', 'upc']], on=['category', 'holding_description'])
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25]['upc'].nunique()/df_results['upc'].nunique()*100: .2f}% of the upcs sold by the client={client}""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25]['holding_description'].nunique()/df_results['holding_description'].nunique()*100: .2f}% of the holdings that sell client={client}""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25]['sum'].sum()/df_results['sum'].sum()*100: .5f}% of client={client}'s dollar sales'""")

# COMMAND ----------

# Nissin products - all upcs

df_results = df_subset.groupby(['upc', 'holding_description']).agg(mean=('dollar_sales', 'mean'), std=('dollar_sales', 'std')).fillna(0).reset_index()
print(f"""There are {df_results.shape[0]} unique individual-upc-holding counts""")
print(f"""There are {df_results[df_results['std']<df_results['mean']*.25].shape[0]} individual-upc-holding counts where the std is less than 25% of the mean""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25].shape[0]/df_results.shape[0]*100: .2f}% of the individual-upc-holding counts""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25]['upc'].nunique()/df_results['upc'].nunique()*100: .2f}% of the upcs sold by the client={client}""")
print(f"""This represents {df_results[df_results['std']<df_results['mean']*.25]['holding_description'].nunique()/df_results['holding_description'].nunique()*100: .2f}% of the holdings that sell client={client}""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate views on 80% dollar_sales capture, looking at total Nissin portfolio and dollar sales sum greater than $0

# COMMAND ----------

df_subset = df_subset[df_subset['dollar_sales']>0]

# COMMAND ----------

# Nissin products - all upcs together

df_agg = df_subset.groupby(['holding_description']).agg(
    dollar_sales_mean=('dollar_sales', 'mean'),
    dollar_sales_std=('dollar_sales', 'std'),
    dollar_sales_sum=('dollar_sales', 'sum'),
    unique_stores_count=('store_tdlinx_id', pd.Series.nunique),
    unique_upcs_count=('upc', pd.Series.nunique)
).fillna(0).reset_index()

# Adding p-values from ks test of normality - no combination is normally distributed here
df_agg['dollar_sales_sum_p_value'] = df_subset.groupby(['holding_description']).apply(lambda x: stats.ks_1samp(x['dollar_sales'], stats.norm.cdf)[1]).values
df_agg['dollar_sales_sum_normally_distributed'] = np.where(df_agg['dollar_sales_sum_p_value']>p_value, 1, 0)

for col in ['dollar_sales_sum', 'unique_stores_count', 'unique_upcs_count']:
    new_col = f"""{col}_share"""
    df_agg[new_col] = df_agg[col]/df_agg[col].sum()

df_agg = df_agg.sort_values(by='dollar_sales_sum_share', ascending=False)
df_agg['dollar_sales_share_cumsum'] = df_agg['dollar_sales_sum_share'].cumsum()

dollar_sales_threshold = 0.8
col = 'holding_description'
name = 'holding'
print(f"""To capture {dollar_sales_threshold*100:.0f}% of dollar sales for client={client}, this results in the following:""")
selected_list = df_agg.loc[df_agg['dollar_sales_share_cumsum']<=dollar_sales_threshold, 'holding_description'].unique()
print(f"""{df_agg.loc[df_agg[col].isin(selected_list), col].nunique():,} {name}(s) selected out of {df_agg[col].nunique():,} total {name}s""")
print(f"""{df_agg.loc[df_agg[col].isin(selected_list), 'unique_stores_count'].sum():,} store(s) selected out of {df_agg['unique_stores_count'].sum():,} total stores""")
print(f"""{df_subset.loc[df_subset[col].isin(selected_list), 'upc'].nunique()} upc(s) selected out of {df_subset['upc'].nunique()} total upcs""")
    

# COMMAND ----------

for selected_list_item_i in selected_list:
    plt.figure(figsize=(6, 4))
    sns.histplot(data=df_subset[df_subset[col]==selected_list_item_i], x='dollar_sales', hue=col)
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate views on 80% dollar_sales capture, looking at total Nissin portfolio and dollar sales sum greater than $115 (first quartile after removing 0s)

# COMMAND ----------

df_subset = df_subset[df_subset['dollar_sales']>=115]

# COMMAND ----------

# Nissin products - all upcs together

df_agg = df_subset.groupby(['holding_description']).agg(
    dollar_sales_mean=('dollar_sales', 'mean'),
    dollar_sales_std=('dollar_sales', 'std'),
    dollar_sales_sum=('dollar_sales', 'sum'),
    unique_stores_count=('store_tdlinx_id', pd.Series.nunique),
    unique_upcs_count=('upc', pd.Series.nunique)
).fillna(0).reset_index()

# Adding p-values from ks test of normality - no combination is normally distributed here
df_agg['dollar_sales_sum_p_value'] = df_subset.groupby(['holding_description']).apply(lambda x: stats.ks_1samp(x['dollar_sales'], stats.norm.cdf)[1]).values
df_agg['dollar_sales_sum_normally_distributed'] = np.where(df_agg['dollar_sales_sum_p_value']>p_value, 1, 0)

for col in ['dollar_sales_sum', 'unique_stores_count', 'unique_upcs_count']:
    new_col = f"""{col}_share"""
    df_agg[new_col] = df_agg[col]/df_agg[col].sum()

df_agg = df_agg.sort_values(by='dollar_sales_sum_share', ascending=False)
df_agg['dollar_sales_share_cumsum'] = df_agg['dollar_sales_sum_share'].cumsum()
    
df_agg.head()

# COMMAND ----------

dollar_sales_threshold = 0.8
col = 'holding_description'
name = 'holding'
print(f"""To capture {dollar_sales_threshold*100:.0f}% of dollar sales for client={client}, this results in the following:""")
selected_list = df_agg.loc[df_agg['dollar_sales_share_cumsum']<=dollar_sales_threshold, 'holding_description'].unique()
print(f"""{df_agg.loc[df_agg[col].isin(selected_list), col].nunique():,} {name}(s) selected out of {df_agg[col].nunique():,} total {name}s""")
print(f"""{df_agg.loc[df_agg[col].isin(selected_list), 'unique_stores_count'].sum():,} store(s) selected out of {df_agg['unique_stores_count'].sum():,} total stores""")
print(f"""{df_subset.loc[df_subset[col].isin(selected_list), 'upc'].nunique()} upc(s) selected out of {df_subset['upc'].nunique()} total upcs""")

# COMMAND ----------

for selected_list_item_i in selected_list:
    plt.figure(figsize=(6, 4))
    sns.histplot(data=df_subset[df_subset[col]==selected_list_item_i], x='dollar_sales', hue=col)
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA - evaluate if std/mean ratios are valid to breakout granularity
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
# MAGIC ### total client portfolio

# COMMAND ----------

# All clients - fails to capture Harry's bc one retailer (Target) accounts for ~88% of their sales

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
    if len(selected_list) == 0:
        selected_list = [df_agg[col][0]]
        
    dict_agg_total_portfolio[client] = df_agg.loc[df_agg[col].isin(selected_list)]

#     df_results_total_portfolio.loc[client, f"""{name}_count"""] = df_agg.loc[df_agg[col].isin(selected_list), col].nunique()
#     df_results_total_portfolio.loc[client, f"""{name}_share"""] = np.round(df_results_total_portfolio.loc[client, f"""{name}_count"""]/df_agg[col].nunique(), 2)
#     df_results_total_portfolio.loc[client, 'store_count'] = df_agg.loc[df_agg[col].isin(selected_list), 'unique_stores_count'].sum()
#     df_results_total_portfolio.loc[client, 'store_share'] = np.round(df_results_total_portfolio.loc[client, 'store_count']/df_agg['unique_stores_count'].sum(), 2)
#     df_results_total_portfolio.loc[client, 'upc_count'] = df_subset.loc[df_subset[col].isin(selected_list), 'upc'].nunique()
#     df_results_total_portfolio.loc[client, 'upc_share'] = np.round(df_results_total_portfolio.loc[client, 'upc_count']/df_subset['upc'].nunique(), 2)
#     df_results_total_portfolio.loc[client, 'dollar_sales_std_to_mean_ratio_mean'] = df_agg['dollar_sales_std_to_mean_ratio'].mean()
#     df_results_total_portfolio.loc[client, 'dollar_sales_std_to_mean_ratio_std'] = df_agg['dollar_sales_std_to_mean_ratio'].std()
    
# df_results_total_portfolio

# COMMAND ----------

# MAGIC %md
# MAGIC ### category

# COMMAND ----------

df_ps['category_holding'] = df_ps.apply(lambda x: x['category'] + '-' + x['holding_description'], axis=1)

# COMMAND ----------

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

#     df_results_category_level.loc[client, f"""{name}_count"""] = df_agg.loc[df_agg[col].isin(selected_list), col].nunique()
#     df_results_category_level.loc[client, f"""{name}_share"""] = np.round(df_results_category_level.loc[client, f"""{name}_count"""]/df_agg[col].nunique(), 2)
#     df_results_category_level.loc[client, 'store_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'store_tdlinx_id'].nunique()
#     df_results_category_level.loc[client, 'store_share'] = np.round(df_results_category_level.loc[client, 'store_count']/df_ps['store_tdlinx_id'].nunique(), 2)
#     df_results_category_level.loc[client, 'upc_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'upc'].nunique()
#     df_results_category_level.loc[client, 'upc_share'] = np.round(df_results_category_level.loc[client, 'upc_count']/df_ps['upc'].nunique(), 2)
#     df_results_category_level.loc[client, 'dollar_sales_std_to_mean_ratio_mean'] = df_agg['dollar_sales_std_to_mean_ratio'].mean()
#     df_results_category_level.loc[client, 'dollar_sales_std_to_mean_ratio_std'] = df_agg['dollar_sales_std_to_mean_ratio'].std()
    
# df_results_category_level

# COMMAND ----------

# MAGIC %md
# MAGIC ### subcategory

# COMMAND ----------

df_ps['subcategory_holding'] = df_ps.apply(lambda x: x['subcategory'] + '-' + x['holding_description'], axis=1)

# COMMAND ----------

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

#     df_results_subcategory_level.loc[client, f"""{name}_count"""] = df_agg.loc[df_agg[col].isin(selected_list), col].nunique()
#     df_results_subcategory_level.loc[client, f"""{name}_share"""] = np.round(df_results_subcategory_level.loc[client, f"""{name}_count"""]/df_agg[col].nunique(), 2)
#     df_results_subcategory_level.loc[client, 'store_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'store_tdlinx_id'].nunique()
#     df_results_subcategory_level.loc[client, 'store_share'] = np.round(df_results_subcategory_level.loc[client, 'store_count']/df_ps['store_tdlinx_id'].nunique(), 2)
#     df_results_subcategory_level.loc[client, 'upc_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'upc'].nunique()
#     df_results_subcategory_level.loc[client, 'upc_share'] = np.round(df_results_subcategory_level.loc[client, 'upc_count']/df_ps['upc'].nunique(), 2)
#     df_results_subcategory_level.loc[client, 'dollar_sales_std_to_mean_ratio_mean'] = df_agg['dollar_sales_std_to_mean_ratio'].mean()
#     df_results_subcategory_level.loc[client, 'dollar_sales_std_to_mean_ratio_std'] = df_agg['dollar_sales_std_to_mean_ratio'].std()
    
# df_results_subcategory_level

# COMMAND ----------

# MAGIC %md
# MAGIC ### brand

# COMMAND ----------

df_ps['brand_holding'] = df_ps.apply(lambda x: x['brand'] + '-' + x['holding_description'], axis=1)

# COMMAND ----------

# All clients - fails to capture Harry's bc one retailer (Target) accounts for ~88% of their sales

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

#     df_results_brand_level.loc[client, f"""{name}_count"""] = df_agg.loc[df_agg[col].isin(selected_list), col].nunique()
#     df_results_brand_level.loc[client, f"""{name}_share"""] = np.round(df_results_brand_level.loc[client, f"""{name}_count"""]/df_agg[col].nunique(), 2)
#     df_results_brand_level.loc[client, 'store_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'store_tdlinx_id'].nunique()
#     df_results_brand_level.loc[client, 'store_share'] = np.round(df_results_brand_level.loc[client, 'store_count']/df_ps['store_tdlinx_id'].nunique(), 2)
#     df_results_brand_level.loc[client, 'upc_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'upc'].nunique()
#     df_results_brand_level.loc[client, 'upc_share'] = np.round(df_results_brand_level.loc[client, 'upc_count']/df_ps['upc'].nunique(), 2)
#     df_results_brand_level.loc[client, 'dollar_sales_std_to_mean_ratio_mean'] = df_agg['dollar_sales_std_to_mean_ratio'].mean()
#     df_results_brand_level.loc[client, 'dollar_sales_std_to_mean_ratio_std'] = df_agg['dollar_sales_std_to_mean_ratio'].std()
    
# df_results_brand_level

# COMMAND ----------

# MAGIC %md
# MAGIC ### brand-category

# COMMAND ----------

df_ps['brand_category_holding'] = df_ps.apply(lambda x: x['brand'] + '-' + x['category'] + '-' + x['holding_description'], axis=1)
df_ps['brand_category'] = df_ps.apply(lambda x: x['brand'] + '-' + x['category'], axis=1)

# COMMAND ----------

dollar_sales_threshold = 0.8
col = 'brand_category_holding'
name = 'brand_category_holding'

df_results_brand_category_level = pd.DataFrame()
dict_agg_brand_category_level = {}

for client in df_ps['client_rollup'].unique():
    df_subset = df_ps[(df_ps['client_rollup']==client) & (df_ps['dollar_sales']>0)]
    df_agg = df_subset.groupby(['brand', 'category', 'holding_description', col]).agg(
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
        
    dict_agg_brand_category_level[client] = df_agg.loc[df_agg[col].isin(selected_list)]

#     df_results_brand_category_level.loc[client, f"""{name}_count"""] = df_agg.loc[df_agg[col].isin(selected_list), col].nunique()
#     df_results_brand_category_level.loc[client, f"""{name}_share"""] = np.round(df_results_brand_category_level.loc[client, f"""{name}_count"""]/df_agg[col].nunique(), 2)
#     df_results_brand_category_level.loc[client, 'store_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'store_tdlinx_id'].nunique()
#     df_results_brand_category_level.loc[client, 'store_share'] = np.round(df_results_brand_category_level.loc[client, 'store_count']/df_ps['store_tdlinx_id'].nunique(), 2)
#     df_results_brand_category_level.loc[client, 'upc_count'] = df_ps.loc[df_ps[col].isin(selected_list), 'upc'].nunique()
#     df_results_brand_category_level.loc[client, 'upc_share'] = np.round(df_results_brand_category_level.loc[client, 'upc_count']/df_ps['upc'].nunique(), 2)
#     df_results_brand_category_level.loc[client, 'dollar_sales_std_to_mean_ratio_mean'] = df_agg['dollar_sales_std_to_mean_ratio'].mean()
#     df_results_brand_category_level.loc[client, 'dollar_sales_std_to_mean_ratio_std'] = df_agg['dollar_sales_std_to_mean_ratio'].std()
    
# df_results_brand_category_level

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparing

# COMMAND ----------

# for client in df_ps['client_rollup'].unique():
#     for holding in df_ps['holding_description'].unique():
#         try:

client = 'Sanofi'
for holding in df_ps['holding_description'].unique():
    try:
        plt.figure(figsize=(20, 8))
        plt.hist(dict_agg_category_level[client].loc[dict_agg_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'], color='blue', label=f'category', alpha=0.5)
        plt.axvline(dict_agg_category_level[client].loc[dict_agg_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), color='blue', label=f'category mean')
        plt.hist(dict_agg_subcategory_level[client].loc[dict_agg_subcategory_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'], color='green', label=f'subcategory', alpha=0.5)
        plt.axvline(dict_agg_subcategory_level[client].loc[dict_agg_subcategory_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), color='green', label=f'subcategory mean')
        plt.hist(dict_agg_brand_level[client].loc[dict_agg_brand_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'], color='orange', label=f'brand', alpha=0.5)
        plt.axvline(dict_agg_brand_level[client].loc[dict_agg_brand_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), color='orange', label=f'brand mean')
        plt.hist(dict_agg_brand_category_level[client].loc[dict_agg_brand_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'], color='pink', label=f'brand_category', alpha=0.5)
        plt.axvline(dict_agg_brand_category_level[client].loc[dict_agg_brand_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), color='pink', label=f'brand_category mean')
        plt.axvline(dict_agg_total_portfolio[client].loc[dict_agg_total_portfolio[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].values, color='red', label='total portfolio')
        plt.legend()
        plt.xlabel('dollar_sales_std_to_mean_ratio')
        plt.ylabel('count')
        holding_sales = df_ps.loc[(df_ps['holding_description']==holding) & (df_ps['client_rollup']==client), 'dollar_sales'].sum()
        total_sales = df_ps.loc[df_ps['client_rollup']==client, 'dollar_sales'].sum()
        plt.title(
            f"{client}-{holding} distributions of dollar_sales_std_to_mean_ratio\nThis holding represents ${holding_sales:,.0f} of value, or {holding_sales/total_sales*100: .5f}% of their total sales")
        plt.show()
    except:
        print(f'{holding} could not be processed')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inflection point investigation - mock

# COMMAND ----------

# Create initial views
client = 'Sanofi'
for holding in df_ps['holding_description'].unique()[:3]:
    try:
        plt.figure(figsize=(10, 6))
        plt.scatter(1, dict_agg_total_portfolio[client].loc[dict_agg_total_portfolio[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].values, label='total portfolio')
        plt.scatter(df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'category'].nunique(), dict_agg_category_level[client].loc[dict_agg_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), label='category')
        plt.scatter(df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'subcategory'].nunique(), dict_agg_subcategory_level[client].loc[dict_agg_subcategory_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), label='subcategory')
        plt.scatter(df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'brand'].nunique(), dict_agg_brand_level[client].loc[dict_agg_brand_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), label='brand')
        plt.scatter(df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'brand_category'].nunique(), dict_agg_brand_category_level[client].loc[dict_agg_brand_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(), label='brand_category')
        plt.legend()
        plt.xlabel('number of items in aggregation')
        plt.ylabel('dollar_sales_std_to_mean_ratio')
        plt.ylim()
        holding_sales = df_ps.loc[(df_ps['holding_description']==holding) & (df_ps['client_rollup']==client), 'dollar_sales'].sum()
        total_sales = df_ps.loc[df_ps['client_rollup']==client, 'dollar_sales'].sum()
        plt.title(
            f"{client}-{holding} scatterplot of dollar_sales_std_to_mean_ratio\nThis holding represents ${holding_sales/1000:,.0f}k of value, or {holding_sales/total_sales*100: .0f}% of their total sales")
        plt.show()
    except:
        print(f'{holding} could not be processed')

# COMMAND ----------

# One view per client - test
client = 'Sanofi'
plt.figure(figsize=(10, 6))
for holding in df_ps['holding_description'].unique():
    try:
        plt.scatter(
            [
                1,
                df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'category'].nunique(),
                df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'subcategory'].nunique(),
                df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'brand'].nunique(),
                df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'brand_category'].nunique()
            ],
            [
                dict_agg_total_portfolio[client].loc[dict_agg_total_portfolio[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].values,
                dict_agg_category_level[client].loc[dict_agg_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(),
                dict_agg_subcategory_level[client].loc[dict_agg_subcategory_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(),
                dict_agg_brand_level[client].loc[dict_agg_brand_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(),
                dict_agg_brand_category_level[client].loc[dict_agg_brand_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean()
            ],
            label=holding
        )
    except:
        continue
    
plt.legend()
plt.xlabel('number of items in aggregation')
plt.ylabel('dollar_sales_std_to_mean_ratio')
plt.ylim()
#     holding_sales = df_ps.loc[(df_ps['holding_description']==holding) & (df_ps['client_rollup']==client), 'dollar_sales'].sum()
#     total_sales = df_ps.loc[df_ps['client_rollup']==client, 'dollar_sales'].sum()
plt.title(
    f"{client} scatterplot of dollar_sales_std_to_mean_ratios across holdings")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inflection point investigation - actual

# COMMAND ----------

# One view per client - actual
for client in df_ps['client_rollup'].unique():
    plt.figure(figsize=(10, 6))
    for holding in df_ps['holding_description'].unique():
        try:
            plt.scatter(
                [
                    1,
                    df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'category'].nunique(),
                    df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'subcategory'].nunique(),
                    df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'brand'].nunique(),
                    df_ps.loc[(df_ps['client_rollup']==client) & (df_ps['holding_description']==holding), 'brand_category'].nunique()
                ],
                [
                    dict_agg_total_portfolio[client].loc[dict_agg_total_portfolio[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].values,
                    dict_agg_category_level[client].loc[dict_agg_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(),
                    dict_agg_subcategory_level[client].loc[dict_agg_subcategory_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(),
                    dict_agg_brand_level[client].loc[dict_agg_brand_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean(),
                    dict_agg_brand_category_level[client].loc[dict_agg_brand_category_level[client]['holding_description']==holding, 'dollar_sales_std_to_mean_ratio'].mean()
                ],
                label=holding
            )
        except:
            continue

    plt.legend()
    plt.xlabel('number of items in aggregation')
    plt.ylabel('dollar_sales_std_to_mean_ratio')
    plt.ylim()
    #     holding_sales = df_ps.loc[(df_ps['holding_description']==holding) & (df_ps['client_rollup']==client), 'dollar_sales'].sum()
    #     total_sales = df_ps.loc[df_ps['client_rollup']==client, 'dollar_sales'].sum()
    plt.title(
        f"{client} scatterplot of dollar_sales_std_to_mean_ratios across holdings")
    plt.show()

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

# MAGIC %md
# MAGIC ## Tyson request

# COMMAND ----------

df_agg = df_ps[df_ps['client_rollup']=='Tyson'].groupby('category').agg(
    upc_count=('upc', pd.Series.nunique),
    store_count=('store_tdlinx_id', pd.Series.nunique),
    price_mean=('price', 'mean'),
    dollar_sales_sum=('dollar_sales', 'sum'),
    volume_sum=('volume', 'sum'),
    num_weeks_sum=('num_weeks', 'sum')
).fillna(0).reset_index()

df_agg['velocity'] = df_agg['volume_sum']/df_agg['num_weeks_sum']

df_agg = df_agg.sort_values(by='velocity', ascending=False)
df_agg.loc[df_agg.shape[0]+1] = [
    'Total',
    np.nan,
    np.nan,
    np.nan,
    df_agg['dollar_sales_sum'].sum(),
    df_agg['volume_sum'].sum(),
    df_agg['num_weeks_sum'].sum(),
    df_agg['volume_sum'].sum()/df_agg['num_weeks_sum'].sum()
]
df_agg['log_dollar_sales_sum'] = np.log(df_agg['dollar_sales_sum'])


df_agg

# COMMAND ----------

df_agg = df_ps[(df_ps['client_rollup']=='Tyson') & (df_ps['holding_description']=='Albertsons')].groupby('category').agg(
    upc_count=('upc', pd.Series.nunique),
    store_count=('store_tdlinx_id', pd.Series.nunique),
    price_mean=('price', 'mean'),
    dollar_sales_sum=('dollar_sales', 'sum'),
    volume_sum=('volume', 'sum'),
    num_weeks_sum=('num_weeks', 'sum')
).fillna(0).reset_index()

df_agg['velocity'] = df_agg['volume_sum']/df_agg['num_weeks_sum']

df_agg = df_agg.sort_values(by='velocity', ascending=False)
df_agg.loc[df_agg.shape[0]+1] = [
    'Total',
    np.nan,
    np.nan,
    np.nan,
    df_agg['dollar_sales_sum'].sum(),
    df_agg['volume_sum'].sum(),
    df_agg['num_weeks_sum'].sum(),
    df_agg['volume_sum'].sum()/df_agg['num_weeks_sum'].sum()
]
df_agg['log_dollar_sales_sum'] = np.log(df_agg['dollar_sales_sum'])


df_agg

# COMMAND ----------

df_agg = df_ps[(df_ps['client_rollup']=='Tyson') & (df_ps['holding_description']=='Target')].groupby('category').agg(
    upc_count=('upc', pd.Series.nunique),
    store_count=('store_tdlinx_id', pd.Series.nunique),
    price_mean=('price', 'mean'),
    dollar_sales_sum=('dollar_sales', 'sum'),
    volume_sum=('volume', 'sum'),
    num_weeks_sum=('num_weeks', 'sum')
).fillna(0).reset_index()

df_agg['velocity'] = df_agg['volume_sum']/df_agg['num_weeks_sum']

df_agg = df_agg.sort_values(by='velocity', ascending=False)
df_agg.loc[df_agg.shape[0]+1] = [
    'Total',
    np.nan,
    np.nan,
    np.nan,
    df_agg['dollar_sales_sum'].sum(),
    df_agg['volume_sum'].sum(),
    df_agg['num_weeks_sum'].sum(),
    df_agg['volume_sum'].sum()/df_agg['num_weeks_sum'].sum()
]
df_agg['log_dollar_sales_sum'] = np.log(df_agg['dollar_sales_sum'])


df_agg

# COMMAND ----------

plt.figure(figsize=(20, 10))
sns.scatterplot(data=df_agg.iloc[:-1], x='velocity', y='log_dollar_sales_sum', hue='category')
plt.legend(bbox_to_anchor=(1,1))
plt.show()

# COMMAND ----------

plt.figure(figsize=(20, 10))
sns.barplot(data=df_agg.iloc[:-1], x='velocity', y='log_dollar_sales_sum', hue='category')
plt.xticks(rotation=45)
plt.legend(bbox_to_anchor=(1,1))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Presentation items

# COMMAND ----------

# All clients - fails to capture Harry's bc one retailer (Target) accounts for ~88% of their sales

dollar_sales_threshold = 0.8
col = 'holding_description'
name = 'holding'
client = "Campbell's"
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
if len(selected_list) == 0:
    selected_list = [df_agg[col][0]]

# COMMAND ----------

# Total view
df_agg = df_ps[(df_ps['client_rollup']=='Campbell\'s') & (df_ps['holding_description'].isin(selected_list))].groupby('upc').agg(
    store_count=('store_tdlinx_id', pd.Series.nunique),
    price_mean=('price', 'mean'),
    volume_sum=('volume', 'sum'),
    num_weeks_sum=('num_weeks', 'sum')
).fillna(0).reset_index()

df_agg['velocity (units per week)'] = df_agg['volume_sum']/df_agg['num_weeks_sum']
y_max = df_agg['velocity (units per week)'].max()*1.2
x_max = df_agg['price_mean'].max()*1.2

df_agg = df_agg.sort_values(by='velocity (units per week)', ascending=False)

plt.figure(figsize=(20,10))
sns.scatterplot(data=df_agg, x='price_mean', y='velocity (units per week)', alpha=0.4)
plt.scatter(x=df_agg['price_mean'].mean(), y=df_agg['velocity (units per week)'].mean(), label='Centroid', marker='*', s=100, color='blue')
plt.xlim(0, x_max)
plt.ylim(0, y_max)
plt.legend(bbox_to_anchor=(1,1))
plt.show()

# COMMAND ----------

# Total view - by retailer (single plot)
df_agg = df_ps[(df_ps['client_rollup']=='Campbell\'s') & (df_ps['holding_description'].isin(selected_list))].groupby(['holding_description', 'upc']).agg(
    store_count=('store_tdlinx_id', pd.Series.nunique),
    price_mean=('price', 'mean'),
    volume_sum=('volume', 'sum'),
    num_weeks_sum=('num_weeks', 'sum')
).fillna(0).reset_index()

df_agg['velocity (units per week)'] = df_agg['volume_sum']/df_agg['num_weeks_sum']

plt.figure(figsize=(20,10))
sns.scatterplot(data=df_agg, x='price_mean', y='velocity (units per week)', alpha=0.4, hue='holding_description')
plt.scatter(x=df_agg['price_mean'].mean(), y=df_agg['velocity (units per week)'].mean(), label='Centroid', marker='*', s=100, color='blue')
plt.xlim(0, x_max)
plt.ylim(0, y_max)
plt.legend(bbox_to_anchor=(1,1))
plt.show()

# COMMAND ----------

# Total view - by category (single plot)
df_agg = df_ps[(df_ps['client_rollup']=='Campbell\'s') & (df_ps['holding_description'].isin(selected_list))].groupby(['category', 'upc']).agg(
    store_count=('store_tdlinx_id', pd.Series.nunique),
    price_mean=('price', 'mean'),
    volume_sum=('volume', 'sum'),
    num_weeks_sum=('num_weeks', 'sum')
).fillna(0).reset_index()

df_agg['velocity (units per week)'] = df_agg['volume_sum']/df_agg['num_weeks_sum']

df_agg = df_agg.groupby('category').mean().fillna(0).reset_index()

plt.figure(figsize=(20,10))
sns.scatterplot(data=df_agg, x='price_mean', y='velocity (units per week)', hue='category')
plt.scatter(x=df_agg['price_mean'].mean(), y=df_agg['velocity (units per week)'].mean(), label='Centroid', marker='*', s=100, color='blue')
plt.xlim(0, x_max)
plt.ylim(0, y_max)
plt.legend(bbox_to_anchor=(1,1))
plt.show()

# COMMAND ----------

# Total view - by category (multiple plot)
df_agg = df_ps[(df_ps['client_rollup']=='Campbell\'s') & (df_ps['holding_description'].isin(selected_list))].groupby(['holding_description', 'category', 'upc']).agg(
    store_count=('store_tdlinx_id', pd.Series.nunique),
    price_mean=('price', 'mean'),
    volume_sum=('volume', 'sum'),
    num_weeks_sum=('num_weeks', 'sum')
).fillna(0).reset_index()

df_agg['velocity (units per week)'] = df_agg['volume_sum']/df_agg['num_weeks_sum']

df_agg = df_agg.groupby(['holding_description', 'category']).mean().fillna(0).reset_index()

for retailer in df_agg['holding_description'].unique():
    plt.figure(figsize=(20,10))
    sns.scatterplot(data=df_agg[df_agg['holding_description']==retailer], x='price_mean', y='velocity (units per week)', hue='category')
    plt.scatter(x=df_agg['price_mean'].mean(), y=df_agg['velocity (units per week)'].mean(), label='Centroid (overall)', marker='*', s=100, color='blue')
    plt.scatter(x=df_agg.loc[df_agg['holding_description']==retailer, 'price_mean'].mean(), y=df_agg.loc[df_agg['holding_description']==retailer, 'velocity (units per week)'].mean(), label='Centroid (retailer)', marker='*', s=100, color='red')
    plt.legend(bbox_to_anchor=(1,1))
    plt.xlim(0, x_max)
    plt.ylim(0, y_max)
    plt.title(f"Campbell's velocity vs price at {retailer}")
    plt.show()

# COMMAND ----------

df_agg
