# Databricks notebook source
# MAGIC %md
# MAGIC # Profile

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load packages, parameters, and data

# COMMAND ----------

import json
import os
import pickle
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as graph
import plotly.graph_objects as go

# COMMAND ----------

# Get configuration
with open(
    "../../dbfs/mnt/processed/davis/exact/value_measurement_test/current_weekly/config.0.1.0.json", "r"
) as config_fp:
    CONFIG = json.load(config_fp)

# COMMAND ----------

# Required parameters
client = CONFIG["value"]["parameters"]["required"]["client"]
path_prefix = CONFIG["value"]["parameters"]["required"]["path_prefix"]
raw_data_name = CONFIG["value"]["parameters"]["required"]["raw_data_name"]
run_name = CONFIG["value"]["parameters"]["required"]["run_name"]
matching_criterion = CONFIG["value"]["parameters"]["required"]["matching_criterion"]
rename_dict = CONFIG["value"]["parameters"]["required"]["rename_dict"]
dates_to_parse = CONFIG["value"]["parameters"]["required"]["dates_to_parse"]
estimator = CONFIG["value"]["parameters"]["required"]["estimator"]

# Optional parameters
advanced_filter_lookup = CONFIG["value"]["parameters"]["optional"]["advanced_filter_lookup"]
breakdown_dict = CONFIG["value"]["parameters"]["optional"]["breakdown_dict"]
cols_to_use = CONFIG["value"]["parameters"]["optional"]["cols_to_use"]
test_tuple = CONFIG["value"]["parameters"]["optional"]["test_tuple"]
filter_tuple = CONFIG["value"]["parameters"]["optional"]["filter_tuple"]
skip_middle_timeframe = CONFIG["value"]["parameters"]["optional"]["skip_middle_timeframe"]
coverage_initiation = CONFIG["value"]["parameters"]["optional"]["coverage_initiation"]
use_overall = CONFIG["value"]["parameters"]["optional"]["use_overall"]
exclude_dates = CONFIG["value"]["parameters"]["optional"]["exclude_dates"]
breakdown_combination = CONFIG["value"]["parameters"]["optional"]["breakdown_combination"]
breakdown_labels = CONFIG["value"]["parameters"]["optional"]["breakdown_labels"]
min_test_stores = CONFIG["value"]["parameters"]["optional"]["min_test_stores"]
min_control_stores = CONFIG["value"]["parameters"]["optional"]["min_control_stores"]
min_ratio_of_control_to_test = CONFIG["value"]["parameters"]["optional"]["min_ratio_of_control_to_test"]

# Fixed parameters
raw_output_path = CONFIG["value"]["parameters"]["required"]["raw_output_path"]
final_output_path = CONFIG["value"]["parameters"]["required"]["final_output_path"]

graph.style.use('fivethirtyeight')

# COMMAND ----------

def valid_breakdown_flagger(df, min_test_stores=min_test_stores, min_control_stores=min_control_stores, min_ratio_of_control_to_test=min_ratio_of_control_to_test):
    df['has_min_test_stores'] = np.where(df['unique_test_store_count']>=min_test_stores, 1, 0)
    df['has_min_control_stores'] = np.where(df['unique_control_store_count']>=min_control_stores, 1, 0)
    df['has_min_control_stores_ratio'] = np.where(df['unique_test_store_count']*min_ratio_of_control_to_test<=df['unique_control_store_count'], 1, 0)
    df['valid_breakdown'] = np.where((df['has_min_test_stores']==1) & ((df['has_min_control_stores']==1) | (df['has_min_control_stores_ratio']==1)), 1, 0)

# COMMAND ----------

# Read and transform the data
df_raw = pd.read_csv(f"{path_prefix}/{client}/{run_name}/raw/profiling_data_input.csv", parse_dates=dates_to_parse, dtype={'store': str})

map_df = df_raw[['store', 'coverage_initiation']].drop_duplicates().dropna()
store_coverage_initiation_dict = {store: date for _, (store, date) in map_df.iterrows()}

df_raw.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the breakdowns, mappings and check test validity

# COMMAND ----------

# Create a column to count is_post
df_raw['is_post'] = df_raw['calendar_date'] >= df_raw['coverage_initiation']
df_agg_stores = df_raw.groupby(['subbanner', 'current_state', 'future_state', 'test', 'store', 'is_post']).agg({'is_post': 'count'}).unstack()
df_agg_stores.columns = df_agg_stores.columns.droplevel()
df_agg_stores.columns.name = None
df_agg_stores = df_agg_stores.rename(columns={True: 'post_intervention_week_count', False: 'pre_intervention_week_count'})

# Pre-allocate columns prior to aggregation
for prefix in ['min_', 'mean_', 'median_', 'max_']:
    for col in ['post_intervention_week_count', 'pre_intervention_week_count']:
        new_col = prefix + col
        df_agg_stores[new_col] = df_agg_stores[col].copy()

df_agg_stores = df_agg_stores.groupby(['subbanner', 'current_state', 'future_state', 'test']).agg(
    {
        'min_pre_intervention_week_count': 'min',
        'mean_pre_intervention_week_count': 'mean',
        'median_pre_intervention_week_count': 'median',
        'max_pre_intervention_week_count': 'max',
        'min_post_intervention_week_count': 'min',
        'mean_post_intervention_week_count': 'mean',
        'median_post_intervention_week_count': 'median',
        'max_post_intervention_week_count': 'max'
    }
)

df_agg_stores = df_agg_stores.reset_index()
df_agg_stores_control = df_agg_stores.loc[df_agg_stores['test']==False].drop(columns=['test', 'future_state'])
df_agg_stores_control.columns = ['control_' + col if 'intervention' in col else col for col in df_agg_stores_control.columns]
df_agg_stores_test = df_agg_stores.loc[df_agg_stores['test']==False].drop(columns=['test', 'future_state'])
df_agg_stores_test.columns = ['test_' + col if 'intervention' in col else col for col in df_agg_stores_control.columns]

# Creating the dataframe to evaluate store counts per breakdown
df_agg = df_raw.groupby(['subbanner', 'current_state', 'future_state', 'test']).agg({'store': pd.Series.nunique}).reset_index()
df_agg_test = df_agg[df_agg['test']==1].rename(columns={'store': 'unique_test_store_count'})
df_agg_control = df_agg[df_agg['test']==0].rename(columns={'store': 'unique_control_store_count'})
df_agg_test = df_agg_test.merge(df_agg_control[['subbanner', 'current_state', 'unique_control_store_count']], on=['subbanner', 'current_state'], how='left').fillna(0)
valid_breakdown_flagger(df_agg_test)

# Join the date counts
df_agg_test = df_agg_test.merge(df_agg_stores_control, on=['subbanner', 'current_state'], how='left').fillna(0)
df_agg_test = df_agg_test.merge(df_agg_stores_test, on=['subbanner', 'current_state'], how='left').fillna(0)


# Generate the loose control pools
df_agg_test_subset = \
df_agg_test.loc[df_agg_test['valid_breakdown']==0, ['subbanner', 'current_state', 'future_state', 'unique_test_store_count', 'unique_control_store_count']]

df_agg_control_loose = \
df_agg_control.groupby(['subbanner']).agg({'unique_control_store_count': 'sum'}).rename(columns={'unique_control_store_count': 'loose_control_store_count'})

df_agg_test_subset = df_agg_test_subset.merge(df_agg_control_loose, on=['subbanner'])

# Appending the new control store counts
df_agg_test['control_pool'] = 'strict'

# Labelling loose mapping for test stores
for i in df_agg_test_subset.index:
    subbanner = df_agg_test_subset.loc[i, 'subbanner']
    current_state = df_agg_test_subset.loc[i, 'current_state']
    future_coverage = df_agg_test_subset.loc[i, 'future_state']
    unique_test_store_count = df_agg_test_subset.loc[i, 'unique_test_store_count']
    unique_control_store_count = df_agg_test_subset.loc[i, 'unique_control_store_count']
    loose_control_store_count = df_agg_test_subset.loc[i, 'loose_control_store_count']
    if unique_control_store_count < loose_control_store_count:
        df_agg_test.loc[
            (df_agg_test['subbanner']==subbanner) & (df_agg_test['current_state']==current_state) & (df_agg_test['future_state']==future_coverage),
            'unique_control_store_count'
        ] = loose_control_store_count
        df_agg_test.loc[
            (df_agg_test['subbanner']==subbanner) & (df_agg_test['current_state']==current_state) & (df_agg_test['future_state']==future_coverage),
            'control_pool'
        ] = 'loose'

# Check if valid
valid_breakdown_flagger(df_agg_test)

df_agg_test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create labels for control stores

# COMMAND ----------

# Creating a new breakdown column
for key in breakdown_combination:
    df_agg_test[key] = \
    df_agg_test[breakdown_combination['breakdown']].apply(lambda x: ' - '.join(x.values.astype(str)), axis=1)

# Create the control store breakdown lookup for the strict mapping criteria
df_agg_control_strict = df_agg_control.merge(
    df_agg_test.loc[(df_agg_test['valid_breakdown']==1) & (df_agg_test['control_pool']=='strict'), ['subbanner', 'current_state', 'breakdown']],
    on=['subbanner', 'current_state'],
    how='inner'
)

# Create the control store breakdown lookup for the loose mapping criteria
df_agg_control_loose = df_agg_control_loose.merge(
    df_agg_test.loc[(df_agg_test['valid_breakdown']==1) & (df_agg_test['control_pool']=='loose'), ['subbanner', 'breakdown']],
    on=['subbanner'],
    how='inner'
)[['subbanner', 'breakdown']]
df_agg_control_loose['test'] = False

# Creating a strict mapping
df_agg_breakdown_labelling = pd.concat(
    [
        df_agg_control_strict[['subbanner', 'current_state', 'future_state', 'test', 'breakdown']],
        df_agg_test[['subbanner', 'current_state', 'future_state', 'test', 'breakdown']]
    ],
    axis=0
)

# Cross join to assign breakdown labels and facilitate duplication for divergent future_state where needed
df_raw_strict = df_raw.merge(
    df_agg_breakdown_labelling[['subbanner', 'current_state', 'future_state', 'test', 'breakdown']],
    on=['subbanner', 'current_state', 'future_state', 'test'],
    how='inner'
)

# Create a subset containing just the loose pool control store sales
df_raw_control_loose = df_raw.merge(
    df_agg_control_loose,
    on=['subbanner', 'test'],
    how='inner'
)

# Concat
df_raw = pd.concat([df_raw_strict, df_raw_control_loose], axis=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced filtering for different dates

# COMMAND ----------

# Advanced filtering on subbanner specific dates
if advanced_filter_lookup:
    df_filter_lookup = pd.read_csv(f'{path_prefix}/{client}/{run_name}/advanced_date_filter.csv')
    df_agg_test = df_agg_test.merge(df_filter_lookup, on=['subbanner', 'current_state', 'future_state'], how='left') 
    for i in df_agg_test[df_agg_test['valid_breakdown']==1].index:
        row = df_agg_test.loc[i]
        if row['include_exclude'] == 'include':
            df_raw = df_raw.loc[
                (df_raw['breakdown']!=row['breakdown']) |
                (
                    (df_raw['breakdown']==row['breakdown']) &\
                    (df_raw['calendar_date']>=row['earliest_date']) &\
                    (df_raw['calendar_date']<=row['latest_date'])
                )
            ]
        else:
            df_raw = df_raw.loc[
                (df_raw['breakdown']!=row['breakdown']) |
                ~(
                    (df_raw['breakdown']==row['breakdown']) &\
                    (df_raw['calendar_date']>=row['earliest_date']) &\
                    (df_raw['calendar_date']<=row['latest_date'])
                )
            ]

    df_agg_test = df_agg_test.drop_duplicates(subset=['breakdown']) # preference to keep includes listed first instead of excludes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick check of validity and creating the breakdown list

# COMMAND ----------

print(f"There are {df_agg_test[df_agg_test['valid_breakdown']==1].shape[0]} valid tests") # 16
print(f"There are {df_agg_test[df_agg_test['valid_breakdown']==0].shape[0]} invalid tests") # 2

# COMMAND ----------

# Basic Sankey diagram of tests - need to be able to ingest multiple coverage_initiations in the future

for breakdown in df_agg_test.loc[df_agg_test['valid_breakdown']==1, 'breakdown'].unique():

    current_state = df_agg_test.loc[df_agg_test['breakdown']==breakdown, 'current_state'].values[0]
    future_state = df_agg_test.loc[df_agg_test['breakdown']==breakdown, 'future_state'].values[0]
    n_test = df_agg_test.loc[df_agg_test['breakdown']==breakdown, 'unique_test_store_count'].values[0]
    n_control = df_agg_test.loc[df_agg_test['breakdown']==breakdown, 'unique_control_store_count'].values[0]
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = [
              f"Before in-store change <br>All stores = {current_state} <br>(n={n_test + n_control})",
              f"Date of in-store change: <br>{coverage_initiation}",
              f"Test stores = {future_state} <br>(n={n_test})",
              f"Control stores = {current_state} <br>(n={n_control})"
          ],
          color = "blue"
        ),
        link = dict(
          source = [0, 1, 1], # indices correspond to labels, eg A1, A2, A1, B1, ...
          target = [1, 2, 3],
          value = [n_test + n_control, n_test, n_control]
      ))])

    fig.update_layout(title_text=breakdown, font_size=10)
    fig.show()

# COMMAND ----------

# Setting up the breakdown dictionary # TODO - figure out how to safely deprecate breakdown_dict

breakdown_dict = {'breakdown': df_agg_test.loc[df_agg_test['valid_breakdown']==1, 'breakdown'].unique()}
if breakdown_dict:
    breakdown_list = [df_raw[df_raw[key].isin(breakdown_dict[key])].groupby(key) for key in breakdown_dict.keys()]
    breakdown_list = [item for sublist in breakdown_list for item in sublist]
    if use_overall:
        breakdown_list = [('OVERALL', df_raw), *breakdown_list]
if breakdown_dict == None:
    breakdown_list = [('OVERALL', df_raw)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Iterate through the breakdowns and profile the data

# COMMAND ----------

# Set up tables to record results
table_of_stats = df_agg_test.set_index("breakdown") # pd.DataFrame()

# Setting up dictionaries to save input dataframes to the proceeding matching step
dict_prices_test = {}
dict_prices_control = {}

overall_flag = 0

# COMMAND ----------

# Iterate through the profiling
for i, (breakdown, df_breakdown) in enumerate(breakdown_list):

    table_of_stats.loc[breakdown, 'Row count'] = df_breakdown.shape[0]
    table_of_stats.loc[breakdown, 'Column count'] = df_breakdown.shape[1]
    table_of_stats.loc[breakdown, 'Test row count'] = df_breakdown[(df_breakdown['test']==True)].shape[0]
    table_of_stats.loc[breakdown, 'Control row count'] = df_breakdown[(df_breakdown['test']==False)].shape[0]
    table_of_stats.loc[breakdown, 'Min observation date'] = df_breakdown['calendar_date'].min()
    table_of_stats.loc[breakdown, 'Mean observation date'] = df_breakdown['calendar_date'].mean()
    table_of_stats.loc[breakdown, 'Median observation date'] = df_breakdown['calendar_date'].quantile(0.5)
    table_of_stats.loc[breakdown, 'Max observation date'] = df_breakdown['calendar_date'].max()
    table_of_stats.loc[breakdown, 'Unique test stores count'] = \
        df_breakdown[(df_breakdown['test']==True)]['store'].nunique()
    table_of_stats.loc[breakdown, 'Unique Control stores count'] = \
        df_breakdown[(df_breakdown['test']==False)]['store'].nunique()

    list_df_hierarchical = []
    list_of_matched_control_store_counts = []
    df_hierarchical = pd.DataFrame()

    table_of_stats.loc[breakdown, 'earliest_coverage'] = df_breakdown['coverage_initiation'].astype(str).min()

    # aggregate the data
    df_store_data = df_breakdown.groupby(['calendar_date', 'store', 'test']).agg({'dollar_sales':'sum'})
    df_store_data = df_store_data.reset_index()

    # check for missing sales data
    # 20210409 - How should we implement thresholding for missing values?
    table_of_stats.loc[breakdown, 'Missing values in sales data'] = df_store_data.dollar_sales.isna().sum()
    assert df_store_data.dollar_sales.isna().sum() == 0, 'Missing values in sales data'

    # create the pivot table of stores vs. sales dates
    df_store_data['transaction_date'] = pd.to_datetime(df_store_data['calendar_date'])
    df_prices = df_store_data.pivot_table(
        index='transaction_date',
        columns=['store'],
        values='dollar_sales',
        aggfunc=np.sum
    ).T.fillna(0)

    n_unique_dates = df_store_data[df_store_data['test'] == True]['calendar_date'].nunique()

    index_lookup = df_prices.reset_index()[['store']]
    index_lookup.index = df_prices.index

    # 20210409 - What happened when this was observed previously? How do we wish to use this information?
    table_of_stats.loc[breakdown, 'Number of dates in data'] = df_prices.shape[1]
    table_of_stats.loc[breakdown, 'Number of unique dates'] = n_unique_dates
    if df_prices.shape[1] != n_unique_dates:
        warnings.warn(f'''
            Number of dates in raw data is different from number of dates in the pivot table.
            {df_prices.shape[1]:,} != {n_unique_dates:,}
            Consider informing the client about this.
        ''')


    # identify the store type
    df_store_type = df_store_data[['store', 'test']].drop_duplicates()
    df_store_type = df_prices.merge(df_store_type, on='store', how='left')[['store', 'test']]

    missing_sales = pd.DataFrame(df_prices.isna().sum(axis=1), columns=['num_missing_weeks'])
    missing_sales = missing_sales.reset_index().sort_values(by='num_missing_weeks', ascending=False)
    missing_sales = missing_sales.merge(right=df_store_type, how='left', on='store')
    missing_sales_counts = pd.DataFrame(missing_sales.groupby(['test', 'num_missing_weeks']).size())
    missing_sales_counts = missing_sales_counts.reset_index()
    missing_sales_counts = missing_sales_counts.rename(columns={0: 'num_stores'})

    missing_sales_counts = missing_sales_counts.rename(
        columns={'test': 'Is test store?',
        'num_missing_weeks': 'Number of weeks with missing sales',
        'num_stores': 'Number of stores'}
    )

#     # drop rows with missing values
#     if skip_middle_timeframe == True:
#         df_prices = df_prices.drop(columns=year1_dates)
#     else:
#         df_prices = df_prices.dropna()

#     # re-pivot the table and resample to get regular cadence of dates
#     df_prices = df_prices.T.resample('W').sum().T

    zero_sales = pd.DataFrame(df_prices.eq(0).sum(axis=1), columns=['num_zero_weeks'])
    zero_sales = zero_sales.reset_index().sort_values(by='num_zero_weeks', ascending=False)
    zero_sales = zero_sales.merge(right=df_store_type, how='left', on='store')
    zero_sales_counts = pd.DataFrame(zero_sales.groupby(['test', 'num_zero_weeks']).size())
    zero_sales_counts = zero_sales_counts.reset_index()
    zero_sales_counts = zero_sales_counts.rename(columns={0: 'num_stores'})

    zero_sales_counts = zero_sales_counts.rename(
        columns={'test': 'Is test store?',
        'num_zero_weeks': 'Number of weeks with zero sales',
        'num_stores': 'Number of stores'}
    )

#     df_prices_control = df_prices[~df_store_type['test'].values]
    df_prices_control = df_prices[df_prices.index.isin(df_store_type.loc[df_store_type["test"]==False, "store"].unique())]
#     df_prices_test = df_prices[df_store_type['test'].values]
    df_prices_test = df_prices[df_prices.index.isin(df_store_type.loc[df_store_type["test"]==True, "store"].unique())]

    table_of_stats.loc[breakdown, 'Number of control stores without zero sales'] = df_prices_control.shape[0]
    table_of_stats.loc[breakdown, 'Number of test stores without zero sales'] = df_prices_test.shape[0]
    table_of_stats.loc[breakdown, 'Ratio of control to test stores (without zero sales)'] = \
        table_of_stats.loc[breakdown, 'Number of control stores without zero sales']/\
        table_of_stats.loc[breakdown, 'Number of test stores without zero sales']

    # 20210409 - What happened when this was observed previously?
    # How do we wish to use this information going forward?
    assert df_prices.isna().values.sum() == 0, 'sum != 0'
    if np.isclose(df_prices.values, 0).sum() > 0:
        warnings.warn('At least 1 store has zero sales in at least 1 week. Consider investigating the cause.')

    # Need to update for differing matching criterion
    dict_prices_test[breakdown] = df_prices_test
    dict_prices_control[breakdown] = df_prices_control

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data

# COMMAND ----------

# Save the results
df_raw.to_csv(f"{path_prefix}/{client}/{run_name}/processing/processed_data.csv", index=True)
table_of_stats.to_csv(f"{path_prefix}/{client}/{run_name}/processing/table_of_stats.csv", index=True)
zero_sales_counts.to_csv(f"{path_prefix}/{client}/{run_name}/processing/zero_sales_counts.csv", index=True)
missing_sales_counts.to_csv(f"{path_prefix}/{client}/{run_name}/processing/missing_sales_counts.csv", index=True)

filename = f"{path_prefix}/{client}/{run_name}/processing/dict_prices_test.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_prices_test, f, protocol=pickle.HIGHEST_PROTOCOL)

filename = f"{path_prefix}/{client}/{run_name}/processing/dict_prices_control.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_prices_control, f, protocol=pickle.HIGHEST_PROTOCOL)

filename = f"{path_prefix}/{client}/{run_name}/processing/store_coverage_initiation_dict.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(store_coverage_initiation_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
