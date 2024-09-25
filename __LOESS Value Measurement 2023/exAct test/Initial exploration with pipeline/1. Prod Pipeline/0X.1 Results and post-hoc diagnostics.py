# Databricks notebook source
# MAGIC %md
# MAGIC # Results and post-hoc diagnostics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load packages, parameters and data

# COMMAND ----------

# Profile
import json
import os
import pickle
import warnings
import arviz as az
import pandas as pd
import numpy as np
import matplotlib.pyplot as graph
import seaborn as sns
import matplotlib.dates as mdates

import exact.matching as ssm
import tensorflow as tf
from tqdm import trange

os.environ["TF_DETERMINISTIC_OPS"] = "1"
pd.set_option('display.max_rows', 500)

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
file_type = CONFIG["value"]["parameters"]["required"]["file_type"]
rename_dict = CONFIG["value"]["parameters"]["required"]["rename_dict"]
dates_to_parse = CONFIG["value"]["parameters"]["required"]["dates_to_parse"]
estimator = CONFIG["value"]["parameters"]["required"]["estimator"]

# Optional parameters
advanced_filter_lookup = CONFIG["value"]["parameters"]["optional"]["advanced_filter_lookup"]
tensorflow_random_seed = CONFIG["value"]["parameters"]["required"][
    "tensorflow_random_seed"
]
earliest_date = CONFIG["value"]["parameters"]["optional"]["earliest_date"]
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

# Read the results
table_of_stats = pd.read_csv(f"{path_prefix}/{client}/{run_name}/final/table_of_stats.csv")
table_of_results = pd.read_csv(f"{path_prefix}/{client}/{run_name}/final/exact_ds_results_weekly.csv")
df_exact_ds_store_estimated_impact_weekly = pd.read_csv(f"{path_prefix}/{client}/{run_name}/final/exact_ds_store_estimated_impact_weekly.csv", dtype={'store': str}, parse_dates=['calendar_date'])

# Getting the outputs of the preceeding steps
df_breakdown = pd.read_csv(
    f"{path_prefix}/{client}/{run_name}/processing/processed_data.csv",
    parse_dates=["calendar_date"],
    dtype={'store': str}
)
df_breakdown = df_breakdown[["store", "test", "breakdown"]].drop_duplicates()

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_hierarchical.pkl",
    "rb"
)
dict_hierarchical = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/matched_controls.pkl",
    "rb",
)
matched_controls = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_prices_control.pkl",
    "rb"
)
dict_prices_control = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_prices_test.pkl",
    "rb"
)
dict_prices_test = pickle.load(filehandler)


filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_relative_scores.pkl",
    "rb"
)
dict_matcher_relative_scores = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_relative_indices.pkl",
    "rb"
)
dict_matcher_relative_indices = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_relative_objects.pkl",
    "rb"
)
dict_matcher_relative_objects = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_absolute_scores.pkl",
    "rb"
)
dict_matcher_absolute_scores = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_absolute_indices.pkl",
    "rb"
)
dict_matcher_absolute_indices = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_absolute_objects.pkl",
    "rb"
)
dict_matcher_absolute_objects = pickle.load(filehandler)


filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_matched_control_mapping.pkl",
    "rb"
)
dict_matched_control_mapping = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_analysis_hsd.pkl",
    "rb"
)
dict_analysis_hsd = pickle.load(filehandler)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results (done)

# COMMAND ----------

table_of_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics (done)

# COMMAND ----------

table_of_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile - missing sales (done)

# COMMAND ----------

# Create graphs for store missing sales
for breakdown in table_of_results['breakdown'].unique():
    graph.figure(figsize=(15, 5))

    df_prices = dict_prices_control[breakdown]
    graph.plot(
        np.array(df_prices.columns),
        np.array(df_prices.isna().sum(axis=0) / df_prices.shape[0]),
        label=f'control stores, n={df_prices.shape[0]}'
    )
    df_prices = dict_prices_test[breakdown]
    graph.plot(
        np.array(df_prices.columns),
        np.array(df_prices.isna().sum(axis=0) / df_prices.shape[0]),
        label=f'test stores, n={df_prices.shape[0]}',
        marker='*',
        linewidth=0
    )
    graph.ylabel('Proportion of Stores')
    graph.xlabel('Date')
    graph.ylim(0, 1)
    graph.legend()
    title = f'breakdown={breakdown} \n stores with missing values in each week'
    graph.title(title)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile - zero sales (done)

# COMMAND ----------

# Create graphs for store zero sales
for breakdown in table_of_results['breakdown'].unique():
    graph.figure(figsize=(15, 5))

    df_prices = dict_prices_control[breakdown]
    graph.plot(
        np.array(df_prices.columns),
        np.array(np.equal(df_prices.values, 0).sum(axis=0) / df_prices.shape[0]),
        label=f'control stores, n={df_prices.shape[0]}'
    )
    df_prices = dict_prices_test[breakdown]
    graph.plot(
        np.array(df_prices.columns),
        np.array(np.equal(df_prices.values, 0).sum(axis=0) / df_prices.shape[0]),
        label=f'test stores, n={df_prices.shape[0]}',
        marker='*',
        linewidth=0
    )
    graph.ylabel('Proportion of Stores')
    graph.xlabel('Date')
    graph.ylim(0, 1)
    graph.legend()
    title = f'breakdown={breakdown} \n stores with zero sales in each week'
    graph.title(title)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match - similarity scores (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    matched_similiarity_scores = dict_matcher_relative_scores[breakdown]
    graph.figure(figsize=(20, 15))
    sns.heatmap(matched_similiarity_scores)
    graph.ylabel('Test Store')
    graph.xlabel('Control Store')
    title = f'breakdown={breakdown}\nSimilarity Score\n(Darker is better)'
    graph.title(title, fontsize=15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Similarity scores histograms (2023-04-12)

# COMMAND ----------

graph.figure(figsize=(20, 10))
for breakdown in table_of_results['breakdown'].unique():
    matched_similiarity_scores = dict_matcher_relative_scores[breakdown]
    graph.hist(matched_similiarity_scores.sum(axis=1).values, alpha=0.25, label=breakdown, density=True)
title = f'Relative Similarity Score distributions'
graph.legend(bbox_to_anchor=(1.5,1))
graph.xlabel('Relative Similarity Score')
graph.ylabel('Count')
graph.title(title, fontsize=15)
graph.show()

# COMMAND ----------

graph.figure(figsize=(20, 10))
for breakdown in table_of_results['breakdown'].unique():
    matched_similiarity_scores = dict_matcher_absolute_scores[breakdown]
    graph.hist(matched_similiarity_scores.sum(axis=1).values, alpha=0.25, label=breakdown, density=True)
title = f'Absolute Similarity Score distributions'
graph.legend(bbox_to_anchor=(1.5,1))
graph.xlabel('Absolute Similarity Score')
graph.ylabel('Count')
graph.title(title, fontsize=15)
graph.show()

# COMMAND ----------

# Matching wasn't great
# Relative and absolute values are the same?
# Higher scores should indicate poorer matching but these show that high scores correspond generally with better average post-fitting r2
df_similarity_scores = pd.DataFrame(columns=["relative_sum_mean", "aboslute_sum_mean"])
for breakdown in table_of_results["breakdown"].unique():
    relative_similiarity_scores = dict_matcher_relative_scores[breakdown].sum(axis=1).mean()
    absolute_similiarity_scores = dict_matcher_absolute_scores[breakdown].sum(axis=1).mean()
    df_similarity_scores.loc[breakdown] = [relative_similiarity_scores, absolute_similiarity_scores]
    
df_similarity_scores

# COMMAND ----------

# Matching wasn't great
# Relative and absolute values are the same?
# Higher scores should indicate poorer matching but these show that high scores correspond generally with better average post-fitting r2
df_similarity_scores = pd.DataFrame(columns=["relative_sum_sum", "aboslute_sum_sum"])
for breakdown in table_of_results["breakdown"].unique():
    relative_similiarity_scores = dict_matcher_relative_scores[breakdown].sum(axis=1).sum()
    absolute_similiarity_scores = dict_matcher_absolute_scores[breakdown].sum(axis=1).sum()
    df_similarity_scores.loc[breakdown] = [relative_similiarity_scores, absolute_similiarity_scores]
    
df_similarity_scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare pre-post sales (2023-04-12)

# COMMAND ----------

df_breakdown_evaluation = pd.DataFrame(
    columns=[
        "zero_sales_mean_count_pre", "test_store_sales_mean_pre_period", "pre_estimate_mean_sales_in_pre_period", "post_estimate_mean_sales_in_pre_period",
        "zero_sales_mean_count_post", "test_store_sales_mean_post_period", "pre_estimate_mean_sales_in_pre_period", "post_estimate_mean_sales_in_pre_period"
    ]
)
for breakdown in table_of_results["breakdown"].unique():
    df_breakdown = dict_analysis_hsd[breakdown].df
    df_breakdown["zero_sales"] = np.where(df_breakdown["y"]==0, 1, 0)

    df_agg = df_breakdown.groupby(["is_post"]).agg(
        zero_sales = ("zero_sales", "mean"),
        test_store_sales = ("y", "mean"),
        y_pre_sales = ("y_given_pre_expected", "mean"),
        y_post_sales = ("y_given_post_expected", "mean"),
    )
    df_breakdown_evaluation.loc[breakdown] = [*df_agg.loc[False].values, *df_agg.loc[True].values]
    
df_breakdown_evaluation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matched sample rework (2023-04-12)

# COMMAND ----------

breakdown = "item=50889726, began=2022-09-06, days measured=21 - no display - display built"
df_breakdown = dict_analysis_hsd[breakdown].df
df_temp = df_breakdown[df_breakdown["group"]=="4216"]
df_temp["date"].max(), df_temp["coverage_initiation"].unique()[0]

# COMMAND ----------

# View stores matched on most readable test
breakdown = "item=50889726, began=2022-09-06, days measured=21 - no display - display built"
df_breakdown = dict_analysis_hsd[breakdown].df

for test_store in df_breakdown["group"].unique():
    df_temp = df_breakdown[df_breakdown["group"]==test_store]
    ymax = max(df_temp[list(range(10))].max().max(), df_temp["y"].max())
    
    graph.figure(figsize=(15,6))
    for control_store in range(10):
        graph.plot(df_temp["date"], df_temp[control_store], linewidth=0.5, color='gray')
    graph.plot(df_temp["date"], df_temp[list(range(10))].mean(axis=1), label='Average Matches\n(weights not optimised)')
    graph.plot(df_temp["date"], df_temp["y"], label='Test')
    graph.vlines(pd.to_datetime(df_temp["coverage_initiation"].unique()[0]).date(), ymin=0, ymax=ymax, color='red', label='intervention')
    graph.xlabel('Days')
    graph.ylabel('Sales ($)')
    graph.gca().xaxis.set_major_locator(mdates.DayLocator((1)))
    graph.setp(graph.gca().get_xticklabels(), rotation=60, ha="right")
    graph.title(f'breakdown={breakdown}\nTest store vs matched control stores sales\ntest_store={test_store}')
    graph.legend(bbox_to_anchor=(1.2,1))
    graph.show()

# COMMAND ----------

# View stores matched on most readable test
breakdown = "item=50889726, began=2022-12-15, days measured=14 - no display - display built"
df_breakdown = dict_analysis_hsd[breakdown].df

for test_store in df_breakdown["group"].unique():
    df_temp = df_breakdown[df_breakdown["group"]==test_store]
    ymax = max(df_temp[list(range(10))].max().max(), df_temp["y"].max())
    
    graph.figure(figsize=(15,6))
    for control_store in range(10):
        graph.plot(df_temp["date"], df_temp[control_store], linewidth=0.5, color='gray')
    graph.plot(df_temp["date"], df_temp[list(range(10))].mean(axis=1), label='Average Matches\n(weights not optimised)')
    graph.plot(df_temp["date"], df_temp["y"], label='Test')
    graph.vlines(pd.to_datetime(df_temp["coverage_initiation"].unique()[0]).date(), ymin=0, ymax=ymax, color='red', label='intervention')
    graph.xlabel('Days')
    graph.ylabel('Sales ($)')
    graph.gca().xaxis.set_major_locator(mdates.DayLocator((1)))
    graph.setp(graph.gca().get_xticklabels(), rotation=60, ha="right")
    graph.title(f'breakdown={breakdown}\nTest store vs matched control stores sales\ntest_store={test_store}')
    graph.legend(bbox_to_anchor=(1.2,1))
    graph.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match - match quality (done)

# COMMAND ----------

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
tf.random.set_seed(tensorflow_random_seed)
n_stores = 10

for breakdown in table_of_results['breakdown'].unique():
    matcher = dict_matcher_relative_objects[breakdown]
    matched_indices = dict_matcher_relative_indices[breakdown]
    df_prices_control = dict_prices_control[breakdown]
    df_prices_test = dict_prices_test[breakdown]
    print(f'Match quality\nbreakdown={breakdown}')
    matcher.plot_match_quality(
        df_prices_control,
        df_prices_test.head(n_stores),
        matched_indices.iloc[:n_stores, :],
        figsize=(20, 15),
        xlabel='Control Store',
        ylabel='Test Store'
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match - match sample (done)

# COMMAND ----------

# Show first 5 test stores per breakdown and the matched controls for those stores
for breakdown in df_breakdown['breakdown'].unique():
    for test_store in dict_matched_control_mapping[breakdown].index[:5]:
        matched_controls = dict_matched_control_mapping[breakdown].loc[test_store].values
        x_test = df_exact_ds_store_estimated_impact_weekly.loc[df_exact_ds_store_estimated_impact_weekly['store']==test_store, ['calendar_date', 'dollar_sales']]
        selected_matches = df_exact_ds_store_estimated_impact_weekly.loc[
            df_exact_ds_store_estimated_impact_weekly['store'].isin(matched_controls),
            ['store', 'calendar_date', 'dollar_sales']
        ]
        selected_matches = selected_matches.pivot_table(
            index='calendar_date',
            columns=['store'],
            values='dollar_sales',
            aggfunc=np.sum
        )
        ymax = np.max([selected_matches.max().max(), x_test['dollar_sales'].max()])
        graph.figure(figsize=(15,8))
        for control_store in selected_matches.columns:
            graph.plot(selected_matches.index, selected_matches[control_store], linewidth=0.5, color='gray')
        graph.plot(selected_matches.index, selected_matches.mean(axis=1), label='Average Matches (weights not optimised)')
        graph.plot(x_test['calendar_date'], x_test['dollar_sales'], label='Test')
        graph.vlines(pd.to_datetime(coverage_initiation).date(), ymin=0, ymax=ymax, color='red', label='intervention')
        graph.xlabel('Days')
        graph.ylabel('Sales ($)')
        graph.gca().xaxis.set_major_locator(mdates.DayLocator((1)))
        graph.setp(graph.gca().get_xticklabels(), rotation=60, ha="right")
        graph.title(f'breakdown={breakdown}\nTest store vs matched control stores sales\ntest_store={test_store}')
        graph.legend()
        graph.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match - distribution of matching scores (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    matched_similiarity_scores = dict_matcher_relative_scores[breakdown]
    graph.figure(figsize=(20,15))
    sns.displot(matched_similiarity_scores.values.flatten(), kde=False, alpha=.5, height=5, aspect=3)
    graph.ylabel('Frequency')
    graph.xlabel('Average matching similarity score')
    graph.setp(graph.gca().get_xticklabels(), rotation=60, ha="right")
    title = f'breakdown={breakdown}\nDistribution of matching scores\n(a lower score indicates a better match)'
    graph.title(title, fontsize=15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - sales of test stores (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    df_hierarchical = dict_hierarchical[breakdown]
    graph.figure(figsize=(20, 10))
    for _, df_breakdown in df_hierarchical.groupby("group"):
        graph.plot(
            np.array(pd.to_datetime(df_breakdown["date"])),
            np.array(df_breakdown["y"]),
            lw=1,
            alpha=0.5,
        )
    graph.xlabel("Date")
    graph.ylabel("Sales ($)")
    title = f"breakdown={breakdown}\nSales of Test Stores\nUnique test store count={df_hierarchical['group'].nunique()}"
    graph.title(title)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - sales of matched control stores (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    unique_matched_controls = set(dict_matched_control_mapping[breakdown].values.flatten())
    df_prices_control = dict_prices_control[breakdown]
    graph.figure(figsize=(20, 10))
    graph.plot(df_prices_control.loc[df_prices_control.index.isin(unique_matched_controls), :].T, lw=1)
    graph.xlabel("Date")
    graph.ylabel("Sales ($)")
    title = f"breakdown={breakdown}\nSales of Matched Control Stores\nUnique control store count={len(unique_matched_controls)}"
    graph.title(title)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - plot cate (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    analysis_hsd = dict_analysis_hsd[breakdown]
    print(f'breakdown={breakdown}\nCATE')
    analysis_hsd.plot_cate()
    print('\n\n')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - plot lift (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    analysis_hsd = dict_analysis_hsd[breakdown]
    print(f'breakdown={breakdown}\nLift')
    analysis_hsd.plot_lift()
    print('\n\n')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - distribution of impact by store (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    analysis_hsd = dict_analysis_hsd[breakdown]
    store_impact = analysis_hsd.df[
        ["y", "group", "date", "y_given_pre_expected", "y_given_post_expected"]
    ].copy()
    store_impact["CATE"] = (
        store_impact["y_given_post_expected"] - store_impact["y_given_pre_expected"]
    )
    store_impact_cate = store_impact.groupby("group")["CATE"].mean()
    graph.figure(figsize=(20, 10))
    impact_histogram = graph.hist(store_impact_cate, bins=20)
    graph.xlabel("Impact ($ per store per week)")
    graph.ylabel("Number of Stores")
    graph.yticks(np.arange(0, max(impact_histogram[0] + 1), 1))
    title = "Distribution of Impact by Store"
    graph.title(f'breakdown={breakdown}\n Distribution of impact by store')
    graph.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - plot aggregate predictions (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    analysis_hsd = dict_analysis_hsd[breakdown]
    analysis_hsd.plot_aggregate_predictions(
      should_plot=False,
      line_intervention_color="red",
      line_no_intervention_color="blue",
    )
    title = f"breakdown={breakdown}\nSales - Actual and Expected"
    graph.title(title)
    graph.ylabel("Sales ($)")
    graph.xlabel("Date")
    graph.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - plot cumulative intervention effect (done)

# COMMAND ----------

for breakdown in table_of_results['breakdown'].unique():
    analysis_hsd = dict_analysis_hsd[breakdown]
    analysis_hsd.plot_cumulative_intervention_effect(
        should_plot=False, compute_intervals=True
    )
    graph.ylabel("Impact ($)")
    graph.xlabel("Date")
    graph.title(f'breakdown={breakdown}\nCumulative intervention effect')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - plot posterior (done)

# COMMAND ----------

# 20210722 - need to fix the trace such that posterior plot can execute
for breakdown in table_of_results['breakdown'].unique():
    analysis_hsd = dict_analysis_hsd[breakdown]
    print(f'breakdown={breakdown}\nPosterior plot of r2')
    az.plot_posterior(analysis_hsd.trace, var_names=['r2_pre', 'r2_post'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate - plot match fit (done)

# COMMAND ----------

n_stores = 10
for breakdown in table_of_results['breakdown'].unique():
    analysis_hsd = dict_analysis_hsd[breakdown]
    print(f'breakdown={breakdown}\nMatch fit')
    analysis_hsd.plot_match_fit(n_limit=n_stores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PowerBI demo - approx impact vs approx baseline (done)

# COMMAND ----------

# Setting up the processed and results dfs
df_processed = df_exact_ds_store_estimated_impact_weekly.copy().sort_values(by='breakdown')
df_processed = df_processed[df_processed['test']==True]

for breakdown in df_processed['breakdown'].unique():
    df_temp = df_processed.loc[df_processed['breakdown']==breakdown]

    # Adding in baseline
    df_results = df_temp.copy()
    df_results = df_results.loc[
        (df_processed['calendar_date']<df_processed['coverage_initiation']) &\
        (df_processed['coverage_initiation'].notna())
    ].groupby('store').agg({'dollar_sales': 'mean'}).rename(columns=({'dollar_sales': 'baseline ($/store/week)'})).reset_index()

    # Adding in impact
    df_agg = df_temp.loc[
        (df_processed['calendar_date']>df_processed['coverage_initiation']) &\
        (df_processed['coverage_initiation'].notna())
    ].groupby('store').agg({'impact': 'mean'}).rename(columns=({'impact': 'impact ($/store/week)'})).reset_index()
    df_results = df_results.merge(df_agg, on='store')

    graph.figure(figsize=(15,6))
    sns.scatterplot(
        data=df_results,
        x='baseline ($/store/week)',
        y='impact ($/store/week)'
    )
    graph.title(f'Average Approximate Impact vs Average Approximate Baseline by store \n breakdown = {breakdown} (# test stores = {df_results.shape[0]})')
    graph.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PowerBI demo - estimated sales impact WoW% (done)

# COMMAND ----------

df_processed = df_exact_ds_store_estimated_impact_weekly.copy()
df_processed = df_processed.groupby(['breakdown', 'calendar_date']).agg({'estimated_sales_without_intervention': 'sum'}).reset_index()
df_processed['period'] = np.where(df_processed['calendar_date']>=coverage_initiation, 'analysis period', 'baseline period')

for breakdown in df_processed['breakdown'].unique():
    df_breakdown = df_processed[df_processed['breakdown']==breakdown]
    graph.figure(figsize=(20, 6))
    sns.lineplot(x='calendar_date', y='estimated_sales_without_intervention', data=df_breakdown, hue='period')
    graph.axvline(pd.to_datetime(coverage_initiation), ymin=0, ymax=df_breakdown['estimated_sales_without_intervention'].max(), color='red', label='intervention')
    graph.legend()
    graph.show()

# COMMAND ----------

import time
time.sleep(7777)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PowerBI demo - test store vs post-fitted sales (done)

# COMMAND ----------

df_matched_store_sales = df_exact_ds_store_estimated_impact_weekly[['breakdown', 'store', 'test', 'calendar_date', 'dollar_sales', 'estimated_sales_without_intervention', 'estimated_sales_with_intervention']].copy()
df_matched_store_sales = df_matched_store_sales.groupby(['breakdown', 'test', 'calendar_date']).agg(
    {
        'store': pd.Series.nunique,
        'dollar_sales': 'sum',
        'estimated_sales_without_intervention': 'sum',
        'estimated_sales_with_intervention': 'sum'
    }
)

df_matched_store_sales = df_matched_store_sales.reset_index()
df_matched_store_sales['calendar_date'] = pd.to_datetime(df_matched_store_sales['calendar_date'])

for breakdown in df_matched_store_sales['breakdown'].unique():
    df_breakdown = df_matched_store_sales[df_matched_store_sales['breakdown']==breakdown]
    x1 = df_breakdown.loc[df_breakdown['test']==True, 'calendar_date']
    y0 = np.repeat(0, len(x1))
    y1 = df_breakdown.loc[df_breakdown['test']==True, 'dollar_sales']
    y2 = df_breakdown.loc[(df_breakdown['test']==False) & (df_breakdown['calendar_date'].isin(x1)), 'dollar_sales']
    y3 = df_breakdown.loc[df_breakdown['test']==True, 'estimated_sales_with_intervention']
    ymax = max((max(y1), max(y3)))*1.2
    
    fig, (ax0) = graph.subplots(nrows=1, ncols=1, sharex=True, figsize=(20, 6))

    ax0.set_title('Test vs post-fitted')
    ax0.plot(x1, y1, label=f"test")
    ax0.fill_between(x1, y0, y1, alpha=0.15)
    ax0.plot(x1, y3, label=f"post-fitted")
    ax0.fill_between(x1, y0, y3, alpha=0.15)
    ax0.set_xlabel('Date')
    ax0.set_ylabel('Aggregated sales ($)')
    ax0.set_ylim(bottom=0, top=ymax)
    ax0.axvline(pd.to_datetime(coverage_initiation), ymin=0, ymax=ymax, color='red', label='intervention')
    ax0.legend()

    
    fig.suptitle(breakdown)
    
    graph.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looking at call data (done)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM acosta_retail_analytics_im.vw_dimension_client 
# MAGIC where lower(description_short) like '%georgia%'
# MAGIC LIMIT 5;

# COMMAND ----------

# # 7.5 mins on largest EXXs_v5 cluster (672 GB memory)
# query = f"""
#     select 
#         distinct response_id,
#         response_type_code,
#         store_tdlinx_id,
#         client_id,
#         item_level_id,
#         call_id,
#         calendar_key,
#         call_planned_date_key,
#         tm_executed_emp_id,
#         um_executed_emp_id,
#         call_complete_status_id,
#         call_completed_date
#     from acosta_retail_analytics_im.vw_fact_question_response 
#     where
#         store_tdlinx_id in {tuple(df_exact_ds_store_estimated_impact_weekly['store'].astype(int).unique())}
#         and call_planned_date_key >= {int(earliest_date.replace('-', ''))}
#         and client_id in (417, 1341)
# """

# df_calls = spark.sql(query).toPandas()
# df_calls.to_csv(f'{path_prefix}/{client}/{run_name}/final/call_data.csv', index=False)

# print(df_calls['calendar_key'].min(), df_calls['calendar_key'].max())
# print(df_calls.shape)
# df_calls.head()

# COMMAND ----------

# How many calls were completed in pre and post
df_calls = pd.read_csv(f'{path_prefix}/{client}/{run_name}/final/call_data.csv')
df_calls_processed = df_calls[df_calls['call_complete_status_id'].isin([1000])].copy()
df_calls_processed['calendar_date'] = pd.to_datetime(df_calls_processed['call_completed_date'])
df_calls_processed['is_post'] = df_calls_processed['calendar_date']>pd.to_datetime(coverage_initiation)

df_agg_calls = df_calls_processed.groupby(['store_tdlinx_id', 'is_post']).agg({'call_id': pd.Series.nunique}).unstack().fillna(0).reset_index()
df_agg_calls.columns = df_agg_calls.columns.droplevel()
df_agg_calls.columns = ['store', 'completed_calls_prior', 'completed_calls_post']

# Merge with coverage data
df_subset = df_exact_ds_store_estimated_impact_weekly[['store', 'breakdown', 'test']].drop_duplicates()
df_subset['store'] = df_subset['store'].astype(int)
df_agg_calls = df_agg_calls.merge(df_subset, on='store')

# Aggregate to sum of calls per test/control
df_agg_calls = df_agg_calls.groupby(['breakdown', 'test']).agg({'completed_calls_prior': 'sum', 'completed_calls_post': 'sum'})

# Add expected
n_days = \
(pd.to_datetime(int(df_calls['calendar_key'].max()), format='%Y%m%d') - pd.to_datetime(coverage_initiation, format='%Y-%m-%d')).days
df_agg_calls['expected_completed_calls_post'] = (df_agg_calls['completed_calls_prior']/365*n_days).astype(int)

df_agg_calls

# COMMAND ----------

# How did the approximate impact trend with the store changes and call activity?
df_agg = df_exact_ds_store_estimated_impact_weekly.copy().sort_values(by='breakdown')
df_agg = df_agg[df_agg['test']==True]
df_agg = df_agg.loc[
    (df_agg['calendar_date']>df_agg['coverage_initiation']) &\
    (df_agg['coverage_initiation'].notna())
].groupby(['breakdown', 'store']).agg({'impact': 'mean'}).rename(columns=({'impact': 'approx impact ($/store/week)'})).reset_index()
df_agg['store'] = df_agg['store'].astype(int)

df_agg_calls = df_calls_processed.copy().rename(columns={'store_tdlinx_id': 'store'})
df_agg_calls = df_agg_calls.merge(df_subset, on='store')
df_agg_calls = df_agg_calls[(df_agg_calls['is_post']==True) & (df_agg_calls['test']==True)]\
    .groupby(['breakdown', 'store', 'calendar_date']).agg({'call_id': pd.Series.nunique}).unstack().fillna(0).reset_index()

df_agg_calls.columns = df_agg_calls.columns.droplevel()
df_agg_calls['completed_calls_post'] = df_agg_calls.sum(axis=1)
df_agg_calls = df_agg_calls.drop(columns=df_agg_calls.filter(regex='\d').columns)
df_agg_calls.columns = ['breakdown', 'store', 'completed_calls_post']
df_agg_calls['completed_calls_post'] = df_agg_calls['completed_calls_post'] - df_agg_calls['store']
df_agg_calls.index.name = 'index'

df_agg = df_agg.merge(df_agg_calls, on=['breakdown', 'store'], how='left').fillna(0).sort_values(by='approx impact ($/store/week)', ascending=False)

graph.figure(figsize=(10, 6))
sns.scatterplot(data=df_agg, x='completed_calls_post', y='approx impact ($/store/week)', hue='breakdown')
graph.legend(bbox_to_anchor=(1, 1))
graph.show()
