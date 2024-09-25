# Databricks notebook source
# MAGIC %md
# MAGIC # Estimate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load packages, parameters, and data

# COMMAND ----------

import json
import re
import os
import pickle
import warnings
import arviz as az
import exact.timeseries as ssc
import exact.matching as ssm
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as graph
import exact.matching as ssm
import tensorflow as tf
from tqdm import trange
from sklearn.metrics import r2_score


os.environ["TF_DETERMINISTIC_OPS"] = "1"

# COMMAND ----------

# Get configuration
with open(
    "../../dbfs/mnt/processed/davis/exact/value_measurement_test/current_weekly/config.0.1.0.json", "r"
) as config_fp:
    CONFIG = json.load(config_fp)

# COMMAND ----------

# Required parameters

client = CONFIG["value"]["parameters"]["required"]["client"]
run_name = CONFIG["value"]["parameters"]["required"]["run_name"]
path_prefix = CONFIG["value"]["parameters"]["required"]["path_prefix"]
baseline_start = CONFIG["value"]["parameters"]["optional"]["baseline_start"]
baseline_end = CONFIG["value"]["parameters"]["optional"]["baseline_end"]
transition_start = CONFIG["value"]["parameters"]["optional"]["transition_start"]
transition_end = CONFIG["value"]["parameters"]["optional"]["transition_end"]
analysis_start = CONFIG["value"]["parameters"]["optional"]["analysis_start"]
analysis_end = CONFIG["value"]["parameters"]["optional"]["analysis_end"]
estimator = CONFIG["value"]["parameters"]["required"]["estimator"]

# Optional parameters
advanced_filter_lookup = CONFIG["value"]["parameters"]["optional"]["advanced_filter_lookup"]
number_of_control_stores_used = CONFIG["value"]["parameters"]["optional"][
    "number_of_control_stores_used"
]
coverage_initiation = CONFIG["value"]["parameters"]["optional"]["coverage_initiation"]
approx = CONFIG["value"]["parameters"]["optional"]["approx"]

# COMMAND ----------

# Helper function to get individual r2 values
def get_individual_r2(self):
    import pandas as pd
    from sklearn.metrics import r2_score

    df_individual_r2 = pd.DataFrame(columns=["r_squared"])
    for i, (group_name, df_group) in enumerate(self.df.groupby(self.group_col_name)):
        # Compute the expected y
        df_pre = df_group.query("is_post == False")
        df_post = df_group.query("is_post == True")

        df_group["y_expected"] = np.nan
        df_group.loc[df_group["is_post"] == False, "y_expected"] = df_pre[
            "y_given_pre_expected"
        ]
        df_group.loc[df_group["is_post"] == True, "y_expected"] = df_post[
            "y_given_post_expected"
        ]

        df_individual_r2.loc[group_name] = r2_score(
            df_group["y"], df_group["y_expected"]
        )

    return df_individual_r2.reset_index().rename(columns={'index': 'store'})


# COMMAND ----------

# Getting the outputs of the preceeding matching step
df_breakdown = pd.read_csv(
    f"{path_prefix}/{client}/{run_name}/processing/processed_data.csv",
    parse_dates=["calendar_date"],
    dtype={'store': str}
)
df_breakdown = df_breakdown[["store", "test", "breakdown"]].drop_duplicates()

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_hierarchical.pkl", "rb"
)
dict_hierarchical = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/list_of_matched_control_store_counts.pkl",
    "rb",
)
list_of_matched_control_store_counts = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_prices_control.pkl", "rb"
)
dict_prices_control = pickle.load(filehandler)

# TODO - deprecate this when the new breakdown is successful
table_of_stats = pd.read_csv(
    f"{path_prefix}/{client}/{run_name}/processing/table_of_stats.csv"
).set_index("breakdown")

table_of_results = pd.DataFrame(
    columns=[
        "Baseline",
        "Lift",
        "Lift - lower bound",
        "Lift - upper bound",
        "Impact",
        "P-Value",
        "Clarity Score",
        "Number of Test Stores",
        "Number of Control Stores",
        "Average Post-Fitting R-Squared",
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate

# COMMAND ----------

# Iterate through the estimation
dict_exact_ds_store_estimated_impact_weekly = {}
dict_analysis_hsd = {}

for i, breakdown in enumerate(
    table_of_stats.loc[table_of_stats["valid_breakdown"] == 1].index
):
    df_hierarchical = dict_hierarchical[breakdown]
    coverage_initiation = df_hierarchical['coverage_initiation'].unique()[0] # new
    df_hierarchical["date"] = df_hierarchical["date"].astype(str)

    earliest_coverage = table_of_stats.loc[
        table_of_stats.index == breakdown, "earliest_coverage"
    ].values[0]

    number_of_test_stores_used = df_hierarchical["group"].nunique()
    table_of_stats.loc[
        breakdown, "Number of test stores uesd"
    ] = number_of_test_stores_used

    df_hierarchical["date"] = pd.to_datetime(df_hierarchical["date"])

    num_weeks_by_intervention = df_hierarchical.groupby("is_post")["date"].nunique()
    num_post_intervention_weeks = num_weeks_by_intervention.loc[True]

    if type(number_of_control_stores_used) != int:
        number_of_control_stores_used = 10

    number_of_control_stores_used = min(
        df_hierarchical.drop(columns=["y", "group", "date", "coverage_initiation", "is_post"]).shape[1],
        num_post_intervention_weeks-2,
    )

    table_of_stats.loc[
        breakdown, "Number of control stores (hsd)"
    ] = number_of_control_stores_used

    table_of_stats.loc[breakdown, "Approximate inference?"] = "No"
    if approx:
        table_of_stats.loc[breakdown, "Approximate inference?"] = "Yes"

    table_of_stats.loc[breakdown, "HSD used"] = estimator

    try:
        if estimator == "normal":
            analysis_hsd = ssc.HierarchicalSplitDoorEstimator(
                df_hierarchical,
                group_col_name="group",
                match_col_names=list(range(number_of_control_stores_used)),
                intervention_date=df_hierarchical["coverage_initiation"],
            )
            # Run inference
            analysis_hsd.fit(approx=approx)

        if estimator == "regularized":
            # Only used when the above observes numerical instability
            analysis_hsd = ssc.RegularizedHierarchicalSplitDoorEstimator(
                df_hierarchical,
                cv=3,
                group_col_name="group",
                match_col_names=list(range(number_of_control_stores_used)),
                intervention_date=df_hierarchical["coverage_initiation"],
            )
            # Run inference
            analysis_hsd.fit(approx=approx)
            y_true = analysis_hsd.df[analysis_hsd.df.index > earliest_coverage][
                "y"
            ].values
            y_pred = (
                analysis_hsd.trace[analysis_hsd.trace.index > earliest_coverage]
                .filter(regex="post")
                .mean(axis=1)
                .values
            )
            average_post_fitting_r2 = r2_score(y_true, y_pred)

        df_result = analysis_hsd.results(should_plot=False)

        # 20210608 - Extract the individual r2 values
        df_individual_r2 = get_individual_r2(analysis_hsd)
        # df_individual_r2.to_excel(writer, sheet_name=f"index {i}, r2")
        df_individual_r2.to_csv(
            f"{path_prefix}/{client}/{run_name}/processing/{breakdown}_store_r2_values.csv",
            index=False,
        )

        baseline = df_result.loc["Baseline", "mean"]
        lift = df_result.loc["Lift", "mean"]
        lift_lower_bound = df_result.loc["Lift", "2.5%"]
        lift_upper_bound = df_result.loc["Lift", "97.5%"]
        impact = df_result.loc["CATE", "mean"]

        # 20210726 - to be fixed
        if type(analysis_hsd) == ssc.RegularizedHierarchicalSplitDoorEstimator:
            p_value = analysis_hsd.get_p_value()

        if type(analysis_hsd) == ssc.HierarchicalSplitDoorEstimator:
            p_value = (analysis_hsd.trace["CATE"] < 0).mean()
            average_post_fitting_r2 = analysis_hsd.trace["r2_post"].mean()

        p_value = min(1 - p_value, p_value)
        clarity_score = 1 - p_value

        dict_exact_ds_store_estimated_impact_weekly[breakdown] = analysis_hsd.df.copy()
        dict_analysis_hsd[breakdown] = analysis_hsd

    except:
        baseline = 0
        lift = 0
        lift_lower_bound = 0
        lift_upper_bound = 0
        impact = 0
        p_value = 0
        clarity_score = 0
        number_of_test_stores_used = 0
        number_of_control_stores_used = 0
        average_post_fitting_r2 = 0
        
    

    table_of_results.loc[breakdown] = [
        baseline,
        lift,
        lift_lower_bound,
        lift_upper_bound,
        impact,
        p_value,
        clarity_score,
        number_of_test_stores_used,
        number_of_control_stores_used,
        average_post_fitting_r2,
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format and save data

# COMMAND ----------

table_of_results

# COMMAND ----------

# Format the results
table_of_results.reset_index(inplace=True)
table_of_results = table_of_results.rename(columns={"index": "test_id"})
table_of_results[["subbanner", "current_state", "future_state"]] = table_of_results[
    "test_id"
].str.split(" - ", expand=True)
table_of_results["test_id"] = f"{client}_" + table_of_results[
    "test_id"
].str.lower().replace(" |\-", "_", regex=True).replace("__", "", regex=True)
table_of_results["test_type"] = table_of_results[
    ["current_state", "future_state"]
].apply(lambda x: " -> ".join(x.values.astype(str)), axis=1)
table_of_results["client_name"] = client
table_of_results["run_name"] = run_name
table_of_results["baseline_start"] = baseline_start
table_of_results["baseline_end"] = baseline_end
table_of_results["transition_start"] = transition_start
table_of_results["transition_end"] = transition_end
table_of_results["analysis_start"] = analysis_start
table_of_results["analysis_end"] = analysis_end
control_median_post_intervention_week_count = table_of_stats[
    "control_median_post_intervention_week_count"
].median()
table_of_results[
    "weeks_after_in_store_change"
] = control_median_post_intervention_week_count
table_of_results["estimated_total_impact"] = table_of_results.apply(
    lambda x: x["Impact"]
    * x["Number of Test Stores"]
    * x["weeks_after_in_store_change"],
    axis=1,
)

# TODO replace this temp df
temp_stats = table_of_stats.reset_index()[
    [
        "breakdown",
        "subbanner",
        "current_state",
        "future_state",
        "unique_control_store_count",
    ]
]
temp_stats = temp_stats.rename(
    columns={
        "current_state": "current_state",
        "future_state": "future_state",
        "unique_control_store_count": "Number of Control Stores",
    }
)
table_of_results = table_of_results.drop(columns=["Number of Control Stores"])
table_of_results = table_of_results.merge(
    temp_stats, on=["subbanner", "current_state", "future_state"]
)
display(table_of_results)

# COMMAND ----------

analysis_hsd.df

# COMMAND ----------

# Format the POS data output
df_exact_ds_store_estimated_impact_weekly = pd.concat(
    [
        dict_exact_ds_store_estimated_impact_weekly[key]
        for key in dict_exact_ds_store_estimated_impact_weekly.keys()
    ]
)
df_exact_ds_store_estimated_impact_weekly.reset_index(drop=True, inplace=True)
df_exact_ds_store_estimated_impact_weekly = (
    df_exact_ds_store_estimated_impact_weekly.drop(columns=[*list(range(10))])
)
df_exact_ds_store_estimated_impact_weekly = (
    df_exact_ds_store_estimated_impact_weekly.rename(
        columns={
            "date": "calendar_date",
            "y": "dollar_sales",
            "group": "store",
            "y_given_pre_expected": "estimated_sales_without_intervention",
            "y_given_pre_lb": "estimated_sales_without_intervention_lb",
            "y_given_pre_ub": "estimated_sales_without_intervention_ub",
            "y_given_post_expected": "estimated_sales_with_intervention",
            "y_given_post_lb": "estimated_sales_with_intervention_lb",
            "y_given_post_ub": "estimated_sales_with_intervention_ub",
        }
    )
)


df_exact_ds_store_estimated_impact_weekly["impact"] = (
    df_exact_ds_store_estimated_impact_weekly["estimated_sales_with_intervention"]
    - df_exact_ds_store_estimated_impact_weekly["estimated_sales_without_intervention"]
)
df_exact_ds_store_estimated_impact_weekly["impact_lower_bound"] = (
    df_exact_ds_store_estimated_impact_weekly["estimated_sales_with_intervention_lb"]
    - df_exact_ds_store_estimated_impact_weekly[
        "estimated_sales_without_intervention_lb"
    ]
)
df_exact_ds_store_estimated_impact_weekly["impact_upper_bound"] = (
    df_exact_ds_store_estimated_impact_weekly["estimated_sales_with_intervention_ub"]
    - df_exact_ds_store_estimated_impact_weekly[
        "estimated_sales_without_intervention_lb"
    ]
)

# Add control store sales data to the results
df_pos_control = {}
for breakdown in dict_prices_control.keys():
    df_pos_control[breakdown] = (
        pd.melt(dict_prices_control[breakdown], ignore_index=False)
        .reset_index()
        .rename(columns={"transaction_date": "calendar_date", "value": "dollar_sales"})
    )

df_pos_control = pd.concat(
    [df_pos_control[key] for key in df_pos_control.keys()], axis=0
)

df_exact_ds_store_estimated_impact_weekly = pd.concat(
    [df_pos_control, df_exact_ds_store_estimated_impact_weekly], axis=0
)

# Add periods
df_exact_ds_store_estimated_impact_weekly["period"] = "Baseline period"
df_exact_ds_store_estimated_impact_weekly.loc[
    df_exact_ds_store_estimated_impact_weekly["calendar_date"] >= transition_start,
    "period",
] = "Transition period"
df_exact_ds_store_estimated_impact_weekly.loc[
    df_exact_ds_store_estimated_impact_weekly["calendar_date"] > transition_end,
    "period",
] = "Analysis period"

# Add breakdown and test fields
df_exact_ds_store_estimated_impact_weekly = (
    df_exact_ds_store_estimated_impact_weekly.merge(df_breakdown, on="store")
)

# Dropping duplicates (not sure why) and sorting
df_exact_ds_store_estimated_impact_weekly = (
    df_exact_ds_store_estimated_impact_weekly.drop_duplicates(
        subset=["breakdown", "store", "calendar_date"]
    ).sort_values(by=["breakdown", "store", "calendar_date"])
)
    
# Filtering unnecessary head/tail ends of data
if advanced_filter_lookup:
    df_filter_lookup = pd.read_csv(f'{path_prefix}/{client}/{run_name}/advanced_date_filter.csv')
    for i in df_filter_lookup.index:
        breakdown = df_filter_lookup.loc[i, 'subbanner'] + ' - ' + df_filter_lookup.loc[i, 'current_state'] + ' - ' + df_filter_lookup.loc[i, 'future_state']
        earliest_date = df_filter_lookup.loc[i, 'earliest_date']
        latest_date = df_filter_lookup.loc[i, 'latest_date']
        df_exact_ds_store_estimated_impact_weekly = \
        df_exact_ds_store_estimated_impact_weekly.loc[
            (df_exact_ds_store_estimated_impact_weekly['breakdown']!=breakdown) |\
            (
                (df_exact_ds_store_estimated_impact_weekly['breakdown']==breakdown) &\
                (df_exact_ds_store_estimated_impact_weekly['calendar_date']>=earliest_date) &\
                (df_exact_ds_store_estimated_impact_weekly['calendar_date']<=latest_date)
            )
        ]

        
# Pad store
df_exact_ds_store_estimated_impact_weekly['store'] = df_exact_ds_store_estimated_impact_weekly['store'].str.pad(7,fillchar='0')

df_exact_ds_store_estimated_impact_weekly.head(2)

# COMMAND ----------

# Save the results
table_of_stats.to_csv(
    f"{path_prefix}/{client}/{run_name}/final/table_of_stats.csv", index=True
)
table_of_results.to_csv(
    f"{path_prefix}/{client}/{run_name}/final/exact_ds_results_weekly.csv", index=True
)
df_exact_ds_store_estimated_impact_weekly.to_csv(
    f"{path_prefix}/{client}/{run_name}/final/exact_ds_store_estimated_impact_weekly.csv",
    index=False,
)

filename = f"{path_prefix}/{client}/{run_name}/processing/dict_analysis_hsd.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_analysis_hsd, f, protocol=pickle.HIGHEST_PROTOCOL)
