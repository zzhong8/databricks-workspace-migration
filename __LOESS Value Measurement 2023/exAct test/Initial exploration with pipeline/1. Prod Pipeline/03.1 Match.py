# Databricks notebook source
# MAGIC %md
# MAGIC # Match

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load packages, parameters, and data

# COMMAND ----------

import json
import os
import pickle
import warnings
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as graph
import exact.matching as ssm
import tensorflow as tf
from tqdm import trange

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
tensorflow_random_seed = CONFIG["value"]["parameters"]["required"][
    "tensorflow_random_seed"
]

graph.style.use("fivethirtyeight")
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

tf.config.run_functions_eagerly(True)
tf.data.experimental.enable_debug_mode()

# COMMAND ----------

# Getting the outputs of the preceeding profiling step
filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_prices_test.pkl", "rb"
)
dict_prices_test = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/dict_prices_control.pkl", "rb"
)
dict_prices_control = pickle.load(filehandler)

filehandler = open(
    f"{path_prefix}/{client}/{run_name}/processing/store_coverage_initiation_dict.pkl",
    "rb",
)
store_coverage_initiation_dict = pickle.load(filehandler)

# TODO - improve generalization of breakdown calls beyond just 1
table_of_stats = pd.read_csv(
    f"{path_prefix}/{client}/{run_name}/processing/table_of_stats.csv"
).set_index("breakdown")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match

# COMMAND ----------

# Iterate through the matching
dict_hierarchical = {}
dict_matched_control_mapping = {}
dict_matcher_relative_scores = {}
dict_matcher_relative_indices = {}
dict_matcher_relative_objects = {}
dict_matcher_absolute_scores = {}
dict_matcher_absolute_indices = {}
dict_matcher_absolute_objects = {}


for i, breakdown in enumerate(
    table_of_stats.loc[table_of_stats["valid_breakdown"] == 1].index
):
    list_df_hierarchical = []
    list_of_matched_control_store_counts = []
    df_hierarchical = pd.DataFrame()

    df_prices_control = dict_prices_control[breakdown]
    df_prices_test = dict_prices_test[breakdown]
    earliest_coverage = table_of_stats.loc[
        table_of_stats.index == breakdown, "earliest_coverage"
    ].values[0]

    tf.random.set_seed(tensorflow_random_seed)

    matcher = ssm.EmbeddingModel(
        df_prices_control.loc[:, :earliest_coverage].values,
        epochs=500,
        model_path=f"{path_prefix}/{client}/{run_name}/processing/{breakdown}_abtest.nn",
        verbose=False,
    )

    num_control_stores = min(10, df_prices_control.shape[0])
    list_of_matched_control_store_counts.append(num_control_stores)
    table_of_stats.loc[
        breakdown, "Number of control stores (matched)"
    ] = num_control_stores

    matched_indices, matched_similiarity_scores = matcher.match(
        df_prices_test.loc[:, :earliest_coverage].values, n_matches=num_control_stores
    )

    dict_matcher_relative_indices[breakdown] = pd.DataFrame(
        matched_indices, index=df_prices_test.index
    )
    dict_matcher_relative_scores[breakdown] = pd.DataFrame(
        matched_similiarity_scores, index=df_prices_test.index
    )
    dict_matcher_relative_objects[breakdown] = matcher

    matcher_2 = ssm.PropensityBasedEmbeddingModel(
        df_prices_control.loc[:, :earliest_coverage].values,
        np.zeros(df_prices_control.shape[0]),
        epochs=500,
    )

    matched_indices_2, matched_similiarity_scores_2 = matcher_2.match(
        df_prices_test.loc[:, :earliest_coverage].values, n_matches=num_control_stores
    )

    dict_matcher_absolute_indices[breakdown] = pd.DataFrame(
        matched_indices_2, index=df_prices_test.index
    )
    dict_matcher_absolute_scores[breakdown] = pd.DataFrame(
        matched_similiarity_scores_2, index=df_prices_test.index
    )
    dict_matcher_absolute_objects[breakdown] = matcher_2
    
    # Instantiating matching quality variables
    quantile = 0.95

    # How many matches per control store?
    df_matched_control_counts = pd.DataFrame(
        matched_indices.reshape([-1, 1])
    ).value_counts()
    name = "Distribution of matches per control store"
    df_matching_quality = pd.DataFrame(df_matched_control_counts.describe())
    df_matching_quality.columns = [name]

    matching_threshold = df_matched_control_counts.quantile(quantile)

# TODO - for some reason the plots prevent the for loop from iterating... need to investigate
#     # What are the zero sales and missing sales like for these top matched stores?
#     store_indices = [
#         x[0]
#         for x in df_matched_control_counts[
#             df_matched_control_counts >= matching_threshold
#         ].index
#     ]
#     store_labels = df_prices_control.iloc[store_indices].index

#     

#     TODO - log zero and missing sales statistics
#         name = f'Distribution of zero sales counts in top {(1-quantile)*100: .0f}% control stores'
#         df_matching_quality = \
#         zero_sales[zero_sales['store'].isin(store_labels)].describe()
#         df_matching_quality.columns = [name]

#         name = f'Distribution of missing sales counts in top {(1-quantile)*100: .0f}% control stores'
#         df_matching_quality = \
#         missing_sales[missing_sales['store'].isin(store_labels)].describe()
#         df_matching_quality.columns = [name]

    # Get matched stores list
    df_matched_results = pd.DataFrame(
        np.zeros_like(matched_indices), index=df_prices_test.index
    )
    for i in trange(len(df_matched_results)):
        df_matched_results.iloc[i, :] = df_prices_control.index.values[
            matched_indices[i, :]
        ]

    df_hierarchical_division_banner = []
    data = pd.DataFrame()
    for i in trange(len(df_prices_test)):
        control_data = df_prices_control.iloc[matched_indices[i, :], :].T
        control_data.columns = list(range(len(control_data.columns)))

        test_data = df_prices_test.T
        test_data = test_data[[test_data.columns[i]]]
        test_data.columns = ["y"]
        test_data["group"] = df_prices_test.index[i]
        test_data["date"] = test_data.index

        # Special code for unique intervention dates: Use dictionary to lookup intervention dates.
        test_data["coverage_initiation"] = store_coverage_initiation_dict[
            df_prices_test.index[i]
        ]

        data = pd.concat([test_data, control_data], axis="columns")

        df_hierarchical_division_banner.append(data)
    df_hierarchical_division_banner = pd.concat(df_hierarchical_division_banner)
    df_hierarchical_division_banner["is_post"] = (
        df_hierarchical_division_banner["date"] >= earliest_coverage
    )

    list_df_hierarchical.append(df_hierarchical_division_banner.copy())
    dict_hierarchical[breakdown] = pd.concat(list_df_hierarchical)

    # Adding the mapping for all control to test stores
    dict_matched_control_mapping[breakdown] = pd.DataFrame(
        index=df_prices_test.index, columns=np.arange(num_control_stores)
    )
    for j, test_i in enumerate(dict_matched_control_mapping[breakdown].index):
        dict_matched_control_mapping[breakdown].loc[test_i] = df_prices_control[
            df_prices_control.reset_index().index.isin(matched_indices[j])
        ].index.values


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data

# COMMAND ----------

# Save the results
table_of_stats.to_csv(
    f"{path_prefix}/{client}/{run_name}/processing/table_of_stats.csv", index=True
)

filename = f"{path_prefix}/{client}/{run_name}/processing/dict_hierarchical.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_hierarchical, f, protocol=pickle.HIGHEST_PROTOCOL)

filename = f"{path_prefix}/{client}/{run_name}/processing/list_of_matched_control_store_counts.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(
        list_of_matched_control_store_counts, f, protocol=pickle.HIGHEST_PROTOCOL
    )

filename = f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_relative_scores.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_matcher_relative_scores, f, protocol=pickle.HIGHEST_PROTOCOL)
    
filename = f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_relative_indices.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_matcher_relative_indices, f, protocol=pickle.HIGHEST_PROTOCOL)
    
filename = f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_relative_objects.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_matcher_relative_objects, f, protocol=pickle.HIGHEST_PROTOCOL)

filename = f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_absolute_scores.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_matcher_relative_scores, f, protocol=pickle.HIGHEST_PROTOCOL)
    
filename = f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_absolute_indices.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_matcher_relative_indices, f, protocol=pickle.HIGHEST_PROTOCOL)
    
filename = f"{path_prefix}/{client}/{run_name}/processing/dict_matcher_absolute_objects.pkl"
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_matcher_relative_objects, f, protocol=pickle.HIGHEST_PROTOCOL)

filename = (
    f"{path_prefix}/{client}/{run_name}/processing/dict_matched_control_mapping.pkl"
)
os.makedirs(os.path.dirname(filename), exist_ok=True)
with open(filename, "wb") as f:
    pickle.dump(dict_matched_control_mapping, f, protocol=pickle.HIGHEST_PROTOCOL)
