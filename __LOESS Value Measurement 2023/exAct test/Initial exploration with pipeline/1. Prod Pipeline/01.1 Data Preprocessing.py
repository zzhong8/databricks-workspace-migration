# Databricks notebook source
# MAGIC %md
# MAGIC # Data Preprocessing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load packages, parameters, and data

# COMMAND ----------

import json
import pandas as pd
import numpy as np
from datetime import datetime as dt

from pyspark.sql.functions import col, count, desc, sum

# COMMAND ----------

# path = '../../dbfs/mnt/processed/davis/exact/value_measurement_test/current_weekly/'
# !rm -rf {path} && mkdir -p {path} && cp -r ../../dbfs/mnt/processed/davis/exact/example_client/current_weekly/. {path}

# COMMAND ----------

# Get configuration
with open(
    "../../dbfs/mnt/processed/davis/exact/value_measurement_test/current_weekly/config.0.1.0.json", "r"
) as config_fp:
    CONFIG = json.load(config_fp)

# COMMAND ----------

# Filtering for one item and select interventions
# item_list = ["50273679", "50889726", "51042501"]
item_list = ["50889726"]
# item_list = ["50273679"]
store_count_threshold = 5
cols_to_keep = ["SALES_DT", "ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID", "POS_AMT", "last_day_measured_window_actual", "first_day_measured_window"]
path_interventions = '/mnt/processed/alerting/value_measurement/processed_data/asda_nestlecore_uk_asda/2023-03-07/measurable_IVS/'
path_pos = '/mnt/processed/tara/value_measurement/intervention_join_pos_data/asda_nestlecore_uk_asda'

# COMMAND ----------

# Parameters
database = CONFIG["value"]["parameters"]["required"]["database"]
client = CONFIG["value"]["parameters"]["required"]["client"]
run_name = CONFIG["value"]["parameters"]["required"]["run_name"]
path_prefix = CONFIG["value"]["parameters"]["required"]["path_prefix"]
raw_data_name = CONFIG["value"]["parameters"]["required"]["raw_data_name"]
earliest_date = CONFIG["value"]["parameters"]["optional"]["earliest_date"]
latest_date = CONFIG["value"]["parameters"]["optional"]["latest_date"]
exclude_stores_list = CONFIG["value"]["parameters"]["optional"]["exclude_stores_list"]
if exclude_stores_list:
  exclude_stores_list.append(
      "placeholder"
  )  # can't just submit a list with 1 item apparently...

coverage_initiation = CONFIG["value"]["parameters"]["optional"]["coverage_initiation"]
subbanner_list = CONFIG["value"]["parameters"]["required"]["subbanner_list"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare data for products

# COMMAND ----------

# Example 1: simplest case where the products file has been made already
df_product_metadata = pd.read_csv(f'{path_prefix}/{client}/{run_name}/example_products.csv')

df_product_metadata.head(2)

# COMMAND ----------

# # Example 2: pulling data from rldw and standardizing it as nielsen format to satisfy powerbi ingestion

# # Pulling product metadata (ex. retailer_client=walmart_georgiapacific)

# retailer_client = 'walmart_georgiapacific'

# query = f"""
#     select *
#     from dwr_{retailer_client}_us_im.vw_dim_item
# """

# df_product_metadata = spark.sql(query).toPandas()

# # Format rldw data the standard "exAct on databricks" output
# df_product_metadata = df_product_metadata.rename(columns={'UPC': 'upc'})
# df_product_metadata['description'] = None
# df_product_metadata['tag'] = None
# df_product_metadata['category'] = None
# df_product_metadata['subcategory'] = None
# df_product_metadata['brand'] = None
# df_product_metadata['department'] = None
# df_product_metadata['be_low'] = None
# df_product_metadata['prod_module'] = None
# df_product_metadata['mdm_item_description'] = None
# df_product_metadata['mdm_upc_description'] = None
# df_product_metadata['mdm_brand_description'] = None
# df_product_metadata['mdm_item_size'] = None
# df_product_metadata['mdm_item_uom'] = None
# df_product_metadata['mdm_item_description_short'] = None
# df_product_metadata['mdm_first_shipped_date'] = None
# df_product_metadata['is_product_in_nielsen_data'] = None

# df_product_metadata.head(2)

# COMMAND ----------

# # Example 3: loading client provided upc list and augmenting with nielsen data
# df_products = pd.read_csv(f"{path_prefix}/{client}/{run_name}/suja_provided_upcs.csv", dtype={'upc': str})

# query = f"""
# SELECT
#     *
# FROM
#     nielsenpos_store_sales_im.vw_upc
# WHERE
#     upc in {tuple(df_products['upc'].unique())}
# """

# df_product_metadata = spark.sql(query).toPandas()
# df_product_metadata = df_products.merge(df_product_metadata, on=["upc"], how='left')
# df_product_metadata[f"is_product_in_nielsen_data"] = np.where(df_product_metadata["description"].isna(), False, True)
# df_product_metadata.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare data for in-store changes

# COMMAND ----------

# Example 1: simplest case where the in-store changes file has been made already
df_stores = pd.read_csv(f'{path_prefix}/{client}/{run_name}/example_store_changes_list.csv', dtype={'store_tdlinx_id': str})

df_stores.head(2)

# COMMAND ----------

# # Example 2: pulling data from rldw

# # Load in client provided store changes file
# df_stores = pd.read_csv(f"{path_prefix}/{client}/{run_name}/gp_provided_store_list.csv", dtype={'customer_store_number': str, 'store_tdlinx_id': str}).drop(columns=['Unnamed: 0'])

# # Create current_state, future_state, test, and test_type columns
# df_stores['current_state'] = 'covered'
# df_stores['future_state'] = np.where(df_stores['test_or_control']=='test', 'uncovered', 'covered')
# df_stores['test'] = np.where(df_stores['test_or_control']=='test', True, False)
# df_stores['test_type'] = df_stores['current_state'] + ' - ' + df_stores['future_state']

# # Minor formatting
# df_stores = df_stores.rename(columns={'AIM top 100': 'subbanner', 'postal_code_x': 'postal_code'})
# df_stores['store_tdlinx_id'] = df_stores['store_tdlinx_id'].str.pad(7,fillchar='0')

# # # Optional step - pull store metadata and add to client provided file (not necessary here as it's been provided)
# # query = f"""
# # SELECT
# #     *
# # FROM
# #     nielsenpos_store_sales_im.vw_dimension_store_tdlinx AS t_stores
# # WHERE
# #     t_stores.store_tdlinx_id in {tuple(df_stores['store_tdlinx_id'].unique())}
# #     AND t_stores.active = 1
# # """

# # df_store_metadata = spark.sql(query).toPandas()
# # df_store_metadata['store_tdlinx_id'] = df_store_metadata['store_tdlinx_id'].astype(str)

# # # No need to merge as all the metadata is already present in df_stores
# # df_stores = df_stores.merge(
# #     df_store_metadata.drop(columns=["subbanner_description"]),
# #     on=["store_tdlinx_id"],
# #     how='left'
# # )

# df_stores.head(2)

# COMMAND ----------

# # Example 3:
# # Load in the store coverage data (this was manually created by Eric Bogart after consulting with Suja)
# df_stores = pd.read_excel(
#     f"{path_prefix}/{client}/{run_name}/suja_final_coverage_plans.xlsx",
#     sheet_name="4. Final Plan",
#     usecols=[
#         "store_tdlinx_id",
#         "Take out - SUJA Flagged",
#         "current_state",
#         "future_state",
#         "subbanner_description",
#     ],
#     dtype={'store_tdlinx_id': str}
# )

# # Fixing the future_state and test role
# df_stores["future_state"] = df_stores["future_state"].str.replace(
#     "Zealot", "Acosta Zealot"
# )

# # Setting up the test flag
# df_stores["test"] = np.where(
#     df_stores["current_state"] != df_stores["future_state"], True, False
# )

# # Suja asked for stores to be removed from the list so we'll manually do that here
# df_stores = df_stores.loc[
#     (df_stores["Take out - SUJA Flagged"].isna())
#     & (df_stores["subbanner_description"].isin(subbanner_list)),
#     [
#         "store_tdlinx_id",
#         "test",
#         "subbanner_description",
#         "current_state",
#         "future_state",
#     ],
# ]

# # Formatting
# df_stores['store_tdlinx_id'] = df_stores['store_tdlinx_id'].str.pad(7,fillchar='0')

# # Pulling store metadata
# query = f"""
# SELECT
#     *
# FROM
#     nielsenpos_store_sales_im.vw_dimension_store_tdlinx AS t_stores
# WHERE
#     t_stores.store_tdlinx_id in {tuple(df_stores['store_tdlinx_id'].unique())}
#     AND t_stores.subbanner_description in {tuple(subbanner_list)}
#     AND t_stores.active = 1
# """

# df_store_metadata = spark.sql(query).toPandas()

# # Formatting and merging metadata available
# df_store_metadata['store_tdlinx_id'] = df_store_metadata['store_tdlinx_id'].str.pad(7,fillchar='0')
# df_stores = df_store_metadata.merge(
#     df_stores.drop(columns=["subbanner_description"]), on=["store_tdlinx_id"]
# )
# df_stores["test_type"] = df_stores[
#     ["current_state", "future_state"]
# ].apply(lambda x: " - ".join(x.values.astype(str)), axis=1)

# df_stores.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare data for sales

# COMMAND ----------

# Load the data
df_interventions = spark.read.format('delta').option("inferSchema", "true").option("header", "true").load(path_interventions)

df_test = df_interventions.toPandas()

# Filtering available tests to a specific subset of interventions
df_test = df_test.loc[
    (df_test['standard_response_text']=='display built') &\
    (df_test['rank']==1) &\
    (df_test['epos_retailer_item_id'].isin(item_list)),
    ["epos_organization_unit_num", "epos_retailer_item_id", "first_day_measured_window"]
].drop_duplicates()

df_test = df_test.rename(
    columns={
        "epos_organization_unit_num": "store",
        "epos_retailer_item_id": "item_id",
        "first_day_measured_window": "subbanner"
    }
)

df_test = df_test.groupby(["item_id", "subbanner"]).agg(store_count=("store", pd.Series.nunique)).sort_values(by="store_count", ascending=False)
select_list = df_test[df_test["store_count"]>=store_count_threshold].index
df_test = df_test.loc[select_list].reset_index().drop(columns=["store_count"])
df_test['test'] = True

df_test.head()

# COMMAND ----------

# Load the data

df = spark.read.format('delta').option("inferSchema", "true").option("header", "true").load(path_pos).select(cols_to_keep)

print(f'N rows = {df.cache().count():,}')

# COMMAND ----------

# Viewing top count items
column_name = "RETAILER_ITEM_ID"
unique_counts = df.groupBy(column_name).agg(count("*").alias("count")).orderBy(desc("count"))
unique_counts.show()

# COMMAND ----------

# Creating general store sales while removing the chosen item and filtered dataset
groupby_cols = ["ORGANIZATION_UNIT_NUM", "SALES_DT"]
agg_col = "POS_AMT"

aggregated = df.filter(~(col("RETAILER_ITEM_ID").isin(item_list)))\
    .dropDuplicates([*groupby_cols, agg_col])\
    .groupBy(groupby_cols).agg(sum(agg_col).alias("sum"))

# Show the resulting dataframe with the aggregated values
print(f'N rows in aggregated = {aggregated.count():,}')
aggregated = aggregated.toPandas()

df_filtered = df.filter(
    ~(col("POS_AMT").isNull() | col("ORGANIZATION_UNIT_NUM").isNull() | col("RETAILER_ITEM_ID").isNull() | col("SALES_DT").isNull()) &\
    (col("RETAILER_ITEM_ID").isin(item_list))
)

print(f'N rows in filtered = {df_filtered.count():,}')

df_filtered = df_filtered.toPandas()

# COMMAND ----------

# Process df_filtered and format
df_processed = df_filtered.copy()

max_date = '2024-01-01'

# Formating date columns and renaming
df_processed['SALES_DT'] = pd.to_datetime(df_processed['SALES_DT'], format='%Y-%m-%d')
df_processed["last_day_measured_window_actual"] = df_processed["last_day_measured_window_actual"].astype(str)
df_processed["last_day_measured_window_actual"] = pd.to_datetime(df_processed["last_day_measured_window_actual"], format="%Y-%m-%d")
df_processed = df_processed.rename(
    columns={
        "ORGANIZATION_UNIT_NUM": "store",
        "RETAILER_ITEM_ID": "item_id",
        "SALES_DT": "calendar_date",
        "first_day_measured_window": "subbanner"
    }
)

# Adding the test designation from the interventions dataset
df_processed = df_processed.merge(df_test, on=["item_id", "subbanner"], how="left")
df_processed["test"] = df_processed["test"].fillna(False)

# Creating the subbanner, days elapsed, and coverage_initiation columns
df_processed["subbanner"] = df_processed["subbanner"].astype(str).fillna(max_date)
df_processed["coverage_initiation"] = df_processed["subbanner"].copy()
df_processed["coverage_initiation"] = pd.to_datetime(df_processed["coverage_initiation"], format="%Y-%m-%d")
df_processed["days_elapsed_for_test"] = (df_processed["last_day_measured_window_actual"] - df_processed["coverage_initiation"]).dt.days
df_processed["subbanner"] = df_processed.apply(lambda x: f"item={x['item_id']}, began={x['subbanner']}", axis=1)
df_processed.loc[df_processed["test"]==False, "subbanner"] = "placeholder"

# Since elapsed days is not uniform across stores I've subsetted for stores where they correspond to the median days elapsed
df_elasped_days_filter = df_processed.loc[df_processed["test"]==True, ["subbanner", "test", "store", "days_elapsed_for_test"]].drop_duplicates().groupby(["subbanner"]).agg(
    median_elapsed_days=("days_elapsed_for_test", "median"),
)

df_elasped_days_filter = df_elasped_days_filter.reset_index()

df_processed = df_processed.merge(df_elasped_days_filter, on=["subbanner"], how="left")

# Filtering for subbanners that have sufficient store counts
df_filter = df_processed.loc[
    (df_processed["test"]==True) &\
    (df_processed["median_elapsed_days"]==df_processed["days_elapsed_for_test"]),
    ["subbanner", "test", "store", "days_elapsed_for_test"]
].drop_duplicates().groupby(["subbanner"]).agg(
    median_elapsed_days=("days_elapsed_for_test", "median"),
    store_count=("store", pd.Series.nunique),
).sort_values(by="store_count", ascending=False)
df_filter = df_filter.loc[df_filter["store_count"]>=5].index

df_processed = df_processed[
    ((df_processed["subbanner"].isin(df_filter)) & ((df_processed["median_elapsed_days"]==df_processed["days_elapsed_for_test"]))) |\
    (df_processed["test"]==False)
]

# Renaming subbanner again with the days elapsed
df_processed["subbanner"] = df_processed.apply(lambda x: f"{x['subbanner']}, days measured={x['days_elapsed_for_test'] + 1}", axis=1)

# Filter for data that has sufficient pre and post period
df_processed['is_post'] = df_processed['calendar_date']>=df_processed['coverage_initiation']
df_processed['is_pre'] = df_processed['calendar_date']<df_processed['coverage_initiation']
df_agg = df_processed.groupby(['subbanner', 'test']).agg(is_post=('is_post', 'sum'), is_pre=('is_pre', 'sum'), num_stores=('store', pd.Series.nunique))
df_agg['is_post_per_store'] = df_agg['is_post']/df_agg['num_stores']
df_agg['is_pre_per_store'] = df_agg['is_pre']/df_agg['num_stores']
df_agg = df_agg[(df_agg['is_post_per_store']>0) & (df_agg['is_pre_per_store']>90)] # Assuming daily

df_processed = df_processed[df_processed["subbanner"].isin(df_agg.reset_index()['subbanner'].values)]



# Adding in control stores - Step 1: Copy the dataframe n times
subbanners = df_processed.loc[df_processed["test"]==True, "subbanner"].unique()
df_subset = df_processed.copy()
df_subset = df_subset.loc[df_subset["test"]==False, ["store", "calendar_date", "POS_AMT"]].drop_duplicates()
dfs = [df_subset.copy() for i in range((len(subbanners)))]

# Adding in control stores - Step 2: Concatenate the dataframes and add a new column to identify each copy
for i, df_i in enumerate(dfs):
    df_i["subbanner"] = subbanners[i]
    df_i["last_day_measured_window_actual"] =\
    df_processed.loc[(df_processed["subbanner"]==subbanners[i]) & (df_processed["test"]==True), "last_day_measured_window_actual"].unique()[0]
    df_i["coverage_initiation"] =\
    df_processed.loc[(df_processed["subbanner"]==subbanners[i]) & (df_processed["test"]==True), "coverage_initiation"].unique()[0]
df_controls = pd.concat(dfs)
df_controls["test"] = False

# Filter out excess days for controls
df_controls = df_controls[df_controls["calendar_date"]<=df_controls["last_day_measured_window_actual"]]

df_processed = pd.concat([df_processed, df_controls])

# Filter out excess days
df_processed = df_processed[df_processed["calendar_date"]<=df_processed["last_day_measured_window_actual"]]

# Aggregation
df_processed = df_processed.groupby(["subbanner", "store", "calendar_date", "test", "coverage_initiation"]).agg(dollar_sales=("POS_AMT", 'sum')).reset_index()
df_processed = df_processed.sort_values(by=["store", "calendar_date", "subbanner", "coverage_initiation"])

# Final formatting
df_processed = pd.concat([df_processed[~df_processed["subbanner"].str.contains('placeholder')], df_controls])
df_processed["current_state"] = "no display"
df_processed["future_state"] = np.where(df_processed["test"]==True, "display built", "no display")
df_processed["test_type"] = df_processed[["current_state", "future_state"]].apply(lambda x: " - ".join(x), axis=1)

# Additional filter to remove dates that don't align with tests...
df_processed = df_processed[df_processed["calendar_date"].isin(df_processed.loc[df_processed["test"]==True, "calendar_date"].unique())]

df_processed.head()

# COMMAND ----------

# Pre-profiling check to see if valid tests are visible here
# - There may be differences in "days measured" vs "is_post_per_store" due to gaps in the data
df_processed['is_post'] = df_processed['calendar_date']>=df_processed['coverage_initiation']
df_processed['is_pre'] = df_processed['calendar_date']<df_processed['coverage_initiation']
df_agg = df_processed.groupby(['subbanner', 'test']).agg(is_post=('is_post', 'sum'), is_pre=('is_pre', 'sum'), num_stores=('store', pd.Series.nunique))
df_agg['is_post_per_store'] = df_agg['is_post']/df_agg['num_stores']
df_agg['is_pre_per_store'] = df_agg['is_pre']/df_agg['num_stores']
df_agg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data

# COMMAND ----------

# Format and save the sales data
cols_to_keep = ['subbanner', 'store', 'calendar_date', 'dollar_sales', 'test', 'current_state', 'future_state', 'test_type', 'coverage_initiation']

df_processed[cols_to_keep].to_csv(f"{path_prefix}/{client}/{run_name}/raw/profiling_data_input.csv", index=False)

# Format and save the in-store changes data
cols_to_keep = ['store_tdlinx_id', 'store_id', 'channel_id', 'channel_description',
       'subbanner_id', 'customer_store_number', 'alt_customer_store_number',
       'customer_name', 'store_sqft', 'store_type_id', 'market_id',
       'store_address_1', 'store_city', 'store_state_or_province',
       'state_country_code', 'postal_code', 'active', 'status', 'area_code',
       'phone', 'swklyvol', 'store_latitude', 'store_longitude', 'store_deli',
       'store_bakery', 'store_pharm', 'store_restaurant', 'store_atm',
       'store_bank', 'store_liquor', 'store_wine', 'store_beer', 'store_gas',
       'subbanner_description', 'banner_id', 'banner_description',
       'division_id', 'division_description', 'holdingid',
       'holding_description', 'store_type_description', 'market_description',
       'subregion_id', 'subregion_description', 'region_id', 'country_id',
       'region_description', 'country_description', 'row_id', 'test',
       'current_state', 'future_state', 'test_type']


df_stores[cols_to_keep].to_csv(
    f"{path_prefix}/{client}/{run_name}/final/exact_ds_store_hierarchy.csv", index=False
) 

# Format and save the products data
cols_to_keep = ['upc', 'description', 'tag', 'category', 'subcategory', 'brand',
       'department', 'be_low', 'prod_module', 'mdm_item_description',
       'mdm_upc_description', 'mdm_brand_description', 'mdm_item_size',
       'mdm_item_uom', 'mdm_item_description_short', 'mdm_first_shipped_date',
       'is_product_in_nielsen_data']

df_product_metadata[cols_to_keep].to_csv(
    f"{path_prefix}/{client}/{run_name}/final/exact_ds_product_hierarchy.csv",
    index=False,
)
