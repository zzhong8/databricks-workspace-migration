# Databricks notebook source
# MAGIC %md
# MAGIC # Fitting Intervention Measurement Models

# COMMAND ----------

from pprint import pprint

import numpy as np
import pandas as pd

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from causalgraphicalmodels import CausalGraphicalModel

from catboost import CatBoostRegressor

from acosta.alerting.helpers import universal_encoder, universal_decoder
from acosta.measurement import (predictor_columns, forecast_columns, required_columns,
                                intervention_col_label, process_notebook_inputs)

import acosta

print(acosta.__version__)


# COMMAND ----------

# Inputs get required inputs
dbutils.widgets.text('country_id', '-1', 'Country ID')
dbutils.widgets.text('client_id', '-1', 'Client ID')
dbutils.widgets.text('holding_id', '-1', 'Holding ID')
dbutils.widgets.text('banner_id', '-1', 'Banner ID')

input_names = ('country_id', 'client_id', 'holding_id', 'banner_id')

country_id, client_id, holding_id, banner_id = [process_notebook_inputs(dbutils.widgets.get(s)) for s in input_names]

print('Country ID =', country_id)
print('Client ID =', client_id)
print('Holding ID =', holding_id)
print('Banner ID =', banner_id)

# COMMAND ----------

cgm = CausalGraphicalModel(
    nodes=['time of year', 'store', 'display', 'price', 'inventory', 'inventory correction', 'order placement',
           'shelf stock', 'shelf changes', 'placed pos', 'N sold'],
    edges=[
        ('time of year', 'inventory'), ('time of year', 'display'), ('time of year', 'display-store'),
        ('time of year', 'price'), ('time of year', 'N sold'),
        ('store', 'display'), ('store', 'inventory'), ('store', 'price'), ('store', 'N sold'),
        ('display', 'price'), ('display', 'inventory'), ('display', 'N sold'),
        ('price', 'N sold'),
        ('inventory', 'N sold'), ('inventory', 'shelf stock'),
        ('inventory correction', 'inventory'),
        ('order placement', 'inventory'),
        ('shelf stock', 'N sold'),
        ('shelf changes', 'shelf stock'), ('shelf changes', 'N sold'),
        ('placed pos', 'N sold')
    ]
)

pprint(cgm.get_all_backdoor_adjustment_sets('display', 'N sold'))

# COMMAND ----------

# Simplified causal diagram
simple_cgm = CausalGraphicalModel(
    nodes=['time of year', 'store', 'intervention', 'price', 'inventory', 'shelf stock', 'N sold'],
    edges=[
        ('time of year', 'inventory'), ('time of year', 'intervention'), ('time of year', 'price'),
        ('time of year', 'N sold'),
        ('store', 'intervention'), ('store', 'inventory'), ('store', 'price'), ('store', 'N sold'),
        ('intervention', 'price'), ('intervention', 'inventory'), ('intervention', 'N sold'),
        ('intervention', 'shelf stock'),
        ('price', 'N sold'),
        ('inventory', 'N sold'), ('inventory', 'shelf stock'),
        ('shelf stock', 'N sold')
    ]
)

pprint(simple_cgm.get_all_backdoor_adjustment_sets('intervention', 'N sold'))

# COMMAND ----------

#######################
### Load from Cache ###
df_merged = spark.read.format('delta').load(
    f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(f'{df_merged.cache().count():,}')

# COMMAND ----------

display(df_merged)

# COMMAND ----------

print('Intervention Summary Statistics')
df_merged.select('is_intervention').describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Causal Models

# COMMAND ----------

compress_data_schema = pyt.StructType([
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('DATAFRAME_PICKLE', pyt.StringType())
])
compress_data_cols = [col.name for col in compress_data_schema]


def compress_data(df):
    return pd.DataFrame(
        [[
            df['RETAILER_ITEM_ID'].unique()[0],
            universal_encoder(df, compress=True)
        ]],
        columns=compress_data_cols
    )


df_compressed = df_merged.select(required_columns).groupby('RETAILER_ITEM_ID').applyInPandas(
    compress_data,
    schema=compress_data_schema
)

print(f'N items = {df_compressed.cache().count():,}')

# COMMAND ----------

# Perform Measurement
measurement_udf_schema = pyt.StructType([
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.StringType()),
    pyt.StructField('response_id', pyt.StringType()),
    pyt.StructField('call_id', pyt.StringType()),
    pyt.StructField('SALES_DT', pyt.DateType()),
    pyt.StructField('standard_response_cd', pyt.StringType()),
    pyt.StructField('intervention_group', pyt.StringType()),
    pyt.StructField('POS_ITEM_QTY', pyt.FloatType()),
    pyt.StructField('PRICE', pyt.FloatType()),
    pyt.StructField('BASELINE', pyt.FloatType()),
    pyt.StructField('EXPECTED', pyt.FloatType()),
    pyt.StructField('INTERVENTION_EFFECT', pyt.FloatType()),
    pyt.StructField('QINTERVENTION_EFFECT', pyt.FloatType()),
    pyt.StructField('diff_day', pyt.IntegerType()),
])
measurement_udf_cols = [col.name for col in measurement_udf_schema]


def distributed_measurement(compressed_payload):
    from sklearn.model_selection import train_test_split

    compressed_payload = compressed_payload.loc[0, :]  # type: pd.Series

    retailer_item_id = compressed_payload['RETAILER_ITEM_ID']
    data = universal_decoder(compressed_payload['DATAFRAME_PICKLE'], decompress=True)
    min_required_cols = [c for c in required_columns if (c not in forecast_columns) or (c not in predictor_columns)]
    min_required_cols += ['standard_response_cd', 'ORGANIZATION_UNIT_NUM', 'PRICE']
    min_required_cols = list(set(min_required_cols))
    pdf_result = data[min_required_cols].copy()  # type: pd.DataFrame

    try:
        indices_train, indices_val = train_test_split(
            data.index,
            test_size=0.1,
            stratify=data[['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID']]
        )
    except ValueError:
        indices_train, indices_val = train_test_split(
            data.index,
            test_size=0.1
        )

    # Setup hyperparameters
    causal_kwargs = dict(
        iterations=int(10e3),
        learning_rate=0.3,
        loss_function='RMSE',
        cat_features=['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'DOW', intervention_col_label],
        early_stopping_rounds=25,
        use_best_model=True,
        verbose=True,
    )

    # Fit models
    try:
        print('Fitting baseline model')
        baseline_model = CatBoostRegressor(**causal_kwargs)
        baseline_model.fit(
            data.loc[indices_train, predictor_columns], data.loc[indices_train, 'POS_ITEM_QTY'],
            eval_set=(data.loc[indices_val, predictor_columns], data.loc[indices_val, 'POS_ITEM_QTY'])
        )

        print('Fitting expected model')
        expected_model = CatBoostRegressor(**causal_kwargs)
        expected_model.fit(
            data.loc[indices_train, forecast_columns], data.loc[indices_train, 'POS_ITEM_QTY'],
            eval_set=(data.loc[indices_val, forecast_columns], data.loc[indices_val, 'POS_ITEM_QTY'])
        )
    except Exception as e:
        print(f'==> Fitting failed on item = {retailer_item_id} = {e}')
        return pd.DataFrame(columns=measurement_udf_cols)  # Required for pyspark
    else:
        # Make the actually measurement
        pdf_result['EXPECTED'] = np.maximum(
            expected_model.predict(data[forecast_columns]),
            0
        )

        data[intervention_col_label] = 'none'
        pdf_result['BASELINE'] = np.maximum(
            baseline_model.predict(data[predictor_columns]),
            0
        )

        # Intervention effect estimates
        intervention_locations = pdf_result['is_intervention'] == 1
        intervention_effect = pdf_result['EXPECTED'] - pdf_result['BASELINE']
        pdf_result['INTERVENTION_EFFECT'] = 0
        pdf_result.loc[intervention_locations, 'INTERVENTION_EFFECT'] = intervention_effect[intervention_locations]
        pdf_result.loc[~intervention_locations, 'BASELINE'] = pdf_result['EXPECTED'][~intervention_locations]

        # Quasi-intervention estimates
        pdf_result['QINTERVENTION_EFFECT'] = 0
        quasi_intervention_effect = pdf_result['POS_ITEM_QTY'] - pdf_result['BASELINE']
        pdf_result.loc[intervention_locations, 'QINTERVENTION_EFFECT'] = quasi_intervention_effect[intervention_locations]

        # Recovery intervention logic
        y0_day0 = pdf_result.groupby('response_id')['BASELINE'].transform('first')
        y1_day0 = pdf_result.groupby('response_id')['EXPECTED'].transform('first')
        recovery_shift = y0_day0 - y1_day0
        recovery_locs = pdf_result['intervention_group'] == 'recovery'
        recovery = pdf_result['INTERVENTION_EFFECT'] + recovery_shift
        quasi_recovery = pdf_result['QINTERVENTION_EFFECT'] + recovery_shift
        pdf_result.loc[recovery_locs, 'INTERVENTION_EFFECT'] = recovery[recovery_locs]
        pdf_result.loc[recovery_locs, 'QINTERVENTION_EFFECT'] = quasi_recovery[recovery_locs]

        # Done
        pdf_result['RETAILER_ITEM_ID'] = retailer_item_id
        return pdf_result[measurement_udf_cols]


df_results = df_compressed.groupby('RETAILER_ITEM_ID').applyInPandas(
    distributed_measurement,
    schema=measurement_udf_schema
)
print(f'N results = {df_results.cache().count():,}')

# COMMAND ----------

display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Results

# COMMAND ----------

# Write data
df_results.write.format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', True) \
    .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-processed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary
# MAGIC
# MAGIC - Interevention effect plots and estimates
# MAGIC - Diagnostic statistics

# COMMAND ----------

# Create summary
display(df_results.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results and Models

# COMMAND ----------

display(df_results.agg(
    (pyf.sum('INTERVENTION_EFFECT') / pyf.sum('BASELINE')).alias('lift')
))

# COMMAND ----------

display(df_results.filter('standard_response_cd != "none"').agg(
    (pyf.sum('INTERVENTION_EFFECT') / pyf.sum('BASELINE')).alias('lift given intervention')
))
