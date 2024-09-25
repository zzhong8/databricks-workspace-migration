# Databricks notebook source
# MAGIC %md
# MAGIC ## Problem Statement
# MAGIC
# MAGIC Predicting generated alerts categories.
# MAGIC
# MAGIC There are two classes:
# MAGIC - True positive (alert was executed)
# MAGIC - False positive (alert was not executed / not actionable)

# COMMAND ----------

import datetime
import pandas as pd

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf

import mlflow

from sklearn import metrics
from sklearn.model_selection import cross_val_score, cross_val_predict

from exact_models.boosting import BoostingClassifier

from expectation.functions import get_pos, get_rep_response, prepare_actionable_model_input
from expectation.model import generate_alert_model_path
from acosta.alerting.preprocessing.functions import get_params_from_database_name
from expectation import parse_widget_or_raise

import matplotlib.pyplot as plt
from rosey_graph import plot_precision, plot_recall, plot_roc_curve


plt.style.use('fivethirtyeight')


def cast_to_double(spark_df, col):
    return spark_df.withColumn(col, pyf.col(col).cast('double'))

# COMMAND ----------

dbutils.widgets.text('database_name', 'market6_kroger_danonewave_us_dv', 'Database Name')
dbutils.widgets.text('retailer_id', '', 'Retailer Id')
dbutils.widgets.text('client_id', '', 'Client Id')
dbutils.widgets.text('start_date', '2021-04-01', 'Start Date')

required_int_inputs = ('retailer_id', 'client_id')
int_parsed = [int(parse_widget_or_raise(dbutils.widgets.get(key))) for key in required_int_inputs]
retailer_id, client_id = int_parsed
database_name = str(dbutils.widgets.get('database_name')).strip().lower()
_, country_code, client, retailer = get_params_from_database_name(database_name).values()
start_date = str(dbutils.widgets.get('start_date')).strip().lower()
print(f'client: {client}, country_code: {country_code}, retailer: {retailer}')

# COMMAND ----------

#TODO (Sep-24-2021) save and read raw alerts from table 
def get_raw_alerts_data(start_date, end_date):
    schema = pyt.StructType([
        pyt.StructField('RETAILER_ITEM_ID', pyt.StringType(), True),
        pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.StringType(), True),
        pyt.StructField('ZSCORE', pyt.DoubleType(), True),
        pyt.StructField('Inventory', pyt.DoubleType() , True),
        pyt.StructField('DateGenerated', pyt.DateType(), True),
        pyt.StructField('LastDateIncluded', pyt.DateType(), True),
        pyt.StructField('ChainRefExternal', pyt.StringType(), True),
    ])
    raw_alerts = sqlContext.createDataFrame([], schema=schema)

    date_list = pd.date_range(start=start_date, end= end_date)
    for dt in date_list:
        dt = str(dt.date())
        try:
            df = spark.read.format('csv')\
                .options(header='true', inferSchema='true')\
                .load(f'/mnt/processed/alerting/fieldTest/results/{client}-{country_code}-{retailer}-daily/{dt}')
        except:
            pass
        raw_alerts = raw_alerts.union(df.select(*raw_alerts.columns))

    raw_alerts = raw_alerts.dropDuplicates()
    return raw_alerts

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Data

# COMMAND ----------

# Define start and end date to read all generated alerts in this interval
end = datetime.date.today()
end_date = end.strftime('%Y-%m-%d')

# Read all generated alerts
df_alerts = get_raw_alerts_data(start_date, end_date)

df_alerts = df_alerts.withColumn('DateGenerated', pyf.to_date('DateGenerated'))\
                     .withColumn('LastDateIncluded', pyf.to_date('LastDateIncluded'))
#response date is one day after dategenerated except for Friday which is on monday(3days after dategenerated)
df_alerts = df_alerts.withColumn(
    'RESPONSE_DATE',
    pyf.when(
      pyf.date_format('DateGenerated', 'EEEE') == 'Friday', pyf.date_add(df_alerts['DateGenerated'], 3)
    ).otherwise(pyf.date_add(df_alerts['DateGenerated'], 1))
)

df_alerts = cast_to_double(df_alerts, 'RETAILER_ITEM_ID')

print(f'N alerts: {df_alerts.cache().count():,}')

# COMMAND ----------

# Read POS data
min_date, max_date = df_alerts.select(pyf.min('LastDateIncluded'), pyf.max('LastDateIncluded')).first()
current_date = datetime.date.today()
duration = abs((current_date - min_date).days)
df_pos = get_pos(database_name, spark, n_days=duration + 14)
df_pos = cast_to_double(df_pos, 'RETAILER_ITEM_ID')
print(f'N pos = {df_pos.cache().count():,}')

# COMMAND ----------

# Responded alerts is extracted to LABEL alerts
df_response = get_rep_response(client_id, start_date, end_date, spark=spark)
df_response = df_response.filter(pyf.col('RETAILER_ID') == retailer_id)
df_response = cast_to_double(df_response, 'PRODUCT_ID')
print(f'Number of responded alerts: {df_response.cache().count():,}')

# COMMAND ----------

# Join alert table with response table
df_alerts = df_alerts.join(
    df_response,
    (
        (df_alerts['ORGANIZATION_UNIT_NUM'] == df_response['STORE_ID']) &
        (df_alerts['RETAILER_ITEM_ID'] == df_response['PRODUCT_ID']) &
        (df_alerts['RESPONSE_DATE'] == df_response['CALL_DATE'])
    ),
    how='left'
)
print(f'N results = {df_alerts.cache().count():,}')

# Filter out non responded alerts
df_alerts = df_alerts.dropna(subset='ANSWER')

# Define LABEL based on responses
df_alerts = df_alerts.withColumn(
    'LABEL',
    pyf.when(
        df_alerts['ANSWER'] == 'Issue resolved prior to call', 0
    ).otherwise(1)
)

# Selected required columns for model and rename column names to be consistent to 'prepare_actionable_model_input' function
df_alerts = df_alerts\
    .selectExpr('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'ZSCORE',
             'Inventory as INVENTORY', 'LABEL', 'ChainRefExternal', 
             'LastDateIncluded as DATE', 'DateGenerated as DATE_GENERATED')
df_alerts = df_alerts.withColumn('DAYS', pyf.dayofmonth(df_alerts['DATE_GENERATED']))

print(f'N results = {df_alerts.cache().count():,}')

# COMMAND ----------

# Join alert_response dataframe with pos dataframe
df_final = df_alerts\
.join(
    df_pos,
    (
        (df_alerts['RETAILER_ITEM_ID'] == df_pos['RETAILER_ITEM_ID']) &
        (df_alerts['ChainRefExternal'] == df_pos['ORGANIZATION_UNIT_NUM'])
    ),
    how='left'
)\
.drop(df_pos['RETAILER_ITEM_ID'])\
.drop(df_pos['ORGANIZATION_UNIT_NUM'])

df_final = df_final.withColumn('RETAILER_ITEM_ID', pyf.col('RETAILER_ITEM_ID').cast('string'))
print(f'N results = {df_final.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Train Model

# COMMAND ----------

df_feature, x, y = prepare_actionable_model_input(df_final)
print(f'N total alerts = {df_feature.shape}')
# Fit and save model
with mlflow.start_run():
    catboost_trainer = BoostingClassifier(verbose=False)
    catboost_trainer.fit(x, y)

    auc_cv_score = cross_val_score(catboost_trainer, x, y, cv=5, scoring='roc_auc')
    y_pred_oos = cross_val_predict(catboost_trainer, x, y, cv=5)
    y_pred_proba_oos = cross_val_predict(catboost_trainer, x, y, cv=5, method='predict_proba')

    # Out-of-sample metrics
    roc_auc_total = metrics.roc_auc_score(y, y_pred_proba_oos[:, 1])
    accuracy = metrics.accuracy_score(y, y_pred_oos)

    # Save model
    model_dir = '/dbfs/mnt/artifacts/actionable-alert-models/'
    model_path, model_name = generate_alert_model_path(retailer, client, country_code, model_dir)

    mlflow.log_metrics({
        'roc-auc-oos': roc_auc_total,
        'roc-auc-oos-mean': auc_cv_score.mean(),
        'roc-auc-oos-sd': auc_cv_score.std(),
        'accuracy': accuracy,
    })
    mlflow.log_params({
        'n_trees': catboost_trainer.model_.tree_count_
    })
    mlflow.catboost.log_model(
        catboost_trainer.model_,
        model_name,
        registered_model_name=model_name
    )

    # Easy to access file
    mlflow.catboost.save_model(
        catboost_trainer.model_,
        model_path
    )

# Diagnostics
print(f'Accuracy OOS = {metrics.accuracy_score(y, y_pred_oos):.3f}')
print(f'CV Score : μ = {auc_cv_score.mean():.3f}, σ = {auc_cv_score.std():.3f}')

plot_precision(y_pred_proba_oos, y, show_graph=True)
plot_recall(y_pred_proba_oos, y, show_graph=True)
plot_roc_curve(y_pred_proba_oos[:, 1], y, show_graph=True)
