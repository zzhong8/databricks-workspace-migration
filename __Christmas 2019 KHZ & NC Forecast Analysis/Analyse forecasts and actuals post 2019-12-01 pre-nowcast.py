# Databricks notebook source
import numpy as np
from pyspark.sql.functions import *

# COMMAND ----------

import numpy as np

EPSILON = 1e-10

def _error(actual: np.ndarray, predicted: np.ndarray):
    """ Simple error """
    return actual - predicted


def _percentage_error(actual: np.ndarray, predicted: np.ndarray):
    """
    Percentage error
    Note: result is NOT multiplied by 100
    """
    return _error(actual, predicted) / (actual + EPSILON)


def _naive_forecasting(actual: np.ndarray, seasonality: int = 1):
    """ Naive forecasting method which just repeats previous samples """
    return actual[:-seasonality]


def _relative_error(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Relative Error """
    if benchmark is None or isinstance(benchmark, int):
        # If no benchmark prediction provided - use naive forecasting
        if not isinstance(benchmark, int):
            seasonality = 1
        else:
            seasonality = benchmark
        return _error(actual[seasonality:], predicted[seasonality:]) /\
               (_error(actual[seasonality:], _naive_forecasting(actual, seasonality)) + EPSILON)

    return _error(actual, predicted) / (_error(actual, benchmark) + EPSILON)


def _bounded_relative_error(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Bounded Relative Error """
    if benchmark is None or isinstance(benchmark, int):
        # If no benchmark prediction provided - use naive forecasting
        if not isinstance(benchmark, int):
            seasonality = 1
        else:
            seasonality = benchmark

        abs_err = np.abs(_error(actual[seasonality:], predicted[seasonality:]))
        abs_err_bench = np.abs(_error(actual[seasonality:], _naive_forecasting(actual, seasonality)))
    else:
        abs_err = np.abs(_error(actual, predicted))
        abs_err_bench = np.abs(_error(actual, benchmark))

    return abs_err / (abs_err + abs_err_bench + EPSILON)


def _geometric_mean(a, axis=0, dtype=None):
    """ Geometric mean """
    if not isinstance(a, np.ndarray):  # if not an ndarray object attempt to convert it
        log_a = np.log(np.array(a, dtype=dtype))
    elif dtype:  # Must change the default dtype allowing array type
        if isinstance(a, np.ma.MaskedArray):
            log_a = np.log(np.ma.asarray(a, dtype=dtype))
        else:
            log_a = np.log(np.asarray(a, dtype=dtype))
    else:
        log_a = np.log(a)
    return np.exp(log_a.mean(axis=axis))


def mse(actual: np.ndarray, predicted: np.ndarray):
    """ Mean Squared Error """
    return np.mean(np.square(_error(actual, predicted)))


def rmse(actual: np.ndarray, predicted: np.ndarray):
    """ Root Mean Squared Error """
    return np.sqrt(mse(actual, predicted))


def nrmse(actual: np.ndarray, predicted: np.ndarray):
    """ Normalized Root Mean Squared Error """
    return rmse(actual, predicted) / (actual.max() - actual.min())


def me(actual: np.ndarray, predicted: np.ndarray):
    """ Mean Error """
    return np.mean(_error(actual, predicted))


def mae(actual: np.ndarray, predicted: np.ndarray):
    """ Mean Absolute Error """
    return np.mean(np.abs(_error(actual, predicted)))


mad = mae  # Mean Absolute Deviation (it is the same as MAE)


def gmae(actual: np.ndarray, predicted: np.ndarray):
    """ Geometric Mean Absolute Error """
    return _geometric_mean(np.abs(_error(actual, predicted)))


def mdae(actual: np.ndarray, predicted: np.ndarray):
    """ Median Absolute Error """
    return np.median(np.abs(_error(actual, predicted)))


def mpe(actual: np.ndarray, predicted: np.ndarray):
    """ Mean Percentage Error """
    return np.mean(_percentage_error(actual, predicted))


def mape(actual: np.ndarray, predicted: np.ndarray):
    """
    Mean Absolute Percentage Error
    Properties:
        + Easy to interpret
        + Scale independent
        - Biased, not symmetric
        - Undefined when actual[t] == 0
    Note: result is NOT multiplied by 100
    """
    return np.mean(np.abs(_percentage_error(actual, predicted)))


def mdape(actual: np.ndarray, predicted: np.ndarray):
    """
    Median Absolute Percentage Error
    Note: result is NOT multiplied by 100
    """
    return np.median(np.abs(_percentage_error(actual, predicted)))


def smape(actual: np.ndarray, predicted: np.ndarray):
    """
    Symmetric Mean Absolute Percentage Error
    Note: result is NOT multiplied by 100
    """
    return np.mean(2.0 * np.abs(actual - predicted) / ((np.abs(actual) + np.abs(predicted)) + EPSILON))


def smdape(actual: np.ndarray, predicted: np.ndarray):
    """
    Symmetric Median Absolute Percentage Error
    Note: result is NOT multiplied by 100
    """
    return np.median(2.0 * np.abs(actual - predicted) / ((np.abs(actual) + np.abs(predicted)) + EPSILON))


def maape(actual: np.ndarray, predicted: np.ndarray):
    """
    Mean Arctangent Absolute Percentage Error
    Note: result is NOT multiplied by 100
    """
    return np.mean(np.arctan(np.abs((actual - predicted) / (actual + EPSILON))))


def mase(actual: np.ndarray, predicted: np.ndarray, seasonality: int = 1):
    """
    Mean Absolute Scaled Error
    Baseline (benchmark) is computed with naive forecasting (shifted by @seasonality)
    """
    return mae(actual, predicted) / mae(actual[seasonality:], _naive_forecasting(actual, seasonality))


def std_ae(actual: np.ndarray, predicted: np.ndarray):
    """ Normalized Absolute Error """
    __mae = mae(actual, predicted)
    return np.sqrt(np.sum(np.square(_error(actual, predicted) - __mae))/(len(actual) - 1))


def std_ape(actual: np.ndarray, predicted: np.ndarray):
    """ Normalized Absolute Percentage Error """
    __mape = mape(actual, predicted)
    return np.sqrt(np.sum(np.square(_percentage_error(actual, predicted) - __mape))/(len(actual) - 1))


def rmspe(actual: np.ndarray, predicted: np.ndarray):
    """
    Root Mean Squared Percentage Error
    Note: result is NOT multiplied by 100
    """
    return np.sqrt(np.mean(np.square(_percentage_error(actual, predicted))))


def rmdspe(actual: np.ndarray, predicted: np.ndarray):
    """
    Root Median Squared Percentage Error
    Note: result is NOT multiplied by 100
    """
    return np.sqrt(np.median(np.square(_percentage_error(actual, predicted))))


def rmsse(actual: np.ndarray, predicted: np.ndarray, seasonality: int = 1):
    """ Root Mean Squared Scaled Error """
    q = np.abs(_error(actual, predicted)) / mae(actual[seasonality:], _naive_forecasting(actual, seasonality))
    return np.sqrt(np.mean(np.square(q)))


def inrse(actual: np.ndarray, predicted: np.ndarray):
    """ Integral Normalized Root Squared Error """
    return np.sqrt(np.sum(np.square(_error(actual, predicted))) / np.sum(np.square(actual - np.mean(actual))))


def rrse(actual: np.ndarray, predicted: np.ndarray):
    """ Root Relative Squared Error """
    return np.sqrt(np.sum(np.square(actual - predicted)) / np.sum(np.square(actual - np.mean(actual))))


def mre(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Mean Relative Error """
    return np.mean(_relative_error(actual, predicted, benchmark))


def rae(actual: np.ndarray, predicted: np.ndarray):
    """ Relative Absolute Error (aka Approximation Error) """
    return np.sum(np.abs(actual - predicted)) / (np.sum(np.abs(actual - np.mean(actual))) + EPSILON)


def mrae(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Mean Relative Absolute Error """
    return np.mean(np.abs(_relative_error(actual, predicted, benchmark)))


def mdrae(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Median Relative Absolute Error """
    return np.median(np.abs(_relative_error(actual, predicted, benchmark)))


def gmrae(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Geometric Mean Relative Absolute Error """
    return _geometric_mean(np.abs(_relative_error(actual, predicted, benchmark)))


def mbrae(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Mean Bounded Relative Absolute Error """
    return np.mean(_bounded_relative_error(actual, predicted, benchmark))


def umbrae(actual: np.ndarray, predicted: np.ndarray, benchmark: np.ndarray = None):
    """ Unscaled Mean Bounded Relative Absolute Error """
    __mbrae = mbrae(actual, predicted, benchmark)
    return __mbrae / (1 - __mbrae)

def mda(actual: np.ndarray, predicted: np.ndarray):
    """ Mean Directional Accuracy """
    return np.mean((np.sign(actual[1:] - actual[:-1]) == np.sign(predicted[1:] - predicted[:-1])).astype(int))

METRICS = {
    'mse': mse,
    'rmse': rmse,
    'nrmse': nrmse,
    'me': me,
    'mae': mae,
    'mad': mad,
    'gmae': gmae,
    'mdae': mdae,
    'mpe': mpe,
    'mape': mape,
    'mdape': mdape,
    'smape': smape,
    'smdape': smdape,
    'maape': maape,
    'mase': mase,
    'std_ae': std_ae,
    'std_ape': std_ape,
    'rmspe': rmspe,
    'rmdspe': rmdspe,
    'rmsse': rmsse,
    'inrse': inrse,
    'rrse': rrse,
    'mre': mre,
    'rae': rae,
    'mrae': mrae,
    'mdrae': mdrae,
    'gmrae': gmrae,
    'mbrae': mbrae,
    'umbrae': umbrae,
    'mda': mda,
}

# def evaluate(actual: np.ndarray, predicted: np.ndarray, metrics=('mape', 'mdape', 'smape', 'smdape', 'maape')):
def evaluate(actual: np.ndarray, predicted: np.ndarray, metrics=('mape', 'mdape', 'maape')):
    results = {}
    for name in metrics:
        try:
            results[name] = METRICS[name](actual, predicted)
        except Exception as err:
            results[name] = np.nan
            print('Unable to compute metric {0}: {1}'.format(name, err))
    return results

# def evaluate_all(actual: np.ndarray, predicted: np.ndarray):
#     return evaluate(actual, predicted, metrics=set(METRICS.keys()))

# COMMAND ----------

def mean_absolute_percentage_error(y_true, y_pred): 
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    
    return mape(y_true, y_pred)

# COMMAND ----------

def evaluate_all(y_true, y_pred):
    actual, predicted = np.array(y_true), np.array(y_pred)
  
    return evaluate(actual, predicted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Asda Kraftheinz

# COMMAND ----------

PATH_RESULTS_INPUT1 = '/mnt/artifacts/hugh/asda-kraftheinz-post-2019-12-01-sales-old-predictions'

# COMMAND ----------

df1 = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT1)

# COMMAND ----------

df1.columns

# COMMAND ----------

display(df1)

# COMMAND ----------

df1_non_zero = df1.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

results_one_store1 = evaluate_all(np.asarray(df1.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df1.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_one_store1)

# COMMAND ----------

results_one_store_non_zero1 = evaluate_all(np.asarray(df1_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df1_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_one_store_non_zero1)

# COMMAND ----------

df1_agg = df1.groupBy(['RETAILER_ITEM_ID', 'SALES_DT']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM'),
                                                            sum('BASELINE_POS_ITEM_QTY').alias('BASELINE_POS_ITEM_QTY'),
                                                            sum('pos_item_qty').alias('POS_ITEM_QTY'),
                                                            sum('pos_amt').alias('POS_AMT'),
                                                            sum('on_hand_inventory_qty').alias('ON_HAND_INVENTORY_QTY')
                                                           ).orderBy(['RETAILER_ITEM_ID', 'SALES_DT'], ascending=True)

# COMMAND ----------

df1_retailer_item_desc = df1.groupBy(['RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM')).drop('TOTAL_STORES_SELLING_ITEM')

# COMMAND ----------

df1_top_selling_items = df1_agg.groupBy(['RETAILER_ITEM_ID']).agg(
  sum('POS_ITEM_QTY').alias('TOTAL_SALES')).orderBy(
  ['TOTAL_SALES'], ascending=False).limit(10)

# COMMAND ----------

df1_agg_top_selling_items = df1_agg.join(df1_top_selling_items, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

display(df1_top_selling_items)

# COMMAND ----------

df1_agg_non_zero = df1_agg.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

display(df1_agg_non_zero)

# COMMAND ----------

results1 = evaluate_all(np.asarray(df1_agg.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df1_agg.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results1)

# COMMAND ----------

results_non_zero1 = evaluate_all(np.asarray(df1_agg_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df1_agg_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_non_zero1)

# COMMAND ----------

df1_agg_top_selling_items_desc = df1_agg_top_selling_items.join(df1_retailer_item_desc, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

results_top_selling_items1 = evaluate_all(np.asarray(df1_agg_top_selling_items.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df1_agg_top_selling_items.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_top_selling_items1)

# COMMAND ----------

df1_agg.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-kraftheinz-post-2019-12-01-sales-old-predictions-agg')

# COMMAND ----------

df1_agg_top_selling_items_desc.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-kraftheinz-agg-top-selling-items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Asda NestleCereals

# COMMAND ----------

PATH_RESULTS_INPUT2 = '/mnt/artifacts/hugh/asda-nestlecereals-post-2019-12-01-sales-new-predictions'

df2 = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT2)

# COMMAND ----------

df2_non_zero = df2.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

results_one_store2 = evaluate_all(np.asarray(df2.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df2.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_one_store2)

# COMMAND ----------

results_one_store_non_zero2 = evaluate_all(np.asarray(df2_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df2_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_one_store_non_zero2)

# COMMAND ----------

df2_retailer_item_desc = df2.groupBy(['RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM')).drop('TOTAL_STORES_SELLING_ITEM')

# COMMAND ----------

df2_agg = df2.groupBy(['RETAILER_ITEM_ID', 'SALES_DT']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM'),
                                                            sum('BASELINE_POS_ITEM_QTY').alias('BASELINE_POS_ITEM_QTY'),
                                                            sum('pos_item_qty').alias('POS_ITEM_QTY'),
                                                            sum('pos_amt').alias('POS_AMT'),
                                                            sum('on_hand_inventory_qty').alias('ON_HAND_INVENTORY_QTY')
                                                           ).orderBy(['RETAILER_ITEM_ID', 'SALES_DT'], ascending=True)

df2_agg_non_zero = df2_agg.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

df2_top_selling_items = df2_agg.groupBy(['RETAILER_ITEM_ID']).agg(
  sum('POS_ITEM_QTY').alias('TOTAL_SALES')).orderBy(
  ['TOTAL_SALES'], ascending=False).limit(10)

df2_agg_top_selling_items = df2_agg.join(df2_top_selling_items, ['RETAILER_ITEM_ID'], 'inner')

df2_agg_top_selling_items_desc = df2_agg_top_selling_items.join(df2_retailer_item_desc, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

results2 = evaluate_all(np.asarray(df2_agg.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df2_agg.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results2)

# COMMAND ----------

results_non_zero2 = evaluate_all(np.asarray(df2_agg_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df2_agg_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_non_zero2)

# COMMAND ----------

results_top_selling_items2 = evaluate_all(np.asarray(df2_agg_top_selling_items.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df2_agg_top_selling_items.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_top_selling_items2)

# COMMAND ----------

df2_agg.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-nestlecereals-post-2019-12-01-sales-new-predictions-agg')

# COMMAND ----------

df2_agg_top_selling_items_desc.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-nestlecereals-agg-top-selling-items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Tesco Kraftheinz

# COMMAND ----------

PATH_RESULTS_INPUT3 = '/mnt/artifacts/hugh/tesco-kraftheinz-post-2019-12-01-sales-old-predictions'

df3 = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT3)

# COMMAND ----------

df3_retailer_item_desc = df3.groupBy(['RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM')).drop('TOTAL_STORES_SELLING_ITEM')

# COMMAND ----------

df3_agg = df3.groupBy(['RETAILER_ITEM_ID', 'SALES_DT']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM'),
                                                            sum('BASELINE_POS_ITEM_QTY').alias('BASELINE_POS_ITEM_QTY'),
                                                            sum('pos_item_qty').alias('POS_ITEM_QTY'),
                                                            sum('pos_amt').alias('POS_AMT'),
                                                            sum('on_hand_inventory_qty').alias('ON_HAND_INVENTORY_QTY')
                                                           ).orderBy(['RETAILER_ITEM_ID', 'SALES_DT'], ascending=True)

df3_agg_non_zero = df3_agg.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

df3_top_selling_items = df3_agg.groupBy(['RETAILER_ITEM_ID']).agg(
  sum('POS_ITEM_QTY').alias('TOTAL_SALES')).orderBy(
  ['TOTAL_SALES'], ascending=False).limit(10)

df3_agg_top_selling_items = df3_agg.join(df3_top_selling_items, ['RETAILER_ITEM_ID'], 'inner')

df3_agg_top_selling_items_desc = df3_agg_top_selling_items.join(df3_retailer_item_desc, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

results3 = evaluate_all(np.asarray(df3_agg.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df3_agg.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results3)

# COMMAND ----------

results_non_zero3 = evaluate_all(np.asarray(df3_agg_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df3_agg_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_non_zero3)

# COMMAND ----------

results_top_selling_items3 = evaluate_all(np.asarray(df3_agg_top_selling_items.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df3_agg_top_selling_items.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_top_selling_items3)

# COMMAND ----------

df3_agg.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-kraftheinz-post-2019-12-01-sales-old-predictions-agg')

# COMMAND ----------

df3_agg_top_selling_items_desc.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-kraftheinz-agg-top-selling-items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Tesco Nestlecereals

# COMMAND ----------

PATH_RESULTS_INPUT4 = '/mnt/artifacts/hugh/tesco-nestlecereals-post-2019-12-01-sales-old-predictions'

df4 = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT4)

# COMMAND ----------

df4_retailer_item_desc = df4.groupBy(['RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM')).drop('TOTAL_STORES_SELLING_ITEM')

# COMMAND ----------

df4_agg = df4.groupBy(['RETAILER_ITEM_ID', 'SALES_DT']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM'),
                                                            sum('BASELINE_POS_ITEM_QTY').alias('BASELINE_POS_ITEM_QTY'),
                                                            sum('pos_item_qty').alias('POS_ITEM_QTY'),
                                                            sum('pos_amt').alias('POS_AMT'),
                                                            sum('on_hand_inventory_qty').alias('ON_HAND_INVENTORY_QTY')
                                                           ).orderBy(['RETAILER_ITEM_ID', 'SALES_DT'], ascending=True)

df4_agg_non_zero = df4_agg.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

df4_top_selling_items = df4_agg.groupBy(['RETAILER_ITEM_ID']).agg(
  sum('POS_ITEM_QTY').alias('TOTAL_SALES')).orderBy(
  ['TOTAL_SALES'], ascending=False).limit(10)

df4_agg_top_selling_items = df4_agg.join(df4_top_selling_items, ['RETAILER_ITEM_ID'], 'inner')

df4_agg_top_selling_items_desc = df4_agg_top_selling_items.join(df4_retailer_item_desc, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

results4 = evaluate_all(np.asarray(df4_agg.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df4_agg.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results4)

# COMMAND ----------

results_non_zero4 = evaluate_all(np.asarray(df4_agg_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df4_agg_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_non_zero4)

# COMMAND ----------

results_top_selling_items4 = evaluate_all(np.asarray(df4_agg_top_selling_items.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df4_agg_top_selling_items.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_top_selling_items4)

# COMMAND ----------

df4_agg.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-nestlecereals-post-2019-12-01-sales-old-predictions-agg')

# COMMAND ----------

df4_agg_top_selling_items_desc.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-nestlecereals-agg-top-selling-items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Morrisons Kraftheinz

# COMMAND ----------

PATH_RESULTS_INPUT5 = '/mnt/artifacts/hugh/morrisons-kraftheinz-post-2019-10-15-sales-old-predictions'

df5 = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT5)

# COMMAND ----------

df5_retailer_item_desc = df5.groupBy(['RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM')).drop('TOTAL_STORES_SELLING_ITEM')

# COMMAND ----------

df5_agg = df5.groupBy(['RETAILER_ITEM_ID', 'SALES_DT']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM'),
                                                            sum('BASELINE_POS_ITEM_QTY').alias('BASELINE_POS_ITEM_QTY'),
                                                            sum('pos_item_qty').alias('POS_ITEM_QTY'),
                                                            sum('pos_amt').alias('POS_AMT'),
                                                            sum('on_hand_inventory_qty').alias('ON_HAND_INVENTORY_QTY')
                                                           ).orderBy(['RETAILER_ITEM_ID', 'SALES_DT'], ascending=True)

df5_agg_non_zero = df5_agg.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

df5_top_selling_items = df5_agg.groupBy(['RETAILER_ITEM_ID']).agg(
  sum('POS_ITEM_QTY').alias('TOTAL_SALES')).orderBy(
  ['TOTAL_SALES'], ascending=False).limit(10)

df5_agg_top_selling_items = df5_agg.join(df5_top_selling_items, ['RETAILER_ITEM_ID'], 'inner')

df5_agg_top_selling_items_desc = df5_agg_top_selling_items.join(df5_retailer_item_desc, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

results5 = evaluate_all(np.asarray(df5_agg.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df5_agg.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results5)

# COMMAND ----------

results_non_zero5 = evaluate_all(np.asarray(df5_agg_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df5_agg_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_non_zero5)

# COMMAND ----------

results_top_selling_items5 = evaluate_all(np.asarray(df5_agg_top_selling_items.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df5_agg_top_selling_items.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_top_selling_items5)

# COMMAND ----------

df5_agg.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/morrisons-kraftheinz-post-2019-10-15-sales-old-predictions-agg')

# COMMAND ----------

df5_agg_top_selling_items_desc.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/morrisons-kraftheinz-agg-top-selling-items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Morrisons Nestlecereals

# COMMAND ----------

PATH_RESULTS_INPUT6 = '/mnt/artifacts/hugh/morrisons-nestlecereals-post-2019-12-01-sales-old-predictions'

df6 = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT6)

# COMMAND ----------

df6_retailer_item_desc = df6.groupBy(['RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM')).drop('TOTAL_STORES_SELLING_ITEM')

# COMMAND ----------

df6_agg = df6.groupBy(['RETAILER_ITEM_ID', 'SALES_DT']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM'),
                                                            sum('BASELINE_POS_ITEM_QTY').alias('BASELINE_POS_ITEM_QTY'),
                                                            sum('pos_item_qty').alias('POS_ITEM_QTY'),
                                                            sum('pos_amt').alias('POS_AMT'),
                                                            sum('on_hand_inventory_qty').alias('ON_HAND_INVENTORY_QTY')
                                                           ).orderBy(['RETAILER_ITEM_ID', 'SALES_DT'], ascending=True)

df6_agg_non_zero = df6_agg.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

df6_top_selling_items = df6_agg.groupBy(['RETAILER_ITEM_ID']).agg(
  sum('POS_ITEM_QTY').alias('TOTAL_SALES')).orderBy(
  ['TOTAL_SALES'], ascending=False).limit(10)

df6_agg_top_selling_items = df6_agg.join(df6_top_selling_items, ['RETAILER_ITEM_ID'], 'inner')

df6_agg_top_selling_items_desc = df6_agg_top_selling_items.join(df6_retailer_item_desc, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

results6 = evaluate_all(np.asarray(df6_agg.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df6_agg.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results6)

# COMMAND ----------

results_non_zero6 = evaluate_all(np.asarray(df6_agg_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df6_agg_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_non_zero6)

# COMMAND ----------

results_top_selling_items6 = evaluate_all(np.asarray(df6_agg_top_selling_items.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df6_agg_top_selling_items.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_top_selling_items6)

# COMMAND ----------

df6_agg.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/morrisons-nestlecereals-post-2019-12-01-sales-old-predictions-agg')

# COMMAND ----------

df6_agg_top_selling_items_desc.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/morrisons-nestlecereals-agg-top-selling-items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Sainsburys Kraftheinz

# COMMAND ----------

PATH_RESULTS_INPUT7 = '/mnt/artifacts/hugh/sainsburys-kraftheinz-post-2019-12-01-sales-old-predictions'

df7 = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT7)

# COMMAND ----------

df7_retailer_item_desc = df7.groupBy(['RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM')).drop('TOTAL_STORES_SELLING_ITEM')

# COMMAND ----------

df7_agg = df7.groupBy(['RETAILER_ITEM_ID', 'SALES_DT']).agg(count('*').alias('TOTAL_STORES_SELLING_ITEM'),
                                                            sum('BASELINE_POS_ITEM_QTY').alias('BASELINE_POS_ITEM_QTY'),
                                                            sum('pos_item_qty').alias('POS_ITEM_QTY'),
                                                            sum('pos_amt').alias('POS_AMT'),
                                                            sum('on_hand_inventory_qty').alias('ON_HAND_INVENTORY_QTY')
                                                           ).orderBy(['RETAILER_ITEM_ID', 'SALES_DT'], ascending=True)

df7_agg_non_zero = df7_agg.where(col('POS_ITEM_QTY') > 0)

# COMMAND ----------

df7_top_selling_items = df7_agg.groupBy(['RETAILER_ITEM_ID']).agg(
  sum('POS_ITEM_QTY').alias('TOTAL_SALES')).orderBy(
  ['TOTAL_SALES'], ascending=False).limit(10)

df7_agg_top_selling_items = df7_agg.join(df7_top_selling_items, ['RETAILER_ITEM_ID'], 'inner')

df7_agg_top_selling_items_desc = df7_agg_top_selling_items.join(df7_retailer_item_desc, ['RETAILER_ITEM_ID'], 'inner')

# COMMAND ----------

results7 = evaluate_all(np.asarray(df7_agg.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df7_agg.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results7)

# COMMAND ----------

results_non_zero7 = evaluate_all(np.asarray(df7_agg_non_zero.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df7_agg_non_zero.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_non_zero7)

# COMMAND ----------

results_top_selling_items7 = evaluate_all(np.asarray(df7_agg_top_selling_items.select('POS_ITEM_QTY').collect()).astype(np.float), np.asarray(df7_agg_top_selling_items.select('BASELINE_POS_ITEM_QTY').collect()).astype(np.float))

print(results_top_selling_items7)

# COMMAND ----------

df7_agg.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/sainsburys-kraftheinz-post-2019-12-01-sales-old-predictions-agg')

# COMMAND ----------

df7_agg_top_selling_items_desc.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/sainsburys-kraftheinz-agg-top-selling-items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Sainsburys Nestlecereals

# COMMAND ----------

# MAGIC %md
# MAGIC N/A as sainsburys nestlecereals does not have any data post Oct 15
