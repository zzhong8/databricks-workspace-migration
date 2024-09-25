# Databricks notebook source
from expectation.functions import get_pos_prod
import pyspark.sql.functions as pyf
import matplotlib.pyplot as plt
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC desc retaillink_asda_beiersdorf_uk_dv.vw_latest_sat_retailer_item_unit_price

# COMMAND ----------

df_num_item_unit_prices_asda_beiersdorf_uk = spark.sql('''select sales_dt, count(*) num_rows, sum(unit_price_amt) unit_price_amt
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , unit_price_amt
      from retaillink_asda_beiersdorf_uk_dv.vw_latest_sat_retailer_item_unit_price) z
where z.sales_dt >= '2023-07-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_item_unit_prices_asda_beiersdorf_uk)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC desc team_retail_alert_beiersdorf_drt_uk_im.alert_inventory_cleanup

# COMMAND ----------

df_num_osa_alerts_asda_beiersdorf_uk = spark.sql('''select sales_dt, count(*) num_alerts, sum(LOST_SALES_AMT) LOST_SALES_AMT
from (select
      Retail_Client
      , HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , LOST_SALES_AMT
      from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability) z
where z.sales_dt >= '2023-07-01'
and z.Retail_Client = 'asda_beiersdorf_uk'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_osa_alerts_asda_beiersdorf_uk)

# COMMAND ----------

df_lsv= spark.sql('''select * from retail_alert_morrisons_kraftheinz_uk_im.vw_latest_lost_sales_value where SALES_DT >= '2024-01-01'
order by SALES_DT''')
display(df_lsv)

# COMMAND ----------

df_lsv_asda_beiersdorf_uk = spark.sql('''select sales_dt, sum(LOST_SALES_AMT) lsv
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , SALES_DT
      , LOST_SALES_AMT
      , ROW_NUMBER() over(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK order by AUDIT_CREATE_TS desc) rnum from retail_alert_asda_beiersdorf_uk_im.vw_latest_lost_sales_value) z

where 
rnum = 1
AND sales_dt >= '2023-07-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_lsv_asda_beiersdorf_uk)

# COMMAND ----------

df_base_unit_asda_beiersdorf_uk = spark.sql('''select sales_dt, sum(baseline_pos_item_qty) BU
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , SALES_DT
      , BASELINE_POS_ITEM_QTY
      , ROW_NUMBER() over(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK order by AUDIT_CREATE_TS desc) rnum from retail_alert_asda_beiersdorf_uk_im.drfe_forecast_baseline_unit) z

where 
rnum = 1
AND sales_dt >= '2023-07-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_base_unit_asda_beiersdorf_uk)

# COMMAND ----------

df_num_osa_alerts_boots_beiersdorf_uk = spark.sql('''select sales_dt, count(*) num_alerts, sum(LOST_SALES_AMT) LOST_SALES_AMT
from (select
      Retail_Client
      , HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , LOST_SALES_AMT
      from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability) z
where z.sales_dt >= '2023-07-01'
and z.Retail_Client = 'boots_beiersdorf_uk'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_osa_alerts_boots_beiersdorf_uk)

# COMMAND ----------

df_base_unit_boots_beiersdorf_uk = spark.sql('''select sales_dt, sum(baseline_pos_item_qty) BU
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , SALES_DT
      , BASELINE_POS_ITEM_QTY
      , ROW_NUMBER() over(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK order by AUDIT_CREATE_TS desc) rnum from retail_alert_boots_beiersdorf_uk_im.drfe_forecast_baseline_unit) z

where 
rnum = 1
AND sales_dt >= '2023-07-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_base_unit_boots_beiersdorf_uk)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC       Retail_Client
# MAGIC       , HUB_ORGANIZATION_UNIT_HK
# MAGIC       , HUB_RETAILER_ITEM_HK
# MAGIC       , ORGANIZATION_UNIT_NUM
# MAGIC       , RETAILER_ITEM_ID
# MAGIC       , SALES_DT
# MAGIC       , LOST_SALES_AMT
# MAGIC       from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
# MAGIC       where SALES_DT >= '2024-04-18'

# COMMAND ----------

df_num_osa_alerts_morrisons_beiersdorf_uk = spark.sql('''select sales_dt, count(*) num_alerts, sum(LOST_SALES_AMT) LOST_SALES_AMT
from (select
      Retail_Client
      , HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , LOST_SALES_AMT
      from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability) z
where z.sales_dt >= '2023-07-01'
and z.Retail_Client = 'morrisons_beiersdorf_uk'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_osa_alerts_morrisons_beiersdorf_uk)

# COMMAND ----------

df_base_unit_morrisons_beiersdorf_uk = spark.sql('''select sales_dt, sum(baseline_pos_item_qty) BU
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , SALES_DT
      , BASELINE_POS_ITEM_QTY
      , ROW_NUMBER() over(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK order by AUDIT_CREATE_TS desc) rnum from retail_alert_morrisons_beiersdorf_uk_im.drfe_forecast_baseline_unit) z

where 
rnum = 1
AND sales_dt >= '2023-07-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_base_unit_morrisons_beiersdorf_uk)

# COMMAND ----------

df_num_osa_alerts_tesco_beiersdorf_uk = spark.sql('''select sales_dt, count(*) num_alerts, sum(LOST_SALES_AMT) LOST_SALES_AMT
from (select
      Retail_Client
      , HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , LOST_SALES_AMT
      from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability) z
where z.sales_dt >= '2023-07-01'
and z.Retail_Client = 'tesco_beiersdorf_uk'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_osa_alerts_tesco_beiersdorf_uk)

# COMMAND ----------

df_base_unit_tesco_beiersdorf_uk = spark.sql('''select sales_dt, sum(baseline_pos_item_qty) BU
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , SALES_DT
      , BASELINE_POS_ITEM_QTY
      , ROW_NUMBER() over(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK order by AUDIT_CREATE_TS desc) rnum from retail_alert_tesco_beiersdorf_uk_im.drfe_forecast_baseline_unit) z

where 
rnum = 1
AND sales_dt >= '2023-07-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_base_unit_tesco_beiersdorf_uk)

# COMMAND ----------

df_num_item_unit_prices_morrisons_kraftheinz_uk = spark.sql('''select sales_dt, count(*) num_rows, sum(pos_item_qty) pos_item_qty, sum(pos_amt) pos_amt
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , pos_item_qty
      , pos_amt
      from msd_morrisons_kraftheinz_uk_dv.vw_latest_sat_epos_summary) z
where z.sales_dt >= '2024-01-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_item_unit_prices_morrisons_kraftheinz_uk)

# COMMAND ----------

df_num_item_unit_prices_morrisons_kraftheinz_uk = spark.sql('''select sales_dt, count(*) num_rows, sum(unit_price_amt) unit_price_amt
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , unit_price_amt
      from msd_morrisons_kraftheinz_uk_dv.vw_latest_sat_retailer_item_unit_price) z
where z.sales_dt >= '2020-07-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_item_unit_prices_morrisons_kraftheinz_uk)

# COMMAND ----------

df_num_osa_alerts_morrisons_kraftheinz_uk = spark.sql('''select sales_dt, count(*) num_alerts, sum(LOST_SALES_AMT) LOST_SALES_AMT
from (select
      Retail_Client
      , HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , ORGANIZATION_UNIT_NUM
      , RETAILER_ITEM_ID
      , SALES_DT
      , LOST_SALES_AMT
      from team_retail_alert_kraftheinz_drt_uk_im.alert_on_shelf_availability) z
where z.sales_dt >= '2024-01-01'
and z.Retail_Client = 'morrisons_kraftheinz_uk'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_num_osa_alerts_morrisons_kraftheinz_uk)

# COMMAND ----------

df_base_unit_morrisons_kraftheinz_uk = spark.sql('''select sales_dt, sum(baseline_pos_item_qty) BU
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , SALES_DT
      , BASELINE_POS_ITEM_QTY
      , ROW_NUMBER() over(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK order by AUDIT_CREATE_TS desc) rnum from retail_alert_morrisons_kraftheinz_uk_im.drfe_forecast_baseline_unit) z

where 
rnum = 1
AND sales_dt >= '2024-01-01'
group by z.SALES_DT
order by z.SALES_DT''')

display(df_base_unit_morrisons_kraftheinz_uk)

# COMMAND ----------

df_lsv= spark.sql('''select * from retail_alert_morrisons_kraftheinz_uk_im.vw_latest_lost_sales_value where SALES_DT >= '2024-01-01'
order by SALES_DT''')
display(df_lsv)

# COMMAND ----------

df_base_unit = spark.sql('''select *
from (select
      HUB_ORGANIZATION_UNIT_HK
      , HUB_RETAILER_ITEM_HK
      , SALES_DT
      , BASELINE_POS_ITEM_QTY
      , ROW_NUMBER() over(PARTITION BY SALES_DT, HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK order by AUDIT_CREATE_TS desc) rnum from retail_alert_morrisons_kraftheinz_uk_im.drfe_forecast_baseline_unit) z

where 
rnum = 1
AND sales_dt >= '2024-01-01'

order by SALES_DT''')
display(df_base_unit)

# COMMAND ----------

df_lsv_baseunit = df_lsv.join(df_base_unit, ['HUB_ORGANIZATION_UNIT_HK', 'HUB_RETAILER_ITEM_HK', 'SALES_DT'])
print(f'N = {df_lsv_baseunit.cache().count():,}')

# COMMAND ----------

# Read POS data
database_name = 'msd_morrisons_kraftheinz_uk_dv'
sales_data_subset = get_pos_prod(
    database_name,
    spark,
    method='Gen1_DLA'
)
df_pos = sales_data_subset.filter(pyf.col('SALES_DT') >= '2024-01-01')
print(f'N = {sales_data_subset.cache().count():,}')

# COMMAND ----------

display(df_pos.groupBy('SALES_DT').agg(pyf.sum('POS_ITEM_QTY').alias('total_qt'), pyf.sum('POS_AMT').alias('total_amt'), pyf.sum('UNIT_PRICE').alias('sum_price'), pyf.count(pyf.when(pyf.col('UNIT_PRICE').isNull(), True)).alias('null_price_count'), pyf.count(pyf.when(pyf.col('POS_ITEM_QTY')==0, True)).alias('zero_qty_count')))

# COMMAND ----------

df_agg_pos = df_pos.groupBy('SALES_DT').agg(pyf.sum('POS_ITEM_QTY').alias('total_qt'), pyf.sum('POS_AMT').alias('total_amt'), pyf.sum('UNIT_PRICE').alias('sum_price'))
df_result = df_agg_pos.join(df_lsv, on=['SALES_DT'])
df_result = df_result.join(df_base_unit, on=['SALES_DT'])
display(df_result)

# COMMAND ----------

display(df_lsv_baseunit.filter((pyf.col('ORGANIZATION_UNIT_NUM') == '363') & (pyf.col('RETAILER_ITEM_ID') == '111316690')))

# COMMAND ----------

df_lsv_baseunit = df_lsv_baseunit.join(df_pos, on=['SALES_DT', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'], how='left')
print(f'N = {df_lsv_baseunit.cache().count():,}')

# COMMAND ----------

display(df_lsv_baseunit)

# COMMAND ----------

df_after_july = df_lsv_baseunit.filter(pyf.col('Sales_DT') >= '2024-07-01')
df_before_july = df_lsv_baseunit.filter(pyf.col('Sales_DT') < '2024-07-01')

# COMMAND ----------

summary_before_july = df_before_july.agg(
    pyf.mean('LOST_SALES_AMT').alias('avg_lsv'),
    pyf.mean('UNIT_PRICE').alias('avg_price'),
    pyf.mean('POS_AMT').alias('avg_pos_amount'),
    pyf.mean('POS_ITEM_QTY').alias('avg_pos_qty'),
    pyf.mean('BASELINE_POS_ITEM_QTY').alias('avg_unit_base')
).collect()

summary_after_july = df_after_july.agg(
    pyf.mean('LOST_SALES_AMT').alias('avg_lsv'),
    pyf.mean('UNIT_PRICE').alias('avg_price'),
    pyf.mean('POS_AMT').alias('avg_pos_amount'),
    pyf.mean('POS_ITEM_QTY').alias('avg_pos_qty'),
    pyf.mean('BASELINE_POS_ITEM_QTY').alias('avg_unit_base')
).collect()

# COMMAND ----------

display(summary_before_july)
display(summary_after_july)

# COMMAND ----------

df_before_july.groupBy('SALES_DT').agg(
    pyf.round(pyf.sum('LOST_SALES_AMT'), 2).cast('float').alias('total_lsv'),
    pyf.round(pyf.avg('UNIT_PRICE'), 2).cast('float').alias('avg_price'),
    pyf.round(pyf.avg('POS_AMT'), 2).cast('float').alias('avg_pos_amount'),
    pyf.round(pyf.avg('POS_ITEM_QTY'), 2).cast('float').alias('avg_pos_qty')
).orderBy('SALES_DT').toPandas().plot(x='SALES_DT', y=['total_lsv', 'avg_price', 'avg_pos_amount', 'avg_pos_qty'], subplots=True, layout=(2, 2), figsize=(15, 8))
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

df_after_july.groupBy('SALES_DT').agg(
    pyf.round(pyf.sum('LOST_SALES_AMT'), 2).cast('float').alias('total_lsv'),
    pyf.round(pyf.avg('UNIT_PRICE'), 2).cast('float').alias('avg_price'),
    pyf.round(pyf.avg('POS_AMT'), 2).cast('float').alias('avg_pos_amount'),
    pyf.round(pyf.avg('POS_ITEM_QTY'), 2).cast('float').alias('avg_pos_qty')
).orderBy('SALES_DT').toPandas().plot(x='SALES_DT', y=['total_lsv', 'avg_price', 'avg_pos_amount', 'avg_pos_qty'], subplots=True, layout=(2, 2), figsize=(15, 8))
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

display(df_lsv_baseunit)

# COMMAND ----------

lsv_drop_df = df_after_july.groupBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').agg(
    pyf.sum('LOST_SALES_AMT').alias('total_lsv_after_july')
).orderBy(pyf.col('total_lsv_after_july').asc())
display(lsv_drop_df)

# COMMAND ----------

df_lsv_baseunit

# COMMAND ----------

df_with_month = df_lsv_baseunit.withColumn('year_month', pyf.date_format(pyf.col('SALES_DT'), 'yyyy-MM'))

# Calculate average price and total LSV per store-item per month
store_item_monthly = df_with_month.groupBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'year_month').agg(
    pyf.avg('UNIT_PRICE').alias('avg_price'),
    pyf.avg('LOST_SALES_AMT').alias('avg_lsv'),
    pyf.avg('BASELINE_POS_ITEM_QTY').alias('avg_base_unit')
).orderBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'year_month')


window_spec = Window.partitionBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').orderBy('year_month')

store_item_monthly = store_item_monthly.withColumn(
    'price_pct_change', 
    (pyf.col('avg_price') - pyf.lag('avg_price').over(window_spec)) / pyf.lag('avg_price').over(window_spec) * 100
).withColumn(
    'lsv_pct_change',
    (pyf.col('avg_lsv') - pyf.lag('avg_lsv').over(window_spec)) / pyf.lag('avg_lsv').over(window_spec) * 100
).withColumn(
    'base_unit_pct_change',
    (pyf.col('avg_base_unit') - pyf.lag('avg_base_unit').over(window_spec)) / pyf.lag('avg_base_unit').over(window_spec) * 100
)


# COMMAND ----------

significant_patterns = store_item_monthly.filter(
    (pyf.abs(pyf.col('price_pct_change')) > 5) | 
    (pyf.abs(pyf.col('base_unit_pct_change')) > 5) & 
    (pyf.abs(pyf.col('lsv_pct_change')) > 5)
)

# COMMAND ----------

display(significant_patterns)

# COMMAND ----------

selected_store_item = store_item_monthly.filter(
    (pyf.col('ORGANIZATION_UNIT_NUM') == '157') & (pyf.col('RETAILER_ITEM_ID') == '100376531')
).orderBy('year_month').toPandas()

plt.figure(figsize=(10, 6))
plt.plot(selected_store_item['year_month'], selected_store_item['avg_price'], label='Average Price', color='blue')
plt.plot(selected_store_item['year_month'], selected_store_item['avg_lsv'], label='Average LSV', color='red')
plt.plot(selected_store_item['year_month'], selected_store_item['avg_base_unit'], label='Average Base Unit', color='green')
plt.xticks(rotation=45)
plt.title('Price vs LSV for Store 1, Item 1')
plt.legend()
plt.show()

# COMMAND ----------

df_with_month_only = df_lsv_baseunit.withColumn('month', pyf.month(pyf.col('SALES_DT')))
seasonality_df = df_with_month_only.groupBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'month').agg(
    pyf.avg('LOST_SALES_AMT').alias('avg_monthly_lsv')
).orderBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'month')

# COMMAND ----------

display(seasonality_df)

# COMMAND ----------

selected_seasonal_store_item = seasonality_df.filter(
    (pyf.col('ORGANIZATION_UNIT_NUM') == '1') & (pyf.col('RETAILER_ITEM_ID') == '100369163')
).orderBy('month').toPandas()

plt.figure(figsize=(10, 6))
plt.plot(selected_seasonal_store_item['month'], selected_seasonal_store_item['avg_monthly_lsv'], marker='o')
plt.xticks(range(1, 13))
plt.title('Seasonality of LSV for Store 1, Item 1')
plt.xlabel('Month')
plt.ylabel('Average LSV')
plt.show()

# COMMAND ----------

display(df_pos.filter((pyf.col('ORGANIZATION_UNIT_NUM') == '1') & (pyf.col('RETAILER_ITEM_ID') == '100369163')))
