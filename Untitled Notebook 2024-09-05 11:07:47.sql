-- Databricks notebook source
select count(*) from RETAIL_FORECAST_ENGINE.country_dates

-- COMMAND ----------

select count(*) from RETAIL_FORECAST_ENGINE.country_dates

-- COMMAND ----------

select count(*) from RETAIL_FORECAST_ENGINE.PERFORMANCE

-- COMMAND ----------

select count(*) from RETAIL_FORECAST_ENGINE.PERFORMANCE
where client in ('SMUCKERS', 'KRAFTHEINZ', 'NESTLECORE')

-- COMMAND ----------

select country_code, retailer, client, min(sales_dt), max(sales_dt), count(*) from RETAIL_FORECAST_ENGINE.PERFORMANCE
group by country_code, retailer, client
order by country_code, retailer, client

-- COMMAND ----------

select * from acosta_retail_analytics_im.performance limit 10

-- COMMAND ----------

DESCRIBE DETAIL RETAIL_FORECAST_ENGINE.country_dates

-- COMMAND ----------

DESCRIBE EXTENDED RETAIL_FORECAST_ENGINE.country_dates

-- COMMAND ----------

show create table RETAIL_FORECAST_ENGINE.country_dates

-- COMMAND ----------

show create table acosta_retail_analytics_im.performance

-- COMMAND ----------

INSERT INTO acosta_retail_analytics_im.performance
SELECT * FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
where client in ('SMUCKERS', 'KRAFTHEINZ', 'NESTLECORE')

-- COMMAND ----------

INSERT INTO acosta_retail_analytics_im.performance
SELECT * FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
where client in ('BARILLAAMERICAINC', 'CAMPBELLSSNACK')

-- COMMAND ----------

select count(*) from acosta_retail_analytics_im.performance

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.PERFORMANCE limit 10

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.DRFE_FORECAST_BASELINE_UNIT_TEMP limit 10

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS acosta_retail_analytics_im.drfe_forecast_baseline_unit
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    BASELINE_POS_ITEM_QTY DECIMAL(15,2),
    MODEL_ID STRING,
    SALES_DT DATE
)
USING DELTA
PARTITIONED BY (SALES_DT)
OPTIONS (
  'delta.autooptimize.autocompact' = 'true',
  'delta.autooptimize.optimizewrite' = 'true')
LOCATION 'abfss://data@eus2psag2dpcoredatalake.dfs.core.windows.net/informationmart/acosta_retail_report/drfe_forecast_baseline_unit'
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

drop table acosta_retail_analytics_im.drfe_forecast_baseline_unit

-- COMMAND ----------

INSERT INTO acosta_retail_analytics_im.drfe_forecast_baseline_unit (HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, LOAD_TS, RECORD_SOURCE_CD, BASELINE_POS_ITEM_QTY, MODEL_ID, SALES_DT)
SELECT * FROM RETAIL_FORECAST_ENGINE.DRFE_FORECAST_BASELINE_UNIT

-- COMMAND ----------

select count(*) from RETAIL_FORECAST_ENGINE.DRFE_FORECAST_BASELINE_UNIT

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.DRFE_FORECAST_BASELINE_UNIT limit 10

-- COMMAND ----------

select RECORD_SOURCE_CD, MODEL_ID, min(LOAD_TS), max(LOAD_TS), min(SALES_DT), max(SALES_DT) from RETAIL_FORECAST_ENGINE.DRFE_FORECAST_BASELINE_UNIT
group by RECORD_SOURCE_CD, MODEL_ID
order by RECORD_SOURCE_CD, MODEL_ID

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.MODEL_TRAINING_RESULTS limit 10

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.CHAMPION_MODELS limit 10

-- COMMAND ----------

drop table RETAIL_FORECAST_ENGINE.DRFE_FORECAST_BASELINE_UNIT

-- COMMAND ----------

select * from acosta_retail_analytics_im.ds_intervention_summary limit 10

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.CHAMPION_MODELS limit 10

-- COMMAND ----------

drop table RETAIL_FORECAST_ENGINE.CHAMPION_MODELS

-- COMMAND ----------

show tables in RETAIL_FORECAST_ENGINE

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.model_training_results limit 10

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.PERFORMANCE limit 10

-- COMMAND ----------

select count(*) from acosta_retail_analytics_im.performance

-- COMMAND ----------

drop table RETAIL_FORECAST_ENGINE.PERFORMANCE

-- COMMAND ----------

show tables in RETAIL_FORECAST_ENGINE

-- COMMAND ----------

show tables in retail_forecast_engine

-- COMMAND ----------

show create table retail_forecast_engine.country_dates

-- COMMAND ----------


