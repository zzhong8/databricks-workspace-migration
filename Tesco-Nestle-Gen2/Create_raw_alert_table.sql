-- Databricks notebook source
DROP TABLE IF EXISTS RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS;

CREATE TABLE IF NOT EXISTS RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS
    (RETAILER string,
    CLIENT string,
    COUNTRY_CODE string,
    RETAILER_ITEM_ID string,
    ORGANIZATION_UNIT_NUM string,
    LKP_PRODUCT_GROUP_ID int,
    UNIVERSAL_PRODUCT_CODE string,
    CHAIN_REF_EXTERNAL string,
    ALERT_ID string,
    Id int,
    LAST_DATE_INCLUDED date,
    LAST_OBS float,
    LAST_RATE float,
    LAST_LOST_SALES float,
    N_DAYS_LAST_SALE int,
    PROB float,
    ZSCORE float,
    R2 float,
    RMSE float,
    PRICE float,
    INVENTORY int,
    LOST_SALES_VALUE float,
    EXECUTED_PREDICTION int,
    EXECUTED_PROBABILITY float,
    ALERT_VALUE_PREDICTED float,
    ISSUE string,
    TAR_SECTION int,
    PROMOTION_NAME string,
    WEEKS_COVER int,
    DAYS_SINCE_PROMO string,
    DATE_GENERATED date,
    ALERT_TYPE string
    )
    USING DELTA
    PARTITIONED BY (RETAILER, CLIENT)

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS

-- COMMAND ----------

Describe RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS

-- COMMAND ----------

DROP TABLE IF EXISTS RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS_OSA_ONLY;

CREATE TABLE IF NOT EXISTS RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS_OSA_ONLY
    (RETAILER string,
    CLIENT string,
    COUNTRY_CODE string,
    RETAILER_ITEM_ID string,
    ORGANIZATION_UNIT_NUM string,
    LKP_PRODUCT_GROUP_ID int,
    UNIVERSAL_PRODUCT_CODE string,
    CHAIN_REF_EXTERNAL string,
    ALERT_ID string,
    Id int,
    LAST_DATE_INCLUDED date,
    LAST_OBS float,
    LAST_RATE float,
    LAST_LOST_SALES float,
    N_DAYS_LAST_SALE int,
    PROB float,
    ZSCORE float,
    R2 float,
    RMSE float,
    PRICE float,
    INVENTORY int,
    LOST_SALES_VALUE float,
    EXECUTED_PREDICTION int,
    EXECUTED_PROBABILITY float,
    ALERT_VALUE_PREDICTED float,
    ISSUE string,
    TAR_SECTION int,
    PROMOTION_NAME string,
    WEEKS_COVER int,
    DAYS_SINCE_PROMO string,
    DATE_GENERATED date,
    ALERT_TYPE string
    )
    USING DELTA
    PARTITIONED BY (RETAILER, CLIENT)

-- COMMAND ----------

Desc RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only

-- COMMAND ----------

select count(*) from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts

-- COMMAND ----------

INSERT INTO RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only
SELECT * FROM RETAIL_FORECAST_ENGINE.gen2_field_test_alerts

-- COMMAND ----------

select count(*) from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only

-- COMMAND ----------

select * from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only

-- COMMAND ----------


