-- Databricks notebook source
select * from interventions_retailer_client_config
where 
mdm_country_id = 30 -- UK
order by
mdm_client_id, mdm_holding_id

-- COMMAND ----------

show tables in retaillink_asda_premier_uk_dv

-- COMMAND ----------

show tables in retail_alert_asda_premier_uk_im

-- COMMAND ----------

show tables in msd_morrisons_premier_uk_dv

-- COMMAND ----------

show tables in retail_alert_morrisons_premier_uk_im

-- COMMAND ----------

show tables in horizon_sainsburys_premier_uk_dv

-- COMMAND ----------

-- show tables in retail_alert_sainsburys_premier_uk_im

-- COMMAND ----------

show tables in tescopartnertoolkit_tesco_premier_uk_dv

-- COMMAND ----------

show tables in retail_alert_tesco_premier_uk_im

-- COMMAND ----------

show tables in retaillink_asda_beiersdorf_uk_dv

-- COMMAND ----------

show tables in retail_alert_asda_beiersdorf_uk_im

-- COMMAND ----------

show tables in msd_morrisons_beiersdorf_uk_dv

-- COMMAND ----------

show tables in retail_alert_morrisons_beiersdorf_uk_im

-- COMMAND ----------

show tables in horizon_sainsburys_beiersdorf_uk_dv

-- COMMAND ----------

show tables in retail_alert_sainsburys_beiersdorf_uk_im

-- COMMAND ----------

show tables in tescopartnertoolkit_tesco_beiersdorf_uk_dv

-- COMMAND ----------

show tables in retail_alert_tesco_beiersdorf_uk_im

-- COMMAND ----------

show tables in retaillink_asda_droetker_uk_dv

-- COMMAND ----------

show tables in msd_morrisons_droetker_uk_dv

-- COMMAND ----------

show tables in horizon_sainsburys_droetker_uk_dv

-- COMMAND ----------

show tables in tescopartnertoolkit_tesco_droetker_uk_dv

-- COMMAND ----------

show tables in retail_alert_asda_droetker_uk_im

-- COMMAND ----------

show tables in retail_alert_morrisons_droetker_uk_im

-- COMMAND ----------

show tables in retail_alert_sainsburys_droetker_uk_im

-- COMMAND ----------

show tables in retail_alert_tesco_droetker_uk_im

-- COMMAND ----------

show tables in retaillink_asda_kraftheinz_uk_dv

-- COMMAND ----------

show tables in msd_morrisons_kraftheinz_uk_dv

-- COMMAND ----------

show tables in horizon_sainsburys_kraftheinz_uk_dv

-- COMMAND ----------

show tables in tescopartnertoolkit_tesco_kraftheinz_uk_dv

-- COMMAND ----------

show tables in retail_alert_asda_kraftheinz_uk_im

-- COMMAND ----------

show tables in retail_alert_morrisons_kraftheinz_uk_im

-- COMMAND ----------

show tables in retail_alert_sainsburys_kraftheinz_uk_im

-- COMMAND ----------

show tables in retail_alert_tesco_kraftheinz_uk_im

-- COMMAND ----------

select * from retail_alert_asda_kraftheinz_uk_im.alert_on_shelf_availability
order by sales_dt desc

-- COMMAND ----------


