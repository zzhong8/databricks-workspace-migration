-- Databricks notebook source
select 27083/12 as monthly_direct_service_cost 


-- COMMAND ----------

select 6500 as monthly_revenue 

-- COMMAND ----------

desc acosta_retail_analytics_im.ds_intervention_summary

-- COMMAND ----------

use acosta_retail_analytics_im;

select sum(total_impact) / 10 from ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and call_date >= "2024-03-01"
and call_date <= "2024-03-31"

-- COMMAND ----------

SELECT count(*) FROM tescopartnertoolkit_tesco_redbull_uk_dv.vw_latest_sat_epos_summary
where sales_dt >= "2024-03-17"

-- COMMAND ----------

SELECT sales_dt as Sales_Date, count(*) as Number_of_Distribution_Points FROM tescopartnertoolkit_tesco_redbull_uk_dv.vw_latest_sat_epos_summary
where sales_dt >= "2024-03-01"
and sales_dt <= "2024-03-31"
group by sales_dt
order by sales_dt desc

-- COMMAND ----------

SELECT SALES_DT as Call_Date, count(*) as Number_of_DLA_Alerts FROM team_retail_alert_redbull_drt_uk_im.alert_on_shelf_availability
where sales_dt >= "2024-03-01"
and sales_dt <= "2024-03-31"
and 
retail_client = "tesco_redbull_uk"
group by SALES_DT
order by SALES_DT desc

-- COMMAND ----------

SELECT count(distinct SALES_DT) as Actual_Days_with_Alerts, 21 as Expected_Days_with_Alerts FROM team_retail_alert_redbull_drt_uk_im.alert_on_shelf_availability
where sales_dt >= "2024-03-01"
and sales_dt <= "2024-03-31"
and 
retail_client = "tesco_redbull_uk"

-- COMMAND ----------

SELECT call_date as Call_Date, count(*) as Number_of_Measurable_Interventions FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
AND
actionable_flg = TRUE
and call_date >= "2024-03-01"
and call_date <= "2024-03-31"
group by call_date
order by call_date desc

-- COMMAND ----------

SELECT count(distinct call_date) as Actual_Days_with_Measured_Value, 21 as Expected_Days_with_Measured_Value FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and call_date >= "2024-03-01"
and call_date <= "2024-03-31"

-- COMMAND ----------

SELECT count(*) as Measured_Interventions FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and call_date >= "2024-03-01"
and call_date <= "2024-03-31"

-- COMMAND ----------

SELECT count(*) as Measured_Interventions FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and call_date >= "2024-03-01"
and call_date <= "2024-03-31"

-- COMMAND ----------

SELECT call_date as call_date, sum(total_impact) as Measured_Value FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and call_date >= "2024-03-01"
and call_date <= "2024-03-31"
group by call_date
order by call_date desc

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.ds_intervention_audit_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco

-- COMMAND ----------

SELECT status_code, count_of_status_code FROM acosta_retail_analytics_im.ds_intervention_audit_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and recency_rank = 1

-- COMMAND ----------

SELECT status_code, count_of_status_code FROM acosta_retail_analytics_im.ds_intervention_audit_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and status_code in ('1 Fully measured', '2 Measurement period not completed', '3 No matching ePOS data', '4 Null or invalid epos_retailer_item_id')
and recency_rank = 33

-- COMMAND ----------

SELECT status_code, count_of_status_code FROM acosta_retail_analytics_im.ds_intervention_audit_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and status_code in ('1 Fully measured', '2 Measurement period not completed', '3 No matching ePOS data', '4 Null or invalid epos_retailer_item_id')
and recency_rank = 22

-- COMMAND ----------

SELECT status_code, count_of_status_code FROM acosta_retail_analytics_im.ds_intervention_audit_summary
where
mdm_country_id = 30 -- UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_client_id = 17687 -- Red Bull
and
mdm_banner_id = 7746 -- Tesco
and status_code in ('1 Fully measured', '2 Measurement period not completed', '3 No matching ePOS data', '4 Null or invalid epos_retailer_item_id')
and recency_rank = 1

-- COMMAND ----------


