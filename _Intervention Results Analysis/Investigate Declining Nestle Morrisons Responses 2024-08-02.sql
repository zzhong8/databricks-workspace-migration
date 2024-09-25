-- Databricks notebook source
-- MAGIC %md
-- MAGIC Q: Are the forecast baselines (used for alerting) stable?
-- MAGIC <br>A: Looks like it based on the chart below.

-- COMMAND ----------

select SALES_DT, sum(BASELINE_POS_ITEM_QTY) 
from retail_alert_morrisons_nestlecore_uk_im.drfe_forecast_baseline_unit
where date(SALES_DT) >= date('2023-07-01')
group by SALES_DT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q: Are $ and QTY sales stable, and are prices stable?<br>
-- MAGIC A: Based on the chart below, the <b>current</b> data currently in the datavault looks fine.

-- COMMAND ----------

select sales_dt, sum(pos_amt), sum(pos_item_qty), mean(pos_amt/pos_item_qty) as price from msd_morrisons_nestlecore_uk_dv.vw_latest_sat_epos_summary
where sales_dt > "2024-01-01"
group by all

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q: Are we seeing any major decline in responses, or only in value? <br>
-- MAGIC A: The drop appears to be confined only to the value measured from interventions, and both total and average impact drop significantly after week 13.

-- COMMAND ----------


SELECT weekofyear(call_date), sum(total_impact), mean(total_impact), count(*) as num_responses FROM acosta_retail_analytics_im.ds_intervention_summary
where
  mdm_country_id = 30 -- UK
  and mdm_client_id = 16320 -- Nestle UK
  and mdm_holding_id = 3257 -- AcostaRetailUK
  AND mdm_banner_id = 7744 -- Morrisons
  and call_date >= '2024-01-01'
GROUP BY ALL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q: Do audits indicate process issues with measurement? <br>
-- MAGIC A: With the exception of a few very low IV count weeks, weeks pre/post week 12 do not look significantly different.

-- COMMAND ----------

select weekofyear(call_date), status_code, count(response_id) from acosta_retail_analytics_im.vw_ds_intervention_audit_current
where mdm_client_nm = "Nestle UK"
  and mdm_banner_id = 7744
  group by all

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q: Do we see similar effects in other retailers? <br>
-- MAGIC A: Based on the ASDA plot below, it is slightly difficult to say. There are 'high weeks' in common, but before/after those weeks are more similar than at Morrison's.

-- COMMAND ----------


SELECT weekofyear(call_date), sum(total_impact), mean(total_impact), count(*) as num_responses FROM acosta_retail_analytics_im.ds_intervention_summary
where
  mdm_country_id = 30 -- UK
  and mdm_client_id = 16320 -- Nestle UK
  and mdm_holding_id = 3257 -- AcostaRetailUK
  AND mdm_banner_id = 7743 -- AsdaA
  and call_date >= '2024-01-01'
GROUP BY ALL

-- COMMAND ----------

select weekofyear(call_date), standard_response_cd, standard_response_text, sum(total_impact), COUNT(DISTINCT call_id) AS distinct_call_id_count, count(*), sum(total_impact)/COUNT(DISTINCT call_id) as value_per_call, mean(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
where mdm_client_id = 16320
  and mdm_banner_id = 7744
  and call_date >= date('2024-01-01')
  --and standard_response_text like '%Display%'
  and standard_response_cd = "display"
group by all


-- COMMAND ----------

select weekofyear(call_date), standard_response_cd, sum(total_impact), count(*) from acosta_retail_analytics_im.ds_intervention_summary
where mdm_client_id = 16320
  and mdm_banner_id = 7744
  and call_date >= date('2024-01-01')
group by all


-- COMMAND ----------

select weekofyear(call_date), standard_response_cd, sum(total_impact), count(*) from acosta_retail_analytics_im.ds_intervention_summary
where mdm_client_id = 16320
  and mdm_banner_id = 7743
  and call_date >= date('2024-01-01')
group by all


-- COMMAND ----------

select weekofyear(call_date), standard_response_text, standard_response_cd, actionable_flg, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
where mdm_client_id = 16320
  and mdm_banner_id = 7744
  and date(call_date) > date("2024-03-01")
group by all

-- COMMAND ----------

select weekofyear(call_date), call_id, epos_retailer_item_id, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
where mdm_client_id = 16320
  and mdm_banner_id = 7744
  and actionable_flg = 1
  and date(call_date) > date("2024-03-01")
group by all

-- COMMAND ----------

select call_date, banner_id, count(*) from acosta_retail_analytics_im.vw_ds_intervention_summary
where client_id = 17687
  and date(call_date) >= date("2024-03-01")
group by all
