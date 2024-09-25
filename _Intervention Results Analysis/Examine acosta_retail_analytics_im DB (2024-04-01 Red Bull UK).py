# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM retaillink_asda_redbull_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM msd_morrisons_redbull_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM horizon_sainsburys_redbull_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM tescopartnertoolkit_tesco_redbull_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM tescopartnertoolkit_tesco_droetker_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(DISTINCT organization_unit_num) FROM tescopartnertoolkit_tesco_redbull_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(DISTINCT retailer_item_id) FROM tescopartnertoolkit_tesco_redbull_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_audit_current
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- audit_ts >= '2024-03-31'
# MAGIC AND
# MAGIC recency_rank = 1
# MAGIC and
# MAGIC call_date >= '2024-03-01'
# MAGIC order by 
# MAGIC response_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from ds_intervention_audit_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC audit_ts >= '2024-03-29'
# MAGIC order by 
# MAGIC status_code

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from ds_intervention_audit_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC audit_ts >= '2024-03-29'
# MAGIC order by 
# MAGIC status_code

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC AND
# MAGIC call_date >= '2024-03-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC AND
# MAGIC call_date >= '2024-03-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC actionable_flg is NULL
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC actionable_flg = 'true'
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC AND
# MAGIC call_date >= '2024-01-01'
# MAGIC GROUP BY call_date
# MAGIC ORDER BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_summary

# COMMAND ----------


