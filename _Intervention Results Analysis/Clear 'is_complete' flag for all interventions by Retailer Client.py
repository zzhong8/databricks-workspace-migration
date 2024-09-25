# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 16320 -- Nestle UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    AND standard_response_text = 'HFSS Compliant Front of Store'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE retaillink_walmart_bluetritonbrands_us_dv;
# MAGIC
# MAGIC select DISTINCT sales_dt from vw_latest_sat_epos_summary
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE tescopartnertoolkit_tesco_kraftheinz_uk_dv;
# MAGIC
# MAGIC select DISTINCT sales_dt from vw_latest_sat_epos_summary
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 16320 -- Nestle UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    AND standard_response_text = 'HFSS Compliant Front of Store'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc RETAIL_FORECAST_ENGINE.MODEL_TRAINING_RESULTS

# COMMAND ----------

# MAGIC %sql
# MAGIC desc retail_forecast_engine.champion_models

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail_forecast_engine.champion_models limit 1

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE', total_intervention_effect = "", total_impact = ""
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 16320 -- Nestle UK
#    AND mdm_country_id = 30     -- UK 
#    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
#    AND standard_response_text = 'HFSS Compliant Front of Store'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17683 -- Premier Foods UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7746') -- Asda, Morrisons, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC SELECT * FROM ds_intervention_summary
# MAGIC    WHERE is_complete = 'TRUE'
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17781 -- BAT
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7746') -- Asda, Morrisons, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC SELECT * FROM ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17781 -- BAT
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7746') -- Asda, Morrisons, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE', total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17686 -- Beiersdorf UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND call_date >= '2023-12-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE', total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17781 -- BAT
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7746') -- Asda, Morrisons, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC SELECT * FROM ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17781 -- BAT
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7746') -- Asda, Morrisons, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct standard_response_text from vw_ds_intervention_input_nars
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 16320 -- Nestle UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    AND actionable_flg
# MAGIC order by standard_response_text

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17682 -- Heineken UK
#    AND mdm_country_id = 30     -- UK 
#    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'FALSE' 
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17682 -- Heineken UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'TRUE' 
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17682 -- Heineken UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17683 -- Premier Foods UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'TRUE' 
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17683 -- Premier Foods UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7746') -- Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17685 -- Dr O UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'TRUE' 
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17685 -- Dr O UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7746') -- Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'FALSE' 
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17685 -- Dr O UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7746') -- Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17687 -- Red Bull
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'FALSE' 
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17687 -- Red Bull
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7746') -- Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'TRUE' 
# MAGIC    AND mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17687 -- Red Bull
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743') -- Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'TRUE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17686 -- Beiersdorf UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17686   -- Beiersdorf UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7996')    -- Boots

# COMMAND ----------


# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17685   -- Dr Oetker
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id IN ('7743','7744','7745','7746')    -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------


# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17687   -- Red Bull
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id IN ('7743','7744','7745','7746')    -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------


# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17682   -- Heineken
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id IN ('7743','7744','7745','7746')    -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------


# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17713   -- Perfetti Van Melle
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id IN ('7743','7744','7745','7746')    -- Asda, Morrisons, Sainsburys, Tesco

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7744    -- Morrisons

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7745    -- Sainsburys

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7746    -- Tesco

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET total_intervention_effect = "", total_impact = ""
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7743    -- Asda
#    AND is_complete = 'FALSE'

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET total_intervention_effect = "", total_impact = ""
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7744    -- Morrisons
#    AND is_complete = 'FALSE'

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET total_intervention_effect = "", total_impact = ""
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7745    -- Sainsburys
#    AND is_complete = 'FALSE'

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET total_intervention_effect = "", total_impact = ""
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7746    -- Tesco
#    AND is_complete = 'FALSE'
