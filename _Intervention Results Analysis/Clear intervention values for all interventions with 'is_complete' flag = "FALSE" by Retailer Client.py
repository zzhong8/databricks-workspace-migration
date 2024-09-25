# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Tesco Nestle Fix
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 16320   -- Nestle
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7746') -- Tesco
# MAGIC    and
# MAGIC    call_date >= '2023-12-19'
# MAGIC    and
# MAGIC    epos_retailer_item_id = 50298452

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Tesco Nestle Fix
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = NULL, total_impact = NULL
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 16320   -- Nestle
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7746') -- Tesco
# MAGIC    AND is_complete = "FALSE"
# MAGIC    and
# MAGIC    call_date >= '2023-12-19'
# MAGIC    and
# MAGIC    epos_retailer_item_id = 50298452

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Asda Beiersdorf Fix
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17686   -- Beiersdorf
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7743') -- Asda
# MAGIC    and
# MAGIC    call_date >= '2023-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = NULL, total_impact = NULL
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17686   -- Beiersdorf
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7743') -- Asda
# MAGIC    and
# MAGIC    call_date >= '2023-08-01'
# MAGIC    AND is_complete = "FALSE"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- New request from Gareth Trueman from 2023-09-15
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17687   -- Red Bull UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    and
# MAGIC    call_date >= '2023-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 16319   -- KraftHeinz UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745') -- Asda, Morrisons, Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 16319 -- Kraft Heinz UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745', '7746') -- Asda, Morrisons, Sainsburys
# MAGIC    AND is_complete = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17682 -- Heineken UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    AND is_complete = 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17683 -- Premier Foods UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    AND is_complete = 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17685 -- Dr O UK
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    AND is_complete = 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17683   -- Premier
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id = 7744    -- Morrisons
# MAGIC    AND is_complete = 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17683   -- Premier
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id = 7746    -- Tesco
# MAGIC    AND is_complete = 'FALSE'

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 17688   -- Kenvue UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7743    -- Asda

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

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'TRUE' 
# MAGIC    and mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17688   -- Kenvue UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id = 7745    -- Sainsburys
# MAGIC    and
# MAGIC    call_date >= '2023-06-11'
# MAGIC    and
# MAGIC    call_date < '2023-07-20'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE is_complete = 'FALSE' 
# MAGIC    and mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17688   -- Kenvue UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id = 7745    -- Sainsburys
# MAGIC    and
# MAGIC    call_date >= '2023-06-11'
# MAGIC    and
# MAGIC    call_date < '2023-07-20'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17688   -- Kenvue UK
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id = 7745    -- Sainsburys
# MAGIC    and
# MAGIC    call_date >= '2023-06-11'
# MAGIC    and
# MAGIC    call_date < '2023-07-20'

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

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id not in (17688, 17686, 17683, 17685, 16319, 17687, 17682, 17713, 17686, 16320)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id <> 16320
# MAGIC    AND mdm_country_id = 30
# MAGIC    AND mdm_banner_id in (7743, 7744, 7745, 7746, )
# MAGIC    AND is_complete = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id in (17688, 17686, 17683, 17685, 16319, 17687, 17682, 17713, 17686)
# MAGIC    AND mdm_country_id = 30
# MAGIC    AND mdm_banner_id in (7743, 7744, 7745, 7746, 7996)
# MAGIC   --  AND is_complete = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE', total_intervention_effect = "", total_impact = ""
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id in (17682,17683,17685,17687,17713)
# MAGIC    AND mdm_country_id = 30
# MAGIC    AND mdm_banner_id in (7743, 7744, 7745, 7746, 7996)

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

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET is_complete = 'FALSE' 
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 16319   -- KraftHeinz UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7746    -- Tesco

# COMMAND ----------

# %sql

# USE acosta_retail_analytics_im;

# UPDATE ds_intervention_summary
#    SET total_intervention_effect = "", total_impact = ""
#    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
#    AND mdm_client_id = 16319   -- KraftHeinz UK
#    AND mdm_country_id = 30     -- UK
#    AND mdm_banner_id = 7746    -- Tesco
#    AND is_complete = 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT mdm_banner_id, count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17687 
# MAGIC group by mdm_banner_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 16319 
# MAGIC AND mdm_banner_id = 7746
# MAGIC AND total_impact >= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17682 
# MAGIC AND mdm_banner_id = 7743
# MAGIC AND is_complete = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17682
# MAGIC AND mdm_banner_id = 7743
# MAGIC AND is_complete = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17682 
# MAGIC AND mdm_banner_id = 7744
# MAGIC AND is_complete = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17682 
# MAGIC AND mdm_banner_id = 7744
# MAGIC AND is_complete = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17683 
# MAGIC AND mdm_banner_id = 7745
# MAGIC AND is_complete = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17683
# MAGIC AND mdm_banner_id = 7745
# MAGIC AND is_complete = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17682 
# MAGIC AND mdm_banner_id = 7746
# MAGIC AND is_complete = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17682 
# MAGIC AND mdm_banner_id = 7746
# MAGIC AND is_complete = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17683
# MAGIC AND mdm_banner_id = 7744
# MAGIC AND is_complete = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT count(*) from ds_intervention_summary
# MAGIC WHERE mdm_client_id = 17683
# MAGIC AND mdm_banner_id = 7744
# MAGIC AND is_complete = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC SELECT mdm_client_id, mdm_banner_id, count(*) from ds_intervention_summary
# MAGIC WHERE
# MAGIC mdm_country_id = 30
# MAGIC AND is_complete = TRUE
# MAGIC GROUP BY
# MAGIC mdm_client_id, mdm_banner_id
# MAGIC ORDER BY 
# MAGIC mdm_client_id, mdm_banner_id

# COMMAND ----------


