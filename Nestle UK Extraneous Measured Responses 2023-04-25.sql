-- Databricks notebook source
desc acosta_retail_analytics_im.ds_intervention_summary

-- COMMAND ----------

select
DISTINCT standard_response_text
FROM acosta_retail_analytics_im.ds_intervention_summary
WHERE mdm_holding_id = 3257 -- AcostaRetailUK
AND mdm_client_id = 16320   -- Nestle
AND mdm_country_id = 30     -- UK 
AND is_complete = 'TRUE'
order by standard_response_text

-- COMMAND ----------

select
mdm_banner_nm, standard_response_text,
sum(total_intervention_effect) AS INTERVENTION_EFFECT,
sum(total_impact) AS IMPACT
FROM acosta_retail_analytics_im.ds_intervention_summary
WHERE mdm_holding_id = 3257 -- AcostaRetailUK
AND mdm_client_id = 16320   -- Nestle
AND mdm_country_id = 30     -- UK 
AND is_complete <> 'TRUE'
AND standard_response_text IN ('Display 1', 'Display 2', 'Display 3', 'Display 4', 'display 1', 'display 2', 'display 3', 'display 4', 'Yes - I built/maintained a display', 'yes - i built/maintained a display')
GROUP BY
mdm_banner_nm, standard_response_text
order by
mdm_banner_nm, standard_response_text

-- COMMAND ----------

select
mdm_banner_nm, standard_response_text,
sum(total_intervention_effect) AS INTERVENTION_EFFECT,
sum(total_impact) AS IMPACT
FROM acosta_retail_analytics_im.ds_intervention_summary
WHERE mdm_holding_id = 3257 -- AcostaRetailUK
AND mdm_client_id = 16320   -- Nestle
AND mdm_country_id = 30     -- UK 
AND is_complete = 'TRUE'
AND standard_response_text IN ('Display 1', 'Display 2', 'Display 3', 'Display 4', 'display 1', 'display 2', 'display 3', 'display 4', 'Yes - I built/maintained a display', 'yes - i built/maintained a display')
GROUP BY
mdm_banner_nm, standard_response_text
order by
mdm_banner_nm, standard_response_text

-- COMMAND ----------

USE acosta_retail_analytics_im;

UPDATE ds_intervention_summary
  SET is_complete = 'FALSE', 
  total_intervention_effect = "", 
  total_qintervention_effect = "",
  total_impact = "",
  total_qimpact = ""
  WHERE mdm_holding_id = 3257 -- AcostaRetailUK
  AND mdm_client_id = 16320   -- Nestle
  AND mdm_country_id = 30     -- UK 
  AND standard_response_text IN ('Display 1', 'Display 2', 'Display 3', 'Display 4', 'display 1', 'display 2', 'display 3', 'display 4', 'Yes - I built/maintained a display', 'yes - i built/maintained a display')

-- COMMAND ----------


