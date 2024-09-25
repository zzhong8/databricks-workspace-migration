-- Databricks notebook source
SELECT TOP (1000) [TEAM_NM]
,[TEAM_COUNTRY]
,[TEAM_TYP]
 ,[ACTIVE_FLG]
 ,[DEPENDENCY_ACOSTA_CUSTOMER]
,[DEPENDENCY_ACOSTA_CUSTOMER_COUNTRY]
,[GLOBAL_CONNECT_MANUFACTURER_ID]
,[GLOBAL_CONNECT_PARENT_CHAIN_ID]
,[AUDIT_CREATE_TS]
 ,[AUDIT_UPDATE_TS]
 FROM [dbo].[DLA_TEAM_ALERT_DEPENDENCY_CONFIG]

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = 1 and -- US
    mdm_client_id = 1992 and -- RBUSA
    mdm_holding_id = 71

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.interventions_parameters
    WHERE
    mdm_country_id = 1 and -- US
    mdm_client_id = 1992 and -- RBUSA
    mdm_holding_id = 71

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = 30 and -- UK
    mdm_client_id = 16319 and -- Kraft Heinz UK
    mdm_holding_id = 3257 and
    mdm_banner_id = 7743 -- Asda

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = 30 and -- UK
    mdm_client_id = 16320 and -- Nestle UK
    mdm_holding_id = 3257 and
    mdm_banner_id = 7743 -- Asda

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = 1 and -- US
    mdm_client_id = 16269 and -- Sanofi
    mdm_holding_id = 71  -- Walmart

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = 1 and -- US
    mdm_client_id = 16279 and -- Bumblebee Foods
    mdm_holding_id = 71 -- Walmart

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = 1 and -- US
    mdm_client_id = 16540 and -- Harrys
    mdm_holding_id = 71  -- Walmart

-- COMMAND ----------

SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = 1 and -- US
    mdm_client_id = 882 and -- Danonewave
    mdm_holding_id = 91 -- Kroger

-- COMMAND ----------

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

USE acosta_retail_analytics_im;
SELECT * from vw_ds_intervention_input_nars
WHERE mdm_client_id = 16319 AND response_id = 121109949

-- COMMAND ----------


