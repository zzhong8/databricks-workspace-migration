-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC import acosta
-- MAGIC
-- MAGIC print(acosta.__version__)

-- COMMAND ----------

describe table formatted acosta_retail_analytics_im.vw_ds_intervention_input_nars 

-- COMMAND ----------

-- This selects the minimum response ID for duplicative responses, and otherwise just pulls in rows if no duplication.

WITH t AS (
    SELECT * 
    FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
        mdm_country_id = 30
        and mdm_client_id = 17686
        and mdm_holding_id = 3257
        and mdm_banner_id = 7743
        and call_id = 61839425
)

SELECT a.*  
FROM t as a  
INNER JOIN (  
  SELECT epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text, count(*), MIN(response_id) AS min_response_id  
  FROM t  
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text  
  HAVING COUNT(*) > 1  
) b  
ON a.epos_organization_unit_num = b.epos_organization_unit_num  
AND a.epos_retailer_item_id = b.epos_retailer_item_id  
AND a.call_id = b.call_id  
AND a.standard_response_text = b.standard_response_text  
AND a.response_id = b.min_response_id
UNION  
SELECT c.*  
FROM t as c  
INNER JOIN (  
  SELECT epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text, count(*), MIN(response_id) AS min_response_id  
  FROM t 
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text  
  HAVING COUNT(*) = 1  
) d  
ON c.epos_organization_unit_num = d.epos_organization_unit_num  
AND c.epos_retailer_item_id = d.epos_retailer_item_id  
AND c.call_id = d.call_id  
AND c.standard_response_text = d.standard_response_text  
ORDER BY epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text, response_id  

-- COMMAND ----------

-- These are the duplicative responses that were eliminated by the above query

WITH t AS (
    SELECT * 
    FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
        mdm_country_id = 30
        AND mdm_client_id = 17686
        AND mdm_holding_id = 3257
        AND mdm_banner_id = 7743
        AND call_id = 61839425
)


SELECT a.*  
FROM t AS a  
INNER JOIN (  
  SELECT epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text, COUNT(*) AS count_rows, MIN(response_id) AS min_response_id  
  FROM t  
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text  
  HAVING COUNT(*) > 1  
) b  
ON a.epos_organization_unit_num = b.epos_organization_unit_num  
AND a.epos_retailer_item_id = b.epos_retailer_item_id  
AND a.call_id = b.call_id  
AND a.standard_response_text = b.standard_response_text  
AND a.response_id <> b.min_response_id

UNION 

SELECT c.*  
FROM t as c  
LEFT ANTI JOIN (  
  SELECT epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text, count(*), MIN(response_id) AS min_response_id  
  FROM t 
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text  
  HAVING COUNT(*) = 1  
) d  
ON c.epos_organization_unit_num = d.epos_organization_unit_num  
AND c.epos_retailer_item_id = d.epos_retailer_item_id  
AND c.call_id = d.call_id  
AND c.standard_response_text = d.standard_response_text  

ORDER BY epos_organization_unit_num, epos_retailer_item_id, call_id, standard_response_text, response_id  

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT * from vw_ds_intervention_input_nars
WHERE 
  call_id = 63300979
  AND epos_retailer_item_id = 50620655 
  --and actionable_flg = 'true'
--GROUP BY month(call_date), objective_typ
-- ORDER BY measured_count DESC

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT month(call_date), count(*) from vw_ds_intervention_input_nars
WHERE mdm_client_id = 17686 
  --and mdm_banner_id = 7746
  --and total_impact >= 0
  and actionable_flg = 'true'
GROUP BY month(call_date)
-- ORDER BY measured_count DESC

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT mdm_client_id, mdm_banner_id, month(call_dt), sum(total_impact) from ds_intervention_summary
WHERE 
  --and mdm_banner_id = 7743
  is_complete = 'true'
  and mdm_country_id = 30
  and mdm_client_id = 17686
--GROUP BY mdm_banner_id, response_id
--ORDER BY measured_count DESC
GROUP BY 1, 2

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT mdm_client_id, mdm_banner_id, count(*) FROM
  (SELECT mdm_client_id, mdm_banner_id, call_id, epos_retailer_item_id, count(*) as measured_count from ds_intervention_summary
  WHERE mdm_country_id = 30 
    AND is_complete = 'true'
    AND mdm_client_id <> '16320'
  GROUP BY mdm_client_id, mdm_banner_id, call_id, epos_retailer_item_id
  )
WHERE measured_count > 1
GROUP BY mdm_client_id, mdm_banner_id

-- COMMAND ----------

SELECT 
mdm_banner_id,
mdm_client_id,
mdm_country_id,
mdm_holding_id,
objective_typ,
epos_retailer_item_id,
response_id,
standard_response_text, COUNT
(
    mdm_banner_id,
    mdm_client_id,
    mdm_country_id,
    mdm_holding_id,
    objective_typ,
    epos_retailer_item_id,
    response_id,
    standard_response_text
)
FROM acosta_retail_analytics_im.ds_intervention_summary
WHERE mdm_country_id = 30
GROUP BY
    mdm_banner_id,
    mdm_client_id,
    mdm_country_id,
    mdm_holding_id,
    objective_typ,
    epos_retailer_item_id,
    response_id,
    standard_response_text
HAVING count
(
    mdm_banner_id,
    mdm_client_id,
    mdm_country_id,
    mdm_holding_id,
    objective_typ,
    epos_retailer_item_id,
    response_id,
    standard_response_text
) > 1

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT mdm_client_id, mdm_banner_id, count(*) FROM
  (SELECT mdm_client_id, mdm_banner_id, call_id, epos_retailer_item_id, count(*) as measured_count from ds_intervention_summary
  WHERE mdm_country_id = 30 
    AND is_complete = 'true'
    AND mdm_client_id <> '16320'
  GROUP BY mdm_client_id, mdm_banner_id, call_id, epos_retailer_item_id
  )
WHERE measured_count > 1
GROUP BY mdm_client_id, mdm_banner_id

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT * from ds_intervention_summary
WHERE mdm_country_id = 30
  and mdm_client_id = 17686 -- Beiersdorf UK 
  and mdm_holding_id = 3257 -- Acosta UK
  and mdm_banner_id = 7743  -- Asda
ORDER BY
  response_id

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT count(*) from ds_intervention_summary
WHERE mdm_country_id = 30

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT count(*) from ds_intervention_summary
WHERE mdm_country_id = 30
AND mdm_client_id <> 16320
AND is_complete = TRUE

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT * from ds_intervention_summary
WHERE mdm_country_id = 30
AND mdm_client_id <> 16320
AND is_complete = TRUE
ORDER BY
  response_id

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT count(*) from ds_intervention_summary
WHERE mdm_country_id = 30
AND is_complete = TRUE

-- COMMAND ----------

USE acosta_retail_analytics_im;
-- SELECT * from vw_ds_intervention_input_nars
-- WHERE mdm_country_id = 30
-- limit 10

select * from acosta_retail_analytics_im.vw_dimension_itemlevel
where upc = '500035491893'
--where sku_id = '558883'
limit 5

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT mdm_client_id, count(*) from ds_intervention_summary
WHERE mdm_country_id = 30
AND mdm_banner_id = 7745  -- Asda
-- AND actionable_flg = 'TRUE'
GROUP BY mdm_client_id

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT mdm_client_nm, mdm_banner_nm, load_ts, count(*) from ds_intervention_summary
WHERE mdm_country_id = 30
AND mdm_client_id = 17682
AND is_complete = TRUE
GROUP BY
  mdm_client_nm, mdm_banner_nm, load_ts

-- COMMAND ----------

Select upc, count(*) from 
(select
    distinct t2.HUB_RETAILER_ITEM_HK,
    t2.retailer_item_id,
    t1.UPC
from retail_alert_tesco_premier_uk_im.LOESS_FORECAST_BASELINE_UNIT_UPC t1
join tescopartnertoolkit_tesco_premier_uk_dv.vw_latest_sat_epos_summary t2
on t1.HUB_RETAILER_ITEM_HK = t2.HUB_RETAILER_ITEM_HK)
group by upc
ORDER BY count(*) desc

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT distinct mdm_holding_id, mdm_holding_nm, mdm_client_id, mdm_client_nm from ds_intervention_summary
WHERE mdm_country_id = 1
AND is_complete = 'TRUE'
ORDER BY
  mdm_holding_id, mdm_client_id

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT year(call_date), month(call_date), count(total_intervention_effect), sum(total_intervention_effect), sum(total_impact), sum(total_impact)/count(total_intervention_effect) as avg_impact from ds_intervention_summary
WHERE mdm_country_id = 1
AND mdm_holding_id = 91 -- Kroger
AND mdm_client_id = 16540 -- Wildcat
GROUP BY
  year(call_date), month(call_date)
ORDER BY
  year(call_date), month(call_date)

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT year(call_date), month(call_date), count(total_intervention_effect), sum(total_intervention_effect), sum(total_impact), sum(total_impact)/count(total_intervention_effect) as avg_impact from ds_intervention_summary
WHERE mdm_country_id = 1
AND mdm_holding_id = 2301 -- Target
AND mdm_client_id = 16540 -- Wildcat
GROUP BY
  year(call_date), month(call_date)
ORDER BY
  year(call_date), month(call_date)

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT year(call_date), month(call_date), count(total_intervention_effect), sum(total_intervention_effect), sum(total_impact), sum(total_impact)/count(total_intervention_effect) as avg_impact from ds_intervention_summary
WHERE mdm_country_id = 1
AND mdm_holding_id = 71 -- Walmart
AND mdm_client_id = 851 -- Campbells
GROUP BY
  year(call_date), month(call_date)
ORDER BY
  year(call_date), month(call_date)

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT year(call_date), month(call_date), count(total_intervention_effect), sum(total_intervention_effect), sum(total_impact), sum(total_impact)/count(total_intervention_effect) as avg_impact from ds_intervention_summary
WHERE mdm_country_id = 1
AND mdm_holding_id = 91 -- Kroger
AND mdm_client_id = 851 -- Campbells
GROUP BY
  year(call_date), month(call_date)
ORDER BY
  year(call_date), month(call_date)

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT year(call_date), month(call_date), count(total_intervention_effect), sum(total_intervention_effect), sum(total_impact), sum(total_impact)/count(total_intervention_effect) as avg_impact from ds_intervention_summary
WHERE mdm_country_id = 1
AND mdm_holding_id = 2301 -- Target
AND mdm_client_id = 851 -- Campbells
GROUP BY
  year(call_date), month(call_date)
ORDER BY
  year(call_date), month(call_date)

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT year(call_date), month(call_date), count(total_intervention_effect), sum(total_intervention_effect), sum(total_impact), sum(total_impact)/count(total_intervention_effect) as avg_impact from ds_intervention_summary
WHERE mdm_country_id = 1
AND mdm_holding_id = 2301 -- Target
AND mdm_client_id = 851 -- Campbells
GROUP BY
  year(call_date), month(call_date)
ORDER BY
  year(call_date), month(call_date)

-- COMMAND ----------

USE acosta_retail_analytics_im;
SELECT year(call_date), month(call_date), count(total_intervention_effect), sum(total_intervention_effect), sum(total_impact), sum(total_impact)/count(total_intervention_effect) as avg_impact from ds_intervention_summary
WHERE mdm_country_id = 1
AND mdm_holding_id = 91 -- Kroger
AND mdm_client_id = 882 -- Danone
GROUP BY
  year(call_date), month(call_date)
ORDER BY
  year(call_date), month(call_date)

-- COMMAND ----------

USE market6_kroger_danonewave_us_dv;
SELECT SUM(pos_item_qty), SUM(pos_amt), sum(on_hand_inventory_qty), sales_dt from vw_latest_sat_epos_summary
where sales_dt >= '2021-01-01'
GROUP BY sales_dt
ORDER BY sales_dt

-- COMMAND ----------

SELECT * from acosta_retail_analytics_im.vw_ds_intervention_input_nars
WHERE 
response_id = 133563275 

-- COMMAND ----------

SELECT * from acosta_retail_analytics_im.interventions_retailer_client_config
ORDER BY
active_flg desc,
mdm_country_id,
mdm_holding_id,
mdm_banner_id,
mdm_client_id

-- COMMAND ----------


