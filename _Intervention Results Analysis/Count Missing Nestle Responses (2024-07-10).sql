-- Databricks notebook source
SELECT mdm_banner_nm, standard_response_text, count(*) as num_responses FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
-- and
-- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
-- and
-- objective_typ='DLA'
and
call_date >= '2024-01-01'
and
standard_response_text IN (
  "Promotional SEL's Sited On Shelf",
  "Promotional SEL's Sited Off shelf",
  'HO Promotion Made Compliant – Merchandised Stock',
  'n-Aisle Promo Bay - Confectionery',
  'Pallet/MU Wrap',
  '1/2 Fixture Bay',
  '1/2 In-Aisle Promo Bay',
  '1/2 In-Aisle Promo Bay Shelf',
  '1/4 In-Aisle Promo Bay Shelf',
  'Fixture Full Bay',
  'In-Aisle Promo Bay Shelf',
  'Stock Merchandised Off Shelf - Other Location',
  '1/2 Fixture Shelf',
  '1/4 Fixture Shelf'
)
GROUP BY mdm_banner_nm, standard_response_text
ORDER BY mdm_banner_nm, standard_response_text

-- COMMAND ----------

USE acosta_retail_analytics_im;
CREATE OR REPLACE VIEW acosta_retail_analytics_im.ds_intervention_summary_nestle_temp_20240710 AS
(
  SELECT mdm_banner_nm, standard_response_text, count(*) as num_responses FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  -- and
  -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
  -- and
  -- objective_typ='DLA'
  and
  call_date >= '2024-01-01'
  and
  standard_response_text IN (
    "Promotional SEL's Sited On Shelf",
    "Promotional SEL's Sited Off shelf",
    'HO Promotion Made Compliant – Merchandised Stock',
    'n-Aisle Promo Bay - Confectionery',
    'Pallet/MU Wrap',
    '1/2 Fixture Bay',
    '1/2 In-Aisle Promo Bay',
    '1/2 In-Aisle Promo Bay Shelf',
    '1/4 In-Aisle Promo Bay Shelf',
    'Fixture Full Bay',
    'In-Aisle Promo Bay Shelf',
    'Stock Merchandised Off Shelf - Other Location',
    '1/2 Fixture Shelf',
    '1/4 Fixture Shelf'
  )
  GROUP BY mdm_banner_nm, standard_response_text
  ORDER BY mdm_banner_nm, standard_response_text
)


-- COMMAND ----------

select * from acosta_retail_analytics_im.ds_intervention_summary_nestle_temp_20240710

-- COMMAND ----------


