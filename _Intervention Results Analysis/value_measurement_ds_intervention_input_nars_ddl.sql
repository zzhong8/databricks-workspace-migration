-- Databricks notebook source
-- MAGIC %md
-- MAGIC #This is the Input View for RSV NARS  tables and Is a Unified View having Responses for all Retailer Client combination currently Active In Sql Server retailer Client Config table 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Input View with Objective Type='DLA'

-- COMMAND ----------

create or replace view acosta_retail_analytics_im.vw_ds_intervention_input_dla as
-- This sub query gets the retailer customer store(Division Number - Store Number)
-- from Global Connect/BOBv2 along with MDM Acosta Store Number
with gc_kroger_stores as
(
  select distinct
  --o.OutletId,
  o.ChainRefExternal as customer_store_number, -- This would match with Data Vault ePOS Store HUB
  cfv.notes as store_acosta_number
  from
    bobv2.vw_BOBv2_DailyCallfileVisit cfv,
    BOBv2.vw_bobv2_outlet o
  where
    cfv.OutletId = o.OutletId and
    o.ParentChainId=955 and -- ParentChain / Holding for Kroger
    trim(coalesce(cfv.notes, '')) <> '' and -- Get rid of empty notes
    cfv.notes not like '%-%' -- Get rid of the GUID like values. e.g., 03eb95cf-05ca-497e-acf3-752a46a5293f
),
-- This sub query gets all the retailer customer store along with
-- MDM Acosta Store Number and a few other location related elements
mdm_store as
(
select
  s.country_code as mdm_country_id,
  h.holdingid as mdm_holding_id,
  h.description as mdm_holding_nm,
  s.banner_id as mdm_banner_id,
  s.banner_description as mdm_banner_nm,
  s.store_acosta_number as store_acosta_number, -- This would match with DPAU/AIAU STORE_ID column
  case when h.holdingid = 91 then gc_kroger_stores.customer_store_number else s.customer_store_number end as customer_store_number
 
from
  mdm_raw.vw_lookup_stores s
  left outer join mdm_raw.hs_division as d on s.division_id = d.divisionid
  left outer join mdm_raw.hs_holding as h on d.holdingid = h.holdingid
  left outer join gc_kroger_stores on s.store_acosta_number = gc_kroger_stores.store_acosta_number
),
intervention_config_mapping_parameter as
(
  select distinct
    c.mdm_country_id as mdm_country_id,
    c.mdm_country_nm as mdm_country_nm,
    c.mdm_holding_id as mdm_holding_id,
    c.mdm_holding_nm as mdm_holding_nm,
    c.mdm_banner_id as mdm_banner_id,
    c.mdm_banner_nm as mdm_banner_nm,
    c.mdm_client_id as mdm_client_id,
    c.mdm_client_nm as mdm_client_nm,
    map.objective_typ as objective_typ,
    map.nars_response_text as nars_response_text,
    map.standard_response_text as standard_response_text,
    map.standard_response_cd as standard_response_cd,
    param.intervention_rank as intervention_rank,
    param.intervention_group as intervention_group,
    param.intervention_start_day as intervention_start_day,
    param.intervention_end_day as intervention_end_day,
    param.actionable_flg as actionable_flg,
    c.active_flg as active_flg
  from 
    acosta_retail_analytics_im.interventions_retailer_client_config c,
    acosta_retail_analytics_im.interventions_response_mapping map,
    acosta_retail_analytics_im.interventions_parameters param
  where
    c.mdm_country_id = map.mdm_country_id and
    map.objective_typ = param.objective_typ and
    coalesce(map.standard_response_text, '') = coalesce(param.standard_response_text, '') and

    c.mdm_holding_id = param.mdm_holding_id and
    coalesce(c.mdm_banner_id, -123) = coalesce(param.mdm_banner_id, -123) and
    c.mdm_client_id = param.mdm_client_id
)
select distinct
  conf.mdm_country_id as mdm_country_id,
  conf.mdm_country_nm as mdm_country_nm,
  conf.mdm_holding_id as mdm_holding_id,
  conf.mdm_holding_nm as mdm_holding_nm,
  conf.mdm_banner_id as mdm_banner_id,
  conf.mdm_banner_nm as mdm_banner_nm,
  mdm_store.store_acosta_number as store_acosta_number,
  mdm_store.customer_store_number as epos_organization_unit_num,
  conf.mdm_client_id as mdm_client_id,
  conf.mdm_client_nm as mdm_client_nm,
  case when mdm_store.mdm_holding_id = 91 then concat('0', dpau.product_id) else dpau.shelfcode end as epos_retailer_item_id, -- Here dpau.product_id = Kroger Consumer UPC
  dpau.type as objective_typ,
  dpau.call_date as call_date,
  dpau.call_id,
  dpau.dp_answer_id as response_id,
  dpau.answer as nars_response_text,
  conf.standard_response_text,
  conf.standard_response_cd ,
  conf.intervention_rank,
  conf.intervention_group,
  conf.intervention_start_day,
  conf.intervention_end_day,
  conf.actionable_flg
from
  nars_raw.dpau
  inner join mdm_store
  on
    dpau.PRODUCT_COUNTRY_ID = mdm_store.mdm_country_id and
    dpau.store_id = mdm_store.store_acosta_number

  -- Using Left Outer Join here because we do not want to skip any NARS Response
  -- that has not been mapped to a standard response by Business / Data Science
  left outer join intervention_config_mapping_parameter conf
  on
     -- Pull data for the configured Holding/Retailer and Clients only
    conf.mdm_country_id = mdm_store.mdm_country_id and
    conf.mdm_client_id = dpau.client_id and
    dpau.answer = conf.nars_response_text and
    dpau.type = conf.objective_typ and
 -- Below two conditions will make sure that no spurious tuples are generated from dpau due to redundencies in parameter table
    conf.mdm_holding_id = mdm_store.mdm_holding_id and
    coalesce(conf.mdm_banner_id, -1) = case when conf.mdm_banner_id is null then -1 else mdm_store.mdm_banner_id end


where
conf.mdm_client_id is not null and
conf.active_flg = 1 and -- Pick only active configurations
dpau.type = 'DLA' and -- Pick only DLA type objective
batch_ts >= 20200701000000 -- This is date of GO Live to avoid full table scan

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Input View for Objective Type='uk_opportunities'

-- COMMAND ----------

create or replace view acosta_retail_analytics_im.vw_ds_intervention_input_uk_opportunities as
-- This sub query gets the retailer customer store(Division Number - Store Number)
-- from Global Connect/BOBv2 along with MDM Acosta Store Number
with gc_kroger_stores as
(
  select distinct
  --o.OutletId,
  o.ChainRefExternal as customer_store_number, -- This would match with Data Vault ePOS Store HUB
  cfv.notes as store_acosta_number
  from
    bobv2.vw_BOBv2_DailyCallfileVisit cfv,
    BOBv2.vw_bobv2_outlet o
  where
    cfv.OutletId = o.OutletId and
    o.ParentChainId=955 and -- ParentChain / Holding for Kroger
    trim(coalesce(cfv.notes, '')) <> '' and -- Get rid of empty notes
    cfv.notes not like '%-%' -- Get rid of the GUID like values. e.g., 03eb95cf-05ca-497e-acf3-752a46a5293f
),
-- This sub query gets all the retailer customer store along with
-- MDM Acosta Store Number and a few other location related elements
mdm_store as
(
select
  s.country_code as mdm_country_id,
  h.holdingid as mdm_holding_id,
  h.description as mdm_holding_nm,
  s.banner_id as mdm_banner_id,
  s.banner_description as mdm_banner_nm,
  s.store_acosta_number as store_acosta_number, -- This would match with aiau/AIAU STORE_ID column
  case when h.holdingid = 91 then gc_kroger_stores.customer_store_number else s.customer_store_number end as customer_store_number
 
from
  mdm_raw.vw_lookup_stores s
  left outer join mdm_raw.hs_division as d on s.division_id = d.divisionid
  left outer join mdm_raw.hs_holding as h on d.holdingid = h.holdingid
  left outer join gc_kroger_stores on s.store_acosta_number = gc_kroger_stores.store_acosta_number
),
intervention_config_mapping_parameter as
(
  select distinct
    c.mdm_country_id as mdm_country_id,
    c.mdm_country_nm as mdm_country_nm,
    c.mdm_holding_id as mdm_holding_id,
    c.mdm_holding_nm as mdm_holding_nm,
    c.mdm_banner_id as mdm_banner_id,
    c.mdm_banner_nm as mdm_banner_nm,
    c.mdm_client_id as mdm_client_id,
    c.mdm_client_nm as mdm_client_nm,
    map.objective_typ as objective_typ,
    map.nars_response_text as nars_response_text,
    map.standard_response_text as standard_response_text,
    map.standard_response_cd as standard_response_cd,
    param.intervention_rank as intervention_rank,
    param.intervention_group as intervention_group,
    param.intervention_start_day as intervention_start_day,
    param.intervention_end_day as intervention_end_day,
    param.actionable_flg as actionable_flg,
    c.active_flg as active_flg
  from 
    acosta_retail_analytics_im.interventions_retailer_client_config c,
    acosta_retail_analytics_im.interventions_response_mapping map,
    acosta_retail_analytics_im.interventions_parameters param
  where
    c.mdm_country_id = map.mdm_country_id and
    map.objective_typ = param.objective_typ and
    coalesce(map.standard_response_text, '') = coalesce(param.standard_response_text, '') and

    c.mdm_holding_id = param.mdm_holding_id and
    coalesce(c.mdm_banner_id, -123) = coalesce(param.mdm_banner_id, -123) and
    c.mdm_client_id = param.mdm_client_id and
    map.objective_typ = 'Opportunity'
),
objective_tag as
(select distinct QUESTION_GROUP_ID
from nars_raw.AIAI
join
  (SELECT ot.obj_id, row_number() over (partition by source,obj_id,ot.tag_id order by ot.batch_ts desc) rn FROM nars_raw.OBJTAG ot
   JOIN 
    (SELECT max(tag_id) tag_id from nars_raw.TAG t WHERE t.NAME = "Opportunity" and t.batch_ts >= 20200701000000) t ON ot.TAG_ID = t.TAG_ID where ot.batch_ts >= 20200701000000) OT 
    ON AIAI.QUESTION_ID = OT.OBJ_ID AND rn = 1 )
 
select distinct
  conf.mdm_country_id as mdm_country_id,
  conf.mdm_country_nm as mdm_country_nm,
  conf.mdm_holding_id as mdm_holding_id,
  conf.mdm_holding_nm as mdm_holding_nm,
  conf.mdm_banner_id as mdm_banner_id,
  conf.mdm_banner_nm as mdm_banner_nm,
  mdm_store.store_acosta_number as store_acosta_number,
  mdm_store.customer_store_number as epos_organization_unit_num,
  conf.mdm_client_id as mdm_client_id,
  conf.mdm_client_nm as mdm_client_nm,
  case when mdm_store.mdm_holding_id = 91 then concat('0', aiau.product_id) else aiau.shelfcode end as epos_retailer_item_id, -- Here aiau.product_id = Kroger Consumer UPC
  conf.objective_typ as objective_typ,
  aiau.call_date as call_date,
  aiau.call_id,
  aiau.QUESTION_ANSWER_ID as response_id,
  aiau.answer as nars_response_text,
  conf.standard_response_text,
  conf.standard_response_cd ,
  conf.intervention_rank,
  conf.intervention_group,
  conf.intervention_start_day,
  conf.intervention_end_day,
  conf.actionable_flg
from
  nars_raw.aiau
  inner join mdm_store
  on
    aiau.PRODUCT_COUNTRY_ID = mdm_store.mdm_country_id and
    aiau.store_id = mdm_store.store_acosta_number

  -- Using Left Outer Join here because we do not want to skip any NARS Response
  -- that has not been mapped to a standard response by Business / Data Science
  left outer join intervention_config_mapping_parameter conf
  on
     -- Pull data for the configured Holding/Retailer and Clients only
    conf.mdm_country_id = mdm_store.mdm_country_id and
    conf.mdm_client_id = aiau.client_id and
    aiau.answer = conf.nars_response_text  and   
 -- Below two conditions will make sure that no spurious tuples are generated from aiau due to redundencies in parameter table
    conf.mdm_holding_id = mdm_store.mdm_holding_id and
    coalesce(conf.mdm_banner_id, -1) = case when conf.mdm_banner_id is null then -1 else mdm_store.mdm_banner_id end

where
conf.mdm_client_id is not null and
conf.active_flg = 1 and -- Pick only active configurations
aiau.QUESTION_GROUP_ID IN ( select * from objective_tag ) and
batch_ts >= 20200701000000 -- This is date of GO Live to avoid full table scan

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Unified View having reponses for both Objective Type ='DLA' and 'uk_opportunities'

-- COMMAND ----------

create or replace view acosta_retail_analytics_im.vw_ds_intervention_input_nars as
select 
mdm_country_id,
mdm_country_nm,
mdm_holding_id,
mdm_holding_nm,
mdm_banner_id,
mdm_banner_nm,
store_acosta_number,
epos_organization_unit_num,
mdm_client_id,
mdm_client_nm,
epos_retailer_item_id, 
objective_typ,
call_date,
call_id,
response_id,
standard_response_cd,
standard_response_text,
nars_response_text,
intervention_rank,
intervention_group,
intervention_start_day,
intervention_end_day,
actionable_flg
 from acosta_retail_analytics_im.vw_ds_intervention_input_dla
 
Union

select 
mdm_country_id,
mdm_country_nm,
mdm_holding_id,
mdm_holding_nm,
mdm_banner_id,
mdm_banner_nm,
store_acosta_number,
epos_organization_unit_num,
mdm_client_id,
mdm_client_nm,
epos_retailer_item_id, 
objective_typ,
call_date,
call_id,
response_id,
standard_response_cd,
standard_response_text,
nars_response_text,
intervention_rank,
intervention_group,
intervention_start_day,
intervention_end_day,
actionable_flg
 from acosta_retail_analytics_im.vw_ds_intervention_input_uk_opportunities
