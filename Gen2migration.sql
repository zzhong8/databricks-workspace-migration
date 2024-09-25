-- Databricks notebook source
show databases like '*nestlecore*'

-- COMMAND ----------

select * from team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability where   ParentChainId='23' 

-- COMMAND ----------

-- select * from nestlecore_drt_uk_team_alerts_im.alert_on_shelf_availability where SALES_DT='2023-08-13'  and ParentChainId='23'


-- COMMAND ----------

show tables in team_retail_alert_nestlecore_drt_uk_im

-- COMMAND ----------

select * from team_retail_alert_nestlecore_drt_uk_im.asda_nestlecore_uk_tmp_hold_audit_team_alerts_osa_sales_dt

-- COMMAND ----------

use retail_alert_asda_nestlecore_uk_im;
select 'LSV',* FROM audit_drfe_lost_sales_last_processed_ts union
select 'INV',* FROM audit_drfe_alerts_invcleanup_last_processed_ts union
select 'OSA',* FROM audit_drfe_alerts_osa_last_processed_ts union
select 'FCST',* FROM audit_drfe_forecast_last_processed_ts 

-- COMMAND ----------

use retail_alert_asda_nestlecore_uk_im;
select 'LOW',* FROM audit_drfe_alerts_osa_low_expected_sale_last_processed_ts union
select 'Invalid',* from audit_drfe_alert_osa_invalid_alerts_last_processed_ts;

-- COMMAND ----------

select count(1) from retail_alert_asda_nestlecore_uk_im.alert_on_shelf_availability
where SALES_DT='2023-08-14'

-- COMMAND ----------

select count(1) from asda_nestlecore_uk_retail_alert_im.alert_on_shelf_availability
where SALES_DT='2023-08-14'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### gen2

-- COMMAND ----------

select * from retail_alert_asda_nestlecore_uk_im.drfe_forecast_baseline_unit where  SALES_DT='2023-08-14' and HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

select * from retail_alert_asda_nestlecore_uk_im.drfe_forecast_baseline_unit where  SALES_DT>'2023-08-11' and HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

select * from retaillink_asda_nestlecore_uk_dv.sat_epos_summary where SALES_DT='2023-08-14' and HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

select * from retail_alert_asda_nestlecore_uk_im.alert_on_shelf_availability
where SALES_DT>'2023-08-13' and HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gen1

-- COMMAND ----------


select * from asda_nestlecore_uk_retail_alert_im.alert_on_shelf_availability
where SALES_DT>'2023-08-13' and HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

select * from  asda_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit where SALES_DT='2023-08-14' and HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

select * from  asda_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit where SALES_DT>'2023-08-11' and HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

show tables in asda_nestlecore_uk_dv

-- COMMAND ----------

select * from  asda_nestlecore_uk_dv.sat_link_epos_summary s  inner join asda_nestlecore_uk_dv.link_epos_summary  l on s.LINK_EPOS_SUMMARY_HK=l.LINK_EPOS_SUMMARY_HK

where s.SALES_DT='2023-08-14' and l.HUB_ORGANIZATION_UNIT_HK='4424d2deec2f9468fb61e2db07ecd6b6' 
and l.HUB_RETAILER_ITEM_HK='0001c425b467a75d78734f03348a1608'

-- COMMAND ----------

select RETAILER_ITEM_ID,ORGANIZATION_UNIT_NUM, SALES_DT, LOST_SALES_AMT  from retail_alert_asda_nestlecore_uk_im.alert_on_shelf_availability
where SALES_DT>'2023-08-13'

-- COMMAND ----------

select HUB_ORGANIZATION_UNIT_HK,HUB_RETAILER_ITEM_HK, SALES_DT, LOST_SALES_AMT  from retail_alert_asda_nestlecore_uk_im.alert_on_shelf_availability
where SALES_DT>'2023-08-13'
minus 
select HUB_ORGANIZATION_UNIT_HK,HUB_RETAILER_ITEM_HK, SALES_DT, LOST_SALES_AMT  from asda_nestlecore_uk_retail_alert_im.alert_on_shelf_availability
where SALES_DT>'2023-08-13'

-- COMMAND ----------


