# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where 
# MAGIC mdm_holding_id = 71 -- Walmart

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
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_id,
# MAGIC objective_typ
# MAGIC

# COMMAND ----------

# %sql

# SELECT distinct
# mdm_country_id,
# mdm_country_nm,
# mdm_holding_id,
# mdm_holding_nm,
# mdm_banner_id,
# mdm_banner_nm,
# mdm_client_id
# FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC mdm_country_id,
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_id,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_id,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm
# MAGIC FROM acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC GROUP BY call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-05-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-05-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date >= '2022-05-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC standard_response_text in
# MAGIC ("Merchandising Display",
# MAGIC "Merchandising Fixture")
# MAGIC and
# MAGIC call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is NULL
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC nars_response_text = 'RCM Stocked Shelf'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date = '2022-05-15'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MONTH(call_date), sum(total_intervention_effect), sum(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC group by MONTH(call_date)
# MAGIC order by MONTH(call_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id = 585801594

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id = '585801594'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id = '585801594'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id in
# MAGIC (
# MAGIC 585801594,
# MAGIC 585801595,
# MAGIC 585801596,
# MAGIC 585758642,
# MAGIC 585758525,
# MAGIC 585758644,
# MAGIC 585758527,
# MAGIC 585758646,
# MAGIC 585758522,
# MAGIC 596856454,
# MAGIC 596856455,
# MAGIC 571838026,
# MAGIC 567451625,
# MAGIC 567451584
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id in
# MAGIC (
# MAGIC 585801594,
# MAGIC 585801595,
# MAGIC 585801596,
# MAGIC 585758642,
# MAGIC 585758525,
# MAGIC 585758644,
# MAGIC 585758527,
# MAGIC 585758646,
# MAGIC 585758522,
# MAGIC 596856454,
# MAGIC 596856455,
# MAGIC 571838026,
# MAGIC 567451625,
# MAGIC 567451584
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC desc vw_dimension_product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC desc vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_dimension_product
# MAGIC where
# MAGIC country_id = 1 -- US
# MAGIC and
# MAGIC client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC holding_id = 71 -- Walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct product_country_id, client_id, retailer_id from nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct product_id, shelfcode from nars_raw.dpau
# MAGIC where
# MAGIC product_country_id = 1 -- US
# MAGIC and
# MAGIC client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC retailer_id = 71 -- Walmart
# MAGIC and
# MAGIC shelfcode is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 585801594,
# MAGIC 585801595,
# MAGIC 585801596,
# MAGIC 585758642,
# MAGIC 585758525,
# MAGIC 585758644,
# MAGIC 585758527,
# MAGIC 585758646,
# MAGIC 585758522,
# MAGIC 596856454,
# MAGIC 596856455,
# MAGIC 571838026,
# MAGIC 567451625,
# MAGIC 567451584
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 567451584
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 596856454,
# MAGIC 596856455
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 571838026
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 585801594,
# MAGIC 585801595,
# MAGIC 585801596
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 567451625
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 585758642,
# MAGIC 585758644,
# MAGIC 585758646
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 585758527,
# MAGIC 585758522
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC organization_unit_num,
# MAGIC retailer_item_id,
# MAGIC pos_item_qty,
# MAGIC pos_amt,
# MAGIC on_hand_inventory_qty,
# MAGIC sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC retailer_item_id in
# MAGIC (
# MAGIC 585758525
# MAGIC )
# MAGIC and
# MAGIC sales_dt >= '2022-03-01'

# COMMAND ----------


