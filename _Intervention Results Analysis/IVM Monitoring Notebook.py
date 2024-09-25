# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Status of the Intervention Configuration Setup for the Balance of the UK Clients

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC mdm_client_id,
# MAGIC mdm_banner_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check UK Nestle

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(sales_dt, 1, 7) AS call_month, count(distinct(sales_dt)), count(POS_AMT), sum(POS_AMT) from asda_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-01-01'
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(sales_dt, 1, 7) AS call_month, sum(POS_AMT) from asda_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2020-01-01'
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(sales_dt, 1, 7) AS call_month, sum(POS_AMT) from asda_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2021-01-01'
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(sales_dt, 1, 7) AS call_month, sum(POS_AMT) from asda_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-01-01'
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(sales_dt), count(distinct(sales_dt)), sum(POS_AMT) from morrisons_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by month(sales_dt)
# MAGIC order by month(sales_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, sum(POS_AMT) from morrisons_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(sales_dt), count(distinct(sales_dt)), sum(POS_AMT) from sainsburys_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by month(sales_dt)
# MAGIC order by month(sales_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, sum(POS_AMT) from sainsburys_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(sales_dt), count(distinct(sales_dt)), sum(POS_AMT) from tesco_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by month(sales_dt)
# MAGIC order by month(sales_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, sum(POS_AMT) from tesco_nestlecore_uk_dv.sat_link_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

sql_acosta_retail_analytics_im_asda_count_rep_responses_with_null_epos_retailer_item_ids_by_month = """
SELECT substr(call_date, 1, 7) AS call_month, count(response_id) AS num_uk_opportunity_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7743 -- Asda
and
epos_retailer_item_id is null
group by 
call_month
ORDER BY
call_month
"""

df_acosta_retail_analytics_im_asda_count_rep_responses_with_null_epos_retailer_item_ids_by_month = spark.sql(sql_acosta_retail_analytics_im_asda_count_rep_responses_with_null_epos_retailer_item_ids_by_month).cache()

display(df_acosta_retail_analytics_im_asda_count_rep_responses_with_null_epos_retailer_item_ids_by_month)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, count(response_id) AS num_uk_opportunity_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, count(response_id) AS num_uk_opportunity_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC and
# MAGIC epos_retailer_item_id is null
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, count(response_id) AS num_uk_opportunity_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC and
# MAGIC standard_response_cd is not null
# MAGIC and
# MAGIC epos_retailer_item_id is not null
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC     
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC WHERE
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda
# MAGIC   and
# MAGIC   (
# MAGIC     --call_date >= '2022-09-01' AND
# MAGIC     --call_date <= '2022-10-31'
# MAGIC     call_date in ('2022-10-10','2022-09-12')
# MAGIC   )
# MAGIC   and epos_organization_unit_num = '4271'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check the data science input view to see if rep responses to UK Opportunities (i.e. objective_typ = 'Opportunity') are flowing into the view after 2022-09-22

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC and
# MAGIC standard_response_cd is not null
# MAGIC and
# MAGIC epos_retailer_item_id is not null
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, standard_response_cd, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, standard_response_cd, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, standard_response_cd, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC and
# MAGIC (
# MAGIC standard_response_text = 'In-Aisle Promo Bay - Biscuits' or
# MAGIC standard_response_text = 'In-Aisle Promo Bay - Confectionery' or
# MAGIC standard_response_text = 'Till Location'
# MAGIC )
# MAGIC and
# MAGIC (
# MAGIC   call_date = '2022-10-31'
# MAGIC )
# MAGIC ORDER BY
# MAGIC standard_response_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC and
# MAGIC (
# MAGIC standard_response_text = 'In-Aisle Promo Bay - Biscuits' or
# MAGIC standard_response_text = 'In-Aisle Promo Bay - Confectionery' or
# MAGIC standard_response_text = 'In-Aisle Promo Bay - Crisps'
# MAGIC )
# MAGIC ORDER BY
# MAGIC standard_response_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2023-02-28'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check the data science output table to see if interventions for UK Opportunities (i.e. objective_typ = 'Opportunity') are being populated in the table after 2022-09-22

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2023-02-28'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, mdm_banner_nm, max(call_date), count(1) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC group by
# MAGIC mdm_banner_id, mdm_banner_nm
# MAGIC order by
# MAGIC mdm_banner_id, mdm_banner_nm

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC objective_typ <> 'Opportunity'
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, count(response_id) AS num_uk_opportunity_responses FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, count(response_id) AS num_uk_opportunity_responses FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK
banner_id = 7743  # Asda

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im_asda = """
  SELECT 
  substr(call_date, 1, 7) AS call_month,
  standard_response_cd,
  count(response_id) as num_uk_opportunity_responses
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
    call_date >= '2022-01-01' AND
    call_date <= '2023-12-31'
  )
  group by 
  call_month,
  standard_response_cd
  ORDER BY
  call_month,
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im_asda = spark.sql(df_sql_query_acosta_retail_analytics_im_asda).cache()

pivot_df = df_acosta_retail_analytics_im_asda.groupBy("call_month").pivot("standard_response_cd").sum("num_uk_opportunity_responses")

display(pivot_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, SUM(total_intervention_effect) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, SUM(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, SUM(total_impact) as total_sales_uplift_from_uk_opportunities FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, SUM(total_impact) / count(response_id) as sales_uplift_per_intervention_from_uk_opportunities FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC substr(call_date, 1, 7) AS call_month, 
# MAGIC
# MAGIC case
# MAGIC   when epos_retailer_item_id is not null then 'Yes'
# MAGIC   else 'No'
# MAGIC end as measurable,
# MAGIC
# MAGIC count(response_id) AS num_responses
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC standard_response_cd is not null -- Measurable interventions
# MAGIC group by 
# MAGIC call_month, measurable
# MAGIC ORDER BY
# MAGIC call_month, measurable

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT substr(call_date, 1, 7) AS call_month, SUM(total_impact) / count(response_id) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   (call_date >= '2022-01-01'AND
# MAGIC   call_date <= '2022-12-31')   
# MAGIC )
# MAGIC group by 
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(call_date, 1, 7) AS call_month, count(*) from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2020-01-01'AND
# MAGIC     call_date <= '2023-12-31')   
# MAGIC   )
# MAGIC
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id
# MAGIC   HAVING count(*) > 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(call_date, 1, 7) AS call_month, count(*) from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2022-01-01'AND
# MAGIC     call_date <= '2022-12-31')   
# MAGIC   )
# MAGIC
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id
# MAGIC   HAVING count(*) > 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT a.*
# MAGIC FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
# MAGIC FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda    
# MAGIC and
# MAGIC (
# MAGIC   (call_date >= '2022-01-01'AND
# MAGIC   call_date <= '2022-12-31')   
# MAGIC )
# MAGIC
# MAGIC GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
# MAGIC HAVING count(*) > 1 ) b
# MAGIC ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC and a.call_date = b.call_date
# MAGIC and a.call_id = b.call_id
# MAGIC and a.standard_response_cd = b.standard_response_cd
# MAGIC ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id

# COMMAND ----------

sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_single_interventions = """
select substr(call_date, 1, 4) AS call_year, sum(total_impact) / count(total_impact) as average_uplift_per_measured_intervention_from_single_interventions
from
(
  SELECT a.*
  FROM acosta_retail_analytics_im.ds_intervention_summary a
  JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, COUNT(*)
  FROM acosta_retail_analytics_im.ds_intervention_summary 

  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  and
  mdm_banner_id = 7743 -- Asda    
  and
  (
    (call_date >= '2022-01-01'AND
    call_date <= '2022-12-31')   
  )

  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id
  HAVING count(*) = 1 ) b
  ON a.epos_organization_unit_num = b.epos_organization_unit_num
  and a.epos_retailer_item_id = b.epos_retailer_item_id
  and a.call_date = b.call_date
  and a.call_id = b.call_id
  ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
)
group by call_year
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_single_interventions = spark.sql(sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_single_interventions).cache()

display(df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_single_interventions)

# COMMAND ----------

sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_unique_interventions = """
select substr(call_date, 1, 4) AS call_year, sum(total_impact) / count(total_impact) as average_uplift_per_measured_intervention_from_unique_interventions
from
(
  SELECT a.*
  FROM acosta_retail_analytics_im.ds_intervention_summary a
  JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
  FROM acosta_retail_analytics_im.ds_intervention_summary 

  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  and
  mdm_banner_id = 7743 -- Asda    
  and
  (
    (call_date >= '2022-01-01'AND
    call_date <= '2022-12-31')   
  )

  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
  HAVING count(*) = 1 ) b
  ON a.epos_organization_unit_num = b.epos_organization_unit_num
  and a.epos_retailer_item_id = b.epos_retailer_item_id
  and a.call_date = b.call_date
  and a.call_id = b.call_id
  and a.standard_response_cd = b.standard_response_cd
  ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
)
group by call_year
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_unique_interventions = spark.sql(sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_unique_interventions).cache()

display(df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_unique_interventions)

# COMMAND ----------

sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_interventions = """
select substr(call_date, 1, 4) AS call_year, sum(total_impact) / count(total_impact) as average_uplift_per_measured_intervention_from_duplicate_interventions
from
(
  SELECT a.*
  FROM acosta_retail_analytics_im.ds_intervention_summary a
  JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
  FROM acosta_retail_analytics_im.ds_intervention_summary 

  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  and
  mdm_banner_id = 7743 -- Asda    
  and
  (
    (call_date >= '2022-01-01'AND
    call_date <= '2022-12-31')   
  )

  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
  HAVING count(*) > 1 ) b
  ON a.epos_organization_unit_num = b.epos_organization_unit_num
  and a.epos_retailer_item_id = b.epos_retailer_item_id
  and a.call_date = b.call_date
  and a.call_id = b.call_id
  and a.standard_response_cd = b.standard_response_cd
  ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
)
group by call_year
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_interventions = spark.sql(sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_interventions).cache()

display(df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_interventions)

# COMMAND ----------

sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_display_and_ladder_interventions = """
select substr(call_date, 1, 4) AS call_year, sum(total_impact) / count(total_impact) as average_uplift_per_measured_intervention_from_duplicate_display_and_ladder_interventions
from
(
  SELECT a.*
  FROM acosta_retail_analytics_im.ds_intervention_summary a
  JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
  FROM acosta_retail_analytics_im.ds_intervention_summary 

  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  and
  mdm_banner_id = 7743 -- Asda    
  and
  (
    (call_date >= '2022-01-01'AND
    call_date <= '2022-12-31')   
  )
  and
  (
    standard_response_cd = 'display'
    or
    standard_response_cd = 'ladder'
  )
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
  HAVING count(*) > 1 ) b
  ON a.epos_organization_unit_num = b.epos_organization_unit_num
  and a.epos_retailer_item_id = b.epos_retailer_item_id
  and a.call_date = b.call_date
  and a.call_id = b.call_id
  and a.standard_response_cd = b.standard_response_cd
  ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
)
group by call_year
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_display_and_ladder_interventions = spark.sql(sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_display_and_ladder_interventions).cache()

display(df_average_sales_uplift_per_measured_intervention_asda_by_call_year_from_duplicate_display_and_ladder_interventions)

# COMMAND ----------

sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_interventions = """
select substr(call_date, 1, 7) AS call_month, standard_response_cd, sum(total_impact) / count(total_impact) as average_uplift_per_measured_intervention_from_unique_interventions
from
(
  SELECT a.*
  FROM acosta_retail_analytics_im.ds_intervention_summary a
  JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
  FROM acosta_retail_analytics_im.ds_intervention_summary 

  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  and
  mdm_banner_id = 7743 -- Asda    
  and
  (
    (call_date >= '2022-01-01'AND
    call_date <= '2022-12-31')   
  )
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
  HAVING count(*) = 1 ) b
  ON a.epos_organization_unit_num = b.epos_organization_unit_num
  and a.epos_retailer_item_id = b.epos_retailer_item_id
  and a.call_date = b.call_date
  and a.call_id = b.call_id
  and a.standard_response_cd = b.standard_response_cd
  ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
)
group by call_month, standard_response_cd
order by call_month, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_interventions = spark.sql(sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_interventions).cache()

pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_interventions = df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_interventions.groupBy("call_month").pivot("standard_response_cd").sum("average_uplift_per_measured_intervention_from_unique_interventions")

display(pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_interventions)

# COMMAND ----------

sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_display_and_ladder_interventions = """
select standard_response_cd, substr(call_date, 1, 7) AS call_month, sum(total_impact) / count(total_impact) as average_uplift_per_measured_intervention_from_unique_display_and_ladder_interventions
from
(
  SELECT a.*
  FROM acosta_retail_analytics_im.ds_intervention_summary a
  JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
  FROM acosta_retail_analytics_im.ds_intervention_summary 

  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  and
  mdm_banner_id = 7743 -- Asda    
  and
  (
    (call_date >= '2022-01-01'AND
    call_date <= '2022-12-31')   
  )
  and
  (
    standard_response_cd = 'display'
    or
    standard_response_cd = 'ladder'
  )
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
  HAVING count(*) = 1 ) b
  ON a.epos_organization_unit_num = b.epos_organization_unit_num
  and a.epos_retailer_item_id = b.epos_retailer_item_id
  and a.call_date = b.call_date
  and a.call_id = b.call_id
  and a.standard_response_cd = b.standard_response_cd
  ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
)
group by standard_response_cd, call_month
order by standard_response_cd, call_month
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_display_and_ladder_interventions = spark.sql(sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_display_and_ladder_interventions).cache()

display(df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_unique_display_and_ladder_interventions)

# COMMAND ----------

sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_duplicate_display_and_ladder_interventions = """
select standard_response_cd, substr(call_date, 1, 7) AS call_month, sum(total_impact) / count(total_impact) as average_uplift_per_measured_intervention_from_duplicate_display_and_ladder_interventions
from
(
  SELECT a.*
  FROM acosta_retail_analytics_im.ds_intervention_summary a
  JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
  FROM acosta_retail_analytics_im.ds_intervention_summary 

  where
  mdm_country_id = 30 -- UK
  and
  mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
  and
  mdm_holding_id = 3257 -- AcostaRetailUK
  and
  mdm_banner_id = 7743 -- Asda    
  and
  (
    (call_date >= '2022-01-01'AND
    call_date <= '2022-12-31')   
  )
  and
  (
    standard_response_cd = 'display'
    or
    standard_response_cd = 'ladder'
  )
  GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
  HAVING count(*) > 1 ) b
  ON a.epos_organization_unit_num = b.epos_organization_unit_num
  and a.epos_retailer_item_id = b.epos_retailer_item_id
  and a.call_date = b.call_date
  and a.call_id = b.call_id
  and a.standard_response_cd = b.standard_response_cd
  ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
)
group by standard_response_cd, call_month
order by standard_response_cd, call_month
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_duplicate_display_and_ladder_interventions = spark.sql(sql_query_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_duplicate_display_and_ladder_interventions).cache()

display(df_average_sales_uplift_per_measured_intervention_asda_by_call_month_from_duplicate_display_and_ladder_interventions)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(call_date, 1, 7) AS call_month, count(*) as num_duplicate_interventions from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2020-01-01'AND
# MAGIC     call_date <= '2023-12-31')   
# MAGIC   )
# MAGIC
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
# MAGIC   HAVING count(*) > 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   and a.standard_response_cd = b.standard_response_cd
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select standard_response_cd, substr(call_date, 1, 7) AS call_month, count(distinct epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd) as num_unique_interventions_from_duplicates from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2020-01-01'AND
# MAGIC     call_date <= '2023-12-31')   
# MAGIC   )
# MAGIC   and
# MAGIC   (
# MAGIC     standard_response_cd = 'display'
# MAGIC     or
# MAGIC     standard_response_cd = 'ladder'
# MAGIC   )
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
# MAGIC   HAVING count(*) > 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   and a.standard_response_cd = b.standard_response_cd
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by standard_response_cd, call_month
# MAGIC order by standard_response_cd, call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select standard_response_cd, substr(call_date, 1, 7) AS call_month, count(*) as num_duplicate_interventions from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2020-01-01'AND
# MAGIC     call_date <= '2023-12-31')   
# MAGIC   )
# MAGIC   and
# MAGIC   (
# MAGIC     standard_response_cd = 'display'
# MAGIC     or
# MAGIC     standard_response_cd = 'ladder'
# MAGIC   )
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
# MAGIC   HAVING count(*) > 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   and a.standard_response_cd = b.standard_response_cd
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by standard_response_cd, call_month
# MAGIC order by standard_response_cd, call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select standard_response_cd, substr(call_date, 1, 7) AS call_month, count(*) as num_single_interventions from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2020-01-01'AND
# MAGIC     call_date <= '2023-12-31')   
# MAGIC   )
# MAGIC   and
# MAGIC   (
# MAGIC     standard_response_cd = 'display'
# MAGIC     or
# MAGIC     standard_response_cd = 'ladder'
# MAGIC   )
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_cd
# MAGIC   HAVING count(*) = 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   and a.standard_response_cd = b.standard_response_cd
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by standard_response_cd, call_month
# MAGIC order by standard_response_cd, call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(call_date, 1, 7) AS call_month, count(*) from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_text, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2020-01-01'AND
# MAGIC     call_date <= '2023-12-31')   
# MAGIC   )
# MAGIC
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_text
# MAGIC   HAVING count(*) > 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   and a.standard_response_text = b.standard_response_text
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(call_date, 1, 7) AS call_month, count(*) from
# MAGIC (
# MAGIC   SELECT a.*
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary a
# MAGIC   JOIN (SELECT epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_text, COUNT(*)
# MAGIC   FROM acosta_retail_analytics_im.ds_intervention_summary 
# MAGIC
# MAGIC   where
# MAGIC   mdm_country_id = 30 -- UK
# MAGIC   and
# MAGIC   mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC   and
# MAGIC   mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC   and
# MAGIC   mdm_banner_id = 7743 -- Asda    
# MAGIC   and
# MAGIC   (
# MAGIC     (call_date >= '2022-01-01'AND
# MAGIC     call_date <= '2022-12-31')   
# MAGIC   )
# MAGIC
# MAGIC   GROUP BY epos_organization_unit_num, epos_retailer_item_id, call_date, call_id, standard_response_text
# MAGIC   HAVING count(*) > 1 ) b
# MAGIC   ON a.epos_organization_unit_num = b.epos_organization_unit_num
# MAGIC   and a.epos_retailer_item_id = b.epos_retailer_item_id
# MAGIC   and a.call_date = b.call_date
# MAGIC   and a.call_id = b.call_id
# MAGIC   and a.standard_response_text = b.standard_response_text
# MAGIC   ORDER BY a.call_id, a.epos_organization_unit_num, a.epos_retailer_item_id
# MAGIC )
# MAGIC group by call_month
# MAGIC order by call_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC group by 
# MAGIC call_date
# MAGIC ORDER BY
# MAGIC call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, mdm_banner_nm, max(call_date), count(1) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC group by
# MAGIC mdm_banner_id, mdm_banner_nm
# MAGIC order by
# MAGIC mdm_banner_id, mdm_banner_nm

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check UK Kenvue (aka J&J)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(sales_dt), count(distinct(sales_dt)), sum(POS_AMT) from retaillink_asda_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by month(sales_dt)
# MAGIC order by month(sales_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, sum(POS_AMT) from retaillink_asda_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(sales_dt), count(distinct(sales_dt)), sum(POS_AMT) from msd_morrisons_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by month(sales_dt)
# MAGIC order by month(sales_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, sum(POS_AMT) from msd_morrisons_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(sales_dt), count(distinct(sales_dt)), sum(POS_AMT) from horizon_sainsburys_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by month(sales_dt)
# MAGIC order by month(sales_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, sum(POS_AMT) from horizon_sainsburys_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(sales_dt), count(distinct(sales_dt)), sum(POS_AMT) from tescopartnertoolkit_tesco_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by month(sales_dt)
# MAGIC order by month(sales_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, sum(POS_AMT) from tescopartnertoolkit_tesco_johnsonandjohnson_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-09-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check the data science input view to see if rep responses to UK Opportunities (i.e. objective_typ = 'Opportunity') are flowing into the view

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2023-02-28'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check the data science output table to see if interventions for UK Opportunities (i.e. objective_typ = 'Opportunity') are being populated in the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2023-02-28'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check the data science input view to see if rep responses for Tesco Kenvue (aka Johnson & Johnson) are being populated in the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, mdm_banner_nm, count(1) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC group by
# MAGIC mdm_banner_id, mdm_banner_nm
# MAGIC order by
# MAGIC mdm_banner_id, mdm_banner_nm

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and 
# MAGIC mdm_banner_id = 7745 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, mdm_banner_nm, max(call_date), count(1) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC group by
# MAGIC mdm_banner_id, mdm_banner_nm
# MAGIC order by
# MAGIC mdm_banner_id, mdm_banner_nm

# COMMAND ----------


