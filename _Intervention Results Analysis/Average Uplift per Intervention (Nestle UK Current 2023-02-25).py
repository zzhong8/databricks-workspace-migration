# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

sql_number_of_interventions_by_retailer_and_call_year = """
SELECT mdm_banner_nm, substr(call_date, 1, 4) AS call_year_current, count(total_impact) as num_interventions FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_year_current
ORDER BY
mdm_banner_nm, call_year_current
"""

df_number_of_interventions_by_retailer_and_call_year = spark.sql(sql_number_of_interventions_by_retailer_and_call_year).cache()

pivot_df_number_of_interventions_by_retailer_and_call_year = df_number_of_interventions_by_retailer_and_call_year.groupBy("call_year_current").pivot("mdm_banner_nm").sum("num_interventions")

display(pivot_df_number_of_interventions_by_retailer_and_call_year)

# COMMAND ----------

sql_number_of_interventions_by_retailer_and_call_period = """
SELECT mdm_banner_nm, 
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as call_period_current,
count(total_impact) as num_interventions FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_period_current
ORDER BY
mdm_banner_nm, call_period_current
"""

df_number_of_interventions_by_retailer_and_call_period = spark.sql(sql_number_of_interventions_by_retailer_and_call_period).cache()

pivot_df_number_of_interventions_by_retailer_and_call_period = df_number_of_interventions_by_retailer_and_call_period.groupBy("call_period_current").pivot("mdm_banner_nm").sum("num_interventions").orderBy("call_period_current")

display(pivot_df_number_of_interventions_by_retailer_and_call_period)

# COMMAND ----------

sql_number_of_interventions_by_retailer_and_call_month = """
SELECT mdm_banner_nm, substr(call_date, 1, 7) AS call_month_current, count(total_impact) as num_interventions FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_month_current
ORDER BY
mdm_banner_nm, call_month_current
"""

df_number_of_interventions_by_retailer_and_call_month = spark.sql(sql_number_of_interventions_by_retailer_and_call_month).cache()

pivot_df_number_of_interventions_by_retailer_and_call_month = df_number_of_interventions_by_retailer_and_call_month.groupBy("call_month_current").pivot("mdm_banner_nm").sum("num_interventions").orderBy("call_month_current")

display(pivot_df_number_of_interventions_by_retailer_and_call_month)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year = """
SELECT mdm_banner_nm, substr(call_date, 1, 4) AS call_year_current, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_year_current
ORDER BY
mdm_banner_nm, call_year_current
"""

df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year).cache()

pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year = df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year.groupBy("call_year_current").pivot("mdm_banner_nm").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_call_period = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as call_period_current,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  call_date >= '2022-01-01' AND
  call_date <= '2022-12-31'   
)
group by 
call_period_current
ORDER BY
call_period_current
"""

df_average_sales_uplift_per_measured_intervention_by_call_period = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_call_period).cache()

display(df_average_sales_uplift_per_measured_intervention_by_call_period)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period = """
SELECT mdm_banner_nm, 
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as call_period_current,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  call_date >= '2022-01-01' AND
  call_date <= '2022-12-31'   
)
group by 
mdm_banner_nm, call_period_current
ORDER BY
mdm_banner_nm, call_period_current
"""

df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period).cache()

pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period = df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period.groupBy("call_period_current").pivot("mdm_banner_nm").sum("average_sales_uplift_per_measured_intervention").orderBy("call_period_current")

display(pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month = """
SELECT mdm_banner_nm, substr(call_date, 1, 7) AS call_month_current, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  call_date >= '2022-01-01' AND
  call_date <= '2022-12-31'   
)
group by 
mdm_banner_nm, call_month_current
ORDER BY
mdm_banner_nm, call_month_current
"""

df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month).cache()

pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month = df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month.groupBy("call_month_current").pivot("mdm_banner_nm").sum("average_sales_uplift_per_measured_intervention").orderBy("call_month_current")

display(pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS asda_call_year_cur, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
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
group by 
asda_call_year_cur, standard_response_cd
ORDER BY
asda_call_year_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd.groupBy("asda_call_year_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as asda_call_period_cur,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
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
group by 
asda_call_period_cur, standard_response_cd
ORDER BY
asda_call_period_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd.groupBy("asda_call_period_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("asda_call_period_cur")

display(pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS asda_call_month_cur, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
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
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
asda_call_month_cur, standard_response_cd
ORDER BY
asda_call_month_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd.groupBy("asda_call_month_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("asda_call_month_cur")

display(pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS morrisons_call_year_cur, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7744 -- Morrisons
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
morrisons_call_year_cur, standard_response_cd
ORDER BY
morrisons_call_year_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd.groupBy("morrisons_call_year_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as morrisons_call_period_cur,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7744 -- Morrisons
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
morrisons_call_period_cur, standard_response_cd
ORDER BY
morrisons_call_period_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd.groupBy("morrisons_call_period_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("morrisons_call_period_cur")

display(pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS morrisons_call_mon, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7744 -- Morrisons
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
morrisons_call_mon, standard_response_cd
ORDER BY
morrisons_call_mon, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd.groupBy("morrisons_call_mon").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("morrisons_call_mon")

display(pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS sainsburys_call_year, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7745 -- Sainsburys
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
sainsburys_call_year, standard_response_cd
ORDER BY
sainsburys_call_year, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd.groupBy("sainsburys_call_year").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as sainsburys_call_period,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7745 -- Sainsburys
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
sainsburys_call_period, standard_response_cd
ORDER BY
sainsburys_call_period, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd.groupBy("sainsburys_call_period").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("sainsburys_call_period")

display(pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS sainsburys_call_month, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7745 -- Sainsburys
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
sainsburys_call_month, standard_response_cd
ORDER BY
sainsburys_call_month, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd.groupBy("sainsburys_call_month").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("sainsburys_call_month")

display(pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS tesco_call_year_cur, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7746 -- Tesco
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
tesco_call_year_cur, standard_response_cd
ORDER BY
tesco_call_year_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd.groupBy("tesco_call_year_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as tesco_call_period_cur,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7746 -- Tesco
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
tesco_call_period_cur, standard_response_cd
ORDER BY
tesco_call_period_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd.groupBy("tesco_call_period_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("tesco_call_period_cur")

display(pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS tesco_call_month_cur, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7746 -- Tesco
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
tesco_call_month_cur, standard_response_cd
ORDER BY
tesco_call_month_cur, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd.groupBy("tesco_call_month_cur").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("tesco_call_month_cur")

display(pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd)

# COMMAND ----------


