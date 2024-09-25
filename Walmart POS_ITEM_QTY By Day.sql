-- Databricks notebook source
use retaillink_walmart_gpconsumerproductsoperation_us_dv;

select sales_dt, sum(pos_item_qty) as total_pos_item_qty
from sat_epos_summary
group by sales_dt
order by total_pos_item_qty desc

-- COMMAND ----------

use retaillink_walmart_barillaamericainc_us_dv;

select sales_dt, sum(pos_item_qty)
from sat_epos_summary
where sales_dt between '2021-05-01' and '2021-05-14'
group by sales_dt
order by sales_dt

-- COMMAND ----------

use retaillink_walmart_edgewellpersonalcare_us_dv;

select sales_dt, sum(pos_item_qty)
from sat_epos_summary
where sales_dt between '2021-05-01' and '2021-05-14'
group by sales_dt
order by sales_dt

-- COMMAND ----------


