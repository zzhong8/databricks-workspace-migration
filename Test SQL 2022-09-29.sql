-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC import sys
-- MAGIC import pyarrow
-- MAGIC
-- MAGIC print(sys.version)
-- MAGIC print(pyarrow.__version__)

-- COMMAND ----------

use acosta_retail_analytics_im;

-- COMMAND ----------

desc acosta_retail_analytics_im.vw_fact_question_response

-- COMMAND ----------

desc acosta_retail_analytics_im.vw_dimension_question

-- COMMAND ----------

select
question_id,
question,
question_name,
question_group,
question_category,
question_status,
question_type
from 
vw_dimension_question
where
client_id = 851 -- Campbells
and
question_type = 'Data Led Alerts'

-- COMMAND ----------

select
a.*,
b.question_id,
b.question,
b.question_name,
b.question_group,
b.question_category,
b.question_status,
b.question_type,

from vw_fact_question_response a 
join vw_dimension_question b
on a.question_id = b.question_id

where a.holding_id = 71 -- Walmart
and a.client_id = 851 -- Campbells
and a.country_id = 1 -- US
and a.call_completed_date >= '2022-01-01'
and a.call_completed_date <= '2022-12-31'
and b.question_type = 'Data Led Alerts'

-- COMMAND ----------


