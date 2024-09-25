# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Data Exploration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Question Response table 
# MAGIC ### Mar 16 - Mar 19

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC  select * from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC   and channel_id = 1
# MAGIC   and client_id = 16320
# MAGIC   and master_client_id = 16320
# MAGIC   and country_id = 30
# MAGIC   and call_completed_date >= '2021-03-16'
# MAGIC   and call_completed_date <= '2021-03-19'

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many responses are in this table? 
# MAGIC _77,720  responses_

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC  
# MAGIC
# MAGIC select count(*) from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and country_id = 30
# MAGIC and call_completed_date >= '2021-03-16'
# MAGIC and call_completed_date <= '2021-03-19'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### How many unique responses category are in this table? 
# MAGIC _116 different responses_

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC  
# MAGIC select distinct response from (
# MAGIC select * from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and country_id = 30
# MAGIC and call_completed_date >= '2021-03-16'
# MAGIC and call_completed_date <= '2021-03-19'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Question Dimension table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date >= '2021-03-16'
# MAGIC and question_start_date <= '2021-03-19'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### How many questions are in this table? 
# MAGIC _299,933 questions_

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from vw_dimension_question
# MAGIC
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date >= '2021-03-16'
# MAGIC and question_start_date <= '2021-03-19'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### How many OSA (slow sales & Phantom Inventory) questions are in this table? 
# MAGIC _140,167 questions_

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from vw_dimension_question
# MAGIC
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date >= '2021-03-16'
# MAGIC and question_start_date <= '2021-03-19'
# MAGIC and (question like '%On Shelf Availability (Slow Sales)%' 
# MAGIC      or question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error)%')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Check a few cases

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC -- and question_start_date >= '2021-03-16'
# MAGIC -- and question_start_date <= '2021-03-19'
# MAGIC
# MAGIC and (question_id = '00U62U6QJ' 
# MAGIC             or question_id = '00U547BVJ' 
# MAGIC             or question_id = '00U5479O6')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select question_group, count(question) from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date >= '2021-01-01'
# MAGIC -- and question_start_date <= '2021-03-19'
# MAGIC
# MAGIC group by question_group

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select question_category, count(question) from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date >= '2021-01-01'
# MAGIC -- and question_start_date <= '2021-03-19'
# MAGIC
# MAGIC group by question_category

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC -- and question_start_date >= '2021-03-16'
# MAGIC -- and question_start_date <= '2021-03-19'
# MAGIC
# MAGIC and (question_id = '00U6HLQ4Y' 
# MAGIC             or question_id = '00U6HLQ0P' 
# MAGIC             or question_id = '00U6HLPO5')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC  select * from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC   and channel_id = 1
# MAGIC   and client_id = 16320
# MAGIC   and master_client_id = 16320
# MAGIC   and country_id = 30
# MAGIC   and call_completed_date >= '2021-03-16'
# MAGIC   and call_completed_date <= '2021-03-19'
# MAGIC   and (question_id = '00U6HLQ4Y' 
# MAGIC             or question_id = '00U6HLQ0P' 
# MAGIC             or question_id = '00U6HLPO5')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_question
# MAGIC
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date >= '2021-03-16'
# MAGIC and question_start_date <= '2021-03-16'
# MAGIC and (question like '%On Shelf Availability (Slow Sales)%' 
# MAGIC      or question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error)%')
# MAGIC   and (question_id = '00U5VJ558' 
# MAGIC             or question_id = '00U5VJ24C' 
# MAGIC             or question_id = '00U5VJ3SZ'
# MAGIC             or question_id = '00U5VJ27L')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC  select * from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC   and channel_id = 1
# MAGIC   and client_id = 16320
# MAGIC   and master_client_id = 16320
# MAGIC   and country_id = 30
# MAGIC   and call_completed_date >= '2021-03-16'
# MAGIC   and call_completed_date <= '2021-03-19'
# MAGIC   and (question_id = '00U5VJ558' 
# MAGIC             or question_id = '00U5VJ24C' 
# MAGIC             or question_id = '00U5VJ3SZ'
# MAGIC             or question_id = '00U5VJ27L')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## join responses and questions tables , and only keep OSA (slow sales & Phantom Inventory)
# MAGIC _8497 records_

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC   r.*, 
# MAGIC   i.category_description,
# MAGIC   q.question,
# MAGIC   q.question_type,
# MAGIC   q.question_start_date
# MAGIC   
# MAGIC   
# MAGIC from 
# MAGIC   (select a.*, b.subbanner_id , b.subbanner_description, b.market_id , b.market_description 
# MAGIC   from vw_fact_question_response a 
# MAGIC join
# MAGIC   (select store_id, subbanner_id , subbanner_description , market_id , market_description 
# MAGIC     from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC   and channel_id = 1
# MAGIC   and client_id = 16320
# MAGIC   and master_client_id = 16320
# MAGIC   and country_id = 30
# MAGIC   and call_completed_date >= '2021-03-16'
# MAGIC   and call_completed_date <= '2021-03-19'
# MAGIC   ) r
# MAGIC
# MAGIC join
# MAGIC (select   question_id,
# MAGIC           question,
# MAGIC           question_type,
# MAGIC           question_start_date
# MAGIC           
# MAGIC from vw_dimension_question
# MAGIC where 
# MAGIC      question like '%On Shelf Availability (Slow Sales)%' 
# MAGIC      or question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error)%'
# MAGIC ) q
# MAGIC on r.question_id = q.question_id
# MAGIC join 
# MAGIC (select item_level_id, category_description from acosta_UK_retail_analytics_im.vw_dimension_itemlevel
# MAGIC where client_id = 16320) i
# MAGIC on i.item_level_id = r.item_level_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Get Data

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

def get_data(start_date, end_date):
    query = f'''
       select
          r.*, 
          i.category_description,
          i.subcategory_description,
          q.question,
          q.question_type,
          q.question_start_date

        from 
          (select a.*, b.subbanner_id , b.subbanner_description, b.market_id , b.market_description 
          from vw_fact_question_response a 
        join
          (select store_id, subbanner_id , subbanner_description , market_id , market_description 
            from vw_dimension_store where banner_id = 7746) b
        on a.store_id = b.store_id
        where holding_id = 3257
        and channel_id = 1
        and client_id = 16320
        and master_client_id = 16320
        and country_id = 30
        and call_completed_date >= "{start_date}"
        and call_completed_date <= "{end_date}"
        ) r

        join
        (select   question_id,
                  question,
                  question_type,
                  question_start_date

        from vw_dimension_question
        where 
             question like '%On Shelf Availability (Slow Sales)%' 
             or question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error))%'
        ) q
        on r.question_id = q.question_id
        join 
        (select item_level_id, category_description, subcategory_description from acosta_UK_retail_analytics_im.vw_dimension_itemlevel
        where client_id = 16320) i
        on i.item_level_id = r.item_level_id
        
        ''' 

    df = spark.sql(query)
    
    return df

  
def response_analysis(start_date, end_date):
    df = get_data(start_date , end_date )
    print(f'N: {df.cache().count():,}')
    
    df = df.toPandas()
    
    print('\n')
    print(df.response.unique())
    print('\n')
    print(df.response.value_counts())
    df.response.value_counts().plot(kind = 'bar')
    plt.show()
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Alerts Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mar 16 - Mar 19

# COMMAND ----------

response_analysis(start_date = '2021-03-16', end_date = '2021-03-19')

# COMMAND ----------

# MAGIC %md
# MAGIC ## One day -Mar 16

# COMMAND ----------

response_analysis(start_date = '2021-03-16', end_date = '2021-03-16')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last 3 months & 7 months

# COMMAND ----------

response_analysis(start_date = '2021-01-01', end_date = '2021-03-30')

# COMMAND ----------

response_analysis(start_date = '2020-09-01', end_date = '2021-03-30')

# COMMAND ----------

# MAGIC %md
# MAGIC # Outlet Analysis
# MAGIC __there is no rep/employee data available__
# MAGIC

# COMMAND ----------

df = get_data(start_date = '2021-03-07', end_date = '2021-04-03')
print(df.cache().count())
display(df)

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subbanner

# COMMAND ----------

summary_table = pd.crosstab(df['subbanner_description'] ,  df['response'] ).sort_values(by = 'No Response', ascending = False)
summary_table

# COMMAND ----------

summary_table.apply(lambda r:r/r.sum(),axis=1).round(2).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Market

# COMMAND ----------

pd.set_option("display.max_rows", 200)

pd.crosstab(df['market_description'] ,  df['response'] ).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

pd.crosstab(df['market_description'] ,  df['response'] ).apply(lambda r:r/r.sum(),axis=1).round(2).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Category Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC category_description,
# MAGIC department_description
# MAGIC from acosta_UK_retail_analytics_im.vw_dimension_itemlevel
# MAGIC where client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(distinct
# MAGIC category_description),
# MAGIC count(distinct
# MAGIC department_description), 
# MAGIC count(distinct subcategory_description)
# MAGIC from acosta_UK_retail_analytics_im.vw_dimension_itemlevel
# MAGIC where client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

pd.crosstab(df['category_description'] ,  df['response'] ).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

pd.crosstab(df['category_description'] ,  df['response'] ).apply(lambda r:r/r.sum(),axis=1).round(2).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Subcategory

# COMMAND ----------

pd.crosstab(df['subcategory_description'] ,  df['response'] ).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

pd.crosstab(df['subcategory_description'] ,  df['response'] ).apply(lambda r:r/r.sum(),axis=1).round(2).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------


