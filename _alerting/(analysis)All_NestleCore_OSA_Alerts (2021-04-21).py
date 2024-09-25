# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Get Data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_dimension_question
# MAGIC
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date < '2021-03-01'
# MAGIC and (question like '%On Shelf Availability%')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_dimension_question
# MAGIC
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date >= '2021-03-01'
# MAGIC and (question like '%On Shelf Availability%')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC  select count(*) from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC   and channel_id = 1
# MAGIC   and client_id = 16320
# MAGIC   and master_client_id = 16320
# MAGIC   and country_id = 30
# MAGIC   and call_completed_date >= '2021-03-16'
# MAGIC   and call_completed_date <= '2021-03-19'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC  select count(*) from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id in (7746)) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC   and channel_id = 1
# MAGIC   and client_id = 16320
# MAGIC   and master_client_id = 16320
# MAGIC   and country_id = 30
# MAGIC   and call_completed_date >= '2021-03-16'
# MAGIC   and call_completed_date <= '2021-03-19'

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
          (select a.*, b.banner_id, b.banner_description, b.subbanner_id, b.subbanner_description, b.market_id, b.market_description 
          from acosta_UK_retail_analytics_im.vw_fact_question_response a 
        join
          (select store_id, banner_id , banner_description, subbanner_id, subbanner_description, market_id, market_description 
            from acosta_UK_retail_analytics_im.vw_dimension_store where (banner_id >= 7743 and banner_id <= 7746)) b
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

        from acosta_UK_retail_analytics_im.vw_dimension_question
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Alerts Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last 3 months & 7 months

# COMMAND ----------

response_analysis(start_date = '2021-01-01', end_date = '2021-03-30')

# COMMAND ----------

# MAGIC %md
# MAGIC # Outlet Analysis
# MAGIC __there is no rep/employee data available__
# MAGIC

# COMMAND ----------

# df = get_data(start_date = '2021-03-07', end_date = '2021-04-03')
# print(df.cache().count())
# display(df)

# COMMAND ----------

df = get_data(start_date = '2021-01-01', end_date = '2021-03-31')
display(df)

# COMMAND ----------

print(df.cache().count())

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Banner and Subbanner

# COMMAND ----------

summary_table = pd.crosstab(df['banner_description'] ,  df['response'] ).sort_values(by = 'No Response', ascending = False)
summary_table

# COMMAND ----------

summary_table.apply(lambda r:r/r.sum(),axis=1).round(2).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

summary_table = pd.crosstab(df['subbanner_description'] ,  df['response'] ).sort_values(by = 'No Response', ascending = False)
summary_table

# COMMAND ----------

summary_table.apply(lambda r:r/r.sum(),axis=1).round(2).sort_values(by = 'No Response', ascending = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Market

# COMMAND ----------

pd.set_option("display.max_rows", 205)

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


