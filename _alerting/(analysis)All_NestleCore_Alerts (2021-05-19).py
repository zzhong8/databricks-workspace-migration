# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Analysis of NestleCore Responses

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Get Data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select distinct
# MAGIC a.response_type_code,
# MAGIC a.response_type,
# MAGIC a.call_type_code,
# MAGIC a.call_type_id,
# MAGIC a.response_category,
# MAGIC q.question_type
# MAGIC from vw_fact_question_response a 
# MAGIC join
# MAGIC (
# MAGIC   select distinct store_id from vw_dimension_store where banner_id = 7746 -- Tesco
# MAGIC ) b
# MAGIC on a.store_id = b.store_id
# MAGIC join
# MAGIC (
# MAGIC   select
# MAGIC   question_id,
# MAGIC   question_type
# MAGIC   from acosta_UK_retail_analytics_im.vw_dimension_question
# MAGIC ) q
# MAGIC on a.question_id = q.question_id
# MAGIC where holding_id = 3257 -- AcostaRetailUK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK
# MAGIC and master_client_id = 16320 -- Nestle UK
# MAGIC and country_id = 30 -- UK
# MAGIC and call_completed_date >= '2021-01-01'
# MAGIC and call_completed_date <= '2021-06-01'

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
# MAGIC ## Alerts Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Last 3 months & 7 months

# COMMAND ----------

# response_analysis(start_date = '2021-01-01', end_date = '2021-03-30')

# COMMAND ----------

spark_df = get_data(start_date = '2021-01-01', end_date = '2021-12-31')
# display(spark_df)

# COMMAND ----------

df = spark_df.toPandas()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df['question_id'].count()

# COMMAND ----------

df['question_type'].unique()

# COMMAND ----------

df_filtered = df[df["response"].isin(['Full Gondola', 'Full Ladder Rack', 'Full Pallet'])]

df_filtered['question_id'].count()

# COMMAND ----------

df_filtered['question_type'].unique()

# COMMAND ----------

df_filtered_selected_columns = df_filtered[['response_type_code',
                                            'response_type',
                                            'call_type_code',
                                            'call_type_id',
                                            'response_category',
                                            'question_type']].drop_duplicates()

# COMMAND ----------

df_filtered_selected_columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Call Completed Date

# COMMAND ----------

pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 130)

summary_table = pd.crosstab(df['call_completed_date'] ,  df['response'])
summary_table
