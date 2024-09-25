# Databricks notebook source
import numpy as np
import pandas as pd

import seaborn as sns
import matplotlib.pyplot as graph

from scipy import stats

import pyspark.sql.functions as pyf

from acosta.alerting.preprocessing.functions import get_params_from_database_name
from expectation.functions import get_pos, pivot_pos


def parse_widget_or_raise(key, raise_error=True):
    x = str(dbutils.widgets.get(key)).strip().lower()
    if raise_error and x == '':
        raise ValueError(f'Could not find key {key}')
    else:
        return x

# COMMAND ----------

dbutils.widgets.text('database', 'market6_kroger_danonewave_us_dv', 'Database Name')

required_inputs = ('database')

database_name  = parse_widget_or_raise('database')
source, country_code, client, retailer = get_params_from_database_name(database_name).values()

print(client)
print(country_code)
print(retailer)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get POS Data

# COMMAND ----------

# pos_database = 'retaillink_walmart_nestlewaters_us_dv'

df_pos = get_pos(database_name, spark)

# df_pos = spark.sql(f'select * from {database}.vw_latest_sat_epos_summary')
print(f'N = {df_pos.cache().count():,}')
df_pos = df_pos.filter(pyf.year(df_pos['sales_dt']) == 2024)
df_pos = df_pos.withColumn('organization_unit_num', pyf.col('organization_unit_num').cast('int'))

print(f'N = {df_pos.cache().count():,}')

# COMMAND ----------

display(df_pos)

# COMMAND ----------

df_pos\
    .write \
    .format('delta') \
    .mode('overwrite') \
    .save(f'/mnt/processed/alerting/fieldTest_Tesco_NestleCore/experiment_{retailer}_{client}_{country_code}_2024')

# COMMAND ----------

path = f'/mnt/processed/alerting/fieldTest_Tesco_NestleCore/experiment_{retailer}_{client}_{country_code}_2024'

df_pos = spark.read.format('delta').option('inferSchema', True).load(path)
print(f'{df_pos.count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Covered Stores

# COMMAND ----------

df_valid_stores = spark.read\
    .options(header=True, inferSchema=True)\
    .csv('/mnt/processed/alerting/fieldTest_Tesco_NestleCore/Tesco_dimension_store_2024.csv')\
    .withColumn('organization_unit_num', pyf.col('StoreNo'))\
    .toPandas()

display(df_valid_stores)

# COMMAND ----------

df_agg = df_pos.select('organization_unit_num', 'pos_item_qty', 'pos_amt').groupby('organization_unit_num').sum().toPandas()
df_agg = df_agg.drop(columns=['sum(organization_unit_num)'])
df_agg.columns = ['organization_unit_num', 'pos_item_qty', 'pos_amt']
df_agg = df_agg.sort_values('pos_amt', ascending=False)

display(df_agg)

# COMMAND ----------

print(df_agg.shape)

# COMMAND ----------

df_experiment = df_valid_stores.merge(
    df_agg,
    on='organization_unit_num'
)

df_experiment = df_experiment\
    .sort_values('pos_amt', ascending=False)\
    .reset_index(drop=True)\

display(df_experiment)

# COMMAND ----------

print(df_experiment.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Historical Alert Responses

# COMMAND ----------

def get_data(start_date, end_date):
    query = f'''
       select
          r.*, 
          i.category_description,
          i.subcategory_description,
          q.question

        from 
          (select a.*, b.subbanner_id , b.subbanner_description, b.market_id , b.market_description 
          from acosta_retail_analytics_im.vw_fact_question_response a 
        join
          (select store_id, subbanner_id , subbanner_description , market_id , market_description 
            from acosta_retail_analytics_im.vw_dimension_store where banner_id = 7746) b
        on a.store_id = b.store_id
        where holding_id = 3257
        and channel_id = 1
        and client_id = 16320
        and country_id = 30
        and call_completed_date >= "{start_date}"
        and call_completed_date <= "{end_date}"
        ) r

        join
        (select   question_id,
                  question

        from acosta_retail_analytics_im.vw_dimension_question
        where 
             question like '%On Shelf Availability (Slow Sales)%' 
        ) q
        on r.question_id = q.question_id
        join 
        (select item_level_id, category_description, subcategory_description from acosta_retail_analytics_im.vw_dimension_itemlevel
        where client_id = 16320) i
        on i.item_level_id = r.item_level_id
        
        ''' 

    df = spark.sql(query)
    
    return df

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT objective_typ, min(call_date), max(call_date) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC -- and 
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and 
# MAGIC call_date >= "2023-01-01"
# MAGIC group by objective_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT objective_typ, min(call_date), max(call_date) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC -- and 
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and 
# MAGIC call_date >= "2023-01-01"
# MAGIC group by objective_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT objective_typ, min(call_date), max(call_date) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC -- and 
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and 
# MAGIC nars_response_text not like '%,%'
# MAGIC and 
# MAGIC call_date >= "2023-01-01"
# MAGIC group by objective_typ

# COMMAND ----------

def get_intervention_data(start_date, end_date):
    query = f'''
        SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
        where
        mdm_country_id = 30 -- UK
        and
        mdm_client_id = 16320 -- Nestle
        and
        mdm_holding_id = 3257 -- Acosta UK
        and
        mdm_banner_id = 7746 -- Tesco
        and 
        objective_typ = 'DLA'
        and 
        nars_response_text not like '%,%'
        and call_date >= "{start_date}"
        and call_date <= "{end_date}"
        ''' 

    df = spark.sql(query)
    
    return df

# COMMAND ----------

def get_measured_intervention_data(start_date, end_date):
    query = f'''
        SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
        where
        mdm_country_id = 30 -- UK
        and
        mdm_client_id = 16320 -- Nestle
        and
        mdm_holding_id = 3257 -- Acosta UK
        and
        mdm_banner_id = 7746 -- Tesco
        and 
        objective_typ = 'DLA'

        and call_date >= "{start_date}"
        and call_date <= "{end_date}"
        ''' 

    df = spark.sql(query)
    
    return df

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table acosta_retail_analytics_im.vw_dimension_question

# COMMAND ----------

# MAGIC %sql
# MAGIC select  question_id,
# MAGIC         question,
# MAGIC         question_type,
# MAGIC         question_start_date,
# MAGIC         master_client_id
# MAGIC
# MAGIC from acosta_retail_analytics_im.vw_dimension_question
# MAGIC where 
# MAGIC      (question like '%On Shelf Availability (Slow Sales)%' 
# MAGIC      or question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error))%')
# MAGIC      -- and question_start_date = '2021-03-17'
# MAGIC      and client_id = 16320
# MAGIC --     and master_client_id = 16320

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(question_id)
# MAGIC
# MAGIC from acosta_retail_analytics_im.vw_dimension_question
# MAGIC where 
# MAGIC      (question like '%On Shelf Availability (Slow Sales)%' 
# MAGIC      or question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error))%')
# MAGIC      and question_start_date = '2021-03-16'
# MAGIC      and client_id = 16320
# MAGIC     and master_client_id = 916320

# COMMAND ----------

# 3 months of alert responses
interventions_df = get_intervention_data('2024-01-01', '2024-03-31')
print(interventions_df.cache().count())

# COMMAND ----------

# # 3 months of alert responses
# alerts_df = get_data('2024-01-01', '2024-03-31')
# print(alerts_df.cache().count())

# COMMAND ----------

# 3 months of alert responses
measured_interventions_df = get_measured_intervention_data('2024-01-01', '2024-03-31')
print(measured_interventions_df.cache().count())

# COMMAND ----------

display(measured_interventions_df)

# COMMAND ----------

alerts_df = measured_interventions_df

# COMMAND ----------

alerts_df = alerts_df.toPandas()

# COMMAND ----------

alerts_df.dtypes

# COMMAND ----------

alerts_df['epos_organization_unit_num'] = alerts_df['epos_organization_unit_num'].astype(int)

# COMMAND ----------

alerts_df.standard_response_text.value_counts()

# COMMAND ----------

print(len(alerts_df.epos_organization_unit_num.unique()))

# COMMAND ----------

alerts_df

# COMMAND ----------

alerts_summary = alerts_df.groupby('epos_organization_unit_num')['total_impact'].agg(['sum','count']).rename(columns={'sum': 'total_impact', 'count': 'total_interventions'}).sort_values(('total_interventions'), ascending = False)
alerts_summary = alerts_summary.reset_index()

alerts_summary['epos_organization_unit_num'] = alerts_summary['epos_organization_unit_num'].astype(str).astype(int)
alerts_summary['total_impact'] = alerts_summary['total_impact'].astype(float)
alerts_summary['total_interventions'] = alerts_summary['total_interventions'].astype(int)

# COMMAND ----------

df_experiment

# COMMAND ----------

# def executed(ans):
#     if ans == 'null':
#         return 0 
#     else:
#         return 1

# alerts_df['EXECUTED'] = alerts_df['standard_response_text'].apply(executed)
                                
                                
# alerts_summary = alerts_df.groupby('epos_organization_unit_num')[['EXECUTED']].agg(['sum', 'count']).rename(columns={'sum': 'Measurable_Alerts', 'count': 'Total_Alerts'}).sort_values(('EXECUTED', 'Total_Alerts'), ascending = False)['EXECUTED']
# alerts_summary['Measurable_prct'] = alerts_summary['Measurable_Alerts']/ alerts_summary['Total_Alerts']
# alerts_summary = alerts_summary.reset_index()
# alerts_summary

# COMMAND ----------

df_expr_alrt = pd.merge( df_experiment,alerts_summary, left_on = 'StoreNo' , right_on = 'epos_organization_unit_num', how = 'left')
df_expr_alrt.shape

# COMMAND ----------

df_expr_alrt.head()

# COMMAND ----------

# df_expr = spark.createDataFrame(df_expr_alrt)
# df_expr.coalesce(1).write.mode('overwrite').option('header', True).csv('/mnt/processed/alerting/fieldTest/NestleWater_intermidatedata_casualporpotional')

# COMMAND ----------

#Total Alerts in this duration for these top 1000 stores
# df_expr_alrt.Total_Alerts.sum() , df_expr_alrt.Total_Alerts.sum()/len(alerts_df)

# COMMAND ----------

# df_expr_alrt = df_expr_alrt.fillna(value = {'Executed_prct' : 0.0 , 'Total_Alerts': 0.0})

# COMMAND ----------

# MAGIC %md
# MAGIC # Assign Stores to Control and Test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latin Hypercube method

# COMMAND ----------

from bisect import bisect
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import ks_2samp

import numpy as np
import pandas as pd
import warnings


np.random.seed = 42


def quantile_list(col, n = 4):
    
    if col.dtypes not in ['int', 'float']:
        col = col.astype('float64')
        
    return col.quantile(np.arange(0,1,1/n)).values


def cube_number(df, row, cols, n= 4):
    c_no = ''
    
    for col in cols:
        c_no  += str(bisect(quantile_list(df[col], n=n), row[col]))
    
    return c_no


def assign_groups(df, cols, n= 4, test_prct = 0.5):
    no_test_stores = int(np.floor(len(df) * test_prct))
    
    df['Cube_NO'] = df.apply(lambda x: cube_number(df = df, row = x, cols =cols, n=n), axis=1)

    df['experiment_group'] = None
    list_of_sampled_groups = []

    for (_ , group) , n in zip(df.groupby('Cube_NO'), df.groupby('Cube_NO').size()):

        n_rows_to_sample = int(np.random.choice([np.ceil(n/2),np.floor(n/2)], 1 )[0])

        sampled_group = group.sample(n_rows_to_sample)
        list_of_sampled_groups.append(sampled_group)
        
        df.loc[sampled_group.index, 'experiment_group'] = 'test'


    n_test = len(df[df.experiment_group == 'test'])

    if n_test > no_test_stores:    
        c_idx = np.random.choice(df.index[df.experiment_group == 'test'], size = n_test - no_test_stores)
        df.loc[c_idx, 'experiment_group'] = None

    if n_test < no_test_stores:
        c_idx = np.random.choice(df.index[df.experiment_group != 'test'], size = no_test_stores - n_test)
        df.loc[c_idx, 'experiment_group'] = 'test'


    df['experiment_group'] = np.where(df['experiment_group'].isna(), 'control', df['experiment_group'])
    print(df['experiment_group'].value_counts())
    
    return df
   
    
def similarity_test(df,cols, cat_cols = None, alpha = 0.05):
    
    for col in cols:
        
        print(f'Kolmogorov-Smirnov for ****{col}*****')
        _, pval = ks_2samp(df.loc[df['experiment_group'] ==  'test'][col].values, df.loc[df['experiment_group']  ==  'control'][col].values)
        
        #TODO: check if it is better to raise a warning
        if pval < alpha:  
            warnings.warn('WARNINGS: Two groups are significantly different!')
        print('***'*8)    
        print(ks_2samp(df.loc[df['experiment_group'] ==  'test'][col].values, df.loc[df['experiment_group']  ==  'control'][col].values))
        
        
        colors= ['blue', 'red']
        plt.figure(figsize = (7,4))
        for i, g in enumerate(['test','control']):
            sns.distplot(df.loc[df['experiment_group']  ==  g][col], kde=True, color = colors[i] )
        plt.show()
        
    if cat_cols is not None:     
        for cat in cat_cols:
            print('***'*8)
            plt.figure(figsize=(8, 10))
            sns.countplot(y=cat, hue='experiment_group', data=df, palette = 'Pastel1')
            plt.show()        
 

# COMMAND ----------

cols = ['pos_item_qty', 'pos_amt', 'total_impact', 'total_interventions']
df = assign_groups(df = df_expr_alrt, cols = cols, n = 4)

df.head(10)

# COMMAND ----------

similarity_test(df, cols = cols, cat_cols = ['StoreName'])

# COMMAND ----------

df.groupby('experiment_group')[['pos_item_qty']].mean()

# COMMAND ----------

df.groupby('experiment_group')[['pos_amt']].mean()

# COMMAND ----------

df.groupby('experiment_group')[['total_impact']].mean()

# COMMAND ----------

df.groupby('experiment_group')[['total_interventions']].mean()

# COMMAND ----------

# Save results
df_summary = df[['StoreName', 'StoreNo', 'Address', 'City', 'pos_amt', 'experiment_group']]
df_summary = spark.createDataFrame(df_summary)
df_summary.coalesce(1).write.mode('overwrite').option('header', True).csv(f'/mnt/processed/alerting/fieldTest_Tesco_NestleCore/experiment_{retailer}_{client}_{country_code}_2024B.csv')
display(df_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assigned stores only based on pos_amt and location

# COMMAND ----------

cols_3d = ['Latitude', 'Longitude', 'pos_amt']
df_3d = assign_groups(df = df_expr_alrt, cols = cols_3d, n = 8)

df_3d.head()

# COMMAND ----------

similarity_test(df_3d, cols = cols_3d, cat_cols = ['TerritoryManager', 'Sub-Banner'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Simpler method - when numeric features are not available

# COMMAND ----------

lhs_test = [i for i in df_experiment.index if i % 2 == 0]
lhs_control = [i for i in df_experiment.index if i % 2 == 1]

print(lhs_test[:5])
print(lhs_control[:5])

# COMMAND ----------

df_experiment['experiment_group'] = 'none'
df_experiment.loc[lhs_test, 'experiment_group'] = 'test'
df_experiment.loc[lhs_control, 'experiment_group'] = 'control'

df_test = df_experiment.loc[lhs_test]
df_control = df_experiment.loc[lhs_control]

# COMMAND ----------

# Save results
df_summary = df_experiment[['Store#', 'TD Linx', 'Division #', 'Market', 'City', 'State', 'pos_amt', 'experiment_group']]
df_summary = spark.createDataFrame(df_summary)
df_summary.coalesce(1).write.mode('overwrite').option('header', True).csv('/mnt/processed/experiment/alerting-g2/experiment-nestlewaters.csv')

display(df_summary)

# COMMAND ----------

graph.figure(figsize=(8, 4))
graph.title(f'KS Test = {stats.ks_2samp(df_test["pos_amt"], df_control["pos_amt"])}')
sns.distplot(df_test['pos_amt'], kde=False)
sns.distplot(df_control['pos_amt'], kde=False)
graph.xscale('log')
graph.show()

for strat in ['Division #', 'Market', 'State']:
    graph.figure(figsize=(8, 10))
    sns.countplot(y=strat, hue='experiment_group', data=df_experiment)
    graph.show()

# COMMAND ----------

new_ctrl= pd.merge( df_expr_alrt, df_summary, on = 'TD Linx' )
new_ctrl.head()

# COMMAND ----------

len(new_ctrl[new_ctrl['experiment_group_y'] == 'test'])

# COMMAND ----------

new_ctrl.sort_values('Cube_NO')[:10]
