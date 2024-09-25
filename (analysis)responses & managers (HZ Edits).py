# Databricks notebook source
def get_data(client_id, retailer_id, start_date, end_date, rep_id_list = None, store_list = None):

    batch_ts_s = int(start_date.replace('-','') +'000000')
    batch_ts_e = int(end_date.replace('-','') +'235959')
    
    query = f"""

      select 

          b.Client_ID,
          b.TYPE,
          b.STORE_TDLINX_ID,
          b.STOREINFO,
          b.ANSWER,
          b.STARTDATE,
          b.ENDDATE,
          b.STORE_ID,
          b.RETAILER_ID,
          b.CALL_DATE, 
          b.CALL_ID,
          b.PRIORITY,
          b.ANSWERTYPE,
          b.ACTIONCOMPLETED,
          b.batch_ts,
          b.PRODUCT_ID,
          b.SHELFCODE,
          b.TM_USERNAME,
          b.TM_EMPNO,
          b.UM_EMPNO,

          a.DATE_EXECUTED,
          a.DURATION,
          a.EXEC_DURATION,
          a.DONE_STATUS,
          a.DONE_EMPNO,
          a.TM_EXEC_EMPNO

    from
        (select * , ROW_NUMBER() OVER(PARTITION BY store_id , product_id , call_id ORDER BY batch_ts desc) as r 
         from nars_raw.dpau as dpau
         where    dpau.client_id = \'{client_id}\'
                  and dpau.batch_ts >= \'{batch_ts_s}\'
                  and dpau.batch_ts <= \'{batch_ts_e}\'
                  and dpau.type = 'DLA'
                  and dpau.answer is not null
                  and dpau.retailer_id = \'{retailer_id}\'
                  ) as b    
        
        join   
          (select call_id, date_executed, extract_date, duration,
          done_status, exec_duration, DONE_EMPNO, TM_EXEC_EMPNO, TM_EXEC_TEAMCODE,
          ROW_NUMBER() OVER(PARTITION BY call_id ORDER BY extract_date desc) as rn 
          from nars_raw.agac
              ) as a

        on b.call_id = a.call_id
        where 
          a.rn = 1
          and b.r = 1
          and a.date_executed >= \'{start_date}\'
          and a.date_executed <= \'{end_date}\'         

    """
               
    df = spark.sql(query)
    df = df.dropDuplicates()
    
    if rep_id_list is not None:
        df = df.filter(pyf.col('DONE_EMPNO').isin(rep_id_list))
    
    if store_list is not None:
        df = df.filter(pyf.col('STORE_TDLINX_ID').isin(store_list))
      
    return df

# COMMAND ----------

config_dict = {
    'nestlewaters': {'company_id': 567, 'parent_id': 950, 'client_id' : '13429'},
    'danonewave': {'company_id': 603, 'parent_id': 955, 'client_id' : '882'},
    'barilla': {'client_id' : '9663'}
}

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # walmart barilla

# COMMAND ----------

retailer_id = 71
location = spark.sql(f"select holding_id, store_tdlinx_id, store_city, store_state_or_province from (select *, ROW_NUMBER() OVER(PARTITION BY STORE_TDLINX_ID ORDER BY store_id desc) as rn from acosta_retail_analytics_im.vw_dimension_store where holding_id = \'{retailer_id}\') as a  where a.rn = 1")\
    .withColumnRenamed('store_tdlinx_id','STORE_TDLINX_ID')
location.cache().count()

# COMMAND ----------

display(location)

# COMMAND ----------

client_id = '9663'
start_date, end_date = '2021-05-01', '2021-10-31'
df = get_data(client_id,retailer_id, start_date, end_date)
df.cache().count()

# COMMAND ----------

ft = spark.sql('select um_empno, sm_empno, rd_empno, vr_empno from (select *,  ROW_NUMBER() OVER(PARTITION BY um_empno ORDER BY extract_date desc) as rn  from nars_raw.ft) as a where a.rn = 1')
ft.cache().count()

# COMMAND ----------

df = df.join(ft, 'UM_EMPNO').join(location, 'STORE_TDLINX_ID')
df.cache().count()

# COMMAND ----------

# display(m)

# COMMAND ----------

# value measurement

def get_response_data_from_view(client_id, start_date , end_date):
    
    query = f"""
    select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars 
    where 
    mdm_client_id = \'{client_id}\' AND
    call_date >= \'{start_date}\' AND 
    call_date <= \'{end_date}\'
    
    """

    df = spark.sql(query)
    
    return df

def get_measurement_effect(client_id, start_date, end_date):
    
    query = f"""
    
          SELECT
          -- mdm_country_nm,
          -- mdm_holding_nm,
          -- mdm_client_nm,
          -- epos_organization_unit_num,
          -- call_id,
          response_id,
          total_intervention_effect,
          total_impact,
          WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week
          FROM 
          acosta_retail_analytics_im.ds_intervention_summary
          WHERE
          mdm_client_id = \'{client_id}\' AND
           call_date >= \'{start_date}\' AND 
            call_date <= \'{end_date}\'
    """
    
    df = spark.sql(query)
    
    return df

# COMMAND ----------

response_ids = get_response_data_from_view(client_id, start_date , end_date)
df = df.join(response_ids, 
#        (df['ORGANIZATION_UNIT_NUM'] == response_ids['epos_organization_unit_num']) #TODO: check if this is required
#         &
    (df['SHELFCODE'] == response_ids['epos_retailer_item_id']) 
    & (df['CALL_ID'] == response_ids['call_id']))\
.drop(response_ids['call_id'])\
.drop(response_ids['call_date'])

print(f'Number of responded alerts: {df.cache().count():,}')


measurement_effect = get_measurement_effect(client_id, start_date, end_date)
df_measurement = df.join(measurement_effect, 'response_id')
print(f'Number of responded alerts with measurement result: {df_measurement.cache().count():,}')


impact = df_measurement.select('DONE_EMPNO','response_id', 
       'total_intervention_effect',
       'total_impact')\
        .groupBy('DONE_EMPNO').sum()

# COMMAND ----------

display(df_measurement)

# COMMAND ----------

display(impact)

# COMMAND ----------

df_measurement.coalesce(1).write.format('csv')\
    .mode('overwrite')\
    .option('overwriteSchema', True)\
    .option('header', 'true')\
    .save(f'/mnt/processed/alerting/fieldTest/adhoc/manager_analysis_barilla_walmart_with_location_measurement_6m')

# COMMAND ----------

df.cache().count()

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

cols = ['RETAILER_ID','UM_EMPNO','TM_EMPNO','DONE_EMPNO', 'TM_EXEC_EMPNO', 'TM_PLAN_EMPNO', 'TM_EXEC_TEAMCODE']

# COMMAND ----------

df.columns

# COMMAND ----------

for c in cols:
    print(df[c].value_counts())

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table nars_raw.agac

# COMMAND ----------

df_mgr615 = df[df['UM_EMPNO'] == '992347615']

# COMMAND ----------

df_mgr615.ANSWER.value_counts()

# COMMAND ----------

df_mgr615.groupby(['TM_EMPNO', 'DONE_EMPNO', 'ANSWER']).size()

# COMMAND ----------

df[df['UM_EMPNO'] == '992373348'].ANSWER.value_counts()

# COMMAND ----------

df[df['UM_EMPNO'] == '992373348'].groupby(['TM_EMPNO', 'DONE_EMPNO', 'ANSWER']).size()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select answer, TM_EXEC_EMPNO, DONE_EMPNO from nars_raw.agac where um_empno = '992347615' and date_executed > '2021-10-15'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table nars_raw.ft

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table nars_raw.tm

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nars_raw.tm
# MAGIC where TM_EMPNO = '992280858' 
# MAGIC   or SUP_EMPNO = '992280858'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nars_raw.ft
# MAGIC where UM_EMPNO = '992347615' 
# MAGIC   or VR_EMPNO = '992347615' 
# MAGIC   or RD_EMPNO = '992347615'
# MAGIC   or SM_EMPNO = '992347615'

# COMMAND ----------

# MAGIC %sql
# MAGIC select TM_EMPNO, SUP_EMPNO from nars_raw.tm where TM_ACTIVE=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct TM_EMPNO), count(distinct SUP_EMPNO) from nars_raw.tm where TM_ACTIVE=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from nars_raw.zusavpr where tm_empno = 992280858

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from nars_raw.agac where done_empno = 992280858

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in nars_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in nars_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nars_raw.dpau limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct TM_EMPNO) from nars_raw.dpau 
# MAGIC where 
# MAGIC   client_id = '9663' 
# MAGIC   and RETAILER_ID = 71 
# MAGIC   and call_date > '2021-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nars_raw.agac limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct TM_EXEC_EMPNO) from nars_raw.agac where EXTRACT_DATE > '2021-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct DONE_EMPNO) from nars_raw.agac where EXTRACT_DATE > '2021-01-01'

# COMMAND ----------

# MAGIC %md
# MAGIC # kroger

# COMMAND ----------

df = get_data('882', 0, '2021-09-26', '2021-10-03')
df.cache().count()

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

cols = ['RETAILER_ID','UM_EMPNO','TM_EMPNO','DONE_EMPNO', 'TM_EXEC_EMPNO']
for c in cols:
    print(df[c].value_counts())

# COMMAND ----------

df[df['UM_EMPNO'] == '992342573'].ANSWER.value_counts().plot(kind='barh')

# COMMAND ----------

df[df['UM_EMPNO'] == '992342573'].groupby(['TM_EMPNO', 'DONE_EMPNO', 'ANSWER']).size()

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

um_mgr = '992227072'
df[df['UM_EMPNO'] == um_mgr].ANSWER.value_counts().plot(kind='barh')
plt.show()
print('')
df[df['UM_EMPNO'] == um_mgr].groupby(['TM_EMPNO', 'DONE_EMPNO', 'ANSWER']).size()

# COMMAND ----------

#kroger 992388240
um_mgr = '992388240'
df[df['UM_EMPNO'] == um_mgr].ANSWER.value_counts().plot(kind='barh')
plt.show()
print('')
df[df['UM_EMPNO'] == um_mgr].groupby(['TM_EMPNO', 'DONE_EMPNO', 'ANSWER']).size()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nars_raw.ft

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct UM_EMPNO), count(distinct VR_EMPNO), count(distinct RD_EMPNO), count(distinct SM_EMPNO) from nars_raw.ft

# COMMAND ----------

# MAGIC %md
# MAGIC # acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_retail_team
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_service limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_store where store_id = 1077372

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_team limit 10
