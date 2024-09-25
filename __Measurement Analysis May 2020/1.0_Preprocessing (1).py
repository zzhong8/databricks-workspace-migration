# Databricks notebook source
# MAGIC %md
# MAGIC # Preprocessing Data for Intervention Measurement
# MAGIC
# MAGIC POS engineered features must have been created and then merged into intervention labelled data before attempting to fit models.

# COMMAND ----------

import datetime


def generate_date_series(start, stop):
    return [start + datetime.timedelta(days=d) for d in range((stop - start).days + 1)]

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')


dbutils.widgets.dropdown('source', 'prod', ['local', 'prod'], 'Data Source')

# COMMAND ----------

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

# RUN_ID = dbutils.widgets.get('runid').strip()
SOURCE = dbutils.widgets.get('source').upper()

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')

# if RUN_ID == '':
#     raise ValueError('\'runid\' is a required parameter. Please provide a value.')

# PATHS (new!)
PATH_RESULTS_OUTPUT = '/mnt{mod}/artifacts/country_code/training_results/retailer={retailer}/client={client}/country_code={country_code}/'.format(
    mod='' if SOURCE == 'LOCAL' else '/prod-ro',
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)

# PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt{mod}/processed/training/{run_id}/engineered/'.format(
#     mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
#     run_id=RUN_ID
# )

# print(RUN_ID)

# COMMAND ----------

# TODO Temp delete these vars and properly replace them
company_id = 446 # TODO rename var

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data
# MAGIC
# MAGIC - Intervention data
# MAGIC - POS data

# COMMAND ----------

sql_statement = """
    SELECT 
        CallfileId 
        , date_add(Min_SurveyResponseDate, DayStart) AS StartDate 
        , date_add(Max_SurveyResponseDate, DayEnd) AS EndDate 
    FROM (SELECT 
                cf.CallfileId 
                , MIN(CAST(sr.VisitDate AS DATE)) AS Min_SurveyResponseDate 
                , MAX(CAST(sr.VisitDate AS DATE)) AS Max_SurveyResponseDate 
            FROM BOBv2.Campaign ca 
            INNER JOIN BOBv2.Callfile cf 
              ON ca.CampaignId = cf.CampaignId 
            INNER JOIN BOBv2.CallfileVisit cfv 
                ON cf.CallfileId = cfv.CallfileId 
            INNER JOIN BOBv2.SurveyResponse sr 
                ON cfv.CallfileVisitId = sr.CallfileVisitId 
            WHERE 
              ca.CompanyId = {company_id} 
            GROUP BY 
                cf.CallfileId) cf 
    CROSS JOIN (SELECT Min(DayStart) AS DayStart, MAX(DayEnd) AS DayEnd
                    FROM BOBv2.reachetl_interventions_parameternew 
                    WHERE CompanyId = {company_id}) ivp 
    WHERE 
        date_add(Max_SurveyResponseDate, DayEnd) >= current_date
    """

test = spark.sql(sql_statement.format(company_id=company_id))
display(test)

# COMMAND ----------

# Load intervention data
df_call_file = spark.sql(f"""
    SELECT 
        CallfileId 
        , date_add(Min_SurveyResponseDate, DayStart) AS StartDate 
        , date_add(Max_SurveyResponseDate, DayEnd) AS EndDate 
    FROM (SELECT 
                cf.CallfileId 
                , MIN(CAST(sr.VisitDate AS DATE)) AS Min_SurveyResponseDate 
                , MAX(CAST(sr.VisitDate AS DATE)) AS Max_SurveyResponseDate 
            FROM BOBv2.Campaign ca 
            INNER JOIN BOBv2.Callfile cf 
              ON ca.CampaignId = cf.CampaignId 
            INNER JOIN BOBv2.CallfileVisit cfv 
                ON cf.CallfileId = cfv.CallfileId 
            INNER JOIN BOBv2.SurveyResponse sr 
                ON cfv.CallfileVisitId = sr.CallfileVisitId 
            WHERE 
              ca.CompanyId = {company_id} 
            GROUP BY 
                cf.CallfileId) cf 
    CROSS JOIN (SELECT Min(DayStart) AS DayStart, MAX(DayEnd) AS DayEnd
                    FROM BOBv2.reachetl_interventions_parameternew 
                    WHERE CompanyId = {company_id}) ivp 
    WHERE 
        date_add(Max_SurveyResponseDate, DayEnd) >= current_date
    """)

display(df_call_file)

# COMMAND ----------


