# Databricks notebook source
# This notebook has a bunch of functions commonly used throughout the solution
# dbutils.widgets.remove('timestamp')

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

dbutils.widgets.dropdown('MODEL_SOURCE', 'local', ['local', 'prod'], 'Model Source')
dbutils.widgets.dropdown('INCLUDE_DISCOUNT_FEATURES', 'No', ['Yes', 'No'], 'Include Discount Features')

dbutils.widgets.text('start_date', '', 'Start Date (YYYYMMDD)')
dbutils.widgets.text('end_date', '', 'End Date (YYYYMMDD)')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

START_DATE = datetime.strptime(dbutils.widgets.get('start_date'), '%Y%m%d')
END_DATE = datetime.strptime(dbutils.widgets.get('end_date'), '%Y%m%d')
RUN_ID = dbutils.widgets.get('runid').strip()

MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'
INCLUDE_DISCOUNT_FEATURES = dbutils.widgets.get('INCLUDE_DISCOUNT_FEATURES').strip().lower() == 'yes'

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

if COUNTRY_CODE  == '':
    raise ValueError('\'countrycode\' is a required parameter.  Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())
elif RUN_ID.lower() == 'auto':
    RUN_ID = '-'.join([auto_model_prefix, CLIENT, END_DATE])

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, START_DATE, END_DATE, RUN_ID, MODEL_SOURCE, INCLUDE_DISCOUNT_FEATURES]:  
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

### INFERENCING SYSTEM

from dateutil import parser

def AddProductionInferencingWidgets():
  dbutils.widgets.text("retailer", "WALMART", "Retailer")
  dbutils.widgets.text("client", "CLOROX", "Client")
  dbutils.widgets.text("firstDate", "10/1/2018", "First Day to Predict")
  dbutils.widgets.text("numberOfDays", "7", "Number of Days to Predict")


def AddInferencingWidgets():
  AddProductionInferencingWidgets()
  dbutils.widgets.text("inferenceMode", "production", "Inference Mode")


def GetProductionInferencingParameters():  
  retailer = dbutils.widgets.get("retailer").strip().upper()
  client = dbutils.widgets.get("client").strip().upper()
  firstDateString = dbutils.widgets.get("firstDate").strip()
  numberOfDaysString = dbutils.widgets.get("numberOfDays").strip()

  if retailer == "":
    raise ValueError("\"retailer\" is a required parameter.  Please provide a value.")

  if client == "":
    raise ValueError("\"client\" is a required parameter.  Please provide a value.")

  if firstDateString == "":
    raise ValueError("\"firstDate\" is a required parameter.  Please provide a value.")
  else:
    try:
      firstDate = parser.parse(firstDateString).date()
    except ValueError:
      raise ValueError("\"{firstDateString}\" is not a valid date.  Please provide a valid date for \"firstDate\".".format(firstDateString = firstDateString))

  if numberOfDaysString == "":
    raise ValueError("\"numberOfDays\" is a required parameter.  Please provide a value.")
  else:
    try:
      numberOfDays = int(numberOfDaysString)
    except ValueError:
      raise ValueError("\"{numberOfDaysString}\" is not a valid integer.  Please provide a valid integer for \"numberOfDays\".".format(numberOfDaysString = numberOfDaysString))

  if numberOfDays <= 0:
    raise ValueError("\"numberOfDays\" must be greater than zero.")
  
  output = {}
  output["retailer"] = retailer
  output["client"] = client
  output["firstDate"] = firstDate
  output["numberOfDays"] = numberOfDays
  
  return output
    
def GetInferencingParameters():
  output = GetProductionInferencingParameters()

  inferenceMode = dbutils.widgets.get("inferenceMode").strip().upper()

  if inferenceMode == "":
    raise ValueError("\"inferenceMode\" is a required parameter.  Please provide a value.")

  if inferenceMode not in ["PRODUCTION", "TESTING"]:
    raise ValueError("\"{inferenceMode}\" is not a valid value for \"inferenceMode\".  It must be \"PRODUCTION\" or \"TESTING\".".format(inferenceMode = inferenceMode))
  
  output["inferenceMode"] = inferenceMode
  
  return output


# COMMAND ----------

# READ SALES DATA
# Read the sales data for a particular database and apply the "Common Transformations"
def ReadSalesData(params):
  spark.conf.set('spark.sql.hive.manageFilesourcePartitions', 'false')
  
  sqlStatement = '''
    SELECT *
       FROM azam.sales
       WHERE RETAILER = '{Retailer}'
          AND CLIENT = '{Client}'
  '''

  sqlStatement = sqlStatement.format(Retailer = params["retailer"], Client = params["client"])

  
  df = sqlContext.sql(sqlStatement)
  
  return df

# COMMAND ----------

print('Got here')
