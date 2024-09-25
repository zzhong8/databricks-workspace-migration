# Databricks notebook source
# This notebook has a bunch of functions commonly used throughout the solution

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


