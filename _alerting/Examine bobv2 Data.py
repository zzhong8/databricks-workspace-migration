# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, date
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf

# COMMAND ----------

survey_item_response = spark.sql('SELECT * FROM bobv2.surveyitemresponse')

survey_item_response.printSchema()

# COMMAND ----------

survey_item_response.count()

# COMMAND ----------

survey_response = spark.sql('SELECT * FROM bobv2.surveyitemresponse')

survey_response.printSchema()

# COMMAND ----------

survey_response.count()

# COMMAND ----------

intervention_parameters = spark.sql('SELECT * FROM bobv2.ReachETL_Interventions_ParameterNew')

intervention_parameters.printSchema()

# COMMAND ----------

intervention_parameters.count()

# COMMAND ----------

display(intervention_parameters)

# COMMAND ----------

company_ids = intervention_parameters.select('CompanyId').distinct().orderBy('CompanyId', ascending=True)

company_ids.count()

# COMMAND ----------

display(company_ids)

# COMMAND ----------


