# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.alerting.preprocessing.functions import get_pos_data
from acosta.measurement import required_columns, process_notebook_inputs

import acosta

print(acosta.__version__)

# COMMAND ----------

# Inputs get required inputs
dbutils.widgets.text('country_id', '-1', 'Country ID')
dbutils.widgets.text('client_id', '-1', 'Client ID')
dbutils.widgets.text('holding_id', '-1', 'Holding ID')
dbutils.widgets.text('banner_id', '-1', 'Banner ID')

input_names = ('country_id', 'client_id', 'holding_id', 'banner_id')

country_id, client_id, holding_id, banner_id = [process_notebook_inputs(dbutils.widgets.get(s)) for s in input_names]

print('Country ID =', country_id)
print('Client ID =', client_id)
print('Holding ID =', holding_id)
print('Banner ID =', banner_id)

# COMMAND ----------

# All Morrisons clients are UK clients, so whatever just run it
dbutils.notebook.run("./1.0_CacheMeasurementData (Morrisons 2024 IVM Test)", 0, {"country_id": country_id, "client_id": client_id, "holding_id": holding_id, "banner_id": banner_id})

dbutils.notebook.run("./2.0_Fitting (Morrisons 2024 IVM Test)", 0, {"country_id": country_id, "client_id": client_id, "holding_id": holding_id, "banner_id": banner_id})

dbutils.notebook.run("./3.0_PostHocCorrections (Morrisons 2024 IVM Test)", 0, {"country_id": country_id, "client_id": client_id, "holding_id": holding_id, "banner_id": banner_id})
