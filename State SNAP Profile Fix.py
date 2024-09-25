# Databricks notebook source
mapped_stores_states = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/orgunitnum_to_state.csv')

    #join unique store names to mapped stores states
    #join mapped state to pay cycles state
    #drop mapped states and original state


# COMMAND ----------

display(mapped_stores_states)

# COMMAND ----------

mapped_stores_states_2 = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/artifacts/reference/orgunitnum_to_state.csv')

    #join unique store names to mapped stores states
    #join mapped state to pay cycles state
    #drop mapped states and original state


# COMMAND ----------

display(mapped_stores_states_2)

# COMMAND ----------


