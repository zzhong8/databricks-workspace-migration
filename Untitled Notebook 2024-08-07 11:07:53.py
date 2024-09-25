# Databricks notebook source
# MAGIC %sql
# MAGIC select c.id, c.soid, a.upc, c.answer
# MAGIC
# MAGIC from premium360_dv.sat_answer_custom_dataentry c
# MAGIC
# MAGIC Left join premium360_dv.sat_store_alerts a on c.externaldataid = a.id and c.soid = a.soid and a.audit_current_ind = 1
# MAGIC
# MAGIC where c.id in ('249407233','250320149','252612195','253359536','255291137','224705948','227829147','228435505','230851742','230858950','231826868','232249322')

# COMMAND ----------


