# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.vw_bobv2_caps

# COMMAND ----------

import pyspark.sql.functions as pyf

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC mdm_country_id,
# MAGIC mdm_country_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm,
# MAGIC mdm_banner_id,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm
# MAGIC from acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.chain

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.chain
# MAGIC where ParentChainId in (16, 950, 955)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.outlet
# MAGIC
# MAGIC WHERE Chainid in (select Chainid from bobv2.chain WHERE parentchainid=955)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.manufacturer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.company

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.vw_bobv2_caps

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.vw_bobv2_caps where ManufacturerId = 182

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.vw_bobv2_caps where ManufacturerId = 199

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from bobv2.vw_bobv2_caps
# MAGIC where ParentId in (16, 950, 955)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from bobv2.vw_bobv2_caps
# MAGIC where CompanyId in (567, 603, 609)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct CompanyId, Company, ParentId, ManufacturerId from bobv2.vw_bobv2_caps
# MAGIC where CompanyId in (567, 603, 609)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from bobv2.vw_bobv2_caps
# MAGIC where CompanyId = 603

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from BOBV2.vw_BOBv2_Product
# MAGIC where CompanyId = 609 #Nestlecore

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in BOBV2

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBV2;
# MAGIC
# MAGIC select *
# MAGIC from chain
# MAGIC where lower(FullName) like '%tesco%'

# COMMAND ----------

parent_id = 609
sql_context = spark
df_caps_value = sql_context.sql(
    f'select * from BOBV2.vw_bobv2_caps where CompanyId = {parent_id}')
df_caps_value = df_caps_value.filter(pyf.col('CapType').isin(['OSA LSV Minimum', 'OSARows']))

display(
    df_caps_value.select(
        'CompanyId', 
        'CapType', 
        'Lkp_productGroupId', 
        'CapValue'
    ).drop_duplicates()
)

# COMMAND ----------

display(df_caps_value.cache())

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBV2;
# MAGIC
# MAGIC select *
# MAGIC from vw_bobv2_caps
# MAGIC where Companyid = 5
