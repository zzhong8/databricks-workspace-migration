# Databricks notebook source
import pyspark
import datetime

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import count, ltrim, rtrim, col, rank, regexp_replace, desc, when

# COMMAND ----------

dbutils.widgets.text('n_sales_days', '60', 'Days Since Last Sale')
dbutils.widgets.text('n_response_days', '60', 'Days Since Last Rep Response')

n_sales_days = int(dbutils.widgets.get('n_sales_days').strip())
n_response_days = dbutils.widgets.get('n_response_days').strip()

# COMMAND ----------

sales_attributes_by_product_schema = StructType([
  StructField('platform', StringType(), True),
  
  StructField('parent_chain_id', IntegerType(), True),
  StructField('parent_chain_name', StringType(), True),
  
  StructField('company_id', IntegerType(), True),
  StructField('company_name', StringType(), True),
  
  StructField('manufacturer_id', IntegerType(), True),
  StructField('manufacturer_name', StringType(), True),
  
  StructField('product_id', IntegerType(), True),
  StructField('product_name', StringType(), True),
  StructField('universal_product_code', StringType(), True),
  
  StructField('retailer_item_id', StringType(), True),
  
  StructField('num_sales_days', IntegerType(), True),
  StructField('num_sales_stores', IntegerType(), True),
  StructField('min_sales_date', DateType(), True),
  StructField('max_sales_date', DateType(), True),
  
  StructField('total_sales_units', DecimalType(15, 2), True),
  StructField('total_sales_dollars', DecimalType(15, 2), True),
  
  StructField('avg_sales_units', DecimalType(10, 4), True),
  StructField('avg_sales_dollars', DecimalType(10, 4), True)
  ])

# COMMAND ----------

# df_DLA_UPCs_schema = StructType([
#   StructField('upc', StringType(), True),
#   StructField('platform', StringType(), True),
  
#   StructField('parent_chain_id', IntegerType(), True),
#   StructField('parent_chain_name', StringType(), True),
  
#   StructField('company_id', IntegerType(), True),
#   StructField('company_name', StringType(), True),
  
#   StructField('manufacturer_id', IntegerType(), True),
#   StructField('manufacturer_name', StringType(), True),
  
#   StructField('product_id', IntegerType(), True),
#   StructField('product_name', StringType(), True),
#   StructField('universal_product_code', StringType(), True),
  
#   StructField('retailer_item_id', StringType(), True),
  
#   StructField('num_sales_days', IntegerType(), True),
#   StructField('num_sales_stores', IntegerType(), True),
#   StructField('min_sales_date', DateType(), True),
#   StructField('max_sales_date', DateType(), True),
  
#   StructField('total_sales_units', DecimalType(15, 2), True),
#   StructField('total_sales_dollars', DecimalType(15, 2), True),
  
#   StructField('avg_sales_units', DecimalType(10, 4), True),
#   StructField('avg_sales_dollars', DecimalType(10, 4), True),
  
#   StructField('mdm_country_id', IntegerType(), True),  
#   StructField('mdm_holding_id', IntegerType(), True),
#   StructField('mdm_client_id', IntegerType(), True),

#   StructField('mdm_country_nm', StringType(), True),
#   StructField('mdm_holding_nm', StringType(), True),
#   StructField('mdm_client_nm', StringType(), True),

#   StructField('mdm_banner_id', IntegerType(), True),  
#   StructField('mdm_banner_nm', StringType(), True),
  
#   StructField('analytics_im_upc', StringType(), True),
  
#   StructField('num_days_with_responses', IntegerType(), True),
#   StructField('num_stores_with_responses', IntegerType(), True),
#   StructField('min_response_date', DateType(), True),
#   StructField('max_response_date', DateType(), True),
  
#   StructField('measurable', StringType(), True)
#   ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###360 DLA Clients

# COMMAND ----------

p360_dla_epos_dvs = [
  "retaillink_walmart_gpconsumerproductsoperation_us_dv",
  "retaillink_walmart_minutemaidcompany_us_dv",
  "retaillink_walmart_dominosugar_us_dv",
  "retaillink_walmart_campbells_us_dv",
  "retaillink_walmart_danoneusllc_us_dv",
  
  "retaillink_walmart_trucoenterprisedbaotborder_us_dv",
  "retaillink_walmart_johnsonandjohnson_us_dv",
  "retaillink_walmart_milos_us_dv",
  "retaillink_walmart_sanofi_us_dv",
  "retaillink_walmart_harrysinc_us_dv"
]

# COMMAND ----------

p360_dla_sales_attributes_by_product = spark.createDataFrame([], sales_attributes_by_product_schema)

for p360_dla_epos_dv in p360_dla_epos_dvs:
  p360_dla_sql_sales_attributes_by_product = """
  select
    '360' as platform,
    pc.ChainId as parent_chain_id,
    pc.FullName as parent_chain_name,    
    
    c.CompanyId as company_id,
    c.FullName as company_name,
    
    m.ManufacturerId as manufacturer_id,
    m.FullName as manufacturer_name,
    
    p.ProductId as product_id,
    p.FullName as product_name,
    p.UniversalProductCode as universal_product_code,

    sales.retailer_item_id as retailer_item_id,
    
    count(distinct sales.sales_dt) as num_sales_days,
    count(distinct sales.organization_unit_num) as num_sales_stores,
    min(sales.sales_dt) as min_sales_date,
    max(sales.sales_dt) as max_sales_date,
    
    sum(sales.pos_item_qty) as total_sales_units,
    sum(sales.pos_amt) as total_sales_dollars,
    
    avg(sales.pos_item_qty) as avg_sales_units,
    avg(sales.pos_amt) as avg_sales_dollars

  from
    """ + p360_dla_epos_dv + """.vw_latest_sat_epos_summary sales
    
    join 
    bobv2.vw_bobv2_product bp
    on
    ltrim('0', ltrim(rtrim(sales.retailer_item_id))) = ltrim('0', ltrim(rtrim(bp.RefExternal)))

    join
    bobv2.chain pc
    on
    bp.ParentChainId = pc.ChainId  
    
    join
    bobv2.company c
    on
    bp.CompanyId = c.CompanyId
    
    join
    bobv2.manufacturer m
    on
    bp.ManufacturerId = m.ManufacturerId  
    
    join    
    bobv2.product p
    on
    bp.ProductId = p.ProductId  

  where
    c.CompanyId = 567 -- Walmart USA_DLA
    and
    sales.sales_dt >= '2022-01-01'

  group by
    parent_chain_id,
    parent_chain_name,    
    company_id,
    company_name,
    manufacturer_id,
    manufacturer_name,
    product_id,
    product_name,
    universal_product_code,
    retailer_item_id

  order by
    company_id,
    manufacturer_id,
    total_sales_dollars desc
  
  """

  p360_dla_df_sales_attributes_by_product = spark.sql(p360_dla_sql_sales_attributes_by_product)
  
  p360_dla_sales_attributes_by_product = p360_dla_sales_attributes_by_product.union(p360_dla_df_sales_attributes_by_product)

# COMMAND ----------

today_date = datetime.date.today()

max_sales_date_filter = (today_date - datetime.timedelta(days=n_sales_days)).strftime(format='%Y-%m-%d')

p360_dla_sales_attributes_by_product = p360_dla_sales_attributes_by_product.where(col("max_sales_date") >= max_sales_date_filter)

# COMMAND ----------

# display(p360_dla_sales_attributes_by_product)

# COMMAND ----------

# p360_dla_sales_attributes_by_product.count()

# COMMAND ----------

sql_active_360_DLA_clients = """
select distinct
  iin.mdm_country_id as mdm_country_id,
  iin.mdm_country_nm as mdm_country_nm,
  iin.mdm_holding_id as mdm_holding_id,
  iin.mdm_holding_nm as mdm_holding_nm,
  iin.mdm_client_id as mdm_client_id,
  iin.mdm_client_nm as mdm_client_nm,
  max(iin.call_date) as max_response_date
  
  from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
  where
  iin.objective_typ = 'Data Led Alerts'
  and
  datediff(current_timestamp, iin.call_date) between 0 and """ + n_response_days + """
  
group by
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm
  
order by 
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm
"""

# COMMAND ----------

df_active_360_DLA_clients = spark.sql(sql_active_360_DLA_clients)

cols = ("mdm_country_nm", "mdm_holding_nm", "mdm_client_nm", "max_response_date")

df_active_360_DLA_clients_filter = df_active_360_DLA_clients.drop(*cols)

# COMMAND ----------

# display(df_active_360_DLA_clients)

# COMMAND ----------

sql_UPCs_by_360_DLA_client = """
select distinct
  iin.mdm_country_id as mdm_country_id,
  iin.mdm_country_nm as mdm_country_nm,
  iin.mdm_holding_id as mdm_holding_id,
  iin.mdm_holding_nm as mdm_holding_nm,
  iin.mdm_client_id as mdm_client_id,
  iin.mdm_client_nm as mdm_client_nm,
  iin.mdm_banner_id as mdm_banner_id,
  iin.mdm_banner_nm as mdm_banner_nm,
  il.upc as analytics_im_upc,
  
  count(distinct iin.call_date) as num_days_with_responses,
  count(distinct iin.epos_organization_unit_num) as num_stores_with_responses,
  
  min(iin.call_date) as min_response_date,
  max(iin.call_date) as max_response_date,

  case
    when iin.mdm_country_id = 2 then 'N/A'
    when iin.epos_retailer_item_id is NULL then 'No'
    else 'Yes'
  end as measurable
  
  from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
  join 
  acosta_retail_analytics_im.vw_fact_question_response fqr
  on 
  iin.response_id = fqr.response_id
  join
  acosta_retail_analytics_im.vw_dimension_itemlevel il
  on 
  fqr.item_level_id = il.item_level_id
  where
  iin.objective_typ='Data Led Alerts'
  and
  iin.call_date >= '2022-01-01'

group by
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm,
  analytics_im_upc,
  measurable

order by 
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm,
  analytics_im_upc,
  measurable
"""

# COMMAND ----------

df_UPCs_by_360_DLA_client = spark.sql(sql_UPCs_by_360_DLA_client)

df_active_UPCs_by_360_DLA_client = df_UPCs_by_360_DLA_client.join(df_active_360_DLA_clients_filter, 
                                                                  ['mdm_country_id',
                                                                   'mdm_holding_id',
                                                                   'mdm_client_id'], "inner")

# COMMAND ----------

# df_UPCs_by_360_DLA_client.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Get a list of all the measurable and non-measurable rep-responded UPCs for all the 360 DLA clients based on year-to-date call data

# COMMAND ----------

# display(df_UPCs_by_360_DLA_client)

# COMMAND ----------

p360_dla_sales_attributes_by_product = p360_dla_sales_attributes_by_product.withColumn('upc', regexp_replace(ltrim(rtrim(col("universal_product_code"))), r'^[0]*', ''))

df_active_UPCs_by_360_DLA_client = df_active_UPCs_by_360_DLA_client.withColumn('upc', regexp_replace(ltrim(rtrim(col("analytics_im_upc"))), r'^[0]*', ''))

p360_dla_sales_attributes_by_product = p360_dla_sales_attributes_by_product.withColumn('retailer_short_name', col("parent_chain_name").substr(1, 3))

df_active_UPCs_by_360_DLA_client = df_active_UPCs_by_360_DLA_client.withColumn('retailer_short_name', col("mdm_holding_nm").substr(1, 3))

df_360_DLA_UPCs = p360_dla_sales_attributes_by_product.join(df_active_UPCs_by_360_DLA_client, ["retailer_short_name", "upc"], "left")

df_360_DLA_UPCs = df_360_DLA_UPCs.drop("retailer_short_name")

# COMMAND ----------

# df_360_DLA_UPCs.count()

# COMMAND ----------

# display(df_360_DLA_UPCs)

# COMMAND ----------

# ### Save the data set ###
# df_360_DLA_UPCs.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/processed/hugh/360-DLA-clients-UPC-tracking-2022-08-15')

# COMMAND ----------

# ### Load from Cache ###
# df_360_DLA_UPCs = spark.read.option("header", True).schema(df_DLA_UPCs_schema).format('csv').load(
#     f'/mnt/processed/hugh/360-DLA-clients-UPC-tracking-2022-08-15')

# COMMAND ----------

# print(f'{df_360_DLA_UPCs.cache().count():,}')

# COMMAND ----------

# display(df_360_DLA_UPCs)

# COMMAND ----------

columns = ("parent_chain_id", "company_id" , "manufacturer_id", "analytics_im_upc")

df_360_DLA_clients = df_360_DLA_UPCs.select(*columns)

df_360_DLA_clients_agg = df_360_DLA_clients.groupBy("parent_chain_id", "company_id", "manufacturer_id").agg(count("analytics_im_upc").alias('num_responded_upcs'))

df_360_DLA_clients_agg_filter = df_360_DLA_clients_agg.where(col("num_responded_upcs") > 0)

df_360_DLA_clients_agg_filter = df_360_DLA_clients_agg_filter.drop("num_responded_upcs")

df_360_DLA_UPCs_filtered = df_360_DLA_UPCs.join(df_360_DLA_clients_agg_filter, ["parent_chain_id", "company_id" , "manufacturer_id"], "inner")

# COMMAND ----------

# display(df_360_DLA_clients_agg_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###NARS DLA Clients

# COMMAND ----------

nars_dla_epos_dvs = [
#   "retaillink_walmart_crownprinceinc_us_dv",
#   "retaillink_walmart_gpconsumerproductsoperation_us_dv",
#   "retaillink_walmart_kao_us_dv",
#   "retaillink_walmart_minutemaidcompany_us_dv",
#   "retaillink_walmart_dominosugar_us_dv",
#   "retaillink_walmart_stonyfieldfarm_us_dv",
#   "retaillink_walmart_campbells_us_dv",
#   "retaillink_walmart_danoneusllc_us_dv",
#   "retaillink_walmart_hopkinsvillemillingco_us_dv",
#   "retaillink_walmart_atkinsnutritional_us_dv",
#   "retaillink_walmart_seafoodamericallc_us_dv",
#   "retaillink_walmart_sabra_us_dv",
#   "retaillink_walmart_trucoenterprisedbaotborder_us_dv",
#   "retaillink_walmart_barillaamericainc_us_dv",
#   "retaillink_walmart_johnsonandjohnson_us_dv",
#   "retaillink_walmart_milos_us_dv",
#   "retaillink_walmart_campbellssnack_us_dv",
#   "retaillink_walmart_edgewellpersonalcare_us_dv",
#   "retaillink_walmart_sanofi_us_dv",
#   "retaillink_walmart_bumblebeefoodsllc_us_dv",
#   "retaillink_walmart_harrysinc_us_dv",
  "market6_kroger_cocacola_us_dv",
  "market6_kroger_campbells_us_dv",
  "market6_kroger_danonewave_us_dv",
  "market6_kroger_barilla_us_dv",
  "market6_kroger_wildcat_us_dv",
  "bigred_target_campbells_us_dv",
  "bigred_target_barilla_us_dv",
  "bigred_target_wildcat_us_dv",
  "retaillink_walmart_catelli_ca_dv",
  "retaillink_walmart_smuckers_ca_dv",
  "retaillink_walmart_voortman_ca_dv",
  "retaillink_walmart_danone_ca_dv"
#   "asda_nestlecore_uk_dv",
#   "morrisons_nestlecore_uk_dv",
#   "sainsburys_nestlecore_uk_dv",
#   "tesco_nestlecore_uk_dv"
]

# COMMAND ----------

nars_dla_sales_attributes_by_product = spark.createDataFrame([], sales_attributes_by_product_schema)

for nars_dla_epos_dv in nars_dla_epos_dvs:
  nars_dla_sql_sales_attributes_by_product = """
  select
    'nars' as platform,
    pc.ChainId as parent_chain_id,
    pc.FullName as parent_chain_name,    
    
    c.CompanyId as company_id,
    c.FullName as company_name,
    
    m.ManufacturerId as manufacturer_id,
    m.FullName as manufacturer_name,
    
    p.ProductId as product_id,
    p.FullName as product_name,
    p.UniversalProductCode as universal_product_code,

    sales.retailer_item_id as retailer_item_id,
    
    count(distinct sales.sales_dt) as num_sales_days,
    count(distinct sales.organization_unit_num) as num_sales_stores,
    min(sales.sales_dt) as min_sales_date,
    max(sales.sales_dt) as max_sales_date,
    
    sum(sales.pos_item_qty) as total_sales_units,
    sum(sales.pos_amt) as total_sales_dollars,
    
    avg(sales.pos_item_qty) as avg_sales_units,
    avg(sales.pos_amt) as avg_sales_dollars

  from
    """ + nars_dla_epos_dv + """.vw_latest_sat_epos_summary sales
    
    join 
    bobv2.vw_bobv2_product bp
    on
    ltrim('0', ltrim(rtrim(sales.retailer_item_id))) = ltrim('0', ltrim(rtrim(bp.RefExternal)))

    join
    bobv2.chain pc
    on
    bp.ParentChainId = pc.ChainId  
    
    join
    bobv2.company c
    on
    bp.CompanyId = c.CompanyId
    
    join
    bobv2.manufacturer m
    on
    bp.ManufacturerId = m.ManufacturerId  
    
    join    
    bobv2.product p
    on
    bp.ProductId = p.ProductId  

  where
    sales.sales_dt >= '2022-01-01'
    
  group by
    parent_chain_id,
    parent_chain_name,    
    company_id,
    company_name,
    manufacturer_id,
    manufacturer_name,
    product_id,
    product_name,
    universal_product_code,
    retailer_item_id

  order by
    company_id,
    manufacturer_id,
    total_sales_dollars desc
    
  """

  nars_dla_df_sales_attributes_by_product = spark.sql(nars_dla_sql_sales_attributes_by_product)
  
  nars_dla_sales_attributes_by_product = nars_dla_sales_attributes_by_product.union(nars_dla_df_sales_attributes_by_product)

# COMMAND ----------

nars_dla_sales_attributes_by_product = nars_dla_sales_attributes_by_product.where(col("max_sales_date") >= max_sales_date_filter)

# COMMAND ----------

# display(nars_dla_sales_attributes_by_product)

# COMMAND ----------

# nars_dla_sales_attributes_by_product.count()

# COMMAND ----------

sql_active_nars_DLA_clients = """
select distinct
  iin.mdm_country_id as mdm_country_id,
  iin.mdm_country_nm as mdm_country_nm,
  iin.mdm_holding_id as mdm_holding_id,
  iin.mdm_holding_nm as mdm_holding_nm,
  iin.mdm_client_id as mdm_client_id,
  iin.mdm_client_nm as mdm_client_nm,
  max(iin.call_date) as max_response_date
  
  from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
  where
  iin.objective_typ = 'DLA'
  and
  datediff(current_timestamp, iin.call_date) between 0 and """ + n_response_days + """
  
group by
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm
  
order by 
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm
"""

# COMMAND ----------

df_active_nars_DLA_clients = spark.sql(sql_active_nars_DLA_clients)

cols = ("mdm_country_nm", "mdm_holding_nm", "mdm_client_nm", "max_response_date")

df_active_nars_DLA_clients_filter = df_active_nars_DLA_clients.drop(*cols)

# COMMAND ----------

# display(df_active_nars_DLA_clients)

# COMMAND ----------

sql_UPCs_by_nars_DLA_client = """
select distinct
  iin.mdm_country_id as mdm_country_id,
  iin.mdm_country_nm as mdm_country_nm,
  iin.mdm_holding_id as mdm_holding_id,
  iin.mdm_holding_nm as mdm_holding_nm,
  iin.mdm_client_id as mdm_client_id,
  iin.mdm_client_nm as mdm_client_nm,
  iin.mdm_banner_id as mdm_banner_id,
  iin.mdm_banner_nm as mdm_banner_nm,
  il.upc as analytics_im_upc,
  
  count(distinct iin.call_date) as num_days_with_responses,
  count(distinct iin.epos_organization_unit_num) as num_stores_with_responses,
  
  min(iin.call_date) as min_response_date,
  max(iin.call_date) as max_response_date,
  
  case
    when iin.mdm_country_id = 2 then 'N/A'
    when iin.epos_retailer_item_id is NULL then 'No'
    else 'Yes'
  end as measurable
  
  from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
  join 
  acosta_retail_analytics_im.vw_fact_question_response fqr
  on 
  iin.response_id = fqr.response_id
  join
  acosta_retail_analytics_im.vw_dimension_itemlevel il
  on 
  fqr.item_level_id = il.item_level_id
  where
  iin.objective_typ = 'DLA'
  and
  iin.call_date >= '2022-01-01'

group by
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm,
  analytics_im_upc,
  measurable

order by 
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm,
  analytics_im_upc,
  measurable
"""

# COMMAND ----------

df_UPCs_by_nars_DLA_client = spark.sql(sql_UPCs_by_nars_DLA_client)

df_active_UPCs_by_nars_DLA_client = df_UPCs_by_nars_DLA_client.join(df_active_nars_DLA_clients_filter, 
                                                                  ['mdm_country_id',
                                                                   'mdm_holding_id',
                                                                   'mdm_client_id'], "inner")

# COMMAND ----------

nars_dla_sales_attributes_by_product = nars_dla_sales_attributes_by_product.withColumn('upc', regexp_replace(ltrim(rtrim(col("universal_product_code"))), r'^[0]*', ''))

df_active_UPCs_by_nars_DLA_client = df_active_UPCs_by_nars_DLA_client.withColumn('upc', regexp_replace(ltrim(rtrim(col("analytics_im_upc"))), r'^[0]*', ''))

nars_dla_sales_attributes_by_product = nars_dla_sales_attributes_by_product.withColumn('retailer_short_name', col("parent_chain_name").substr(1, 3))

df_active_UPCs_by_nars_DLA_client = df_active_UPCs_by_nars_DLA_client.withColumn('retailer_short_name', col("mdm_holding_nm").substr(1, 3))

df_nars_DLA_UPCs = nars_dla_sales_attributes_by_product.join(df_active_UPCs_by_nars_DLA_client, ["retailer_short_name", "upc"], "left")

df_nars_DLA_UPCs = df_nars_DLA_UPCs.drop("retailer_short_name")

# COMMAND ----------

# ### Save the data set ###
# df_nars_DLA_UPCs.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/processed/hugh/nars-DLA-clients-UPC-tracking-2022-08-19')

# COMMAND ----------

# ### Load from Cache ###
# df_nars_DLA_UPCs = spark.read.option("header", True).schema(df_DLA_UPCs_schema).format('csv').load(
#     f'/mnt/processed/hugh/nars-DLA-clients-UPC-tracking-2022-08-18')

# COMMAND ----------

# print(f'{df_nars_DLA_UPCs.cache().count():,}')

# COMMAND ----------

# display(df_nars_DLA_UPCs)

# COMMAND ----------

columns = ("parent_chain_id", "company_id" , "manufacturer_id", "analytics_im_upc")

df_nars_DLA_clients = df_nars_DLA_UPCs.select(*columns)

df_nars_DLA_clients_agg = df_nars_DLA_clients.groupBy("parent_chain_id", "company_id", "manufacturer_id").agg(count("analytics_im_upc").alias('num_responded_upcs'))

df_nars_DLA_clients_agg_filter = df_nars_DLA_clients_agg.where(col("num_responded_upcs") > 0)

df_nars_DLA_clients_agg_filter = df_nars_DLA_clients_agg_filter.drop("num_responded_upcs")

df_nars_DLA_UPCs_filtered = df_nars_DLA_UPCs.join(df_nars_DLA_clients_agg_filter, ["parent_chain_id", "company_id" , "manufacturer_id"], "inner")

# COMMAND ----------

# display(df_nars_DLA_clients_agg_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##UK DLA Clients

# COMMAND ----------

uk_dla_epos_dvs = [
  "asda_nestlecore_uk_dv",
  "morrisons_nestlecore_uk_dv",
  "sainsburys_nestlecore_uk_dv",
  "tesco_nestlecore_uk_dv"
]

# COMMAND ----------

uk_dla_sales_attributes_by_product = spark.createDataFrame([], sales_attributes_by_product_schema)

for uk_dla_epos_dv in uk_dla_epos_dvs:
  uk_dla_sql_sales_attributes_by_product = """
  select
    'uk' as platform,
    pc.ChainId as parent_chain_id,
    pc.FullName as parent_chain_name,    
    
    c.CompanyId as company_id,
    c.FullName as company_name,
    
    bp.ManufacturerId as manufacturer_id,
    '' as manufacturer_name,
    
    p.ProductId as product_id,
    p.FullName as product_name,
    p.UniversalProductCode as universal_product_code,

    hri.retailer_item_id as retailer_item_id,
    
    count(distinct vsles.sales_dt) as num_sales_days,
    count(distinct hou.organization_unit_num) as num_sales_stores,
    min(vsles.sales_dt) as min_sales_date,
    max(vsles.sales_dt) as max_sales_date,
    
    sum(vsles.pos_item_qty) as total_sales_units,
    sum(vsles.pos_amt) as total_sales_dollars,
    
    avg(vsles.pos_item_qty) as avg_sales_units,
    avg(vsles.pos_amt) as avg_sales_dollars

  from
    """ + uk_dla_epos_dv + """.vw_sat_link_epos_summary vsles
    
    join
    """ + uk_dla_epos_dv + """.hub_retailer_item hri
    ON
    hri.hub_retailer_item_hk = vsles.hub_retailer_item_hk
    
    join
    """ + uk_dla_epos_dv + """.hub_organization_unit hou
    ON 
    hou.hub_organization_unit_hk = vsles.hub_organization_unit_hk
    
    join 
    bobv2.vw_bobv2_product bp
    on
    ltrim('0', ltrim(rtrim(hri.retailer_item_id))) = ltrim('0', ltrim(rtrim(bp.RefExternal)))

    join
    bobv2.chain pc
    on
    bp.ParentChainId = pc.ChainId  
    
    join
    bobv2.company c
    on
    bp.CompanyId = c.CompanyId
    
    join    
    bobv2.product p
    on
    bp.ProductId = p.ProductId  

  where
    c.CompanyId = 609 -- Nestle Grocery
    and
    vsles.sales_dt >= '2022-01-01'
    
  group by
    parent_chain_id,
    parent_chain_name,    
    company_id,
    company_name,
    manufacturer_id,
    manufacturer_name,
    product_id,
    product_name,
    universal_product_code,
    retailer_item_id

  order by
    company_id,
    manufacturer_id,
    total_sales_dollars desc
  
  """

  uk_dla_df_sales_attributes_by_product = spark.sql(uk_dla_sql_sales_attributes_by_product)
  
  uk_dla_sales_attributes_by_product = uk_dla_sales_attributes_by_product.union(uk_dla_df_sales_attributes_by_product)

# COMMAND ----------

uk_dla_sales_attributes_by_product = uk_dla_sales_attributes_by_product.where(col("max_sales_date") >= max_sales_date_filter)

# COMMAND ----------

# display(uk_dla_sales_attributes_by_product)

# COMMAND ----------

# uk_dla_sales_attributes_by_product.count()

# COMMAND ----------

sql_active_uk_DLA_clients = """
select distinct
  iin.mdm_country_id as mdm_country_id,
  iin.mdm_country_nm as mdm_country_nm,
  iin.mdm_holding_id as mdm_holding_id,
  iin.mdm_holding_nm as mdm_holding_nm,
  iin.mdm_client_id as mdm_client_id,
  iin.mdm_client_nm as mdm_client_nm,
  iin.mdm_banner_id as mdm_banner_id,
  iin.mdm_banner_nm as mdm_banner_nm,
  max(iin.call_date) as max_response_date
  
  from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
  where
  iin.objective_typ = 'DLA'
  and
  datediff(current_timestamp, iin.call_date) between 0 and """ + n_response_days + """
  
group by
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm
  
order by 
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm
"""

# COMMAND ----------

df_active_uk_DLA_clients = spark.sql(sql_active_uk_DLA_clients)

cols = ("mdm_country_nm", "mdm_holding_nm", "mdm_client_nm", "mdm_banner_nm", "max_response_date")

df_active_uk_DLA_clients_filter = df_active_uk_DLA_clients.drop(*cols)

# COMMAND ----------

# display(df_active_uk_DLA_clients)

# COMMAND ----------

sql_UPCs_by_uk_DLA_client = """
select distinct
  iin.mdm_country_id as mdm_country_id,
  iin.mdm_country_nm as mdm_country_nm,
  iin.mdm_holding_id as mdm_holding_id,
  iin.mdm_holding_nm as mdm_holding_nm,
  iin.mdm_client_id as mdm_client_id,
  iin.mdm_client_nm as mdm_client_nm,
  iin.mdm_banner_id as mdm_banner_id,
  iin.mdm_banner_nm as mdm_banner_nm,
  il.upc as analytics_im_upc,
  
  count(distinct iin.call_date) as num_days_with_responses,
  count(distinct iin.epos_organization_unit_num) as num_stores_with_responses,
  
  min(iin.call_date) as min_response_date,
  max(iin.call_date) as max_response_date,
  
  case
    when iin.mdm_country_id = 2 then 'N/A'
    when iin.epos_retailer_item_id is NULL then 'No'
    else 'Yes'
  end as measurable
  
  from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
  join 
  acosta_retail_analytics_im.vw_fact_question_response fqr
  on 
  iin.response_id = fqr.response_id
  join
  acosta_retail_analytics_im.vw_dimension_itemlevel il
  on 
  fqr.item_level_id = il.item_level_id
  where
  iin.objective_typ = 'DLA'
  and
  iin.call_date >= '2022-01-01'

group by
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm,
  analytics_im_upc,
  measurable

order by 
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm,
  mdm_banner_id,
  mdm_banner_nm,
  analytics_im_upc,
  measurable
"""

# COMMAND ----------

df_UPCs_by_uk_DLA_client = spark.sql(sql_UPCs_by_uk_DLA_client)

df_active_UPCs_by_uk_DLA_client = df_UPCs_by_uk_DLA_client.join(df_active_uk_DLA_clients_filter, 
                                                                  ['mdm_country_id',
                                                                   'mdm_holding_id',
                                                                   'mdm_client_id',
                                                                   'mdm_banner_id'], "inner")

# COMMAND ----------

uk_dla_sales_attributes_by_product = uk_dla_sales_attributes_by_product.withColumn('upc_temp', regexp_replace(ltrim(rtrim(col("universal_product_code"))), r'^[0]*', ''))

uk_dla_sales_attributes_by_product = uk_dla_sales_attributes_by_product.withColumn('upc', when(col("upc_temp") >= 1000000000000, col("upc_temp").substr(1, 12)).otherwise(col("upc_temp")))
                                                                                   
uk_dla_sales_attributes_by_product = uk_dla_sales_attributes_by_product.drop("upc_temp")

df_active_UPCs_by_uk_DLA_client = df_active_UPCs_by_uk_DLA_client.withColumn('upc', regexp_replace(ltrim(rtrim(col("analytics_im_upc"))), r'^[0]*', ''))

uk_dla_sales_attributes_by_product = uk_dla_sales_attributes_by_product.withColumn('retailer_short_name', col("parent_chain_name").substr(1, 4))

df_active_UPCs_by_uk_DLA_client = df_active_UPCs_by_uk_DLA_client.withColumn('retailer_short_name', col("mdm_banner_nm").substr(1, 4))

df_uk_DLA_UPCs = uk_dla_sales_attributes_by_product.join(df_active_UPCs_by_uk_DLA_client, ["retailer_short_name", "upc"], "left")

df_uk_DLA_UPCs = df_uk_DLA_UPCs.drop("retailer_short_name")

# COMMAND ----------

# df_active_UPCs_by_uk_DLA_client.count()

# COMMAND ----------

# Filter the UK DLA dataframe on only retailer/clients in BOBv2 that have at least one rep response

columns = ("parent_chain_id", "company_id", "analytics_im_upc")

df_uk_DLA_clients = df_uk_DLA_UPCs.select(*columns)

df_uk_DLA_clients_agg = df_uk_DLA_clients.groupBy("parent_chain_id", "company_id").agg(count("analytics_im_upc").alias('num_responded_upcs'))

df_uk_DLA_clients_agg_filter = df_uk_DLA_clients_agg.where(col("num_responded_upcs") > 0)

df_uk_DLA_clients_agg_filter = df_uk_DLA_clients_agg_filter.drop("num_responded_upcs")

df_uk_DLA_UPCs_filtered = df_uk_DLA_UPCs.join(df_uk_DLA_clients_agg_filter, ["parent_chain_id", "company_id"], "inner")

# COMMAND ----------

# Order the columns so that they are in the same order as df_360_DLA_UPCs_filtered and df_nars_DLA_UPCs_filtered

na_DLA_clients_columns = df_nars_DLA_UPCs_filtered.columns

df_uk_DLA_UPCs_filtered = df_uk_DLA_UPCs_filtered.select(*na_DLA_clients_columns)

# COMMAND ----------

# ### Save the data set ###

# df_uk_DLA_UPCs.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/processed/hugh/uk-DLA-clients-UPC-tracking-2022-08-23')

# COMMAND ----------

# ### Save the list of 'active' UPCs (i.e. UPCs with a rep response to a DLA within the last 60 days) ###

# df_active_UPCs_by_uk_DLA_client.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/processed/hugh/uk-DLA-clients-active-UPCs-2022-08-23')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Combine DLA Clients

# COMMAND ----------

df_na_DLA_UPCs_filtered = df_360_DLA_UPCs_filtered.union(df_nars_DLA_UPCs_filtered)
df_DLA_UPCs_filtered = df_na_DLA_UPCs_filtered.union(df_uk_DLA_UPCs_filtered)

# COMMAND ----------

# Sort dataframe and add a rank by total dollar sales within with retailer/client

df_DLA_UPCs_filtered = df_DLA_UPCs_filtered.orderBy(
  col("platform").asc(),
  col("parent_chain_id").asc(),
  col("company_id").asc(),
  col("manufacturer_id").asc(),
  col("total_sales_dollars").desc())

column_list = ["parent_chain_id", "company_id", "manufacturer_id"]

df_DLA_UPCs_filtered = df_DLA_UPCs_filtered.withColumn(
  "total_sales_rank", rank().over(Window.partitionBy(*column_list).orderBy(desc("total_sales_dollars"))))

# COMMAND ----------

final_column_order = [
  'platform',
  'parent_chain_id',
  'parent_chain_name',
  'company_id',
  'company_name',
  'manufacturer_id',
  'manufacturer_name',
  'product_id',
  'product_name',
  'upc',
  'universal_product_code',
  'retailer_item_id',
  'num_sales_days',
  'num_sales_stores',
  'min_sales_date',
  'max_sales_date',
  'total_sales_units',
  'total_sales_dollars',
  'total_sales_rank',
  'avg_sales_units',
  'avg_sales_dollars',
  'mdm_country_id',
  'mdm_country_nm',
  'mdm_holding_id',
  'mdm_holding_nm',
  'mdm_client_id',
  'mdm_client_nm',
  'mdm_banner_id',
  'mdm_banner_nm',
  'analytics_im_upc',
  'num_days_with_responses',
  'num_stores_with_responses',
  'min_response_date',
  'max_response_date',
  'measurable'  
]

df_DLA_UPCs_final = df_DLA_UPCs_filtered.select(*final_column_order)

# COMMAND ----------

# df_DLA_UPCs_final.printSchema()

# COMMAND ----------

# display(df_DLA_UPCs_final)

# COMMAND ----------

df_DLA_UPCs_final.cache()

# COMMAND ----------

### Save the final data set ###
df_DLA_UPCs_final.coalesce(1).write.format('csv')\
    .mode('overwrite')\
    .option('overwriteSchema', True)\
    .option('header', True)\
    .save(f'/mnt/processed/alerting/DLA-clients-UPC-tracking/{today_date}')
