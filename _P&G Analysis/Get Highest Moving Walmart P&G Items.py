# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# RUN ME
import datetime
import pyspark
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DateType
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import min

# COMMAND ----------

# df_vw_latest_sat_epos = spark.sql('select * from dw_walmart_procterandgamble_us_dv.vw_latest_sat_epos')

# display(df_vw_latest_sat_epos)

# COMMAND ----------

df_dvs_epos_columns = spark.sql('SHOW COLUMNS IN dw_walmart_procterandgamble_us_dv.dvs_epos')

display(df_dvs_epos_columns)

# COMMAND ----------

dvs_epos_sql_statement = """
	SELECT
		  'WALMART' AS RETAILER,
		  'PROCTERANDGAMBLE' AS CLIENT,
		  'US' AS COUNTRY_CODE,
		  LINK_EPOS_HK,
		  STORE_NBR,
		  WHSE_NBR,
		  ITEM_NBR,
          POS_SALES,
          POS_QTY,
          POS_COST,
          CURR_STR_ON_HAND_QTY,
          WM_WEEK,
          SALES_DT
	   FROM dw_walmart_procterandgamble_us_dv.dvs_epos DE
"""

# COMMAND ----------

df_dvs_epos = spark.sql(dvs_epos_sql_statement)

display(df_dvs_epos)

# COMMAND ----------

# df_sat_epos_curr_snap_columns = spark.sql('SHOW COLUMNS IN dw_walmart_procterandgamble_us_dv.sat_epos_curr_snap')

# display(df_sat_epos_curr_snap_columns)

# COMMAND ----------

df_link_epos_columns = spark.sql('SHOW COLUMNS IN dw_walmart_procterandgamble_us_dv.link_epos')

display(df_link_epos_columns)

# COMMAND ----------

df_sat_epos_columns = spark.sql('SHOW COLUMNS IN dw_walmart_procterandgamble_us_dv.sat_epos')

display(df_sat_epos_columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC SALES_DT 
# MAGIC from dw_walmart_procterandgamble_us_dv.sat_epos

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC SALES_DT 
# MAGIC from dw_walmart_procterandgamble_us_dv.sat_epos
# MAGIC order by
# MAGIC SALES_DT
# MAGIC desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC MODULAR_DEPT_NBR,
# MAGIC MODULAR_DEPT_DESC,
# MAGIC MODULAR_CATG_NBR,
# MAGIC MODULAR_CATG_DESC,
# MAGIC MODULAR_PLAN_DESC
# MAGIC from 
# MAGIC dw_walmart_procterandgamble_us_dv.sat_epos

# COMMAND ----------

# MAGIC %sql
# MAGIC select count
# MAGIC (
# MAGIC   distinct  
# MAGIC   MODULAR_DEPT_NBR,
# MAGIC   MODULAR_DEPT_DESC
# MAGIC )
# MAGIC from 
# MAGIC dw_walmart_procterandgamble_us_dv.sat_epos

# COMMAND ----------

# MAGIC %sql
# MAGIC select count
# MAGIC (
# MAGIC   distinct  
# MAGIC   MODULAR_CATG_NBR,
# MAGIC   MODULAR_CATG_DESC
# MAGIC )
# MAGIC from 
# MAGIC dw_walmart_procterandgamble_us_dv.sat_epos

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC MODULAR_DEPT_NBR,
# MAGIC MODULAR_DEPT_DESC,
# MAGIC MODULAR_CATG_NBR,
# MAGIC MODULAR_CATG_DESC
# MAGIC from 
# MAGIC dw_walmart_procterandgamble_us_dv.sat_epos

# COMMAND ----------

sat_epos_sql_statement = """

SELECT
	  'WALMART' AS RETAILER,
	  'PROCTERANDGAMBLE' AS CLIENT,
	  'US' AS COUNTRY_CODE,
	  HI.ITEM_NBR,
	  HOU.STORE_NBR,
      SE.POS_SALES,
	  SE.POS_QTY,
	  SE.POS_COST,
	  SE.CURR_STR_ON_HAND_QTY,
	  SE.POS_SALES / SE.POS_QTY AS UNIT_PRICE,
      SE.WM_WEEK,
      SE.SALES_DT
      
   FROM dw_walmart_procterandgamble_us_dv.link_epos LE
   
   JOIN dw_walmart_procterandgamble_us_dv.sat_epos SE
		ON LE.LINK_EPOS_HK = SE.LINK_EPOS_HK
   
   JOIN dw_walmart_procterandgamble_us_dv.hub_item HI
		ON LE.HUB_ITEM_HK = HI.HUB_ITEM_HK
		
   JOIN dw_walmart_procterandgamble_us_dv.hub_organization_unit HOU
		ON LE.HUB_ORGANIZATION_UNIT_HK = HOU.HUB_ORGANIZATION_UNIT_HK
        
   LIMIT 10
   
"""

# COMMAND ----------

df_sat_epos = spark.sql(sat_epos_sql_statement)

display(df_sat_epos)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC 	  'WALMART' AS RETAILER,
# MAGIC 	  'PROCTERANDGAMBLE' AS CLIENT,
# MAGIC 	  'US' AS COUNTRY_CODE,
# MAGIC 	  HI.ITEM_NBR,
# MAGIC 	  HOU.STORE_NBR,
# MAGIC       SE.POS_SALES,
# MAGIC 	  SE.POS_QTY,
# MAGIC 	  SE.POS_COST,
# MAGIC 	  SE.CURR_STR_ON_HAND_QTY,
# MAGIC 	  SE.POS_SALES / SE.POS_QTY AS UNIT_PRICE,
# MAGIC       SE.WM_WEEK,
# MAGIC       SE.SALES_DT
# MAGIC       
# MAGIC    FROM dw_walmart_procterandgamble_us_dv.link_epos LE
# MAGIC    
# MAGIC    JOIN dw_walmart_procterandgamble_us_dv.sat_epos SE
# MAGIC 		ON LE.LINK_EPOS_HK = SE.LINK_EPOS_HK
# MAGIC    
# MAGIC    JOIN dw_walmart_procterandgamble_us_dv.hub_item HI
# MAGIC 		ON LE.HUB_ITEM_HK = HI.HUB_ITEM_HK
# MAGIC 		
# MAGIC    JOIN dw_walmart_procterandgamble_us_dv.hub_organization_unit HOU
# MAGIC 		ON LE.HUB_ORGANIZATION_UNIT_HK = HOU.HUB_ORGANIZATION_UNIT_HK
# MAGIC         
# MAGIC    WHERE 
# MAGIC         SE.SALES_DT >= '2019-04-01' AND SE.SALES_DT < '2020-04-01'
# MAGIC    AND
# MAGIC         SE.POS_QTY > 0
# MAGIC    
# MAGIC    LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC 	  'WALMART' AS RETAILER,
# MAGIC 	  'PROCTERANDGAMBLE' AS CLIENT,
# MAGIC 	  'US' AS COUNTRY_CODE,
# MAGIC 	  HI.ITEM_NBR,
# MAGIC 	  SE.POS_QTY
# MAGIC       
# MAGIC    FROM dw_walmart_procterandgamble_us_dv.link_epos LE
# MAGIC    
# MAGIC    JOIN dw_walmart_procterandgamble_us_dv.sat_epos SE
# MAGIC 		ON LE.LINK_EPOS_HK = SE.LINK_EPOS_HK
# MAGIC    
# MAGIC    JOIN dw_walmart_procterandgamble_us_dv.hub_item HI
# MAGIC 		ON LE.HUB_ITEM_HK = HI.HUB_ITEM_HK
# MAGIC         
# MAGIC    WHERE 
# MAGIC         SE.SALES_DT >= '2019-04-01' AND SE.SALES_DT < '2020-04-01'
# MAGIC    AND
# MAGIC         SE.POS_QTY > 0
# MAGIC    
# MAGIC    GROUP BY 
# MAGIC         
# MAGIC    LIMIT 10
# MAGIC
