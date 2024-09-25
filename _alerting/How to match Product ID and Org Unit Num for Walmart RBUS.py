# Databricks notebook source
# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC desc callfilevisit

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from callfilevisit
# MAGIC where OutletId = 701645
# MAGIC -- and StartDateTime >= '2022-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC desc vw_bobv2_outlet

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_bobv2_outlet 
# MAGIC where ParentChainId = 950 -- Walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_bobv2_outlet 
# MAGIC where ParentChainId = 950 -- Walmart
# MAGIC and OutletId = 701645

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC where CompanyId = 577
# MAGIC and LastVisitDate >= '2020-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC where CompanyId = 577
# MAGIC and LastVisitDate >= '2021-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 577 -- RBUS
# MAGIC and ParentChainId = 950 -- Walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 577 -- RBUS
# MAGIC and ParentChainId = 950 -- Walmart
# MAGIC and RefExternal in
# MAGIC (
# MAGIC 578000883,
# MAGIC 576545335,
# MAGIC 554021649,
# MAGIC 552048485,
# MAGIC 590478663,
# MAGIC 583499276,
# MAGIC 573916215,
# MAGIC 569846741,
# MAGIC 565295205,
# MAGIC 555115349,
# MAGIC 578042472,
# MAGIC 566792190,
# MAGIC 557360716,
# MAGIC 563446666,
# MAGIC 572931397,
# MAGIC 554123491,
# MAGIC 567284242,
# MAGIC 557655333,
# MAGIC 557634344,
# MAGIC 574258299,
# MAGIC 565284735,
# MAGIC 557634343,
# MAGIC 260004735,
# MAGIC 564250243,
# MAGIC 584268027,
# MAGIC 4011351,
# MAGIC 571972241,
# MAGIC 1306682,
# MAGIC 568941339,
# MAGIC 577813167,
# MAGIC 575182521,
# MAGIC 554021646,
# MAGIC 1311637,
# MAGIC 1348481,
# MAGIC 552068775,
# MAGIC 551587007,
# MAGIC 554021648,
# MAGIC 575546081,
# MAGIC 565431024,
# MAGIC 567215787,
# MAGIC 578359233,
# MAGIC 554238781,
# MAGIC 581619441,
# MAGIC 586262523,
# MAGIC 579134589,
# MAGIC 553113177,
# MAGIC 554238754,
# MAGIC 1391094,
# MAGIC 573660948,
# MAGIC 567284249,
# MAGIC 555142138,
# MAGIC 595496432,
# MAGIC 590479907,
# MAGIC 571972239,
# MAGIC 575111056,
# MAGIC 574439788,
# MAGIC 553113176,
# MAGIC 574563039,
# MAGIC 554981589,
# MAGIC 554931176,
# MAGIC 581623211,
# MAGIC 587315187,
# MAGIC 550315321,
# MAGIC 1314426,
# MAGIC 4083901,
# MAGIC 553463782,
# MAGIC 575171906,
# MAGIC 573660947,
# MAGIC 551586998,
# MAGIC 567284252,
# MAGIC 567215785,
# MAGIC 571951428,
# MAGIC 1318244,
# MAGIC 567215786,
# MAGIC 567284253,
# MAGIC 581359336,
# MAGIC 570289569,
# MAGIC 566991523,
# MAGIC 587727268,
# MAGIC 587572923,
# MAGIC 551493341,
# MAGIC 594926733,
# MAGIC 554123492,
# MAGIC 7904550,
# MAGIC 584592076,
# MAGIC 1300556,
# MAGIC 587520552,
# MAGIC 575489175,
# MAGIC 550307352,
# MAGIC 1149131,
# MAGIC 550315315,
# MAGIC 568140763)

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 577 -- RBUS
# MAGIC and ParentChainId = 950 -- Walmart
# MAGIC and ProductId in
# MAGIC (
# MAGIC 174291,
# MAGIC 174303,
# MAGIC 174331,
# MAGIC 174484,
# MAGIC 174496,
# MAGIC 174557,
# MAGIC 174589,
# MAGIC 174595,
# MAGIC 174607,
# MAGIC 174619,
# MAGIC 174627,
# MAGIC 174644,
# MAGIC 174673,
# MAGIC 174702,
# MAGIC 174705,
# MAGIC 174722,
# MAGIC 174735,
# MAGIC 174752,
# MAGIC 176115,
# MAGIC 176121,
# MAGIC 176149,
# MAGIC 176193,
# MAGIC 176206,
# MAGIC 176225,
# MAGIC 176228,
# MAGIC 176252,
# MAGIC 176285,
# MAGIC 176287,
# MAGIC 176331,
# MAGIC 176383,
# MAGIC 176414,
# MAGIC 176444,
# MAGIC 176447,
# MAGIC 176451,
# MAGIC 176454,
# MAGIC 176512,
# MAGIC 176518,
# MAGIC 176563,
# MAGIC 176564,
# MAGIC 176575,
# MAGIC 176600,
# MAGIC 176609,
# MAGIC 176611,
# MAGIC 176622,
# MAGIC 176633,
# MAGIC 176639,
# MAGIC 176641,
# MAGIC 176642,
# MAGIC 176645,
# MAGIC 176647
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select count(*) from vw_BOBv2_Product  
# MAGIC where CompanyId = 577 -- RBUS
# MAGIC and ParentChainId = 950 -- Walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from Product  
# MAGIC where CompanyId = 577 -- RBUS

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select count(*) from Product  
# MAGIC where CompanyId = 577 -- RBUS

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC  
# MAGIC select distinct RefExternal from vw_bobv2_Product  
# MAGIC where CompanyId = 577 -- RBUS
# MAGIC -- and ParentChainId = 16 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC use tesco_nestlecore_uk_dv;
# MAGIC
# MAGIC select distinct retailer_item_id from hub_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC  
# MAGIC select * from vw_bobv2_outlet  
# MAGIC -- where ParentChainId = 16 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC  
# MAGIC select distinct ChainRefExternal from vw_bobv2_outlet  
# MAGIC where ParentChainId = 16 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC use tesco_nestlecore_uk_dv;
# MAGIC
# MAGIC select distinct ORGANIZATION_UNIT_NUM from hub_organization_unit

# COMMAND ----------


