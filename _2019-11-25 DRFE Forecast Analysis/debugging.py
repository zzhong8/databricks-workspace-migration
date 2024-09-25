# Databricks notebook source
# MAGIC %sql
# MAGIC select * from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SALES_DT from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select MIN(LOAD_TS), MAX(LOAD_TS) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select MIN(LOAD_TS), MAX(LOAD_TS) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where LOAD_TS <= '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select MIN(LOAD_TS), MAX(LOAD_TS) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where LOAD_TS > '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null
# MAGIC and LOAD_TS <= '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null
# MAGIC and LOAD_TS > '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is not null
# MAGIC and LOAD_TS <= '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is not null
# MAGIC and LOAD_TS > '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where ON_HAND_INVENTORY_QTY is not null
# MAGIC and LOAD_TS <= '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary
# MAGIC where ON_HAND_INVENTORY_QTY is not null
# MAGIC and LOAD_TS > '2019-09-06'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_clorox_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_campbellssnack_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_tysonhillshire_us_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_smuckers_ca_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SALES_DT from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SALES_DT from asda_nestlecereals_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null
# MAGIC and LOAD_TS <= '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is not null
# MAGIC and LOAD_TS <= '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where ON_HAND_INVENTORY_QTY is null
# MAGIC and LOAD_TS <= '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where ON_HAND_INVENTORY_QTY is null
# MAGIC and LOAD_TS <= '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is null
# MAGIC and LOAD_TS > '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is not null
# MAGIC and LOAD_TS > '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where ON_HAND_INVENTORY_QTY is null
# MAGIC and LOAD_TS > '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where ON_HAND_INVENTORY_QTY is not null
# MAGIC and LOAD_TS <= '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(pos_item_qty) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is not null
# MAGIC and LOAD_TS <= '2019-10-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(pos_item_qty) from asda_kraftheinz_uk_dv.vw_sat_link_epos_summary
# MAGIC where pos_item_qty is not null
# MAGIC and LOAD_TS > '2019-10-17'

# COMMAND ----------


