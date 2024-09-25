# Databricks notebook source
from expectation.functions import get_pos
import pyspark.sql.functions as pyf

client = 'nestlewaters'
config_dict = {
    'nestlewaters': {'company_id': 567, 'parent_id': 950, 'manufacturer_id': 199, 'client_id': 13429}
}

# COMMAND ----------

def get_product_dim(sql_context, company_id, parent_id, manufacturer_id):
    df_itemId_productId = sql_context.sql(
        'select * from BOBv2.vw_BOBv2_Product'
    )

    if manufacturer_id is not None:
        df_itemId_productId = df_itemId_productId.filter(
            f'ManufacturerId == {manufacturer_id}'
        )
        
    df_itemId_productId = df_itemId_productId.filter(
        f'CompanyId == {company_id} and ParentChainId == {parent_id}' 
    ).selectExpr('ProductId', 'RefExternal as RETAILER_ITEM_ID', 'Lkp_productGroupId')
    
    df_productId_upc = sql_context.sql(
        'select * from BOBv2.Product'
    ).filter(
        f'CompanyId == {company_id}'
    ).select(
      'ProductId',
      'UniversalProductCode'
    ).dropna(subset='UniversalProductCode')

    df_link_item_id_upc = df_itemId_productId.join(
        df_productId_upc,
        ['ProductId']
    ).drop(df_itemId_productId['ProductId'])
    return df_link_item_id_upc
     

def reformat_retailer_item_id(df):
    """
    Dataplatform code to join two tables on key RETAILER_ITEM_ID is to add leading zero to length of 20 and take the the first 20
    Characters from right.
    """
    df = df.withColumn(
        'ITEM_ID', 
        pyf.lpad(
            pyf.ltrim(pyf.rtrim('RETAILER_ITEM_ID')), 20, '0'
        ).substr(-20, 20)) 
    return df

# COMMAND ----------

df = get_pos('retaillink_walmart_nestlewaters_us_dv', spark, n_days=150)
print(f'{df.cache().count():,}')

# COMMAND ----------

display(df)

# COMMAND ----------

df_item_upc = get_product_dim(
    spark, 
    config_dict[client]['company_id'],
    config_dict[client]['parent_id'],
    config_dict[client]['manufacturer_id']
) 

display(df_item_upc)

# COMMAND ----------

df = reformat_retailer_item_id(df).join(
    reformat_retailer_item_id(df_item_upc).drop('RETAILER_ITEM_ID'),
    ['ITEM_ID']
).drop('ITEM_ID')

# COMMAND ----------

display(df)
