# Databricks notebook source
def get_cap_values(sql_context, company_id, manufacturer_id):
    df = sql_context.sql(f'select * from BOBV2.vw_bobv2_caps where CompanyId = {company_id}')
    if manufacturer_id is not None:
        df = df.filter(f'ManufacturerId == {manufacturer_id}')

    df = df.select(
        'CapType',
        'Lkp_productGroupId',
        'CapValue'
    ).drop_duplicates()
    return df

# COMMAND ----------

def create_assertion_message(dfi):
    assertion_message = f'Item = {dfi["RETAILER_ITEM_ID"].unique()[0]} & TestStore = {dfi["TEST_ORGANIZATION_UNIT_NUM"].unique()[0]}'
    return assertion_message

# COMMAND ----------

def label_data_as_safe_udf(dfi):
    assertion_message = create_assertion_message(dfi)
    assert dfi['RETAILER_ITEM_ID'].nunique() == 1, 'Check GroupBy code: ' + assertion_message
    assert dfi['TEST_ORGANIZATION_UNIT_NUM'].nunique() == 1, 'Check data preprocessing process: ' + assertion_message

    test_store_num = dfi['TEST_ORGANIZATION_UNIT_NUM'].unique()[0]
    n_stores = dfi['ORGANIZATION_UNIT_NUM'].nunique()

    n_store_cond_passed = n_stores > 1
    test_store_is_present = test_store_num in set(dfi['ORGANIZATION_UNIT_NUM'].values)
    is_safe_test_store = n_store_cond_passed and test_store_is_present

    # Process data
    cols_to_drop = ['RETAILER_ITEM_ID', 'TEST_ORGANIZATION_UNIT_NUM', 'DISTANCE', 'ProductId',
                    'Lkp_productGroupId', 'UniversalProductCode', 'OutletId', 'notes', 'ChainRefExternal']
    data = dfi.drop(columns=cols_to_drop)
    data = data.sort_values(by='ORGANIZATION_UNIT_NUM')
    data = data.dropna(axis='columns', how='all')
    data = data.fillna(0)
    data.index = data['ORGANIZATION_UNIT_NUM']

    # Check if is safe
    is_safe_data = True
    try:
        _ = data.drop(columns='ORGANIZATION_UNIT_NUM', index=test_store_num).values.T
        _ = data.drop(columns='ORGANIZATION_UNIT_NUM').loc[test_store_num].values
        _ = _[-1]
    except:
        is_safe_data = False

    dfi['is_safe'] = is_safe_data and is_safe_test_store
    return dfi
